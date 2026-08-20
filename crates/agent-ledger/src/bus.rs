//! The single ordered fan-out primitive: one hub, one sequence, one order.
//!
//! The bus is generic over the event type it carries. The runtime emits its own
//! [`CoreEvent`] values through [`EventBus::emit`]; a consumer parameterises the
//! bus with an event type of its own that a `CoreEvent` converts into, so the
//! library never holds an enum a consumer has to edit. This is the same idea the
//! block layer applies to kinds, applied once more here — not a second idea.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::event::CoreEvent;

/// How far behind an attached push subscriber may fall before it loses events:
/// the hub keeps this many envelopes and no more.
///
/// An attach whose cursor still sits inside the window replays every buffered
/// envelope after it, gap-free. A cursor older than the window cannot be served —
/// those envelopes are gone — and the attach says so with
/// [`AttachOutcome::Gapped`] so the subscriber takes a fresh snapshot instead of
/// continuing across a hole it has no way to notice.
const PUSH_REPLAY_CAPACITY: usize = 1024;

/// How far behind an in-process broadcast subscriber may fall before it loses
/// events: the per-receiver backlog of the channel [`EventBus::subscribe`] hands
/// out.
///
/// A receiver that has not caught up when its backlog is full has its OLDEST
/// unread events dropped and sees `RecvError::Lagged` on the next receive.
/// Nothing re-sends them, and the hub's replay ring does not cover this plane.
/// The payload-carrying variants of [`CoreEvent`] are therefore lost outright
/// when it happens — see [`CoreEvent`] for which those are and what that costs.
/// Raising this number moves the threshold; it does not close the hole.
const BROADCAST_BACKLOG_CAPACITY: usize = 256;

/// One push, typed: the event plus its ordering metadata (`seq` assigned under
/// the hub lock, `ts` the wall clock at publish). Every subscriber receives this
/// and serializes it for its own transport. The event itself IS the carried
/// type, so there is nothing to convert on the way out.
#[derive(Clone)]
pub struct PushEnvelope<E> {
    /// Monotonic sequence number, assigned under the hub lock.
    ///
    /// It counts the pushes of THIS process and nothing more. A fresh bus
    /// starts at 1, and the envelope carries no marker of which process run
    /// assigned the number, so a cursor is meaningful only to the process that
    /// issued it. A cursor persisted across a restart does NOT survive: the new
    /// process numbers from 1 again, an attach with the old cursor replays
    /// nothing (its number is ahead of everything buffered) and then delivers
    /// sequence numbers lower than the ones the subscriber already saw, with no
    /// field in the envelope by which either side could detect it. Re-read
    /// [`EventBus::last_seq`] after every restart and take a fresh snapshot with
    /// it; do not store one.
    pub seq: u64,
    /// Wall clock at publish, in nanoseconds since the Unix epoch.
    pub ts: u64,
    /// The event itself.
    pub event: E,
}

/// A push subscriber. `deliver` returns `false` when the sink is closed, so the
/// hub prunes it on the next publish. A consumer wraps whatever its transport
/// is — a channel to a desktop shell, a websocket connection's outbound
/// queue — in one of these.
///
/// Contract: an implementation MUST NOT block or await inside `deliver` — hand
/// the envelope to a non-blocking queue and return. It also SHOULD NOT panic; a
/// panic is nonetheless isolated by the hub (in `deliver_isolated`) and treated
/// as a closed sink (pruned), so one broken sink can never poison the lock and
/// take down every attached transport.
///
/// Three properties of the call the implementation has to be built for:
///
/// - **Deliveries may overlap.** `deliver` can run on two threads AT ONCE for
///   the same sink — two concurrent publishes, or a publish racing the replay of
///   an [`EventBus::attach`]. `PushSink` is `Sync` and `deliver` takes `&self`
///   for that reason: the implementation does its own interior locking, and
///   whatever it does must stay correct when a second call is already inside it.
/// - **Arrival order may differ from `seq` order.** Nothing serialises the fan
///   out, so a sink can see seq 2 before seq 1 — a slow delivery no longer holds
///   the next one back, and replayed envelopes may interleave with live ones.
/// - **A subscriber that cares about order sequences by `seq`.** That number is
///   where the bus's one global order lives (see [`PushEnvelope::seq`]); arrival
///   at the sink is not it. A transport that forwards in arrival order forwards
///   out of order.
///
/// No hub lock is held while `deliver` runs — not on the publish path and not
/// during attach replay — so a sink may call back into the bus from inside
/// `deliver`: [`EventBus::last_seq`], an [`EventBus::attach`], even another
/// send.
pub trait PushSink<E>: Send + Sync {
    /// Hand one envelope to the transport. Return `false` once closed.
    fn deliver(&self, envelope: &PushEnvelope<E>) -> bool;
}

/// Deliver to one sink, isolating a panic. A well-behaved sink returns `false`
/// when closed (and is pruned); a *misbehaving* sink that panics must not
/// poison the hub's `Mutex` — every other publish site would then panic on
/// `.lock().unwrap()` and the push bus would be dead for the rest of the
/// process. So a panic is caught here and treated as a closed sink. The
/// isolation bounds the blast radius of a broken sink to that sink.
fn deliver_isolated<E>(sink: &Arc<dyn PushSink<E>>, envelope: &PushEnvelope<E>) -> bool {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink.deliver(envelope)))
        .unwrap_or_else(|_| {
            tracing::error!(seq = envelope.seq, "push sink panicked; pruning it");
            false
        })
}

/// The fan-out hub. One lock serializes sequence assignment, replay-buffer
/// append and the sink snapshot; delivery runs with the lock released. That
/// split buys three properties:
///
/// - **One global order.** `seq` is monotonic and equal to replay-buffer order,
///   and there is no per-subscriber bespoke emission path. Order is carried by
///   `seq`, not by arrival: two threads publishing at once may reach a given
///   sink in either order, so a subscriber sequences by `seq`.
/// - **Gap-free attach.** `attach` takes the replay tail after the cursor AND
///   registers the sink under ONE acquisition of the same lock a publish takes
///   to buffer its envelope and snapshot the sinks. A concurrent publish
///   therefore either buffered first (it is in the tail the attach took, and the
///   sink was not in that publish's snapshot) or snapshotted first (the sink was
///   already registered and gets it live, and it was not in the tail) — never
///   lost, never duplicated. The replayed envelopes are then handed over with
///   the lock released, so a live push may interleave with them; that is
///   correct, because the order is carried by `seq`.
/// - **Re-entrant delivery.** A sink may touch the bus from inside `deliver`,
///   because no lock is held while ANY delivery runs — publish or attach replay.
///   Delivering under the lock hung the process outright: `std::sync::Mutex` is
///   not reentrant, so a sink that called back — `last_seq` was enough — blocked
///   forever on a lock its own thread already held. Under `attach` it was worse
///   still: the stuck sink held the hub lock, so every publisher anywhere in the
///   process piled up behind it and the whole bus froze.
struct PushHub<E> {
    state: Mutex<HubState<E>>,
}

struct HubState<E> {
    next_seq: u64,
    sinks: Vec<Arc<dyn PushSink<E>>>,
    replay: VecDeque<PushEnvelope<E>>,
}

impl<E: Clone> PushHub<E> {
    fn new() -> Self {
        Self {
            state: Mutex::new(HubState {
                next_seq: 1,
                sinks: Vec::new(),
                replay: VecDeque::with_capacity(PUSH_REPLAY_CAPACITY),
            }),
        }
    }

    /// Assign the next seq, build the envelope, buffer it and snapshot the
    /// sinks — that much under the lock, so ordering and gap-free attach hold.
    /// Then RELEASE the lock and fan out, because `deliver` runs consumer code
    /// that may touch this same bus. Sinks that reported closed are pruned
    /// afterwards, under a fresh acquisition. Returns the seq.
    fn publish(&self, event: E) -> u64 {
        let (envelope, sinks) = {
            let mut state = self.state.lock().unwrap();
            let seq = state.next_seq;
            state.next_seq += 1;

            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            // Saturating rather than truncating: a clock past the u64 nanosecond
            // horizon must not wrap the timestamp back into the past.
            let ts = u64::try_from(nanos).unwrap_or(u64::MAX);

            let envelope = PushEnvelope { seq, ts, event };
            if state.replay.len() == PUSH_REPLAY_CAPACITY {
                state.replay.pop_front();
            }
            state.replay.push_back(envelope.clone());

            (envelope, state.sinks.clone())
        };

        let closed: Vec<Arc<dyn PushSink<E>>> = sinks
            .into_iter()
            .filter(|sink| !deliver_isolated(sink, &envelope))
            .collect();

        // Reconcile rather than overwrite: the list may have gained sinks while
        // the lock was free, and an attach that happened during delivery must
        // not be dropped on the floor by a stale snapshot. Identity is the Arc,
        // so a sink attached twice is pruned exactly where it reported closed.
        if !closed.is_empty() {
            let mut state = self.state.lock().unwrap();
            state
                .sinks
                .retain(|sink| !closed.iter().any(|dead| Arc::ptr_eq(sink, dead)));
        }

        envelope.seq
    }

    /// Drop ONE registration of this sink, the last one — which is the one the
    /// caller that is unwinding its own `attach` added. Another registration of
    /// the same `Arc` belongs to another caller and stays.
    fn unregister(&self, sink: &Arc<dyn PushSink<E>>) {
        let mut state = self.state.lock().unwrap();
        if let Some(at) = state
            .sinks
            .iter()
            .rposition(|registered| Arc::ptr_eq(registered, sink))
        {
            state.sinks.remove(at);
        }
    }

    /// Register a sink. With `replay_after: Some(seq)`, first replays every
    /// buffered envelope after that seq (gap-free reconnect); with `None`, the
    /// sink only sees future pushes, which is what a subscriber that re-fetches
    /// its state on every change needs. The returned [`AttachOutcome`] says
    /// whether the replay could be served in full, and whether the sink was
    /// registered at all.
    fn attach(&self, sink: Arc<dyn PushSink<E>>, replay_after: Option<u64>) -> AttachOutcome {
        // Two handles on the one sink: `sink` is moved into the hub's list as
        // the registration, `delivering` stays here for the fan-out below. Same
        // allocation, so the `Arc::ptr_eq` identity the pruning uses still holds.
        let delivering = Arc::clone(&sink);

        // Take the tail AND register, under one acquisition: that pairing is
        // what makes the attach gap-free against a concurrent publish. Delivery
        // waits until the lock is gone — `deliver` runs consumer code that may
        // touch this same bus, and doing it here froze the hub for every thread.
        let (tail, outcome) = {
            let mut state = self.state.lock().unwrap();
            let mut outcome = AttachOutcome::Complete;

            let tail = match replay_after {
                Some(after) => {
                    // A cursor older than the ring's oldest surviving envelope
                    // cannot be served: what is missing was evicted, so the sink
                    // would silently resume across a hole. Say so instead.
                    if let Some(oldest) = state.replay.front().map(|envelope| envelope.seq)
                        && oldest > after.saturating_add(1)
                    {
                        outcome = AttachOutcome::Gapped {
                            oldest_replayed: oldest,
                        };
                    }
                    state
                        .replay
                        .iter()
                        .filter(|envelope| envelope.seq > after)
                        .cloned()
                        .collect()
                }
                None => Vec::new(),
            };

            state.sinks.push(sink);
            (tail, outcome)
        };

        for envelope in &tail {
            // A sink that reports closed mid-replay is closed: leaving it
            // registered would put a dead sink in the list the next publish has
            // to prune, which is the pruning this file does everywhere else.
            if !deliver_isolated(&delivering, envelope) {
                self.unregister(&delivering);
                return AttachOutcome::Closed;
            }
        }

        outcome
    }
}

/// What an [`EventBus::attach`] did — the answer a reconnecting subscriber needs
/// before it trusts the state it already has.
///
/// Ignoring it is how a subscriber ends up resuming across events it never
/// received and cannot detect, so the type says so at the call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "an incomplete replay leaves the subscriber with a hole it cannot otherwise see"]
pub enum AttachOutcome {
    /// The sink is registered and received every buffered envelope after the
    /// cursor it named — or named none, and starts from the next publish.
    ///
    /// Complete is stated in terms of this process's counter; a cursor from an
    /// earlier process run is not comparable at all (see [`PushEnvelope::seq`]).
    Complete,
    /// The sink is registered, but envelopes after its cursor had already been
    /// evicted from the replay ring, so what it received begins at
    /// `oldest_replayed` with a hole in front of it. The subscriber must throw
    /// its state away and take a fresh snapshot, then attach again from the seq
    /// that snapshot was taken at.
    Gapped {
        /// Seq of the oldest envelope still buffered — the first one the sink
        /// received, and the far edge of the hole.
        oldest_replayed: u64,
    },
    /// The sink reported closed (or panicked) while receiving replay, so it was
    /// unregistered again and receives nothing further. A push that was already
    /// live when the replay began may have reached it before that — the sink
    /// said it was closed, so what it did with those is its own business.
    Closed,
}

/// The event bus — one ordered push hub for every attached transport, plus an
/// in-process broadcast for reactor loops.
///
/// Transports subscribe through [`EventBus::attach`]; in-process loops subscribe
/// through [`EventBus::subscribe`]. The runtime publishes its own events through
/// [`EventBus::emit`] and anything else goes through [`EventBus::send`]; both
/// fan out to the hub (ordered, replayable) and the broadcast. Events are
/// multiplexed by conversation id at the consumer.
///
/// `E` is the consumer's event type. Where the runtime emits, the bound is
/// `E: From<CoreEvent>`, so a consumer's composed enum carries core events
/// without the library ever naming the consumer's own.
pub struct EventBus<E> {
    hub: PushHub<E>,
    broadcast: tokio::sync::broadcast::Sender<E>,
}

impl<E: Clone + Send + 'static> EventBus<E> {
    /// Create an empty bus with no sinks and no subscribers.
    #[must_use]
    pub fn new() -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(BROADCAST_BACKLOG_CAPACITY);
        Self {
            hub: PushHub::new(),
            broadcast: tx,
        }
    }

    /// Attach a push subscriber. A live-only transport passes `None` for
    /// `replay_after`; a reconnecting one passes `Some(last_seen_seq)` and reads
    /// the returned [`AttachOutcome`] to learn whether that cursor could still
    /// be served — [`AttachOutcome::Gapped`] means the events between the cursor
    /// and the replay ring are gone and the subscriber has to re-snapshot rather
    /// than carry on.
    ///
    /// The replayed envelopes are delivered with no lock held, so a push
    /// published while the replay is running may reach the sink in the middle of
    /// it. Nothing is lost or duplicated by that, and the order stands in `seq`
    /// (see [`PushSink`]).
    ///
    /// # Panics
    ///
    /// If the hub lock was poisoned by a panic outside `deliver` (a panic
    /// inside `deliver` is isolated and cannot poison it).
    pub fn attach(&self, sink: Arc<dyn PushSink<E>>, replay_after: Option<u64>) -> AttachOutcome {
        self.hub.attach(sink, replay_after)
    }

    /// The seq of the most recently published push (0 before the first).
    ///
    /// This is the snapshot cursor of the attach model: read it BEFORE taking a
    /// snapshot, hand it to the subscriber with the snapshot, and a subsequent
    /// `attach(replay_after = Some(seq))` delivers every event published after
    /// the capture exactly once — or reports [`AttachOutcome::Gapped`] when the
    /// subscriber took too long to come back. Events published between the
    /// capture and the snapshot read may already be reflected in the snapshot
    /// AND arrive as pushes — harmless by design: pushes are re-fetch triggers,
    /// not state carriers.
    ///
    /// The number counts this process's pushes and restarts at 1 with the
    /// process; it is not a durable cursor. See [`PushEnvelope::seq`] for what
    /// that rules out.
    ///
    /// # Panics
    ///
    /// If the hub lock was poisoned.
    #[must_use]
    pub fn last_seq(&self) -> u64 {
        self.hub.state.lock().unwrap().next_seq - 1
    }

    /// Subscribe in-process. Reactor loops take a receiver here instead of
    /// attaching a sink, because they need the event, not a transport envelope.
    ///
    /// This plane is lossy under sustained lag and has no replay: a receiver
    /// further behind than the channel's backlog (`BROADCAST_BACKLOG_CAPACITY`
    /// events, one number declared once at the top of this module) loses its
    /// oldest unread events and observes `RecvError::Lagged` instead. For most of
    /// [`CoreEvent`] that costs nothing — they are wakeups and the receiver
    /// re-reads the ledger — but the three payload-carrying variants hold the
    /// only copy of their intent, so a lagging receiver loses it outright. A
    /// receiver of those must handle `Lagged` as data loss, not as a hiccup.
    #[must_use]
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<E> {
        self.broadcast.subscribe()
    }

    /// Assign a monotonic sequence number and send the event. Fans out to the
    /// ordered push hub (every attached transport) and the in-process broadcast
    /// (every reactor). Returns the assigned seq.
    ///
    /// The hub plane buffers; the broadcast plane does not re-send what a
    /// lagging receiver missed (see [`Self::subscribe`]).
    ///
    /// # Panics
    ///
    /// If the hub lock was poisoned.
    pub fn send(&self, event: E) -> u64 {
        // In-process broadcast — reactors pick this up. The discarded result is
        // only "nobody is listening", which is a legitimate state. The loss this
        // plane really has is elsewhere and is NOT reported here: a receiver
        // that falls further behind than the channel's backlog has its oldest
        // unread events dropped by the channel itself, with no error reaching
        // this side at all. For a wakeup that is free; for the three payload
        // variants of `CoreEvent` it destroys the only copy of an intent. The
        // transport is not changed here, so the honest statement of it lives on
        // `CoreEvent` and on `subscribe`, where a reader meets it.
        let _ = self.broadcast.send(event.clone());

        // Ordered push hub — every attached transport picks this up.
        let seq = self.hub.publish(event);
        tracing::debug!(seq, "bus send");
        seq
    }

    /// Publish one of the runtime's own events, converted into the carried
    /// type. This is the only path the library itself uses, and the reason the
    /// bound is `From<CoreEvent>` rather than an enum the consumer must edit.
    ///
    /// # Panics
    ///
    /// If the hub lock was poisoned.
    pub fn emit(&self, event: CoreEvent) -> u64
    where
        E: From<CoreEvent>,
    {
        let label: &'static str = (&event).into();
        let seq = self.send(E::from(event));
        tracing::debug!(seq, event = label, "core event emitted");
        seq
    }
}

impl<E: Clone + Send + 'static> Default for EventBus<E> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc::RecvTimeoutError;
    use std::time::{Duration, Instant};

    /// Run a test body with a deadline, on a thread of its own.
    ///
    /// Every deadlock this module has had turned a test into a HANG rather than
    /// a failure: the suite has no timeout of its own, so the run stalled until
    /// something outside killed it, and a stalled run reports nothing. Anything
    /// that would park forever if a lock moved back across a delivery goes
    /// through here, and comes back as a red test with a name in it.
    ///
    /// The deadlocked thread is abandoned rather than joined — it can never make
    /// progress, and the harness exits the process once the tests are done.
    ///
    /// Every test in this module goes through it, including the ones that only
    /// touch the hub lock in passing: a lock regression parks whichever test
    /// reaches it first, and which one that is, is not knowable in advance.
    fn within<T, F>(what: &str, body: F) -> T
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        /// Long enough that a sleeping sink on a loaded machine is never
        /// mistaken for a deadlocked one, short enough that the suite still
        /// finishes. One number, one place.
        const DEADLINE: Duration = Duration::from_secs(30);

        let (finished, waiting) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let outcome = body();
            // Fails only if the deadline already fired and nobody is waiting.
            let _ = finished.send(());
            outcome
        });

        match waiting.recv_timeout(DEADLINE) {
            // Either the body returned, or — on `Disconnected` — it panicked and
            // dropped its sender. The join tells the two apart and carries the
            // original panic back out, so the assertion that failed is the one
            // the harness reports.
            Ok(()) | Err(RecvTimeoutError::Disconnected) => worker
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic)),
            Err(RecvTimeoutError::Timeout) => {
                panic!("`{what}` did not finish within {DEADLINE:?} — the bus deadlocked")
            }
        }
    }

    /// A consumer's composed event type, defined outside the library's own
    /// enum: the proof that the bus seam is generic and needs no edit here for
    /// an event the library never heard of.
    #[derive(Clone, Debug)]
    enum AppEvent {
        Core(CoreEvent),
        #[allow(
            dead_code,
            reason = "the point is that the library never constructs it"
        )]
        SearchProvidersChanged,
    }

    impl From<CoreEvent> for AppEvent {
        fn from(event: CoreEvent) -> Self {
            Self::Core(event)
        }
    }

    struct CollectingSink<E>(Mutex<Vec<PushEnvelope<E>>>);

    impl<E: Clone + Send + Sync> PushSink<E> for CollectingSink<E> {
        fn deliver(&self, envelope: &PushEnvelope<E>) -> bool {
            self.0.lock().unwrap().push(envelope.clone());
            true
        }
    }

    fn blocks_changed(block_id: i64) -> CoreEvent {
        CoreEvent::BlocksChanged {
            conversation_id: 7,
            block_id,
        }
    }

    /// The generic seam: a bus parameterised by an event type the library does
    /// not know about carries a core event to both planes, arriving as the
    /// consumer's own type.
    #[test]
    fn a_consumer_event_type_carries_core_events() {
        within("a_consumer_event_type_carries_core_events", || {
            let bus: EventBus<AppEvent> = EventBus::new();
            let sink = Arc::new(CollectingSink(Mutex::new(Vec::new())));
            assert_eq!(bus.attach(sink.clone(), None), AttachOutcome::Complete);
            let mut reactor = bus.subscribe();

            let seq = bus.emit(blocks_changed(3));
            assert_eq!(seq, 1, "the first publish is seq 1");

            let delivered = sink.0.lock().unwrap();
            let [envelope] = &delivered[..] else {
                panic!("the sink received exactly one envelope");
            };
            let AppEvent::Core(CoreEvent::BlocksChanged {
                conversation_id,
                block_id,
            }) = &envelope.event
            else {
                panic!("the core event arrived as the consumer's own variant");
            };
            assert_eq!((*conversation_id, *block_id), (7, 3));

            let AppEvent::Core(core) = reactor.try_recv().unwrap() else {
                panic!("the in-process subscriber saw the same event");
            };
            assert_eq!(core.conversation_id(), Some(7));
        });
    }

    /// Attaching with a known seq replays the buffered tail and nothing else,
    /// so a reconnecting transport sees every event after its cursor exactly
    /// once.
    #[test]
    fn attach_with_replay_after_delivers_only_the_tail() {
        within("attach_with_replay_after_delivers_only_the_tail", || {
            let bus: EventBus<CoreEvent> = EventBus::new();
            for block_id in 1..=3 {
                bus.send(blocks_changed(block_id));
            }

            let sink = Arc::new(CollectingSink(Mutex::new(Vec::new())));
            assert_eq!(
                bus.attach(sink.clone(), Some(1)),
                AttachOutcome::Complete,
                "the cursor is inside the replay ring, so the replay is whole"
            );
            bus.send(blocks_changed(4));

            // Pair each sequence number with its own payload. Asserting the
            // sequence alone would pass a replay that emitted the right ordering
            // carrying the wrong events — and payload-to-sequence pairing is the
            // no-loss, no-duplication property this test exists to hold.
            let delivered = sink.0.lock().unwrap();
            let seen: Vec<(u64, i64)> = delivered
                .iter()
                .map(|envelope| match envelope.event {
                    CoreEvent::BlocksChanged { block_id, .. } => (envelope.seq, block_id),
                    ref other => panic!("unexpected event replayed: {other:?}"),
                })
                .collect();
            assert_eq!(
                seen,
                vec![(2, 2), (3, 3), (4, 4)],
                "the tail after seq 1, then the live push, each seq carrying its own event"
            );
            assert_eq!(bus.last_seq(), 4);
        });
    }

    /// A misbehaving sink that panics inside `deliver` must not poison the hub
    /// lock: the panic is caught, that sink is pruned, and every well-behaved
    /// sink plus every later publish keep working. Without the isolation, the
    /// `std::sync::Mutex` would poison and every subsequent `bus.send()`
    /// anywhere in the process would panic on `.lock().unwrap()`.
    #[test]
    fn a_panicking_sink_is_pruned_and_never_poisons_the_bus() {
        within(
            "a_panicking_sink_is_pruned_and_never_poisons_the_bus",
            || {
                struct PanicSink;
                impl PushSink<CoreEvent> for PanicSink {
                    fn deliver(&self, _: &PushEnvelope<CoreEvent>) -> bool {
                        panic!("sink blew up");
                    }
                }

                struct CountingSink(Arc<AtomicUsize>);
                impl PushSink<CoreEvent> for CountingSink {
                    fn deliver(&self, _: &PushEnvelope<CoreEvent>) -> bool {
                        self.0.fetch_add(1, Ordering::SeqCst);
                        true
                    }
                }

                let bus: EventBus<CoreEvent> = EventBus::new();
                let count = Arc::new(AtomicUsize::new(0));
                assert_eq!(
                    bus.attach(Arc::new(PanicSink), None),
                    AttachOutcome::Complete
                );
                assert_eq!(
                    bus.attach(Arc::new(CountingSink(count.clone())), None),
                    AttachOutcome::Complete
                );

                // First publish prunes the panicking sink; the good sink still receives.
                bus.emit(blocks_changed(1));
                // Second publish proves the lock was not poisoned by the first.
                bus.emit(blocks_changed(1));

                assert_eq!(
                    count.load(Ordering::SeqCst),
                    2,
                    "the good sink received both"
                );
                assert_eq!(bus.last_seq(), 2, "the bus kept assigning seqs — no poison");
            },
        );
    }

    /// A sink that asks the bus a question from inside `deliver`. Used from both
    /// delivery paths: a publish and an attach replay.
    struct ReenteringSink {
        bus: Mutex<std::sync::Weak<EventBus<CoreEvent>>>,
        seen: Mutex<Vec<u64>>,
    }

    impl PushSink<CoreEvent> for ReenteringSink {
        fn deliver(&self, _: &PushEnvelope<CoreEvent>) -> bool {
            let bus = self.bus.lock().unwrap().upgrade().expect("bus alive");
            // The read that deadlocked: the sink asks the bus a question while
            // the bus is delivering to it.
            let seq = bus.last_seq();
            self.seen.lock().unwrap().push(seq);
            true
        }
    }

    fn reentering(bus: &Arc<EventBus<CoreEvent>>) -> Arc<ReenteringSink> {
        Arc::new(ReenteringSink {
            bus: Mutex::new(Arc::downgrade(bus)),
            seen: Mutex::new(Vec::new()),
        })
    }

    /// A sink that calls back into the bus from inside `deliver` must complete.
    /// Delivering under the hub lock made this hang forever — `std::sync::Mutex`
    /// is not reentrant, so the sink blocked on a lock its own thread held, and
    /// the process had to be killed. If delivery moves back under the lock, the
    /// deadline turns that hang into a failure.
    #[test]
    fn a_sink_that_reenters_the_bus_during_delivery_completes() {
        within(
            "a_sink_that_reenters_the_bus_during_delivery_completes",
            || {
                let bus = Arc::new(EventBus::<CoreEvent>::new());
                let sink = reentering(&bus);
                assert_eq!(bus.attach(sink.clone(), None), AttachOutcome::Complete);

                bus.send(blocks_changed(1));
                bus.send(blocks_changed(2));

                assert_eq!(
                    *sink.seen.lock().unwrap(),
                    vec![1, 2],
                    "both deliveries ran to completion with the bus reachable from inside"
                );
            },
        );
    }

    /// The same claim on the OTHER delivery path: replay. `attach` used to hand
    /// the buffered envelopes over while still holding the hub lock, so this sink
    /// blocked forever on the lock its own thread held — and, because the lock
    /// was held, took every other publisher in the process down with it.
    #[test]
    fn a_sink_that_reenters_the_bus_during_replay_completes() {
        within(
            "a_sink_that_reenters_the_bus_during_replay_completes",
            || {
                let bus = Arc::new(EventBus::<CoreEvent>::new());
                for block_id in 1..=3 {
                    bus.send(blocks_changed(block_id));
                }

                let sink = reentering(&bus);
                assert_eq!(
                    bus.attach(sink.clone(), Some(0)),
                    AttachOutcome::Complete,
                    "the whole ring replays, from inside a sink that re-enters the bus"
                );

                assert_eq!(
                    sink.seen.lock().unwrap().len(),
                    3,
                    "all three replayed envelopes were delivered"
                );
                // Registration happened under the same lock that took the tail,
                // so the sink is live afterwards and the next push reaches it.
                bus.send(blocks_changed(4));
                assert_eq!(sink.seen.lock().unwrap().len(), 4);
            },
        );
    }

    /// A slow replay must not stop the rest of the process from publishing.
    /// While the replay ran under the hub lock, a `send` on another thread
    /// waited for the whole replay before its event even got a seq — and
    /// forever, if the replaying sink never returned.
    #[test]
    fn a_slow_replay_does_not_block_an_unrelated_publish() {
        within("a_slow_replay_does_not_block_an_unrelated_publish", || {
            /// Slow on the two buffered envelopes it is handed as replay,
            /// immediate on everything published afterwards. So what the
            /// timing below measures is the replay in the way of a publish,
            /// not a sink in the way of its own delivery.
            struct SlowReplay {
                live: Arc<AtomicUsize>,
            }
            impl PushSink<CoreEvent> for SlowReplay {
                fn deliver(&self, envelope: &PushEnvelope<CoreEvent>) -> bool {
                    if envelope.seq <= 2 {
                        std::thread::sleep(Duration::from_millis(400));
                    } else {
                        self.live.fetch_add(1, Ordering::SeqCst);
                    }
                    true
                }
            }

            let bus = Arc::new(EventBus::<CoreEvent>::new());
            for block_id in 1..=2 {
                bus.send(blocks_changed(block_id));
            }

            let live = Arc::new(AtomicUsize::new(0));
            let replaying = {
                let bus = Arc::clone(&bus);
                let live = Arc::clone(&live);
                std::thread::spawn(move || bus.attach(Arc::new(SlowReplay { live }), Some(0)))
            };
            // Long enough that the attach is inside its first delivery.
            std::thread::sleep(Duration::from_millis(100));

            let started = Instant::now();
            let seq = bus.send(blocks_changed(3));
            let waited = started.elapsed();

            assert_eq!(replaying.join().unwrap(), AttachOutcome::Complete);
            assert_eq!(seq, 3);
            assert!(
                waited < Duration::from_millis(300),
                "publishing waited {waited:?} behind a replay that holds the hub lock"
            );
            assert_eq!(
                live.load(Ordering::SeqCst),
                1,
                "the live push reached the attaching sink, interleaved with its replay"
            );
        });
    }

    /// The contract on [`PushSink`] says two deliveries can be inside one sink at
    /// once. Pinned here so the sentence and the code cannot drift apart: each
    /// delivery waits for the other to arrive, which only ever completes if the
    /// fan-out really is concurrent.
    #[test]
    fn deliver_runs_on_two_threads_at_once_for_one_sink() {
        within("deliver_runs_on_two_threads_at_once_for_one_sink", || {
            struct Overlapping {
                in_flight: AtomicUsize,
                peak: AtomicUsize,
            }
            impl PushSink<CoreEvent> for Overlapping {
                fn deliver(&self, _: &PushEnvelope<CoreEvent>) -> bool {
                    let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    self.peak.fetch_max(now, Ordering::SeqCst);

                    // Wait for company, but never forever: if delivery is
                    // serialised the wait times out and `peak` stays at 1,
                    // which is the assertion below going red.
                    let deadline = Instant::now() + Duration::from_secs(5);
                    while self.in_flight.load(Ordering::SeqCst) < 2 && Instant::now() < deadline {
                        std::thread::yield_now();
                    }
                    self.peak
                        .fetch_max(self.in_flight.load(Ordering::SeqCst), Ordering::SeqCst);

                    self.in_flight.fetch_sub(1, Ordering::SeqCst);
                    true
                }
            }

            let sink = Arc::new(Overlapping {
                in_flight: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            });
            let bus = Arc::new(EventBus::<CoreEvent>::new());
            assert_eq!(bus.attach(sink.clone(), None), AttachOutcome::Complete);

            let publishers: Vec<_> = (1..=2)
                .map(|block_id| {
                    let bus = Arc::clone(&bus);
                    std::thread::spawn(move || bus.send(blocks_changed(block_id)))
                })
                .collect();
            for publisher in publishers {
                publisher.join().unwrap();
            }

            assert_eq!(
                sink.peak.load(Ordering::SeqCst),
                2,
                "two deliveries were inside the same sink at the same time"
            );
        });
    }

    /// A cursor older than the replay ring cannot be served. The subscriber must
    /// be told, or it resumes across a hole it has no way to detect.
    #[test]
    fn attach_reports_a_gap_when_the_cursor_fell_out_of_the_ring() {
        within(
            "attach_reports_a_gap_when_the_cursor_fell_out_of_the_ring",
            || {
                let bus: EventBus<CoreEvent> = EventBus::new();
                let published = u64::try_from(PUSH_REPLAY_CAPACITY).unwrap() + 500;
                for block_id in 1..=published {
                    bus.send(blocks_changed(i64::try_from(block_id).unwrap()));
                }

                let sink = Arc::new(CollectingSink(Mutex::new(Vec::new())));
                let oldest = published - u64::try_from(PUSH_REPLAY_CAPACITY).unwrap() + 1;
                assert_eq!(
                    bus.attach(sink.clone(), Some(1)),
                    AttachOutcome::Gapped {
                        oldest_replayed: oldest
                    },
                    "seqs 2..{oldest} were evicted and the attach says so"
                );

                let delivered = sink.0.lock().unwrap();
                assert_eq!(
                    delivered.len(),
                    PUSH_REPLAY_CAPACITY,
                    "the ring, not the tail"
                );
                assert_eq!(delivered[0].seq, oldest);
            },
        );
    }

    /// A sink that closes during replay does not stay registered: leaving it in
    /// would seed the sink list with a dead entry that the next publish has to
    /// prune. It is registered before the replay is delivered — that pairing is
    /// what makes the attach gap-free — so the attach takes it back out again.
    #[test]
    fn a_sink_that_closes_during_replay_does_not_stay_registered() {
        within(
            "a_sink_that_closes_during_replay_does_not_stay_registered",
            || {
                struct ClosedSink;
                impl PushSink<CoreEvent> for ClosedSink {
                    fn deliver(&self, _: &PushEnvelope<CoreEvent>) -> bool {
                        false
                    }
                }

                let bus: EventBus<CoreEvent> = EventBus::new();
                for block_id in 1..=3 {
                    bus.send(blocks_changed(block_id));
                }

                assert_eq!(
                    bus.attach(Arc::new(ClosedSink), Some(0)),
                    AttachOutcome::Closed
                );
                assert!(
                    bus.hub.state.lock().unwrap().sinks.is_empty(),
                    "the closed sink was not added to the sink list"
                );
            },
        );
    }
}
