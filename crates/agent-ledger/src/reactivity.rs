//! Solid.js-style fine-grained reactivity for async Rust.
//!
//! # Primitives
//!
//! - [`ReadSignal<T>`]/[`WriteSignal<T>`] via [`create_signal`]: Owned reactive
//!   value, split into a reader (auto-tracks on `.get()`) and a writer
//!   (notifies subscribers on `.set()`).
//! - [`DeferSignal`]: External change source with no owned value. Call
//!   `.react()` to declare a dependency; the producer calls the trigger
//!   closure to wake subscribers.
//! - [`ChangeLog`]: Multi-consumer change broadcaster. Each consumer gets
//!   an independent [`Consumer`] (payload) or watcher (wakeup-only) via
//!   [`ChangeLog::consumer`]/[`ChangeLog::watcher`]; every change fans out
//!   to all of them, so draining one never starves another.
//! - [`reactive!`](crate::reactive): Macro that loops a closure, re-tracking
//!   dependencies each iteration and awaiting any change before re-running.
//!
//! This module names no domain concept, and it must stay that way: it is the
//! scheduler's heartbeat, not part of the ledger.

use std::sync::{Arc, Mutex, Weak};

use tokio::sync::Notify;

/// Re-exported so [`reactive!`](crate::reactive) can name it through `$crate` instead of through
/// the caller's own dependency list. A `macro_rules!` expansion is resolved in
/// the *calling* crate, so an absolute `::tokio::…` path inside one compiles
/// only where the caller happens to depend on a crate by that name — which a
/// consumer of this library has no reason to.
pub use tokio::sync::Notify as ScopeNotify;

// ─── Subscriber list ────────────────────────────────────────────────────

type Subscribers = Arc<Mutex<Vec<Weak<Notify>>>>;

/// Add the current scope's Notify to a signal's subscriber list, dropping the
/// entries whose scope is already gone.
///
/// Pruning belongs HERE and not only in [`wake`]: a scope lives one iteration,
/// so every read leaves a `Weak` behind, and `wake` — the only other pruner —
/// runs when the signal is WRITTEN. A loop that reads a signal nobody ever
/// writes therefore grew the list, and pinned one `Notify` allocation per
/// iteration with it, for as long as the loop ran. Dead entries are pruned where
/// they accumulate, which is where they are added.
fn subscribe(subscribers: &Subscribers, scope: &Arc<Notify>) {
    let mut subs = subscribers.lock().unwrap();
    subs.retain(|weak| weak.strong_count() > 0);
    subs.push(Arc::downgrade(scope));
}

/// Wake all live subscribers and prune dead ones.
fn wake(subscribers: &Subscribers) {
    let mut subs = subscribers.lock().unwrap();
    subs.retain(|weak| {
        if let Some(notify) = weak.upgrade() {
            notify.notify_one();
            true
        } else {
            false
        }
    });
}

// ─── Scope tracking ────────────────────────────────────────────────────

tokio::task_local! {
    /// The current reactive scope's Notify. Signals subscribe this when read.
    pub static SCOPE: Arc<Notify>;
}

/// Track a subscriber list as a dependency of the current scope. Returns
/// whether there was a scope to register with: outside a `reactive!` block
/// there is none, and nothing is registered.
///
/// Callers that also hand back a value (a signal read) may legitimately run
/// outside a scope — reading the current value of a signal from ordinary code is
/// a supported thing to do — so they ignore the answer. Callers whose ONLY
/// effect is the registration must not: see [`track_required`].
fn track(subscribers: &Subscribers) -> bool {
    SCOPE
        .try_with(|scope| {
            subscribe(subscribers, scope);
        })
        .is_ok()
}

/// Track from a call that exists for nothing else. `react()` returns no value:
/// outside a `reactive!` block it registers no dependency and does nothing at
/// all, so the loop that was meant to wake on this source simply never re-runs,
/// and it does so in silence.
///
/// The debug assertion names that mistake where it happens. It compiles out of a
/// release build — the behaviour there is unchanged, still a silent no-op — and
/// fires in every test build.
fn track_required(subscribers: &Subscribers) {
    let tracked = track(subscribers);
    debug_assert!(
        tracked,
        "react() called outside a reactive! block: no reactive scope is in effect, \
         so no dependency was registered and nothing will ever be woken by it"
    );
}

// ─── Signal<T> ──────────────────────────────────────────────────────────

struct SignalInner<T> {
    value: Mutex<T>,
    subscribers: Subscribers,
}

/// Readable half of a signal. `.get()` reads the value and auto-tracks
/// as a dependency of the enclosing `reactive!` scope.
pub struct ReadSignal<T> {
    inner: Arc<SignalInner<T>>,
}

/// Writable half of a signal. `.set()` updates the value and wakes all
/// reactive subscribers.
pub struct WriteSignal<T> {
    inner: Arc<SignalInner<T>>,
}

impl<T: Clone> ReadSignal<T> {
    /// Read the current value, tracking this signal as a dependency of the
    /// enclosing reactive scope.
    ///
    /// # Panics
    ///
    /// If a subscriber list or the value lock was poisoned by a panic.
    #[must_use]
    pub fn get(&self) -> T {
        // Deliberately not `track_required`: a read hands back the value, and
        // reading a signal from ordinary code outside any reactive loop is
        // supported (the `create_signal` example does exactly that). Only the
        // registration-only
        // calls can tell a missing scope apart from a plain read.
        let _ = track(&self.inner.subscribers);
        self.inner.value.lock().unwrap().clone()
    }
}

impl<T> WriteSignal<T> {
    /// Replace the value and wake every subscriber, changed or not.
    ///
    /// # Panics
    ///
    /// If a subscriber list or the value lock was poisoned by a panic.
    pub fn set(&self, value: T) {
        *self.inner.value.lock().unwrap() = value;
        wake(&self.inner.subscribers);
    }
}

impl<T: PartialEq> WriteSignal<T> {
    /// Replace the value only when it differs, so an idempotent write does not
    /// re-run every reactive loop that reads it.
    ///
    /// # Panics
    ///
    /// If a subscriber list or the value lock was poisoned by a panic.
    pub fn set_if_changed(&self, value: T) {
        let mut guard = self.inner.value.lock().unwrap();
        if *guard != value {
            *guard = value;
            drop(guard);
            wake(&self.inner.subscribers);
        }
    }
}

impl<T> Clone for ReadSignal<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Clone for WriteSignal<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Create a signal with an initial value, returning `(reader, writer)`.
///
/// ```
/// use agent_ledger::reactivity::create_signal;
///
/// let (count, set_count) = create_signal(0);
/// set_count.set(1);
/// assert_eq!(count.get(), 1);
/// ```
pub fn create_signal<T>(initial: T) -> (ReadSignal<T>, WriteSignal<T>) {
    let inner = Arc::new(SignalInner {
        value: Mutex::new(initial),
        subscribers: Arc::new(Mutex::new(Vec::new())),
    });
    (
        ReadSignal {
            inner: Arc::clone(&inner),
        },
        WriteSignal { inner },
    )
}

// ─── DeferSignal ────────────────────────────────────────────────────────

/// A reactive dependency backed by an external source. Has no owned value.
/// The producer calls the trigger closure to wake subscribers; consumers
/// call `.react()` to declare a dependency.
pub struct DeferSignal {
    subscribers: Subscribers,
}

impl DeferSignal {
    /// Create a deferred signal. The `setup` closure receives a trigger
    /// callback — call it whenever the external source changes.
    ///
    /// ```ignore
    /// let db_changes = DeferSignal::new(|trigger| {
    ///     sqlite.update_hook(move |_, _, _, _| trigger());
    /// });
    /// ```
    pub fn new<F>(setup: F) -> Self
    where
        F: FnOnce(Arc<dyn Fn() + Send + Sync>),
    {
        let subscribers: Subscribers = Arc::new(Mutex::new(Vec::new()));
        let trigger = {
            let subs = Arc::clone(&subscribers);
            Arc::new(move || wake(&subs))
        };
        setup(trigger);
        Self { subscribers }
    }

    /// Declare a dependency without reading a value. Call it inside a
    /// `reactive!` block: it has no other effect, so a call from anywhere else
    /// does nothing.
    ///
    /// # Panics
    ///
    /// If the subscriber list was poisoned by a panic. In a debug build, also
    /// when called outside a `reactive!` block.
    pub fn react(&self) {
        track_required(&self.subscribers);
    }
}

impl Clone for DeferSignal {
    fn clone(&self) -> Self {
        Self {
            subscribers: Arc::clone(&self.subscribers),
        }
    }
}

// ─── ChangeLog ─────────────────────────────────────────────────────────

/// A change event from an external source's update hook — the store's row
/// change hook is the one this library wires up.
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    /// The row action code (9 = DELETE, 18 = INSERT, 23 = UPDATE — the database
    /// driver's `Action` representation, passed through unchanged).
    pub action: i32,
    /// Table the row lives in.
    pub table: String,
    /// The row's id.
    pub rowid: i64,
}

/// One consumer's private view of the change stream. Each holds its own
/// event queue and wakeup, so consumers never steal each other's events:
/// the producer fans every change into every live consumer's queue.
struct ConsumerInner {
    /// `None` for wakeup-only consumers (the `react()` form) that re-read
    /// stored state themselves and never inspect the payload — they avoid an
    /// unbounded queue. `Some` for `drain()` consumers that need the events.
    buffer: Option<Mutex<Vec<ChangeEvent>>>,
    /// Reactive scopes that have tracked this consumer; woken on each change.
    subscribers: Subscribers,
}

/// A reactive dependency over an external change source. The `ChangeLog`
/// is the producer/registry; each consumer obtains an independent
/// [`Consumer`] via [`ChangeLog::consumer`] (payload) or
/// [`ChangeLog::watcher`] (wakeup-only). The producer broadcasts every
/// change to all live consumers.
pub struct ChangeLog {
    consumers: Arc<Mutex<Vec<Weak<ConsumerInner>>>>,
}

/// One consumer's handle to a [`ChangeLog`]. Not clonable: each consumer
/// is independent by construction, so a buffer is never shared between two
/// reactive loops. Drop the handle to unregister (the producer prunes it).
pub struct Consumer {
    inner: Arc<ConsumerInner>,
}

impl ChangeLog {
    /// Create a change log. The `setup` closure receives a push callback —
    /// call it with change metadata whenever the external source mutates.
    /// Each push fans out to every registered consumer.
    ///
    /// ```ignore
    /// let db_changes = ChangeLog::new(|push| {
    ///     conn.update_hook(Some(move |action, _db, table, rowid| {
    ///         push(action as i32, table, rowid);
    ///     }));
    /// });
    /// ```
    ///
    /// # Panics
    ///
    /// The push callback panics if the consumer registry was poisoned by a
    /// panic.
    pub fn new<F>(setup: F) -> Self
    where
        F: FnOnce(Arc<dyn Fn(i32, &str, i64) + Send + Sync>),
    {
        let consumers: Arc<Mutex<Vec<Weak<ConsumerInner>>>> = Arc::new(Mutex::new(Vec::new()));
        let push = {
            let consumers = Arc::clone(&consumers);
            Arc::new(move |action: i32, table: &str, rowid: i64| {
                let mut list = consumers.lock().unwrap();
                list.retain(|weak| {
                    let Some(consumer) = weak.upgrade() else {
                        return false;
                    };
                    if let Some(buffer) = &consumer.buffer {
                        buffer.lock().unwrap().push(ChangeEvent {
                            action,
                            table: table.to_owned(),
                            rowid,
                        });
                    }
                    wake(&consumer.subscribers);
                    true
                });
            })
        };
        setup(push);
        Self { consumers }
    }

    /// Register a payload consumer with its own private event queue.
    /// Use with the `reactive!(consumer, change, { … })` form.
    ///
    /// # Panics
    ///
    /// If the consumer registry was poisoned by a panic.
    #[must_use]
    pub fn consumer(&self) -> Consumer {
        self.register(Some(Mutex::new(Vec::new())))
    }

    /// Register a wakeup-only consumer that re-reads stored state itself and
    /// never inspects the payload. Avoids buffering. Use with the bare
    /// `reactive! { consumer.react(); … }` form.
    ///
    /// # Panics
    ///
    /// If the consumer registry was poisoned by a panic.
    #[must_use]
    pub fn watcher(&self) -> Consumer {
        self.register(None)
    }

    /// Register one consumer, dropping the entries whose consumer is already
    /// gone.
    ///
    /// Pruning belongs HERE and not only in the push callback, for the same
    /// reason it belongs in [`subscribe`]: the push is the only other pruner and
    /// it runs when a change FIRES. A caller that takes short-lived consumers or
    /// watchers from a source that stays quiet therefore grew the registry, and
    /// pinned one `ConsumerInner` allocation per registration with it, for as
    /// long as the log lived. Dead entries are pruned where they accumulate,
    /// which is where they are added.
    fn register(&self, buffer: Option<Mutex<Vec<ChangeEvent>>>) -> Consumer {
        let inner = Arc::new(ConsumerInner {
            buffer,
            subscribers: Arc::new(Mutex::new(Vec::new())),
        });
        let mut consumers = self.consumers.lock().unwrap();
        consumers.retain(|weak| weak.strong_count() > 0);
        consumers.push(Arc::downgrade(&inner));
        drop(consumers);
        Consumer { inner }
    }
}

impl Consumer {
    /// Drain this consumer's accumulated changes and subscribe for future
    /// ones. Called internally by `reactive!(consumer, change) { body }`.
    /// Only valid on a [`ChangeLog::consumer`] handle.
    ///
    /// # Panics
    ///
    /// If this consumer's buffer or subscriber list was poisoned by a panic.
    #[must_use]
    pub fn drain(&self) -> Vec<ChangeEvent> {
        // Not `track_required`: a drain hands back the buffered events, so it is
        // usable from ordinary code that polls the queue itself.
        let _ = track(&self.inner.subscribers);
        match &self.inner.buffer {
            Some(buffer) => std::mem::take(&mut *buffer.lock().unwrap()),
            None => Vec::new(),
        }
    }

    /// Declare a dependency without consuming changes. For loops that
    /// re-read state themselves and only need to be woken. Call it inside a
    /// `reactive!` block: it has no other effect, so a call from anywhere else
    /// does nothing.
    ///
    /// # Panics
    ///
    /// If this consumer's subscriber list was poisoned by a panic. In a debug
    /// build, also when called outside a `reactive!` block.
    pub fn react(&self) {
        track_required(&self.inner.subscribers);
    }
}

impl Clone for ChangeLog {
    fn clone(&self) -> Self {
        Self {
            consumers: Arc::clone(&self.consumers),
        }
    }
}

// ─── reactive! ──────────────────────────────────────────────────────────

/// Run a reactive loop. Two forms:
///
/// **Signal-only** — body re-runs on any tracked dependency change:
/// ```ignore
/// reactive! {
///     db_changes.react();          // subscribe FIRST
///     if !latched.get() { /* ... */ }
/// }
/// ```
///
/// # Ordering contract: subscribe before you work
///
/// Every `.react()` and every `.get()` a body depends on must run BEFORE the
/// body does the work that the change could invalidate. This is a contract, not
/// a style preference.
///
/// The loop subscribes while the body runs and parks afterwards. A change that
/// fires between the start of the body and the `.react()` that would have
/// subscribed to it reaches no subscriber: the previous iteration's scope is
/// already gone and this iteration's is not yet registered. The loop then parks
/// on a wakeup that has already happened and waits for the next one — which, for
/// a source that only fires when something changes, may be never. Putting the
/// subscription first shrinks that window to the work the body has not started
/// yet, so a change during the work wakes the loop and it re-runs.
///
/// The window is not zero even then: the primitives here subscribe, they do not
/// replay. A body whose correctness cannot tolerate one lost wakeup wants a
/// source that queues (a [`ChangeLog::consumer`](crate::reactivity::ChangeLog::consumer)
/// and the change-driven form below), not a bare `react()`.
///
/// **Change-driven** — body runs once per `ChangeEvent` drained from a
/// [`ChangeLog::consumer`](crate::reactivity::ChangeLog::consumer). Each
/// consumer has its own queue, so this never steals events from another
/// reactive loop. The caller names the variable:
/// ```ignore
/// let changes = store.changes.consumer();
/// reactive!(changes, change, {
///     if change.table == "message_blocks" {
///         // handle block change
///     }
/// });
/// ```
///
/// The expansion names only `::std` and `$crate`, so a caller needs no
/// dependency of its own beyond this library. It still has to run inside an
/// async runtime, but it does not have to name one to compile.
#[macro_export]
macro_rules! reactive {
    ($source:expr, $change:ident, $($body:tt)*) => {
        loop {
            let scope = ::std::sync::Arc::new($crate::reactivity::ScopeNotify::new());
            $crate::reactivity::SCOPE
                .scope(::std::sync::Arc::clone(&scope), async {
                    for $change in $source.drain() {
                        let _ = &$change;
                        $($body)*
                    }
                })
                .await;
            scope.notified().await;
        }
    };
    ($($body:tt)*) => {
        loop {
            let scope = ::std::sync::Arc::new($crate::reactivity::ScopeNotify::new());
            $crate::reactivity::SCOPE
                .scope(::std::sync::Arc::clone(&scope), async {
                    $($body)*
                })
                .await;
            scope.notified().await;
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{Duration, sleep};

    /// Where a test parks the producer's push callback so it can fire changes
    /// by hand, standing in for the store's change hook.
    type PushSlot = Arc<Mutex<Option<Arc<dyn Fn(i32, &str, i64) + Send + Sync>>>>;

    /// The same parking spot for a [`DeferSignal`]'s trigger callback.
    type TriggerSlot = Arc<Mutex<Option<Arc<dyn Fn() + Send + Sync>>>>;

    #[tokio::test]
    async fn signal_get_set() {
        let (count, set_count) = create_signal(0);
        assert_eq!(count.get(), 0);
        set_count.set(42);
        assert_eq!(count.get(), 42);
    }

    #[tokio::test]
    async fn set_if_changed_skips_same_value() {
        let (val, set_val) = create_signal(10);
        let runs = Arc::new(AtomicUsize::new(0));

        let runs_clone = Arc::clone(&runs);
        let val_clone = val.clone();
        let handle = tokio::spawn(async move {
            reactive! {
                let _ = val_clone.get();
                runs_clone.fetch_add(1, Ordering::SeqCst);
            }
        });

        sleep(Duration::from_millis(10)).await;
        let after_first = runs.load(Ordering::SeqCst);
        assert!(after_first >= 1);

        // Set same value — should NOT re-trigger.
        set_val.set_if_changed(10);
        sleep(Duration::from_millis(10)).await;
        assert_eq!(runs.load(Ordering::SeqCst), after_first);

        // Set different value — should re-trigger.
        set_val.set_if_changed(20);
        sleep(Duration::from_millis(10)).await;
        assert!(runs.load(Ordering::SeqCst) > after_first);

        handle.abort();
    }

    #[tokio::test]
    async fn reactive_reruns_on_signal_change() {
        let (count, set_count) = create_signal(0);
        let observed = Arc::new(Mutex::new(Vec::new()));

        let obs = Arc::clone(&observed);
        let count_clone = count.clone();
        let handle = tokio::spawn(async move {
            reactive! {
                let v = count_clone.get();
                obs.lock().unwrap().push(v);
            }
        });

        // Let the first iteration run.
        sleep(Duration::from_millis(10)).await;

        set_count.set(1);
        sleep(Duration::from_millis(10)).await;

        set_count.set(2);
        sleep(Duration::from_millis(10)).await;

        handle.abort();

        let values = observed.lock().unwrap().clone();
        assert_eq!(&values[..3], &[0, 1, 2]);
    }

    #[tokio::test]
    async fn defer_signal_wakes_reactive() {
        let trigger = Arc::new(std::sync::Mutex::new(None::<Arc<dyn Fn() + Send + Sync>>));
        let trigger_clone = Arc::clone(&trigger);

        let ds = DeferSignal::new(move |trigger_fn| {
            *trigger_clone.lock().unwrap() = Some(trigger_fn);
        });

        let runs = Arc::new(AtomicUsize::new(0));
        let runs_clone = Arc::clone(&runs);
        let ds_clone = ds.clone();
        let handle = tokio::spawn(async move {
            reactive! {
                ds_clone.react();
                runs_clone.fetch_add(1, Ordering::SeqCst);
            }
        });

        sleep(Duration::from_millis(10)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 1);

        // Fire the external trigger.
        let fire = trigger.lock().unwrap().clone().unwrap();
        fire();
        sleep(Duration::from_millis(10)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 2);

        fire();
        sleep(Duration::from_millis(10)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 3);

        handle.abort();
    }

    #[tokio::test]
    async fn mixed_signal_and_defer() {
        let (latched, set_latched) = create_signal(true);
        let trigger = Arc::new(std::sync::Mutex::new(None::<Arc<dyn Fn() + Send + Sync>>));
        let trigger_clone = Arc::clone(&trigger);
        let db = DeferSignal::new(move |trigger_fn| {
            *trigger_clone.lock().unwrap() = Some(trigger_fn);
        });

        let work_done = Arc::new(AtomicUsize::new(0));
        let wd = Arc::clone(&work_done);
        let latched_clone = latched.clone();
        let db_clone = db.clone();
        let handle = tokio::spawn(async move {
            reactive! {
                if !latched_clone.get() {
                    wd.fetch_add(1, Ordering::SeqCst);
                }
                db_clone.react();
            }
        });

        sleep(Duration::from_millis(10)).await;
        // Latched — no work done.
        assert_eq!(work_done.load(Ordering::SeqCst), 0);

        // A store change while latched — still no work.
        let fire = trigger.lock().unwrap().clone().unwrap();
        fire();
        sleep(Duration::from_millis(10)).await;
        assert_eq!(work_done.load(Ordering::SeqCst), 0);

        // Unlatch — work runs.
        set_latched.set(false);
        sleep(Duration::from_millis(10)).await;
        assert!(work_done.load(Ordering::SeqCst) >= 1);

        handle.abort();
    }

    /// Reading a signal that nobody ever writes must not make the subscriber
    /// list grow. Only a write prunes dead entries, and there is no write here:
    /// before the fix each iteration left one dead `Weak` and one pinned
    /// `Notify` allocation behind, measured at ~812 kB of RSS over ~11,500
    /// iterations, for as long as the loop lived.
    #[tokio::test]
    async fn reading_a_never_written_signal_does_not_grow_the_subscriber_list() {
        let (value, _set_value) = create_signal(0);

        // One scope per iteration, exactly as `reactive!` builds one per pass,
        // dropped when the iteration ends.
        for _ in 0..2_000 {
            let scope = Arc::new(Notify::new());
            SCOPE
                .scope(scope, async {
                    let _ = value.get();
                })
                .await;
        }

        let live = value.inner.subscribers.lock().unwrap().len();
        assert!(
            live <= 2,
            "the subscriber list stays bounded across iterations, found {live}"
        );
    }

    /// The losing shape from the ordering contract on [`reactive!`], pinned so
    /// the defect lives in the tree rather than only in a report: a change that
    /// fires inside the body BEFORE the `.react()` that would subscribe to it
    /// reaches nobody, and the loop then parks on a wakeup that already
    /// happened. It never re-runs, and this test fails on the second-iteration
    /// assertion.
    ///
    /// Ignored because it records a known defect this pass deliberately did not
    /// fix: closing the window means changing the wakeup semantics (a replaying
    /// or edge-latching source), which is a design change of its own. The
    /// documented contract — subscribe first — is the mitigation that shipped.
    #[tokio::test]
    #[ignore = "records the known lost-wakeup window: a change firing before .react() in the same iteration is lost and the loop parks forever; fixing the wakeup semantics is a separate design change"]
    async fn a_change_before_react_in_the_same_iteration_is_lost() {
        let trigger: TriggerSlot = Arc::new(Mutex::new(None));
        let trigger_slot = Arc::clone(&trigger);
        let source = DeferSignal::new(move |trigger_fn| {
            *trigger_slot.lock().unwrap() = Some(trigger_fn);
        });

        let runs = Arc::new(AtomicUsize::new(0));
        let runs_in_loop = Arc::clone(&runs);
        let trigger_in_loop = Arc::clone(&trigger);
        let source_in_loop = source.clone();
        let handle = tokio::spawn(async move {
            reactive! {
                // The work comes first and the subscription last — the shape the
                // documentation used to recommend.
                if runs_in_loop.fetch_add(1, Ordering::SeqCst) == 0 {
                    let fire = trigger_in_loop.lock().unwrap().clone().unwrap();
                    fire();
                }
                source_in_loop.react();
            }
        });

        sleep(Duration::from_millis(50)).await;
        let observed = runs.load(Ordering::SeqCst);
        handle.abort();
        assert!(
            observed >= 2,
            "the change fired during iteration 1 and the loop should have re-run; it ran {observed} time(s)"
        );
    }

    /// A change must reach every payload consumer — draining one must not
    /// starve another. This is the regression test for the fork bug where
    /// the block watcher drained the shared buffer before the conversations
    /// watcher could see the `conversations` insert.
    #[test]
    fn change_log_fans_out_to_all_consumers() {
        let push_slot: PushSlot = Arc::new(Mutex::new(None));
        let slot = Arc::clone(&push_slot);
        let log = ChangeLog::new(move |push| {
            *slot.lock().unwrap() = Some(push);
        });

        let blocks = log.consumer();
        let conversations = log.consumer();

        let push = push_slot.lock().unwrap().clone().unwrap();
        push(18, "conversations", 54);

        // The order consumers drain in must not matter: both see the event.
        let drained_blocks = blocks.drain();
        let drained_conversations = conversations.drain();

        assert_eq!(drained_blocks.len(), 1);
        assert_eq!(drained_conversations.len(), 1);
        assert_eq!(drained_conversations[0].table, "conversations");
        assert_eq!(drained_conversations[0].rowid, 54);
    }

    /// `react()` outside a reactive scope registers nothing and does nothing:
    /// the loop it was meant to wake never re-runs. That used to be silent. A
    /// debug assertion now names it; a release build is unchanged, so the test
    /// only exists where the assertion does.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "react() called outside a reactive! block")]
    fn react_outside_a_reactive_block_is_a_loud_mistake() {
        let source = DeferSignal::new(|_trigger| {});
        source.react();
    }

    /// Reading a signal outside a reactive scope stays legal and silent: it
    /// hands back the value, which is a supported thing to want. The assertion
    /// above is for the calls that have no other effect.
    #[test]
    fn reading_a_signal_outside_a_reactive_block_is_allowed() {
        let (value, set_value) = create_signal(1);
        set_value.set(2);
        assert_eq!(value.get(), 2);
    }

    /// Short-lived consumers and watchers must not make the registry grow while
    /// the source stays quiet. Only a pushed change prunes dead entries, and
    /// there is no change here: before the fix each registration left one dead
    /// `Weak` and one pinned `ConsumerInner` allocation behind, for as long as
    /// the log lived. The same shape, and the same test, as the subscriber list
    /// of a signal nobody writes.
    #[test]
    fn registering_consumers_without_a_change_does_not_grow_the_registry() {
        let log = ChangeLog::new(|_push| {});

        for _ in 0..2_000 {
            drop(log.consumer());
            drop(log.watcher());
        }

        let live = log.consumers.lock().unwrap().len();
        assert!(
            live <= 2,
            "the consumer registry stays bounded across registrations, found {live}"
        );
    }

    /// Wakeup-only watchers don't accumulate a payload queue, and dropping
    /// a consumer unregisters it so the producer prunes the dead entry.
    #[test]
    fn watcher_does_not_buffer_and_drop_unregisters() {
        let push_slot: PushSlot = Arc::new(Mutex::new(None));
        let slot = Arc::clone(&push_slot);
        let log = ChangeLog::new(move |push| {
            *slot.lock().unwrap() = Some(push);
        });

        let watcher = log.watcher();
        let consumer = log.consumer();
        let push = push_slot.lock().unwrap().clone().unwrap();

        push(18, "blocks", 1);
        assert!(watcher.drain().is_empty()); // wakeup-only: never buffers
        assert_eq!(consumer.drain().len(), 1);

        // Dropping a consumer removes it from the producer's registry.
        drop(consumer);
        push(18, "blocks", 2);
        assert_eq!(log.consumers.lock().unwrap().len(), 1); // only `watcher` remains
    }
}
