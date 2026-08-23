//! The per-turn dispatch seam (2026-08-22): the actor-owned slot that carries
//! a turn's dispatch anchor from the dispatch to the writes the turn produces.
//!
//! The provider channel deliberately carries neutral values and never ledger
//! identity, so the anchor cannot travel with the stream. Instead the actor —
//! the one dispatcher — sets the dispatched turn's anchor here at dispatch
//! and clears it at the stream's close, and the ingestion reader on the same
//! binding reads it at every insert. One slot per binding: a torn-down
//! reader keeps its own, already-cleared slot, so its late writes can never
//! borrow a successor turn's identity.
//!
//! The slot is STREAM-scoped, not turn-scoped (amended 2026-08-22): a tool
//! turn spans several streams — one per round — and the identity that spans
//! them is actor state, held by the dispatcher from the turn's first
//! dispatch until a close that ends the turn and re-set here at every
//! continuation dispatch. The seam never decides whose turn is open; it only
//! carries the open dispatch's answer to the reader.
//!
//! A per-turn VALUE handed to the reader was considered and rejected
//! (2026-08-22): the reader consumes exactly one channel, and a second
//! actor-to-reader channel carrying the anchor has no ordering against the
//! provider channel — the reader could ingest a turn's first delta before
//! the value for that turn arrives, and buffering until it does would stall
//! ingestion on a race the slot decides for free. The slot set BEFORE the
//! stream request is sent is what guarantees the anchor is visible to the
//! turn's first insert, and the fresh-slot-per-binding rule is the
//! generation scoping that keeps a torn-down reader off a successor's turn.
//!
//! The seam also carries the abandoned-turn fence (2026-08-22). A provider
//! that ends its message and then stalls past the reader's drain deadline
//! gets its turn closed by the reader — but the provider may still wake later
//! and deliver the turn's held tail, the trailing tool lifecycles and the
//! done, into the same channel. Every event on that channel is neutral, so
//! once a successor turn is dispatched on the same binding the stalled turn's
//! late tail and the successor's own events are indistinguishable to the
//! reader: a reader-local filter was considered and rejected for exactly that
//! reason — whichever way it decides, one of the two histories is
//! misattributed. The sound fence is to stop reusing the binding: the reader
//! records each dispatch's epoch, and when the drain deadline closes a turn
//! it marks that epoch abandoned here before emitting the terminal, then
//! exits. The actor observes the mark at that close and retires the binding,
//! so the next turn rebinds fresh — new channel, new reader, new seam — and
//! the stalled provider's late tail dies with the dropped channel. A bare
//! reader exit without the mark is not enough: the actor would keep
//! dispatching turns into a stream nobody reads.

use std::sync::{Arc, Mutex, PoisonError};

/// One binding's seam state, behind the shared lock.
#[derive(Default)]
struct Seam {
    /// The open dispatch's turn anchor, `None` between streams.
    anchor: Option<i64>,
    /// Counts the dispatches on this binding: bumped at every [`TurnAnchor::set`],
    /// so each turn the binding ever carries has a distinct epoch.
    epoch: u64,
    /// The epoch of a turn the reader abandoned at its drain deadline, if any.
    abandoned: Option<u64>,
}

/// The shared slot. Cheap to clone — clones observe one value.
#[derive(Clone, Default)]
pub(crate) struct TurnAnchor {
    current: Arc<Mutex<Seam>>,
}

impl TurnAnchor {
    /// An empty slot: no turn is open on this binding.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The slot's lock. No code ever runs while it is held, so a poisoned
    /// lock carries no broken invariant and is simply taken over.
    fn lock(&self) -> std::sync::MutexGuard<'_, Seam> {
        self.current.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record the dispatched turn's anchor and give the turn its epoch.
    /// Called by the actor, at dispatch.
    pub(crate) fn set(&self, anchor: i64) {
        let mut seam = self.lock();
        seam.epoch += 1;
        seam.anchor = Some(anchor);
    }

    /// Close the stream's seam. Called by the actor on the closed signal,
    /// the error signal and the interrupt teardown — the three close edges.
    /// Whether the TURN ends there is the actor's own decision, held outside
    /// this slot (amended 2026-08-22): a continuation round re-sets the same
    /// anchor at its dispatch.
    pub(crate) fn clear(&self) {
        self.lock().anchor = None;
    }

    /// The open dispatch's turn anchor, `None` between streams.
    pub(crate) fn get(&self) -> Option<i64> {
        self.lock().anchor
    }

    /// The epoch of the most recently dispatched turn on this binding.
    pub(crate) fn epoch(&self) -> u64 {
        self.lock().epoch
    }

    /// Mark a turn abandoned: its provider stalled past the drain deadline
    /// and the reader closed the turn itself. Called by the reader, with the
    /// epoch it recorded when the drain began, BEFORE it emits the terminal —
    /// so the actor's close is guaranteed to observe the mark.
    pub(crate) fn mark_abandoned(&self, epoch: u64) {
        self.lock().abandoned = Some(epoch);
    }

    /// Whether the reader has abandoned this binding. Unconditional on
    /// purpose (revised 2026-08-22, after verification proved the epoch
    /// guard wrong): the mark records that the READER EXITED, which is a
    /// binding-liveness fact, not a turn fact — a reader gone is gone for
    /// every later turn on the binding, so any mark means the binding must
    /// be retired. The guarded form discarded a real mark when an
    /// out-of-band close and an unlatch had bumped the epoch between the
    /// drain's start and the deadline, and the conversation wedged on a
    /// dead channel. The epoch's remaining job is scoping the drain TIMER
    /// (the reader fires it only for the turn that armed it).
    pub(crate) fn is_abandoned(&self) -> bool {
        self.lock().abandoned.is_some()
    }
}
