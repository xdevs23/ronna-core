//! The ratchet — the ONE place a processed cursor moves.
//!
//! It drives every block's [`Agency`](super::Agency) hooks blindly, forward from the persisted
//! cursor, and reports whether the frontier owes a model turn. It never names a
//! block type and never branches on a domain concept; per-kind behavior lives
//! on the kinds.
//!
//! One machinery, two ledgers: the drive runs over whatever [`LedgerSource`]
//! the caller instantiates it with and never learns which. That is what makes
//! "the second ledger" a second instantiation rather than a second scheduler.

use std::future::Future;

use crate::block::Block;
use crate::bus::RuntimeEvent;
use crate::store::StoreError;
use crate::types::Awaiting;

use super::{AgencyCtx, RuntimeKind, ToolCall};

/// The drive's one seam onto persistence: list the ledger, read its cursor,
/// persist its cursor.
///
/// Each instantiation hands in its own triple, and the ratchet stays
/// ledger-blind. A closed set with static dispatch, nothing boxed.
///
/// Like [`Agency`](super::Agency), the hooks are declared `fn … -> impl Future<Output = …> +
/// Send` so their Send-ness is nameable from a bound; implementors keep
/// writing native `async fn`, and each such future MUST be `Send` — the
/// implementor's obligation on a multi-threaded runtime, checked at the impl.
/// `Sync` is a supertrait because the default [`tail`](Self::tail) borrows the
/// source across an await.
pub trait LedgerSource: Sync {
    /// Every block in this ledger, in ledger order.
    ///
    /// # Errors
    ///
    /// If the underlying read fails.
    fn list<E: RuntimeEvent>(
        &self,
        ctx: &AgencyCtx<E>,
    ) -> impl Future<Output = Result<Vec<Block>, StoreError>> + Send;

    /// This ledger's LAST block right now, or `None` when it is empty.
    ///
    /// The frontier decision asks through this after the drive loop, so the
    /// answer reflects what the hooks — and anything else that appended while
    /// they ran — left behind, rather than the snapshot the drive started
    /// with. The default re-reads the whole ledger, which is always correct; both
    /// store-backed ledgers override it with a one-row query.
    ///
    /// # Errors
    ///
    /// If the underlying read fails.
    fn tail<E: RuntimeEvent>(
        &self,
        ctx: &AgencyCtx<E>,
    ) -> impl Future<Output = Result<Option<Block>, StoreError>> + Send {
        async { Ok(self.list(ctx).await?.pop()) }
    }

    /// This ledger's persisted cursor; 0 when nothing is confirmed.
    ///
    /// # Errors
    ///
    /// If the underlying read fails.
    fn cursor<E: RuntimeEvent>(
        &self,
        ctx: &AgencyCtx<E>,
    ) -> impl Future<Output = Result<i64, StoreError>> + Send;

    /// Advance this ledger's persisted cursor to a confirmed block id.
    ///
    /// # Errors
    ///
    /// If the underlying write fails.
    fn persist_cursor<E: RuntimeEvent>(
        &self,
        ctx: &AgencyCtx<E>,
        confirmed_id: i64,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// The conversation ledger: junction-ordered blocks, cursored on the
/// conversation's last processed block.
pub struct ConversationLedger;

impl LedgerSource for ConversationLedger {
    async fn list<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<Vec<Block>, StoreError> {
        ctx.store.list_blocks(ctx.conversation_id).await
    }

    async fn cursor<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<i64, StoreError> {
        ctx.store.cursor(ctx.conversation_id).await
    }

    async fn persist_cursor<E: RuntimeEvent>(
        &self,
        ctx: &AgencyCtx<E>,
        confirmed_id: i64,
    ) -> Result<(), StoreError> {
        ctx.store
            .update_cursor(ctx.conversation_id, confirmed_id)
            .await
    }

    async fn tail<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<Option<Block>, StoreError> {
        ctx.store.latest_block(ctx.conversation_id).await
    }
}

/// The metadata ledger: insertion-ordered metadata rows surfaced as the same
/// block shape, cursored on the conversation's last processed metadata row.
///
/// Handed to the SAME [`drive_ledger`] the conversation ledger goes through.
/// The proof that the ratchet is ledger-blind is that this type contributes
/// three reads and nothing else — no branch anywhere in the machinery names it.
pub struct MetadataLedger;

impl LedgerSource for MetadataLedger {
    async fn list<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<Vec<Block>, StoreError> {
        ctx.store.list_metadata_blocks(ctx.conversation_id).await
    }

    async fn cursor<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<i64, StoreError> {
        ctx.store.metadata_cursor(ctx.conversation_id).await
    }

    async fn persist_cursor<E: RuntimeEvent>(
        &self,
        ctx: &AgencyCtx<E>,
        confirmed_id: i64,
    ) -> Result<(), StoreError> {
        ctx.store
            .update_metadata_cursor(ctx.conversation_id, confirmed_id)
            .await
    }

    async fn tail<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<Option<Block>, StoreError> {
        ctx.store.latest_metadata_block(ctx.conversation_id).await
    }
}

/// The result of one drive over a ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Outcome {
    /// The persisted cursor after the drive — the last DURABLE block id it
    /// confirmed, 0 when nothing is confirmed yet. An ephemeral block the
    /// drive confirmed is not a candidate: see
    /// [`Agency::durable`](super::Agency::durable).
    pub cursor: i64,
    /// The frontier gate: the drive confirmed its whole SNAPSHOT without
    /// parking, AND the tail — read fresh after the drive, through a dead
    /// turn's trailing closure run (`frontier_block`, 2026-08-23) —
    /// awaits the model. Never a backward scan beyond that scoped skip. The
    /// fresh tail can include blocks appended after the snapshot that the
    /// drive never ran; every kind that answers a model ask has an inert
    /// `run()`, so nothing is skipped by firing on one, and the next drive
    /// confirms them.
    pub owes_turn: bool,
    /// The drive stopped before confirming the tail — a block reported
    /// not-done, or its `run()` failed. A fresh tick re-drives it.
    pub parked: bool,
    /// The frontier block's own ask, straight off
    /// [`Agency::awaiting`](super::Agency::awaiting), read from
    /// the same fresh frontier `owes_turn` is decided on. Published on
    /// conversation state so a consumer can tell an out-of-band ask from a
    /// chat reply. Orthogonal to `owes_turn`, which additionally requires the
    /// drive to have finished unparked.
    pub awaiting: Option<Awaiting>,
}

/// Where a drive resumes: the position in ledger order the persisted cursor
/// anchors to.
///
/// The cursor's OWN block first (2026-08-31): a cursor names a block, and
/// where that block sits is a fact the ledger answers directly.
///
/// The id scan behind it was the whole rule while ids ascended along
/// junction order in every conversation, and it stopped being the whole rule
/// the moment one could not — a conversation opened with FRESH blocks in
/// front of junction rows inherited from an older one holds a descent at
/// that seam, and the id scan then lands on the fresh front block and
/// re-derives the entire history on every single drive. Nothing about that
/// is unsound, because the inclusive re-drive is idempotent; it is simply
/// the whole ledger walked forever, per tick, for nothing.
///
/// The id scan stays as the fallback for the case it was written for: a
/// cursor whose own block is GONE — detached — lands on the first block at
/// or after it. Its contract is that it never lands PAST work the drive has
/// not confirmed; what it costs depends on the ledger it lands in, and the
/// difference is worth stating because the seam above is where it bites:
///
/// - Ids ascending along ledger order — every conversation but a compacted
///   thread: the scan lands right after the vanished row, and the re-drive
///   costs the tail behind it.
/// - Ids descending at a seam — a compacted thread, whose own front appends
///   outrank everything it inherited: a cursor that named an inherited block
///   compares below those front appends, so the scan lands on the FRONT and
///   the drive re-walks the whole ledger. Reachable only through the erasure
///   scrub, which detaches blocks from a clone; a cursor naming one of them
///   pays a full re-derive on its next drive and nothing more, because the
///   inclusive re-drive is idempotent.
///
/// Cheaper is available and unsound, which is why it is not here: scanning
/// from the END for the last block below the cursor answers the seam case
/// exactly, and lands past unconfirmed blocks in the ordinary one — a
/// detached FRONT-append's cursor would skip the whole inherited tail behind
/// it. Costly and never wrong beats exact-and-sometimes-wrong.
///
/// Only a cursor past every block left has nowhere to land; that re-derives
/// from the start, which the inclusive re-drive makes always safe.
fn resume_at(ledger: &[Block], cursor: i64) -> usize {
    ledger
        .iter()
        .position(|block| block.id == cursor)
        .or_else(|| ledger.iter().position(|block| block.id >= cursor))
        .unwrap_or(0)
}

/// The owed-turn decision over one tail block, written once: does the tail
/// await the model? (Amended 2026-08-23: the anchor half this function used
/// to carry moved to [`fresh_turn_anchor`], because the seventh break
/// proved a fresh dispatch's anchor is a fact about the SNAPSHOT, not about
/// the tail alone — the owed decision is the one thing the tail answers by
/// itself.)
///
/// Two sites answer with this rule and they are the same rule ON DIFFERENT
/// SNAPSHOTS by design (2026-08-22): the scheduler's drive decides it on the
/// fresh post-drive tail to know whether to signal, and the actor's delivery
/// re-decides it on the very snapshot the model request is built from —
/// which is where a signal that went stale stands down. Carrying the drive's
/// answer to the delivery inside the signal was rejected in the slice's
/// decision record: the ledger can move between the two reads, and a
/// signal-borne value races the very appends the dispatch identity exists to
/// record. Both sites hand this rule the block [`frontier_block`] resolves —
/// the tail read through a dead turn's trailing closure run (2026-08-23),
/// so the transparency cannot drift between them either.
pub(crate) fn frontier_owes_turn<K: RuntimeKind>(tail: &Block) -> bool {
    K::from_block(tail).awaiting() == Some(Awaiting::Model)
}

/// The block the frontier decision reads over one snapshot: the tail, read
/// THROUGH a dead turn's trailing closure run — the rows that STORE a turn's
/// end and the trailing blocks those rows answer
/// (2026-08-23, the verified burial defect). An opaque closure row at the
/// tail buried an addressed message absorbed into the dead turn's window:
/// the close's re-check read the row, decided nothing owed, and rested —
/// and the non-latching closed edge re-checks exactly once, so no turn ever
/// fired for the message. The latching error edge never had the hole: the
/// latch's release re-engages there.
///
/// Two shapes store an end and both are read through (2026-08-30): the
/// close's turn-end marker, and the resolution an ends-turn tool stamped —
/// which ends its turn while asking nothing, the exact shape the burial
/// lives in. The walk names neither: it asks
/// [`Agency::frontier_transparent`](super::Agency::frontier_transparent),
/// and each kind answers from its own stored row.
///
/// The walk reads through the dead turn's WHOLE trailing run, not through
/// the closure rows alone (amended 2026-08-23, the verified regression on the
/// transparency's first cut): a message absorbed between the turn's call
/// and its RESULT — the tool-execution window — sits under the
/// outcome-plus-marker pair, and a read that skipped only the closure rows
/// stopped on the outcome, found it answered, and rested — the same burial
/// wearing one more block. Three rules, all scoped to the closure rows the
/// walk has skipped:
///
/// - A trailing turn-closure row is skipped, and the turn it is anchored
///   on is disowned from here back.
/// - A trailing block anchored on a disowned turn — the dead turn's own
///   product, wherever it sits in the trailing run — is skipped too: the
///   closure row recorded that turn's end, and stopping on its outcome would
///   either bury what the outcome hides or redispatch the turn that row
///   exists to close. A block anchored on a turn no skipped row disowns
///   stays opaque, which is what lets an outcome landing AFTER the closure —
///   an approval resolving late — summon its resumption as before.
/// - The first block outside the run is the frontier — a model-owed message
///   absorbed anywhere in the dead turn's window owes, and dispatches
///   anchored on itself. One bound: a stop on a disowned turn's own SUMMONS
///   rests on the tail instead — that block's turn is the very one the
///   closure row recorded as ended, and owing it again would redispatch it.
///
/// Transparency is a kind-level answer
/// ([`Agency::frontier_transparent`](super::Agency::frontier_transparent)),
/// scoped to exactly the rows that store a turn's end — the turn-closure
/// machine keys and the ends-turn-stamped resolution; the interrupt's status
/// stays opaque, and its capping under the latch is that path's recorded
/// semantics.
pub(crate) fn frontier_block<K: RuntimeKind>(snapshot: &[Block]) -> Option<&Block> {
    let tail = snapshot.last()?;
    let mut disowned: Vec<i64> = Vec::new();
    for block in snapshot.iter().rev() {
        if K::from_block(block).frontier_transparent() {
            if let Some(anchor) = block.dispatch_anchor
                && !disowned.contains(&anchor)
            {
                disowned.push(anchor);
            }
            continue;
        }
        if block
            .dispatch_anchor
            .is_some_and(|anchor| disowned.contains(&anchor))
        {
            continue;
        }
        if disowned.contains(&block.id) {
            // The dead turn's own summons: answered by the marker's record,
            // so the frontier rests on the tail — the marker, which asks
            // nothing.
            return Some(tail);
        }
        return Some(block);
    }
    // Nothing but the dead turn's trailing run: nothing sits behind it, the
    // tail rests.
    Some(tail)
}

/// The FRESH-dispatch anchor resolution, over the dispatch's own snapshot
/// (2026-08-22; amended 2026-08-23, the verified seventh break — the
/// resolution is ledger-first). `tail` is `snapshot`'s own model-owed tail,
/// the block [`frontier_owes_turn`] just answered on. Consulted only when
/// the actor holds no open turn: a dispatch while a turn is open is that
/// turn's continuation and reuses the held identity. Three arms, in order:
///
/// - **A turn product inherits its own anchor.** A non-null-anchored tail —
///   a tool result, a status — is itself some turn's product, and the
///   dispatch it summons is that turn's continuation.
/// - **A null-anchored tail inherits the newest unanswered outcome's
///   anchor** ([`ToolCall::unanswered_outcome_anchor`]). A released turn —
///   a parked interactive call resumed by its approval, a restart
///   recovering a round — is owed its continuation by the LEDGER, not by
///   any actor's memory, and a message absorbed behind the outcome must not
///   capture that continuation's identity.
/// - **Otherwise the tail starts a new identity**, anchored on itself: a
///   message, a consumer append, summoning a turn of its own.
///
/// The seventh break, for the record: this resolution read only the tail,
/// so a released turn lost its identity whenever a message was absorbed
/// behind its outcome — the continuation anchored on the absorbed line, the
/// consumer's original escalation. The middle arm closes both proven
/// shapes, and it demotes the actor's held identity to a consistency cache
/// over a ledger-derivable fact: a fresh actor over the same ledger — the
/// restart shape — resolves the same turn the live actor was holding.
pub(crate) fn fresh_turn_anchor(snapshot: &[Block], tail: &Block) -> i64 {
    tail.dispatch_anchor
        .or_else(|| ToolCall::unanswered_outcome_anchor(snapshot))
        .unwrap_or(tail.id)
}

/// The conversation instantiation of [`drive_ledger`] — the turn scheduler's
/// drive.
///
/// `K` is the kind the runtime is instantiated over; every call site in this
/// library names [`BlockKind`](super::BlockKind).
///
/// # Errors
///
/// If listing the ledger, reading the cursor or persisting it fails.
pub async fn drive<K: RuntimeKind, E: RuntimeEvent>(
    ctx: &AgencyCtx<E>,
) -> Result<Outcome, StoreError> {
    drive_ledger::<K, E>(ctx, &ConversationLedger).await
}

/// Drive a ledger forward, inclusively, from its persisted cursor.
///
/// Each block's `run()` does its own work and reports doneness. On done the
/// cursor advances to that block id and persists; on not-done the drive parks
/// there.
///
/// The ordering is the crash contract and is not an implementation detail:
/// `run()`'s own effects commit BEFORE the cursor persists, so a crash between
/// the two leaves the cursor BEHIND a persisted block, and the inclusive
/// re-drive re-runs an idempotent, now-done block. The other order would leave
/// the cursor ahead of work that never happened, which no later drive can
/// detect. The cursor only ever takes ids of DURABLE blocks iterated from the
/// ledger.
///
/// The frontier question is answered afterwards, from a fresh read of the
/// tail: the hooks this drive just ran can append, and so can anything else
/// while they ran. Deciding it off the opening snapshot would let a turn fire
/// past a cap that had already been committed.
///
/// # Errors
///
/// If listing the ledger, reading the cursor or persisting it fails. A block's
/// own `run()` failing is NOT an error here — it parks the drive.
pub async fn drive_ledger<K: RuntimeKind, E: RuntimeEvent>(
    ctx: &AgencyCtx<E>,
    source: &impl LedgerSource,
) -> Result<Outcome, StoreError> {
    let ledger = source.list(ctx).await?;
    let mut cursor = source.cursor(ctx).await?;

    let start = resume_at(&ledger, cursor);

    let mut parked = false;
    for block in &ledger[start..] {
        let kind = K::from_block(block);
        match kind.run(ctx).await {
            Ok(true) => {
                // An ephemeral row is confirmed but never anchored: its
                // finalization deletes it, and a cursor left there would be
                // anchored to a row that no longer exists.
                if kind.durable() && cursor != block.id {
                    source.persist_cursor(ctx, block.id).await?;
                    cursor = block.id;
                }
            }
            Ok(false) => {
                parked = true;
                break;
            }
            Err(error) => {
                // Never advance past a failed block — a fresh tick re-drives.
                tracing::error!(
                    conversation_id = ctx.conversation_id,
                    block_id = block.id,
                    error = %error,
                    "ratchet: run() failed — parked for re-drive"
                );
                parked = true;
                break;
            }
        }
    }

    // The frontier, off the ledger as it stands NOW — not off the snapshot the
    // loop above walked. `parked` stays the drive's own answer: it is a fact
    // about what this drive reached, not about the current tail. The owed
    // turn itself is the shared frontier rule, so this site and the actor's
    // delivery-time re-check cannot drift. A transparent tail — a row that
    // stores a turn's end, a marker or an ends-turn-stamped resolution —
    // costs one extra full read here: the one-row tail cannot see behind
    // itself, and [`frontier_block`] needs the snapshot to decide what the
    // closure row buried. The common shapes keep the one-row read.
    let tail = source.tail(ctx).await?;
    let frontier = match tail {
        Some(tail) if K::from_block(&tail).frontier_transparent() => {
            let snapshot = source.list(ctx).await?;
            frontier_block::<K>(&snapshot).cloned()
        }
        tail => tail,
    };
    let awaiting = frontier
        .as_ref()
        .and_then(|block| K::from_block(block).awaiting());
    let owes_turn = !parked && frontier.as_ref().is_some_and(frontier_owes_turn::<K>);

    Ok(Outcome {
        cursor,
        owes_turn,
        parked,
        awaiting,
    })
}

#[cfg(test)]
mod resume_tests {
    use super::resume_at;
    use crate::block::Block;

    fn ledger(ids: &[i64]) -> Vec<Block> {
        ids.iter()
            .map(|id| Block {
                id: *id,
                role: None,
                block_type: "text".into(),
                created_at: String::new(),
                dispatch_anchor: None,
                fields: serde_json::Map::new(),
            })
            .collect()
    }

    /// The ordinary shape: ids ascending along ledger order, and the drive
    /// resumes AT the cursor's own block — inclusively, which is the crash
    /// contract.
    #[test]
    fn an_ascending_ledger_resumes_at_the_cursors_own_block() {
        let ledger = ledger(&[1, 2, 3, 4]);
        assert_eq!(resume_at(&ledger, 0), 0, "nothing confirmed re-derives");
        assert_eq!(resume_at(&ledger, 3), 2);
        assert_eq!(resume_at(&ledger, 4), 3);
    }

    /// A conversation opened with fresh blocks in front of inherited ones —
    /// the compacted thread's shape — resumes at the cursor's own POSITION,
    /// not at the first higher id it meets. Anchoring by id there lands on
    /// the fresh front block and re-derives the whole ledger every tick.
    #[test]
    fn a_ledger_whose_ids_descend_at_a_seam_still_resumes_at_the_cursor() {
        // Three fresh blocks, then the junction rows of an older
        // conversation.
        let ledger = ledger(&[100, 101, 102, 7, 8, 9]);
        assert_eq!(
            resume_at(&ledger, 9),
            5,
            "the cursor's own block is where the drive resumes"
        );
        assert_eq!(resume_at(&ledger, 8), 4);
    }

    /// A cursor whose block is gone — detached — falls back to the first
    /// block at or after it, and one past every block left re-derives from
    /// the start.
    #[test]
    fn a_vanished_cursor_lands_on_what_followed_it() {
        let ledger = ledger(&[1, 2, 5, 6]);
        assert_eq!(resume_at(&ledger, 3), 2, "the block that followed it");
        assert_eq!(resume_at(&ledger, 99), 0, "past everything re-derives");
    }

    /// The fallback's COST across the seam, pinned so the doc above cannot
    /// promise a bound the scan does not keep: a detached cursor that named
    /// an inherited block compares below the thread's own front appends, so
    /// the scan lands on the front and the whole ledger is re-walked. Never
    /// wrong — the re-drive is inclusive and idempotent — and never a skip,
    /// which is the property the fallback actually owes.
    #[test]
    fn a_vanished_inherited_cursor_re_derives_the_whole_compacted_thread() {
        // Three fresh front appends, then inherited rows; the cursor named
        // an inherited block that an erasure scrub has since detached.
        let ledger = ledger(&[100, 101, 102, 7, 9]);
        let at = resume_at(&ledger, 8);
        assert_eq!(
            at, 0,
            "the id scan cannot see the seam, so the re-drive starts at the front"
        );
        assert!(
            ledger[at..].iter().any(|block| block.id == 9),
            "the block that followed the vanished row is inside the re-driven span, \
             which is the property the fallback owes"
        );
    }
}

/// The frontier oracle: drive a real stored ledger through the ratchet and
/// observe the [`Outcome`], keeping the unit tests off the full reactive stack
/// while still going through the store rather than a hand-built vector.
///
/// Every drive asserts the cursor invariant: the persisted cursor only ever
/// takes ids of blocks present in the ledger and durable enough to stay there.
/// A cursor holding a vanished id anchors on whatever followed it, and one
/// past the end re-derives the whole history — so the invariant is checked on
/// every single drive rather than in a test of its own.
#[cfg(test)]
pub(crate) mod oracle {
    use std::sync::Arc;

    use tokio::sync::broadcast::error::TryRecvError;

    use crate::block::Role;
    use crate::bus::EventBus;
    use crate::event::CoreEvent;
    use crate::store::{Store, ToolCallInsert};

    use super::super::{AgencyCtx, BlockKind};
    use super::Outcome;

    pub(crate) struct Oracle {
        pub ctx: AgencyCtx<CoreEvent>,
        pub rx: tokio::sync::broadcast::Receiver<CoreEvent>,
    }

    impl Oracle {
        pub async fn new() -> Self {
            let store = Store::in_memory().unwrap();
            let conversation_id = store
                .create_conversation("p1".into(), "model".into(), "model".into(), String::new())
                .await
                .unwrap();
            let bus = Arc::new(EventBus::new());
            let rx = bus.subscribe();
            Self {
                ctx: AgencyCtx {
                    conversation_id,
                    store,
                    bus,
                },
                rx,
            }
        }

        /// A second oracle over the same store and bus, scoped to another
        /// conversation (the fork tests). Subscribes fresh, so it only observes
        /// emissions from this point on.
        pub fn scoped_to(&self, conversation_id: i64) -> Self {
            Self {
                ctx: AgencyCtx {
                    conversation_id,
                    store: self.ctx.store.clone(),
                    bus: Arc::clone(&self.ctx.bus),
                },
                rx: self.ctx.bus.subscribe(),
            }
        }

        pub async fn drive(&self) -> Outcome {
            let outcome = super::drive::<BlockKind, _>(&self.ctx).await.unwrap();
            let persisted = self
                .ctx
                .store
                .cursor(self.ctx.conversation_id)
                .await
                .unwrap();
            assert_eq!(
                outcome.cursor, persisted,
                "the outcome mirrors the persisted cursor"
            );
            if persisted != 0 {
                let ledger = self
                    .ctx
                    .store
                    .list_blocks(self.ctx.conversation_id)
                    .await
                    .unwrap();
                assert!(
                    ledger.iter().any(|block| block.id == persisted),
                    "the cursor only ever takes block ids present in the ledger"
                );
            }
            outcome
        }

        pub async fn cursor(&self) -> i64 {
            self.ctx
                .store
                .cursor(self.ctx.conversation_id)
                .await
                .unwrap()
        }

        pub async fn ledger_ids(&self) -> Vec<i64> {
            self.ctx
                .store
                .list_blocks(self.ctx.conversation_id)
                .await
                .unwrap()
                .iter()
                .map(|block| block.id)
                .collect()
        }

        pub async fn user_text(&self, content: &str) -> i64 {
            self.ctx
                .store
                .insert_text_block(self.ctx.conversation_id, Role::User, content.into())
                .await
                .unwrap()
        }

        pub async fn assistant_text(&self, content: &str) -> i64 {
            self.ctx
                .store
                .insert_text_block(self.ctx.conversation_id, Role::Assistant, content.into())
                .await
                .unwrap()
        }

        pub async fn call(&self, tool_call_id: &str) -> i64 {
            self.ctx
                .store
                .insert_tool_call_block(
                    self.ctx.conversation_id,
                    Role::Assistant,
                    ToolCallInsert {
                        tool_call_id: tool_call_id.into(),
                        name: "read_file".into(),
                        input: "{}".into(),
                        interactive: false,
                    },
                    None,
                )
                .await
                .unwrap()
        }

        /// Resolve the call recorded under this provider id with a result,
        /// through the one conditional door — keyed on the call's BLOCK id,
        /// which the helper reads back off the ledger (the LAST call under the
        /// id, the one still owed an outcome in every ledger these tests
        /// build).
        pub async fn result(&self, tool_call_id: &str) -> i64 {
            let call = self.call_block(tool_call_id).await;
            self.ctx
                .store
                .complete_tool_call_block(
                    self.ctx.conversation_id,
                    tool_call_id.into(),
                    "ok".into(),
                    call,
                )
                .await
                .unwrap()
                .expect("the call is unresolved")
        }

        /// The block id of the last `tool_call` recorded under this provider
        /// id — a result or error write is keyed on it, and a wakeup names it.
        pub async fn call_block(&self, tool_call_id: &str) -> i64 {
            self.ctx
                .store
                .list_blocks(self.ctx.conversation_id)
                .await
                .unwrap()
                .iter()
                .rev()
                .find(|block| {
                    block.block_type == "tool_call"
                        && block.fields.get("tool_call_id").and_then(|v| v.as_str())
                            == Some(tool_call_id)
                })
                .unwrap_or_else(|| panic!("no tool_call recorded under '{tool_call_id}'"))
                .id
        }

        pub async fn status(&self, status: &str) -> i64 {
            self.ctx
                .store
                .insert_status_block(self.ctx.conversation_id, status.into(), None)
                .await
                .unwrap()
        }

        #[track_caller]
        pub fn expect_wakeup(&mut self, call_block_id: i64) {
            match self.rx.try_recv().expect("expected a ToolCallReady wakeup") {
                CoreEvent::ToolCallReady {
                    conversation_id,
                    call_block_id: id,
                } => {
                    assert_eq!(conversation_id, self.ctx.conversation_id);
                    assert_eq!(id, call_block_id);
                }
                other => panic!("expected ToolCallReady, got {other:?}"),
            }
        }

        #[track_caller]
        pub fn expect_silence(&mut self) {
            assert!(
                matches!(self.rx.try_recv(), Err(TryRecvError::Empty)),
                "expected no bus emission"
            );
        }
    }
}
