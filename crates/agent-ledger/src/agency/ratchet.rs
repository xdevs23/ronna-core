//! The ratchet — the ONE place a processed cursor moves.
//!
//! It drives every block's [`Agency`] hooks blindly, forward from the persisted
//! cursor, and reports whether the frontier owes a model turn. It never names a
//! block type and never branches on a domain concept; per-kind behavior lives
//! on the kinds.
//!
//! One machinery, two ledgers: the drive runs over whatever [`LedgerSource`]
//! the caller instantiates it with and never learns which. That is what makes
//! "the second ledger" a second instantiation rather than a second scheduler.

use crate::block::Block;
use crate::bus::RuntimeEvent;
use crate::store::StoreError;
use crate::types::Awaiting;

use super::{Agency, AgencyCtx, BlockKind};

/// The drive's one seam onto persistence: list the ledger, read its cursor,
/// persist its cursor.
///
/// Each instantiation hands in its own triple, and the ratchet stays
/// ledger-blind. A closed set with static dispatch, so the hooks stay native
/// `async fn` with nothing boxed.
#[allow(async_fn_in_trait)]
pub trait LedgerSource {
    /// Every block in this ledger, in ledger order.
    ///
    /// # Errors
    ///
    /// If the underlying read fails.
    async fn list<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<Vec<Block>, StoreError>;

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
    async fn tail<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<Option<Block>, StoreError> {
        Ok(self.list(ctx).await?.pop())
    }

    /// This ledger's persisted cursor; 0 when nothing is confirmed.
    ///
    /// # Errors
    ///
    /// If the underlying read fails.
    async fn cursor<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<i64, StoreError>;

    /// Advance this ledger's persisted cursor to a confirmed block id.
    ///
    /// # Errors
    ///
    /// If the underlying write fails.
    async fn persist_cursor<E: RuntimeEvent>(
        &self,
        ctx: &AgencyCtx<E>,
        confirmed_id: i64,
    ) -> Result<(), StoreError>;
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
    /// drive confirmed is not a candidate: see [`Agency::durable`].
    pub cursor: i64,
    /// The frontier gate: the drive confirmed its whole SNAPSHOT without
    /// parking, AND the tail — read fresh after the drive — awaits the model.
    /// Never a backward scan. The fresh tail can include blocks appended after
    /// the snapshot that the drive never ran; every kind that answers a model
    /// ask has an inert `run()`, so nothing is skipped by firing on one, and
    /// the next drive confirms them.
    pub owes_turn: bool,
    /// The drive stopped before confirming the tail — a block reported
    /// not-done, or its `run()` failed. A fresh tick re-drives it.
    pub parked: bool,
    /// The tail block's own ask, straight off [`Agency::awaiting`], read from
    /// the same fresh tail `owes_turn` is decided on. Published on
    /// conversation state so a consumer can tell an out-of-band ask from a
    /// chat reply. Orthogonal to `owes_turn`, which additionally requires the
    /// drive to have finished unparked.
    pub awaiting: Option<Awaiting>,
}

/// The conversation instantiation of [`drive_ledger`] — the turn scheduler's
/// drive.
///
/// # Errors
///
/// If listing the ledger, reading the cursor or persisting it fails.
pub async fn drive<E: RuntimeEvent>(ctx: &AgencyCtx<E>) -> Result<Outcome, StoreError> {
    drive_ledger(ctx, &ConversationLedger).await
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
pub async fn drive_ledger<E: RuntimeEvent>(
    ctx: &AgencyCtx<E>,
    source: &impl LedgerSource,
) -> Result<Outcome, StoreError> {
    let ledger = source.list(ctx).await?;
    let mut cursor = source.cursor(ctx).await?;

    // Anchor the persisted cursor to a POSITION in the ledger order: the first
    // block at or after it. Ids ascend along ledger order, so an anchor whose
    // own block is gone lands on the block that followed it, and the re-drive
    // costs the tail after the vanished row rather than the whole history.
    // Only a cursor past every block left has nowhere to land; that re-derives
    // from the start, which the inclusive re-drive makes always safe.
    let start = ledger
        .iter()
        .position(|block| block.id >= cursor)
        .unwrap_or(0);

    let mut parked = false;
    for block in &ledger[start..] {
        let kind = BlockKind::from_block(block);
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
    // about what this drive reached, not about the current tail.
    let awaiting = source
        .tail(ctx)
        .await?
        .and_then(|tail| BlockKind::from_block(&tail).awaiting());
    let owes_turn = !parked && awaiting == Some(Awaiting::Model);

    Ok(Outcome {
        cursor,
        owes_turn,
        parked,
        awaiting,
    })
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

    use super::super::AgencyCtx;
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
            let outcome = super::drive(&self.ctx).await.unwrap();
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

        pub async fn result(&self, tool_call_id: &str) -> i64 {
            self.ctx
                .store
                .insert_tool_result_block(
                    self.ctx.conversation_id,
                    tool_call_id.into(),
                    "ok".into(),
                )
                .await
                .unwrap()
        }

        pub async fn status(&self, status: &str) -> i64 {
            self.ctx
                .store
                .insert_status_block(self.ctx.conversation_id, status.into(), None)
                .await
                .unwrap()
        }

        #[track_caller]
        pub fn expect_wakeup(&mut self, tool_call_id: &str) {
            match self.rx.try_recv().expect("expected a ToolCallReady wakeup") {
                CoreEvent::ToolCallReady {
                    conversation_id,
                    tool_call_id: id,
                } => {
                    assert_eq!(conversation_id, self.ctx.conversation_id);
                    assert_eq!(id, tool_call_id);
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
