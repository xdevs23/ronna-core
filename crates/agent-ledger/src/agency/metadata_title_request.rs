//! The metadata ledger's ask for a derived title.

use std::collections::VecDeque;

use crate::block::Block;
use crate::bus::RuntimeEvent;
use crate::event::CoreEvent;
use crate::store::StoreError;
use crate::types::Awaiting;

use super::projection::Projection;
use super::{Agency, AgencyCtx, BlockKind, FromBlock};

/// The metadata ledger's ask for a derived title.
///
/// The SYSTEM owes the derivation: `run()` emits the fulfillment wakeup and
/// stays not-done until a response follows it in the metadata ledger. That
/// parks the metadata cursor here, and the per-tick re-emit IS the retry loop —
/// there is no separate scheduler for retries, because a second retry mechanism
/// is a second thing that can disagree about whether the work is still owed.
/// The ticking itself belongs to the actor slice and is not in this tree: here
/// a retry happens exactly when a caller drives the ledger again.
///
/// The block only asks; the subsystem that fulfills it owns the provider.
#[derive(Debug, Clone)]
pub struct MetadataTitleRequest {
    /// The metadata row this request is.
    pub id: i64,
}

impl super::LeafKind for MetadataTitleRequest {
    const KINDS: &'static [&'static str] = &["title_request"];

    fn parse(block: &Block) -> Self {
        Self { id: block.id }
    }
}

impl MetadataTitleRequest {
    /// Has a response settled THIS request?
    ///
    /// The one settling predicate — the cursor's doneness and the fulfillment
    /// subsystem's delivery idempotency both answer through it.
    ///
    /// **The pairing is positional, because the schema carries no reference**:
    /// a response row records a derived title and nothing that names the
    /// request it answers. So the ledger is walked in order, requests queue up
    /// as they appear, and each response settles the earliest one still
    /// outstanding — first asked, first settled. Keying on kind alone instead
    /// would let one response settle every outstanding request at once, and
    /// every request but one would be dropped with its work never done.
    ///
    /// What counts as a request or a response is read through [`BlockKind`],
    /// not off the stored type string: the parse site is the single place a
    /// type string becomes a kind, and a comparison here would be a second one.
    #[must_use]
    pub fn settled_in(&self, ledger: &[Block]) -> bool {
        let mut outstanding: VecDeque<i64> = VecDeque::new();
        for block in ledger {
            match BlockKind::from_block(block) {
                BlockKind::MetadataTitleRequest(request) => outstanding.push_back(request.id),
                BlockKind::MetadataTitleResponse(_) => {
                    // The pop is the pairing: this response takes the earliest
                    // outstanding request, whether or not it is the one asking.
                    let settled_request = outstanding.pop_front();
                    if settled_request == Some(self.id) {
                        return true;
                    }
                }
                _ => {}
            }
        }
        false
    }
}

impl Agency for MetadataTitleRequest {
    fn awaiting(&self) -> Option<Awaiting> {
        Some(Awaiting::System)
    }

    async fn run<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<bool, StoreError> {
        let ledger = ctx.store.list_metadata_blocks(ctx.conversation_id).await?;
        if self.settled_in(&ledger) {
            return Ok(true);
        }
        ctx.bus.emit(CoreEvent::MetadataRequestReady {
            conversation_id: ctx.conversation_id,
            request_id: self.id,
        });
        Ok(false)
    }
}

/// Lives in the metadata ledger, which never enters the neutral pass —
/// invisible by inertness, with nothing special-cased to keep it out.
impl Projection for MetadataTitleRequest {}
