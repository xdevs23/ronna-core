//! The redispatch walk — the deferred-work side of the ratchet family.
//!
//! Each unlatched tick it anchors on the LATEST block: if that block routes
//! (its `post_gate_id` is not `None`), the walk follows the chain to its
//! terminus and unwinds, calling `run_post_gate` on each block, terminus first.
//!
//! It runs ungated by the model-turn axis — deferred work resumes even when no
//! turn is owed — and it never names a block type: routing AND idempotency live
//! entirely in the kinds' `post_gate_id` returning `None` once nothing is owed,
//! which is what makes re-driving on every tick safe. The scheduler is the
//! single driver per conversation, so there are no concurrent walks; the same
//! discipline the cursor drive relies on.
//!
//! That single driver is the actor slice's guarantee and it is not in this
//! tree: nothing here serializes two callers walking the same conversation at
//! once, so the caller owes that discipline until the actor lands.

use crate::bus::RuntimeEvent;
use crate::store::StoreError;

use super::{AgencyCtx, RuntimeKind};

/// Walk this conversation's deferred-work chain and unwind it, terminus first.
///
/// `K` is the kind the runtime is instantiated over; every call site in this
/// library names [`BlockKind`](super::BlockKind).
///
/// # Errors
///
/// If listing the ledger fails, or a block's deferred body fails.
pub async fn walk<K: RuntimeKind, E: RuntimeEvent>(ctx: &AgencyCtx<E>) -> Result<(), StoreError> {
    let ledger = ctx.store.list_blocks(ctx.conversation_id).await?;
    let Some(anchor) = ledger.last() else {
        return Ok(());
    };

    let mut chain = vec![K::from_block(anchor)];
    let mut visited = vec![anchor.id];
    // The chain starts non-empty and only grows, so the head always exists; the
    // loop ends on a kind that routes nowhere, on a cycle, or on a dangling id.
    while let Some(head) = chain.last() {
        let Some(next_id) = head.post_gate_id(&ledger) else {
            break;
        };
        // A routing cycle is malformed recorded data — stop, do not spin.
        if visited.contains(&next_id) {
            tracing::warn!(
                conversation_id = ctx.conversation_id,
                block_id = next_id,
                "redispatch: routing cycle — walk stopped"
            );
            break;
        }
        let Some(next) = ledger.iter().find(|block| block.id == next_id) else {
            break;
        };
        visited.push(next_id);
        chain.push(K::from_block(next));
    }

    if chain.len() == 1 {
        return Ok(());
    }
    for kind in chain.iter().rev() {
        kind.run_post_gate(ctx).await?;
    }
    Ok(())
}
