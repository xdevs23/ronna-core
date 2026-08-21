//! The system's ask for a human's clearance on a tool call.

use crate::block::Block;
use crate::types::Awaiting;

use super::projection::Projection;
use super::{Agency, ApprovalDecision, BlockKind, FromBlock};

/// The system's ask for a human's clearance on a tool call.
///
/// Awaits an out-of-band action ([`Awaiting::OutOfBand`] — parks like a user
/// ask, and a consumer's interface additionally disables its composer), does no
/// work of its own, and routes the redispatch walk to the covered call only
/// while an approved decision covers this request AND the call still owes its
/// body.
///
/// Stored with role user for a mechanical reason: the fork's group walk reads
/// the RAW role, so role user is what keeps approval blocks inside the
/// surrounding user turn's group boundary. Any other role splits the group and
/// a fork inherits half an approval chain.
#[derive(Debug, Clone)]
pub struct ApprovalRequest {
    /// The ledger row this request is.
    pub id: i64,
    /// The call this request covers.
    pub for_block_id: i64,
}

impl super::LeafKind for ApprovalRequest {
    const KINDS: &'static [&'static str] = &["approval_request"];

    fn parse(block: &Block) -> Self {
        Self {
            id: block.id,
            for_block_id: super::i64_field(block, "for_block_id"),
        }
    }
}

impl ApprovalRequest {
    /// The request covering `call_block_id`, if one has landed — the reverse
    /// lookup, symmetric with [`decision_in`](Self::decision_in).
    ///
    /// Id-keyed on the covered call and read off the LOCAL ledger only, so the
    /// runner's approval read and the routing predicates can never drift.
    ///
    /// An absent `for_block_id` parses to 0, which is no row's id, so a
    /// request that names no call covers nothing rather than covering every
    /// other id-less block.
    #[must_use]
    pub fn covering(ledger: &[Block], call_block_id: i64) -> Option<Self> {
        if call_block_id <= 0 {
            return None;
        }
        ledger
            .iter()
            .find_map(|block| match BlockKind::from_block(block) {
                BlockKind::ApprovalRequest(request) if request.for_block_id == call_block_id => {
                    Some(request)
                }
                _ => None,
            })
    }

    /// The human's decision on THIS request, if one has landed.
    ///
    /// Id-keyed on the request — the decision-reading counterpart to
    /// [`ToolCall::resolved_in`](super::ToolCall::resolved_in), and THE
    /// decision-reading discipline: this request's own routing and the runner's
    /// approval read share it so the two cannot drift.
    ///
    /// Reads only the ledger it is handed, which is conversation-local, so a
    /// junction-shared request is decided per conversation and never globally.
    ///
    /// Keyed on this request's own ROW id, which no parse can invent: a
    /// decision whose payload names no request parses to 0 and answers no
    /// request at all. The guard says so rather than leaving it to the fact
    /// that no row is numbered 0.
    #[must_use]
    pub fn decision_in(&self, ledger: &[Block]) -> Option<ApprovalDecision> {
        if self.id <= 0 {
            return None;
        }
        ledger
            .iter()
            .find_map(|block| match BlockKind::from_block(block) {
                BlockKind::ApprovalDecision(decision) if decision.for_block_id == self.id => {
                    Some(decision)
                }
                _ => None,
            })
    }
}

impl Agency for ApprovalRequest {
    fn awaiting(&self) -> Option<Awaiting> {
        Some(Awaiting::OutOfBand)
    }

    fn post_gate_id(&self, ledger: &[Block]) -> Option<i64> {
        if !self
            .decision_in(ledger)
            .is_some_and(|decision| decision.approved)
        {
            return None;
        }
        // Parsed ids are matched against ROW ids only, and no row is numbered
        // 0, so a request naming no call routes nowhere.
        if self.for_block_id <= 0 {
            return None;
        }
        // Id-keyed and position-aware: the route stays open only while the
        // covered call itself is unresolved. An unrelated result landing later
        // can never close it, and the call's own result closes it for good —
        // that IS the walk's idempotency guard, and without it the walk
        // re-executes an already-answered call on every tick.
        let call_block = ledger.iter().find(|block| block.id == self.for_block_id)?;
        match BlockKind::from_block(call_block) {
            BlockKind::ToolCall(call) if !call.resolved_in(ledger) => Some(self.for_block_id),
            _ => None,
        }
    }
}

/// System-only: the model must never learn permission was asked — no content,
/// no message boundary. Every hook stays at the invisible default.
impl Projection for ApprovalRequest {}
