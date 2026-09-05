//! The human's verdict on an approval request.

use serde_json::Value;

use crate::block::Block;
use crate::types::{ApprovalChoice, Awaiting};

use super::projection::Projection;
use super::tool_call::unresolved_call_named;
use super::{Agency, BlockKind, FromBlock};

/// The human's verdict on an approval request.
///
/// Approved routes the walk to its request, which routes on to the call, while
/// the underlying call still owes its body. Denied routes nowhere, ever — the
/// submit path ([`submit_approval`](crate::tools::submit_approval)) resolved the
/// call with its tool error in the same step, so there is nothing left to run.
#[derive(Debug, Clone)]
pub struct ApprovalDecision {
    /// The ledger row this decision is.
    pub id: i64,
    /// The request this decision answers, read through the crate's `id_field`,
    /// which answers `None` for a decision that names none.
    pub for_block_id: Option<i64>,
    /// Whether the verdict was approval.
    pub approved: bool,
}

impl super::LeafKind for ApprovalDecision {
    const KINDS: &'static [&'static str] = &["approval_decision"];

    fn parse(block: &Block) -> Self {
        Self {
            id: block.id,
            for_block_id: super::id_field(block, "for_block_id"),
            approved: block.fields.get("decision").and_then(Value::as_str)
                == Some(ApprovalChoice::Approved.as_str()),
        }
    }
}

impl Agency for ApprovalDecision {
    fn awaiting(&self) -> Option<Awaiting> {
        Some(Awaiting::OutOfBand)
    }

    fn post_gate_id(&self, ledger: &[Block]) -> Option<i64> {
        if !self.approved {
            return None;
        }
        // A decision naming no request routes nowhere, and neither does a
        // request naming no call: the id read answers `None` for both.
        let answered = self.for_block_id?;
        let request_block = ledger.iter().find(|block| block.id == answered)?;
        let BlockKind::ApprovalRequest(request) = BlockKind::from_block(request_block) else {
            return None;
        };
        // The route runs to the request, which routes on to the call, so it
        // stays open on the same terms the request's own routing reads: once
        // the underlying call's outcome exists, nothing is owed.
        unresolved_call_named(ledger, request.for_block_id).map(|_| answered)
    }
}

/// System-only: the model must never learn permission was decided — no
/// content, no message boundary. Every hook stays at the invisible default.
impl Projection for ApprovalDecision {}
