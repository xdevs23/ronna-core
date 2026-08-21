//! The human's verdict on an approval request.

use serde_json::Value;

use crate::block::Block;
use crate::types::{ApprovalChoice, Awaiting};

use super::projection::Projection;
use super::{Agency, BlockKind};

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
    /// The request this decision answers.
    pub for_block_id: i64,
    /// Whether the verdict was approval.
    pub approved: bool,
}

impl ApprovalDecision {
    pub(super) fn parse(block: &Block) -> Self {
        Self {
            id: block.id,
            for_block_id: super::i64_field(block, "for_block_id"),
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
        // Parsed ids are matched against ROW ids only, and no row is numbered
        // 0, so a decision naming no request routes nowhere instead of
        // pairing with another id-less block.
        if self.for_block_id <= 0 {
            return None;
        }
        let request_block = ledger.iter().find(|block| block.id == self.for_block_id)?;
        let BlockKind::ApprovalRequest(request) = BlockKind::from_block(request_block) else {
            return None;
        };
        // The same id-keyed, position-aware discipline as the request's own
        // routing: once the underlying call's result exists, nothing is owed.
        let call_block = ledger
            .iter()
            .find(|block| block.id == request.for_block_id)?;
        match BlockKind::from_block(call_block) {
            BlockKind::ToolCall(call) if !call.resolved_in(ledger) => Some(self.for_block_id),
            _ => None,
        }
    }
}

/// System-only: the model must never learn permission was decided — no
/// content, no message boundary. Every hook stays at the invisible default.
impl Projection for ApprovalDecision {}

/// The tool error a denial records on the originating call.
///
/// Built so the model learns WHO denied and why: a system reason means the
/// runtime auto-rejected, a user reason means the human typed it. Who denied is
/// structural — it is the presence of one field or the other, never a flag on
/// the verdict — and this one function is where that structure becomes the
/// sentence the model reads. Every caller composing the text itself would be a
/// second vocabulary for the same fact.
#[must_use]
pub fn denial_error_text(system_reason: Option<&str>, user_reason: Option<&str>) -> String {
    match (system_reason, user_reason) {
        (Some(system), Some(user)) => {
            format!("The system denied this action automatically: {system} The user added: {user}")
        }
        (Some(system), None) => format!("The system denied this action automatically: {system}"),
        (None, Some(user)) => format!("The user denied this action. Reason: {user}"),
        (None, None) => "The user denied this action.".to_string(),
    }
}
