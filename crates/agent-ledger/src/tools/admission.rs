//! Admission: where a gated call stands with the human, and how a human's
//! answer is recorded.
//!
//! Two halves of one chain, kept in one file because they are two ends of the
//! same decision:
//!
//! - `approval_state` is the READ — a fold over the ledger the runner consults
//!   on every wakeup. Crate-internal, because the runner is its one caller and
//!   a second reader of a consent fact is how two answers start.
//! - [`submit_approval`] is the WRITE — the one path a human's verdict takes
//!   into the ledger.
//!
//! Neither consults anything but recorded blocks. That is the whole discipline:
//! consent is durable ledger state, and a question answered from live data
//! somewhere else would be a second answer.

use crate::agency::{ApprovalRequest, denial_error_text};
use crate::block::Block;
use crate::store::{Store, StoreError};
use crate::types::ApprovalChoice;

/// Where a gated call stands with the human, read off the ledger and nowhere
/// else.
///
/// The four states are exhaustive on purpose. The failure this replaced
/// inferred consent from the *absence* of a request; naming "no request yet" as
/// its own state is what makes that inference impossible to write here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApprovalState {
    /// No approval request covers the call yet — the tool's own gate speaks.
    Unrequested,
    /// A request exists and nobody has answered it — the body stays parked.
    Undecided,
    /// A human approved: the body may run, exactly once, and the call's own
    /// result is what stops it running again.
    Approved,
    /// A human denied. The body never runs, and nothing here has to record the
    /// refusal: the denial resolved the call with its tool error in the same
    /// transaction that recorded the verdict.
    Denied,
}

/// Fold the ledger for the call's standing with the human.
///
/// Id-keyed on the call's own row through the same predicates the approval
/// blocks route on, so the runner's read and the redispatch walk's routing
/// cannot drift apart. Two readers of one fact, one implementation of it.
pub(crate) fn approval_state(ledger: &[Block], call_block_id: i64) -> ApprovalState {
    let Some(request) = ApprovalRequest::covering(ledger, call_block_id) else {
        return ApprovalState::Unrequested;
    };
    match request.decision_in(ledger) {
        None => ApprovalState::Undecided,
        Some(decision) if decision.approved => ApprovalState::Approved,
        Some(_) => ApprovalState::Denied,
    }
}

/// Record a human's verdict on one approval request.
///
/// Append-if-undecided: the write is conditional on no decision already
/// answering this request, so the loser of a two-client race gets a clean error
/// instead of a second, contradicting verdict. Validity is the decision's own
/// question — an approval is legitimate exactly while its request is undecided.
///
/// On a denial the originating call is resolved with its tool error in the SAME
/// transaction. That atomicity is not tidiness: a crash between the two writes
/// would leave a call denied but unresolved, parked forever behind a verdict
/// nobody can act on. The error text is built in one place so the model always
/// learns WHO denied — a system reason means the world changed under the
/// request, a user reason means a person typed it — and never has to infer that
/// from a flag.
///
/// A grant of standing permission may accompany an approval but never a denial;
/// that pairing belongs to the consumer's own endpoint, which calls this.
///
/// # Errors
///
/// If the request does not belong to this conversation, if it is already
/// decided, or if the write fails.
pub async fn submit_approval(
    store: &Store,
    conversation_id: i64,
    request_block_id: i64,
    decision: ApprovalChoice,
    system_reason: Option<String>,
    user_reason: Option<String>,
) -> Result<i64, StoreError> {
    let denial_error = matches!(decision, ApprovalChoice::Denied)
        .then(|| denial_error_text(system_reason.as_deref(), user_reason.as_deref()));
    store
        .insert_approval_decision_block(
            conversation_id,
            request_block_id,
            decision,
            system_reason,
            user_reason,
            denial_error,
        )
        .await
}
