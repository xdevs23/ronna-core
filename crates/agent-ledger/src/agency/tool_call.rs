//! A model's request for tool work.

use serde_json::Value;

use crate::block::{Block, Role};
use crate::bus::RuntimeEvent;
use crate::event::CoreEvent;
use crate::store::StoreError;
use crate::types::Awaiting;

use super::projection::{ContentPart, Projection};
use super::{Agency, AgencyCtx, BlockKind};

/// A model's request for tool work.
///
/// A regular call owes the SYSTEM its execution: `run()` emits the wakeup and
/// stays not-done until a result or an error with this call's id follows in the
/// ledger. Parking the cursor here IS the guard against streaming a fresh turn
/// while calls still dangle — without it a later sibling's result green-lights
/// a turn and the earlier call is never answered.
///
/// An interactive call owes the USER the reply; the system does nothing.
#[derive(Debug, Clone)]
pub struct ToolCall {
    /// The ledger row this call is.
    pub id: i64,
    /// Whose voice the block speaks in.
    pub role: Option<Role>,
    /// The provider's id for this call.
    pub tool_call_id: String,
    /// The tool's registered name.
    pub name: String,
    /// The raw input string as stored. Parsed to JSON only at projection time,
    /// so malformed input degrades instead of panicking.
    pub input: String,
    /// Stamped on the block at insert — a fact about the tool at execution
    /// time, surfaced by the block loader. Absent means false.
    pub interactive: bool,
}

impl ToolCall {
    pub(super) fn parse(block: &Block) -> Self {
        Self {
            id: block.id,
            role: block.role,
            tool_call_id: super::string_field(block, "tool_call_id"),
            name: super::string_field(block, "name"),
            input: super::string_field(block, "input"),
            interactive: block
                .fields
                .get("interactive")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }
    }

    /// Does a result or an error for this call follow it in the ledger?
    ///
    /// Keyed on the specific call id, never on kind alone — an unrelated result
    /// landing later must never settle this call. This is THE resolution
    /// predicate: the approval kinds' routing and the runner's ledger
    /// idempotency all answer through it, so they cannot drift apart.
    ///
    /// The kinds it accepts are read through [`BlockKind`], not off the stored
    /// type string. Comparing the string here would be a second place that
    /// decides what a tool outcome is, and the two would answer differently the
    /// first time either changed.
    ///
    /// **An absent id matches nothing.** A payload without `tool_call_id`
    /// parses to the empty string, and two such blocks would cross-match on
    /// it — an unrelated failure would silently resolve this call. The store
    /// constrains its own writes `NOT NULL`, but this predicate runs over
    /// parsed JSON rather than over that constraint, so it answers false the
    /// moment there is no id to key on.
    #[must_use]
    pub fn resolved_in(&self, ledger: &[Block]) -> bool {
        if self.tool_call_id.is_empty() {
            return false;
        }
        ledger
            .iter()
            .skip_while(|block| block.id != self.id)
            .skip(1)
            .any(|block| match BlockKind::from_block(block) {
                BlockKind::ToolResult(result) => result.tool_call_id == self.tool_call_id,
                BlockKind::ToolError(error) => error.tool_call_id == self.tool_call_id,
                _ => false,
            })
    }
}

impl Agency for ToolCall {
    fn awaiting(&self) -> Option<Awaiting> {
        Some(if self.interactive {
            Awaiting::User
        } else {
            Awaiting::System
        })
    }

    async fn run<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<bool, StoreError> {
        if self.interactive {
            return Ok(true);
        }
        let ledger = ctx.store.list_blocks(ctx.conversation_id).await?;
        if self.resolved_in(&ledger) {
            return Ok(true);
        }
        ctx.bus.emit(CoreEvent::ToolCallReady {
            conversation_id: ctx.conversation_id,
            call_block_id: self.id,
        });
        Ok(false)
    }

    /// The deferred body IS the block's own `run()` — the ONE execution path:
    /// the walk's unwind re-emits the same wakeup the cursor emits, so the
    /// runner chokepoint stays the only place a tool body executes. A second
    /// execution path here is a second place a call can run twice.
    async fn run_post_gate<E: RuntimeEvent>(&self, ctx: &AgencyCtx<E>) -> Result<(), StoreError> {
        self.run(ctx).await.map(|_| ())
    }
}

impl Projection for ToolCall {
    fn group_role(&self) -> Option<Role> {
        self.role
    }

    /// A native part only — the model's own request is never echoed back as
    /// text. Non-JSON input rides as a JSON string, verbatim: degrade, never
    /// panic.
    fn llm_parts(&self) -> Option<Vec<ContentPart>> {
        let input =
            serde_json::from_str(&self.input).unwrap_or_else(|_| Value::String(self.input.clone()));
        Some(vec![ContentPart::ToolUse {
            id: self.tool_call_id.clone(),
            name: self.name.clone(),
            input,
        }])
    }

    fn forces_parts(&self) -> bool {
        true
    }
}
