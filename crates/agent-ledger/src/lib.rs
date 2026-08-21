//! An append-only block ledger and orchestration runtime for LLM agents.
//!
//! A session is an ordered list of **blocks**. The machinery never branches on
//! which kind a block is and never names a domain concept: behavior lives on
//! the kind, and everything derivable — conversation state, tool admission,
//! spend — is folded from the ledger rather than stored beside it.
//!
//! The foundation layer is the value types every later layer names, the event
//! vocabulary the runtime emits, the fan-out bus that carries it, and the
//! reactive primitives the scheduler ticks on. On top of it sits the
//! [`store`]: one connection behind one writer, holding the append-only block
//! ledger, the conversations that index it, the cursor that walks it, and the
//! change hook that wakes the scheduler when any of it moves.
//!
//! On top of both sits [`agency`], the layer that makes a block an actor: one
//! trait every kind implements, one module per kind, and the ratchet that
//! drives those hooks forward from the persisted cursor without ever learning
//! which kind it just ran.
//!
//! [`providers`] is the model boundary on top of that: one trait per provider
//! type, one neutral vocabulary every vendor is translated into and out of, and
//! one streaming contract that carries the retry, watchdog and reconnect
//! behavior so no vendor module writes its own. **No vendor ships enabled** —
//! each is a feature, because choosing a model provider is the consumer's
//! decision, not this library's.
//!
//! [`tools`] closes the loop the model opens: a registry of handlers a consumer
//! writes, and the runner that is the single place a tool body executes and
//! therefore the single place admission is enforced. It advances a call from
//! recorded facts only — a decision that travelled in memory beside a durable
//! record is the failure that shaped this layer. The library ships no tool of
//! its own; a tool knows what a product does.
//!
//! ```
//! use agent_ledger::{bus::EventBus, event::CoreEvent};
//!
//! // The runtime emits its own events; the bus carries whatever type the
//! // consumer composed them into. Here that type is `CoreEvent` itself.
//! let bus: EventBus<CoreEvent> = EventBus::new();
//! let seq = bus.emit(CoreEvent::ConversationsChanged {});
//! assert_eq!(seq, 1);
//! ```

pub mod agency;
pub mod block;
pub mod bus;
pub mod event;
pub mod providers;
pub mod reactivity;
pub mod store;
pub mod tools;
pub mod types;

pub use agency::{Agency, AgencyCtx, BlockKind, ContentPart, GateDecision, Projection};
pub use block::{
    Block, OpaquePayload, RESERVED_FIELD_NAMES, ReasoningDetailEntry, Role, ToolCallResult,
};
pub use bus::{AttachOutcome, EventBus, PushEnvelope, PushSink, RuntimeEvent};
pub use event::CoreEvent;
pub use providers::{
    LlmError, ProviderModule, ProviderRegistry, ProviderRequest, ProviderResponse, StreamEvent,
    blocks_to_messages,
};
pub use store::{Store, StoreError};
pub use tools::{ToolContext, ToolHandler, ToolOutcome, ToolRegistry, ToolRunner, submit_approval};
pub use types::{ApprovalChoice, Awaiting, InputBlock, StopReason, StreamUsage, denial_error_text};
