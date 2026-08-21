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
//! ```
//! use agent_ledger::{bus::EventBus, event::CoreEvent};
//!
//! // The runtime emits its own events; the bus carries whatever type the
//! // consumer composed them into. Here that type is `CoreEvent` itself.
//! let bus: EventBus<CoreEvent> = EventBus::new();
//! let seq = bus.emit(CoreEvent::ConversationsChanged {});
//! assert_eq!(seq, 1);
//! ```

pub mod block;
pub mod bus;
pub mod event;
pub mod reactivity;
pub mod store;
pub mod types;

pub use block::{
    Block, OpaquePayload, RESERVED_FIELD_NAMES, ReasoningDetailEntry, Role, ToolCallResult,
};
pub use bus::{AttachOutcome, EventBus, PushEnvelope, PushSink};
pub use event::CoreEvent;
pub use store::{Store, StoreError};
pub use types::{ApprovalChoice, Awaiting, InputBlock, StopReason, StreamUsage};
