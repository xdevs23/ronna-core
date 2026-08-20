//! An append-only block ledger and orchestration runtime for LLM agents.
//!
//! A session is an ordered list of **blocks**. The machinery never branches on
//! which kind a block is and never names a domain concept: behavior lives on
//! the kind, and everything derivable — conversation state, tool admission,
//! spend — is folded from the ledger rather than stored beside it.
//!
//! This is the foundation layer: the value types every later layer names, the
//! event vocabulary the runtime emits, the fan-out bus that carries it, and the
//! reactive primitives the scheduler ticks on. Nothing here reaches a store, a
//! provider or a tool.
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
pub mod types;

pub use block::{Block, RESERVED_FIELD_NAMES, Role, ToolCallResult};
pub use bus::{AttachOutcome, EventBus, PushEnvelope, PushSink};
pub use event::CoreEvent;
pub use types::{ApprovalChoice, Awaiting, InputBlock, StopReason, StreamUsage};
