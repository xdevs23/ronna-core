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
//! [`actor`] is the runtime's top: a consumer bundles the store, the bus, the
//! provider registry and the tool registry into a [`RuntimeContext`] and calls
//! [`spawn_reactor`] once. The context is generic over the block kind the
//! runtime is instantiated over — [`BlockKind`] for the library's own kinds, a
//! composing enum for a consumer's — and over the consumer's event type, and
//! naming the kind at the context is what instantiates every layer below it.
//! From then on every intent travels the bus — an append, an interrupt, an
//! approval verdict — and per conversation one scheduler drives the ratchet,
//! one actor owns the provider channel and the latch, the ingestion reader
//! turns the provider's stream into blocks, and the metadata worker drives the
//! second ledger behind the same latch.
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use agent_ledger::{
//!     BlockKind, CoreEvent, EventBus, ProviderRegistry, RuntimeContext, Store, ToolRegistry,
//!     spawn_reactor,
//! };
//!
//! // The two type choices a consumer makes, named once, at the context:
//! // the kind the runtime is instantiated over (here the library's own
//! // `BlockKind`) and the event type the bus carries (here `CoreEvent`
//! // itself, the runtime's own vocabulary).
//! let ctx: RuntimeContext<BlockKind, CoreEvent> = RuntimeContext::new(
//!     Store::in_memory().expect("an in-memory store"),
//!     Arc::new(EventBus::new()),
//!     Arc::new(ProviderRegistry::new()),
//!     Arc::new(ToolRegistry::new()),
//! );
//! spawn_reactor(ctx);
//! ```

pub mod actor;
pub mod agency;
pub mod block;
pub mod bus;
pub mod event;
mod ingestion;
mod metadata;
pub mod providers;
pub mod reactivity;
pub mod store;
pub mod tools;
pub mod types;

pub use actor::{RuntimeContext, spawn_reactor};
pub use agency::{
    Agency, AgencyCtx, BlockKind, ContentPart, FromBlock, GateDecision, Projection, RuntimeKind,
};
pub use block::{
    Block, OpaquePayload, RESERVED_FIELD_NAMES, ReasoningDetailEntry, Role, ToolCallResult,
};
pub use bus::{AttachOutcome, EventBus, PushEnvelope, PushSink, RuntimeEvent};
pub use event::{AsCoreEvent, CoreEvent};
pub use providers::{
    LlmError, ProviderModule, ProviderRegistry, ProviderRequest, ProviderResponse, StreamEvent,
    blocks_to_messages,
};
pub use store::{
    Column, ColumnRef, ColumnType, ContentDescriptor, DomainMigrations, Store, StoreConfig,
    StoreError,
};
pub use tools::{ToolContext, ToolHandler, ToolOutcome, ToolRegistry, ToolRunner, submit_approval};
pub use types::{ApprovalChoice, Awaiting, InputBlock, StopReason, StreamUsage, denial_error_text};
