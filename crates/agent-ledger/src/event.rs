//! The event vocabulary the runtime itself emits.
//!
//! [`CoreEvent`] holds exactly the events this library produces and consumes:
//! the streaming plane, the data plane, the intents that reach a conversation
//! actor, and conversation state. It holds nothing else, and a consumer never
//! edits it.
//!
//! A consumer that has events of its own composes them:
//!
//! ```
//! use agent_ledger::event::CoreEvent;
//!
//! #[derive(Clone)]
//! enum AppEvent {
//!     Core(CoreEvent),
//!     SearchProvidersChanged,
//! }
//!
//! impl From<CoreEvent> for AppEvent {
//!     fn from(event: CoreEvent) -> Self {
//!         Self::Core(event)
//!     }
//! }
//! ```
//!
//! and parameterises [`crate::bus::EventBus`] with its own type. There is no
//! second class of event and no boxing: the runtime emits `CoreEvent`, the bus
//! carries whatever the consumer composed.

use serde::Serialize;

use crate::types::{ApprovalChoice, Awaiting, InputBlock, StopReason, StreamUsage};

/// Every event the runtime broadcasts on the single ordered push bus.
///
/// What a dropped event costs is a property of the variant, not of this type.
/// Most variants are wakeups: they carry ids, and the receiver re-reads the
/// ledger and acts on what it finds there, so dropping or duplicating one is
/// harmless. The three intent variants that carry a payload —
/// [`BlocksAppended`](Self::BlocksAppended),
/// [`ToolCallReceived`](Self::ToolCallReceived) and
/// [`ApprovalSubmitted`](Self::ApprovalSubmitted) — carry the only copy of it:
/// the producer has stored it nowhere, so a dropped event loses the intent
/// outright. Each variant states its own contract below.
///
/// # What the transport actually guarantees
///
/// Carrying the only copy is a property of the variant. It is NOT a promise the
/// bus keeps, and the two must be read together:
///
/// - [`EventBus::attach`](crate::bus::EventBus::attach) — the push plane —
///   buffers a bounded tail and reports a cursor it can no longer serve, so a
///   subscriber there learns that it missed events.
/// - [`EventBus::subscribe`](crate::bus::EventBus::subscribe) — the in-process
///   broadcast plane — drops a lagging receiver's OLDEST unread events once its
///   backlog is full. The receiver sees `RecvError::Lagged`; nothing re-sends
///   them. A payload variant dropped this way is gone, and the intent it carried
///   with it.
///
/// So a receiver of the three payload variants must not treat this bus as
/// durable delivery: the layer that consumes them has to make the intent
/// recoverable — persist it before emitting, or re-drive it from stored state —
/// because the transport under it does not.
#[derive(Debug, Clone, Serialize, strum::IntoStaticStr)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CoreEvent {
    /// A conversation row was created.
    ConversationCreated {
        /// The conversation.
        conversation_id: i64,
    },
    /// A conversation's title changed.
    TitleUpdated {
        /// The conversation.
        conversation_id: i64,
        /// The new title.
        title: String,
    },

    // ─── Streaming plane (ephemeral, not persisted) ───────────────────
    /// Ephemeral status update for a consumer's interface.
    /// An empty label signals "content is flowing" (clear the label).
    StreamStatus {
        /// The conversation.
        conversation_id: i64,
        /// Short status label, empty to clear.
        label: String,
        /// Optional second line.
        subtitle: Option<String>,
    },
    /// A turn's stream finished.
    StreamDone {
        /// The conversation.
        conversation_id: i64,
        /// Token usage, when the provider reported it.
        usage: Option<StreamUsage>,
        /// Why the provider stopped, when it said.
        stop_reason: Option<StopReason>,
    },
    /// A turn's stream failed.
    StreamError {
        /// The conversation.
        conversation_id: i64,
        /// The failure, already rendered for display.
        error: String,
    },
    /// The provider stream has fully closed. Distinct from `StreamDone`
    /// (which fires per-turn) — this fires once when the stream exits.
    StreamClosed {
        /// The conversation.
        conversation_id: i64,
    },

    // ─── Data plane (stored state → consumer re-fetch) ───────────────────
    /// A block changed in the store. A wakeup: it carries ids only and the
    /// receiver re-fetches the block, so a dropped event is harmless.
    BlocksChanged {
        /// The conversation.
        conversation_id: i64,
        /// The block that changed.
        block_id: i64,
    },

    // ─── Intent events (consumer edge → reactor) ───────────────────────
    /// A stored draft became real input. A wakeup: the draft is already in
    /// the store, so the receiver re-reads it and a dropped event is harmless.
    DraftPromoted {
        /// The conversation.
        conversation_id: i64,
    },
    /// Composed input arrived and awaits appending.
    ///
    /// Not a wakeup: `blocks` is the only copy of the composed input. The
    /// producer has appended nothing, so the receiver must treat the payload
    /// as truth, and a dropped event loses the input outright.
    BlocksAppended {
        /// The conversation.
        conversation_id: i64,
        /// The composed blocks, in composer order.
        blocks: Vec<InputBlock>,
    },
    /// A tool call arrived out of band rather than through the stream.
    ///
    /// Not a wakeup: `name` and `input` are the only copy of the call. No
    /// ledger row holds them yet, so a dropped event loses the call outright.
    ToolCallReceived {
        /// The conversation.
        conversation_id: i64,
        /// The provider's id for this call.
        tool_call_id: String,
        /// The tool's registered name.
        name: String,
        /// The raw argument payload.
        input: String,
    },
    /// The human's verdict on an approval request block. The conversation
    /// actor validates the request and appends the decision block; the
    /// redispatch walk picks up an approved route on the next tick.
    ///
    /// Not a wakeup: `decision` and the two reasons are the only copy of the
    /// verdict — the decision block does not exist until the actor appends it,
    /// so a dropped event loses the verdict outright.
    ApprovalSubmitted {
        /// The conversation.
        conversation_id: i64,
        /// The request block the verdict answers.
        request_block_id: i64,
        /// The verdict.
        decision: ApprovalChoice,
        /// Machine-side reason, when the runtime decided.
        system_reason: Option<String>,
        /// Human-side reason, when the human wrote one.
        user_reason: Option<String>,
    },
    /// A tool call awaits execution. Carries IDs, never payloads-as-truth:
    /// the consumer re-reads the ledger and acts iff no result exists yet.
    /// Re-emitted on every re-drive of the unresolved call, so a dropped
    /// event is harmless.
    ToolCallReady {
        /// The conversation.
        conversation_id: i64,
        /// The block id of the call that awaits execution — the call's one
        /// identity: a model's `tool_call_id` can repeat, the block id cannot.
        call_block_id: i64,
    },
    /// A metadata request awaits fulfillment. Same wakeup contract as
    /// `ToolCallReady`: IDs only, never payloads-as-truth — the fulfillment
    /// subsystem re-reads the metadata ledger and acts iff the request is
    /// still unanswered. Re-emitted on every re-drive of the unsettled
    /// request, so a dropped event is harmless.
    MetadataRequestReady {
        /// The conversation.
        conversation_id: i64,
        /// The unsettled request.
        request_id: i64,
    },
    /// Someone asked for the current turn to stop.
    InterruptRequested {
        /// The conversation.
        conversation_id: i64,
    },
    /// Someone asked one conversation's latch to be released.
    UnlatchRequested {
        /// The conversation.
        conversation_id: i64,
    },
    /// Every latched conversation is released.
    UnlatchAll {},

    // ─── State plane (runtime state → consumer) ─────────────────────
    /// Conversation state changed. A consumer derives stop-button visibility
    /// and the rest of its interface state from these flags. `awaiting` is the
    /// frontier block's own ask — `out_of_band` is the signal to disable the
    /// composer, distinct from `user`.
    ConversationState {
        /// The conversation.
        conversation_id: i64,
        /// The conversation is latched: nothing drives it until released.
        latched: bool,
        /// The runtime owes work on this conversation.
        work_due: bool,
        /// Who owes the frontier block's next move, if anyone.
        awaiting: Option<Awaiting>,
    },

    /// The conversation table changed (insert/update/delete). The receiver
    /// re-fetches the conversation list.
    ConversationsChanged {},
}

/// The reverse of the `From<CoreEvent>` conversion the bus is built on: give
/// the runtime back its own event, when this one is one.
///
/// The bus carries the CONSUMER'S event type, and the runtime publishes into it
/// through `From<CoreEvent>`. That covers the outbound direction only. The
/// runtime also has in-process loops of its own — the reactor that routes
/// intents to a conversation actor, the executor that consumes tool wakeups,
/// the metadata fulfillment actor — and those subscribe to the same bus, so
/// what they receive is the consumer's type. This trait is how they recognise
/// the runtime's events inside it without the library ever matching on a
/// consumer's variants.
///
/// A consumer's composed enum implements it in one line per direction:
///
/// ```
/// use agent_ledger::event::{AsCoreEvent, CoreEvent};
///
/// #[derive(Clone)]
/// enum AppEvent {
///     Core(CoreEvent),
///     SearchProvidersChanged,
/// }
///
/// impl From<CoreEvent> for AppEvent {
///     fn from(event: CoreEvent) -> Self {
///         Self::Core(event)
///     }
/// }
///
/// impl AsCoreEvent for AppEvent {
///     fn as_core(&self) -> Option<&CoreEvent> {
///         match self {
///             Self::Core(event) => Some(event),
///             Self::SearchProvidersChanged => None,
///         }
///     }
/// }
/// ```
///
/// The pairing is deliberately NOT part of the bus's own bound: publishing
/// needs only `From<CoreEvent>`, and a consumer that never runs the session
/// actor should not owe an implementation to a seam it does not use. The two
/// traits describe one event type travelling in two directions — not two kinds
/// of event.
pub trait AsCoreEvent {
    /// The runtime's own view of this event, or `None` when it is a variant
    /// the consumer added and the runtime has no business reading.
    fn as_core(&self) -> Option<&CoreEvent>;
}

impl AsCoreEvent for CoreEvent {
    fn as_core(&self) -> Option<&CoreEvent> {
        Some(self)
    }
}

impl CoreEvent {
    /// Extract the `conversation_id` for routing. Returns `None` for global
    /// events.
    #[must_use]
    pub fn conversation_id(&self) -> Option<i64> {
        match self {
            Self::ConversationCreated {
                conversation_id, ..
            }
            | Self::TitleUpdated {
                conversation_id, ..
            }
            | Self::StreamStatus {
                conversation_id, ..
            }
            | Self::StreamDone {
                conversation_id, ..
            }
            | Self::StreamError {
                conversation_id, ..
            }
            | Self::StreamClosed {
                conversation_id, ..
            }
            | Self::BlocksChanged {
                conversation_id, ..
            }
            | Self::DraftPromoted {
                conversation_id, ..
            }
            | Self::BlocksAppended {
                conversation_id, ..
            }
            | Self::ToolCallReceived {
                conversation_id, ..
            }
            | Self::ApprovalSubmitted {
                conversation_id, ..
            }
            | Self::ToolCallReady {
                conversation_id, ..
            }
            | Self::MetadataRequestReady {
                conversation_id, ..
            }
            | Self::InterruptRequested {
                conversation_id, ..
            }
            | Self::UnlatchRequested {
                conversation_id, ..
            }
            | Self::ConversationState {
                conversation_id, ..
            } => Some(*conversation_id),

            Self::UnlatchAll { .. } | Self::ConversationsChanged { .. } => None,
        }
    }
}
