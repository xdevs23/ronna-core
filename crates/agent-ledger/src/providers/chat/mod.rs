//! The chat-completions surface, in two pieces.
//!
//! **[`sse`] is the stream decoder**: the reasoning boundary, the deferred end
//! of turn, the tool-call buffering — everything reading a chat-completions
//! event stream involves. Every vendor whose RESPONSES take that shape reads
//! it, including one whose requests do not compose with the base beside it. Two
//! decoders for one wire shape is how a fix made for one vendor stops reaching
//! the other, which is exactly the drift that lost events on the second one.
//!
//! **[`base`] is the request half**: the request shape, the model listing and
//! the vendor seams, for the vendors whose requests do compose.
//!
//! The two arrive on separate features for that reason. `_chat_stream` carries
//! the decoder, which is all a vendor with its own request builder needs;
//! `_chat` carries the base and implies the decoder.

pub(crate) mod sse;

/// The chat-completions envelope for a JSON Schema answer: the schema itself
/// under a constant name, and `strict`, which is what makes the endpoint hold
/// the model to the schema instead of merely suggesting it. The name is
/// required by the dialect and means nothing to a caller, so it is fixed.
///
/// It sits beside the decoder rather than the base for the decoder's own
/// reason: a vendor with a request builder of its own still speaks this
/// dialect's envelope, and two spellings of one envelope is how a fix made
/// for one vendor stops reaching the other.
pub(crate) fn json_schema_response_format(schema: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "type": "json_schema",
        "json_schema": { "name": "response", "strict": true, "schema": schema },
    })
}

#[cfg(feature = "_chat")]
mod base;
#[cfg(feature = "_chat")]
mod wire;

// What a vendor on this base reaches for is the base's contract, and which of
// it a given build actually uses depends on which vendors are compiled: one
// fills every seam, another fills none. An unused name here is a statement
// about the feature selection rather than about this module.
#[cfg(feature = "_chat")]
#[allow(unused_imports)]
pub(crate) use base::{
    AssistantTextPart, ChatProvider, fold_assistant_content, provider_from_config,
};
#[cfg(feature = "_chat")]
#[allow(unused_imports)]
pub(crate) use sse::{SseState, decode_string_content};
#[cfg(feature = "_chat")]
#[allow(unused_imports)]
pub(crate) use wire::{WireMessageContent, WireModel};

#[cfg(all(test, feature = "_chat"))]
mod tests;
