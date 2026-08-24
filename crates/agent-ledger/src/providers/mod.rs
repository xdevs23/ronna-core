//! The model boundary: one trait, one neutral vocabulary, one streaming
//! contract, and a module per vendor.
//!
//! # The isolation rule
//!
//! **Nothing outside a vendor module builds a wire shape.** The layers below
//! speak [`ContentPart`] and [`StreamEvent`]; a vendor module translates those into
//! its own JSON on the way out and back on the way in, and that translation
//! exists in exactly one file per vendor.
//!
//! The rule is not tidiness. Its absence is a specific, silent failure: shared
//! code that emitted one vendor's request shape produced, for the vendor whose
//! shape differed, a request that parsed fine and simply contained no tool
//! calls. The model answered as if no tools existed. Nothing errored, nothing
//! logged, and the only symptom was a model that had apparently stopped using
//! its tools. The goldens in each vendor module pin that vendor's exact bytes
//! for the same neutral input, which is what makes such a drift a failing test
//! instead of a support ticket.
//!
//! # What is always here, and what a feature adds
//!
//! The trait, the neutral vocabulary, the stream contract, the render pass, the
//! bind loop and the registry are unconditional. A vendor feature adds a wire —
//! never a capability the rest of the layer depends on — and **no vendor is on
//! by default**, because a library that picks a model provider for its consumer
//! has made the one decision the consumer most wanted to make itself.
//!
//! # What a consumer supplies
//!
//! This trait names no capability beyond reaching a model. A vendor that can
//! also, say, search the web exposes that through a hook its consumer wires up,
//! not through a method on this trait: a trait that named one such capability
//! would owe a method to every other one, and the boundary would grow a surface
//! per product feature rather than per model concern.
//!
//! # The two source tests this layer does not carry
//!
//! The moved tests are the only check that the move changed nothing, so a test
//! that does not come across is named here with its reason rather than left to
//! be inferred from a count. Of the 118 tests the source's model layer offers
//! this slice, 116 are here; these two are the whole of the difference.
//!
//! - **`kimi::reasoning_part_folds_identically_to_text`.** Its subject is that
//!   vendor's *second* request builder in the source — the one that folded a
//!   reasoning part into the assistant content, with the test pinning that fold
//!   as a no-op on the wire. This layer keeps one request path per vendor, and
//!   for that vendor it consumes the neutral messages like every other and
//!   carries reasoning in the vendor's own sibling field, never folded into
//!   the content — folding it in is what that endpoint rejects. So the
//!   assertion has no subject left to make: porting it would mean restoring
//!   the content fold, which is the shape this vendor's encoder exists to
//!   avoid. Where that vendor's reasoning must land is pinned by its own tests
//!   instead.
//! - **`config::tool_definitions_complete`.** Its subject is the source
//!   application's own catalogue of tool definitions, and the descriptions it
//!   asserts on name stay-behind subsystems this library must never name.
//!   Porting it would put that vocabulary under `src/` and fail the vocabulary
//!   scan. The tools belong to a consumer, and so does the test.

use std::future::Future;
use std::pin::Pin;

use serde_json::Value;

use crate::store::StoreError;

pub mod bind;
// The empty-content drop every wire applies. It is compiled with the wires
// rather than always: with no vendor enabled there is no wire to apply it, and
// the decision is not a capability the rest of the layer depends on.
#[cfg(any(
    feature = "anthropic",
    feature = "kimi",
    feature = "openai",
    feature = "_chat"
))]
pub(crate) mod empty;
pub mod http;
pub mod http_store;
pub mod registry;
pub mod render;
pub mod types;

#[cfg(feature = "anthropic")]
pub mod anthropic;
#[cfg(feature = "_chat_stream")]
pub(crate) mod chat;
#[cfg(feature = "kimi")]
pub mod kimi;
#[cfg(feature = "mistral")]
pub mod mistral;
#[cfg(feature = "openai")]
pub mod openai;
#[cfg(feature = "openrouter")]
pub mod openrouter;

pub use bind::{OpenedTurn, run_http_bind_loop, run_http_bind_loop_with_replay};
pub use registry::ProviderRegistry;
pub use render::{blocks_to_messages, render_blocks_to_text, render_conversation};
pub use types::{
    CompletionRequest, ContentPart, EventStream, FinalContentBlock, LlmError, Message,
    MessageContent, MessageRole, ModelInfo, ModelSelector, ProviderRequest, ProviderResponse,
    ProviderRx, ProviderTx, ReasoningCapability, ReasoningLevel, StreamEvent, ToolDefinition,
    Usage,
};

/// A boxed, sendable future — the shape every trait method returns.
///
/// The trait is object-safe on purpose: a registry holds one boxed
/// implementation per provider type, including implementations a consumer
/// wrote, and object safety is what makes "register your own" possible at all.
/// Boxing is the price, and it is paid once per call rather than once per
/// stream event.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Mask a secret for display, for example `sk-ant-a…wxyz`.
///
/// Char-counted, never byte-sliced. A pasted secret is arbitrary user input, and
/// a byte index landing inside a multi-byte character panics — which is a crash
/// in the settings screen, triggered by a value nobody wants printed in the
/// backtrace.
#[must_use]
pub fn mask_secret(s: &str) -> String {
    let char_count = s.chars().count();
    if char_count <= 12 {
        "\u{2022}".repeat(char_count.min(8))
    } else {
        let head: String = s.chars().take(8).collect();
        let tail: String = s.chars().skip(char_count - 4).collect();
        format!("{head}…{tail}")
    }
}

/// A self-contained provider module: its own config table, its own config
/// shape, and its own conversation with a model.
///
/// One implementation per provider *type*, registered once. A provider
/// *instance* — a particular account, with a particular credential — is named
/// by the `provider_id` every method takes, so one implementation serves every
/// instance of its type.
///
/// Every method that touches storage goes through the module's own store
/// handle, never a borrowed connection: the store has one writer, and a
/// provider that held a connection across a network call would be holding it
/// for as long as a model takes to answer.
pub trait ProviderModule: Send + Sync {
    // ── Identity ──────────────────────────────────────────────────────────

    /// The stable string this provider type is registered and stored under.
    ///
    /// It outlives wire surfaces: a provider that moves to a different API
    /// keeps its type id, because every persisted instance names it and a
    /// rename would orphan them all.
    fn type_id(&self) -> &'static str;

    /// The name a human reads.
    fn display_name(&self) -> &'static str;

    /// One line on what this provider offers.
    fn description(&self) -> &'static str;

    // ── One instance's configuration ──────────────────────────────────────

    /// Read one instance's configuration.
    fn get_config(&self, provider_id: String) -> BoxFuture<'_, Result<Option<Value>, StoreError>>;

    /// Write one instance's configuration, inserting or updating.
    fn save_config(
        &self,
        provider_id: String,
        config: Value,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Forget one instance's configuration.
    fn delete_config(&self, provider_id: String) -> BoxFuture<'_, Result<(), StoreError>>;

    /// A short subtitle for one instance — a masked credential, an endpoint.
    fn summary(&self, provider_id: String) -> BoxFuture<'_, Result<Option<String>, StoreError>>;

    // ── Talking to the model ──────────────────────────────────────────────

    /// Bind a channel for streaming.
    ///
    /// The caller sends [`ProviderRequest`]s and reads [`ProviderResponse`]s.
    /// The provider spawns whatever it needs internally, and dropping the
    /// sender tears that down.
    ///
    /// The channel is scoped to one conversation. `conversation_id` matters to
    /// providers that keep per-conversation state and is ignored by those that
    /// do not; `provider_id` is what a provider persists a refreshed credential
    /// under.
    fn bind(
        &self,
        conversation_id: i64,
        provider_id: String,
        config: Value,
    ) -> (ProviderTx, ProviderRx);

    /// List the models one instance can reach.
    ///
    /// Each entry carries its reasoning capability, read during this same
    /// listing pass rather than fetched again — a second round trip per model
    /// is how a model picker becomes slow enough to feel broken.
    fn list_models(&self, config: Value) -> BoxFuture<'_, Result<Vec<ModelInfo>, LlmError>>;

    // ── Optional lifecycle ────────────────────────────────────────────────

    /// Report what is available on this machine for this provider type.
    ///
    /// Runs before any instance exists, so it reads no configuration.
    fn probe(&self) -> BoxFuture<'_, Result<Value, LlmError>> {
        Box::pin(async { Ok(serde_json::json!({})) })
    }

    /// Prepare an instance before its first use, reporting progress, and return
    /// the configuration to persist.
    ///
    /// A separate phase because preparation can be slow and can fail for
    /// reasons that have nothing to do with a chat request; folding it into the
    /// first request turns a setup problem into a cryptic failure mid-sentence.
    fn preflight(
        &self,
        config: Value,
        progress: Box<dyn Fn(String) + Send + Sync>,
    ) -> BoxFuture<'_, Result<Value, LlmError>> {
        let _ = progress;
        Box::pin(async move { Ok(config) })
    }

    /// Prepare an existing instance once at startup, and return the
    /// configuration to persist. A provider that needs no preparation inherits
    /// the identity.
    fn startup_init(&self, config: Value) -> BoxFuture<'_, Result<Value, LlmError>> {
        Box::pin(async move { Ok(config) })
    }

    // ── Optional interactive authorization ────────────────────────────────

    /// Begin a device-style authorization flow, returning what the human needs
    /// in order to approve it.
    fn auth_start(&self, config: Value) -> BoxFuture<'_, Result<Value, LlmError>> {
        let _ = config;
        Box::pin(async { Err(LlmError::Config("auth not supported".into())) })
    }

    /// Wait for that authorization to be granted, returning the configuration
    /// with the resulting credentials.
    fn auth_poll(&self, config: Value, poll_data: Value) -> BoxFuture<'_, Result<Value, LlmError>> {
        let _ = (config, poll_data);
        Box::pin(async { Err(LlmError::Config("auth not supported".into())) })
    }
}

#[cfg(test)]
mod mask_secret_tests {
    use super::mask_secret;

    /// A secret containing multi-byte characters must mask by character count,
    /// not by byte index. Slicing the first eight BYTES of a secret made of
    /// two-byte characters lands mid-character and panics.
    #[test]
    fn multibyte_secret_masks_without_panicking() {
        assert_eq!(mask_secret("ßßßßßßßßßßßßß"), "ßßßßßßßß…ßßßß");
        assert_eq!(mask_secret("ßß"), "\u{2022}\u{2022}");
    }

    #[test]
    fn ascii_secret_keeps_the_head_and_tail_shape() {
        assert_eq!(mask_secret("sk-ant-abcdefwxyz"), "sk-ant-a…wxyz");
        assert_eq!(
            mask_secret("short"),
            "\u{2022}\u{2022}\u{2022}\u{2022}\u{2022}"
        );
    }
}

#[cfg(test)]
mod isolation_tests;
