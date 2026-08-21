//! The pure-record block kinds: no ask, trivially-done `run()`, and at most a
//! grouping-role fact on the projection side. Each stays its own type with its
//! own implementations, so the compiler forces a decision the moment one gains
//! real behavior — at which point it graduates to a module of its own.
//!
//! The role-carrying records group under their stored role while contributing
//! no content in either mode: a record sitting between two same-role blocks
//! must not split their message. The per-kind ladder this trait replaced fell
//! back to the raw role for exactly that reason, and the property is pinned
//! here rather than re-derived.

use crate::block::{Block, Role};

use super::{Agency, LeafKind, Projection};

/// Caps the frontier by HAVING no ask — no turn fires past an interrupt.
#[derive(Debug, Clone)]
pub struct Status {
    /// The role the row carries, under which the record groups.
    pub role: Option<Role>,
}

impl LeafKind for Status {
    const KINDS: &'static [&'static str] = &["status"];

    fn parse(block: &Block) -> Self {
        Self { role: block.role }
    }
}

impl Agency for Status {}

impl Projection for Status {
    fn group_role(&self) -> Option<Role> {
        self.role
    }
}

/// Ephemeral tail while text streams in. Boundary-invisible on the wire: a
/// boundary taken from an unfinalized tail is the empty-message hazard.
///
/// Not durable: the finalization that replaces this tail DELETES the row, so
/// the cursor must never come to rest on it.
#[derive(Debug, Clone, Copy)]
pub struct Streaming;

impl LeafKind for Streaming {
    const KINDS: &'static [&'static str] = &["streaming"];

    fn parse(_: &Block) -> Self {
        Self
    }
}

impl Agency for Streaming {
    fn durable(&self) -> bool {
        false
    }
}

impl Projection for Streaming {}

/// Ephemeral tail while reasoning streams in. Deleted by its finalization, so
/// the cursor never rests on it.
#[derive(Debug, Clone, Copy)]
pub struct StreamingThinking;

impl LeafKind for StreamingThinking {
    const KINDS: &'static [&'static str] = &["streaming_thinking"];

    fn parse(_: &Block) -> Self {
        Self
    }
}

impl Agency for StreamingThinking {
    fn durable(&self) -> bool {
        false
    }
}

impl Projection for StreamingThinking {}

/// Ephemeral tail while a call's input streams in. Boundary-invisible: one of
/// these exists BEFORE its committed `tool_call` counterpart lands, and a
/// boundary here would leak an empty assistant message mid-stream.
///
/// Not durable: the committed call deletes it, so the cursor never rests on
/// it.
#[derive(Debug, Clone, Copy)]
pub struct StreamingToolCall;

impl LeafKind for StreamingToolCall {
    const KINDS: &'static [&'static str] = &["streaming_tool_call"];

    fn parse(_: &Block) -> Self {
        Self
    }
}

impl Agency for StreamingToolCall {
    fn durable(&self) -> bool {
        false
    }
}

impl Projection for StreamingToolCall {}

/// A block type this build does not know.
///
/// Fully inert agency — never asks, never parks, never emits — so an old build
/// reading a newer ledger fails safe instead of misinterpreting it. Its
/// projection is content-invisible; the parse site already warned. It still
/// groups under its stored role, so an unknown record between two same-role
/// blocks does not split their message.
///
/// A consumer kind that was never composed into the runtime's kind set lands
/// here too, and the conformance kit's parse check is what catches that
/// mistake. A composed kind — a variant of a consumer's derived enum — resolves
/// through its own implementation, so only a genuinely unrecognised type
/// reaches this fallback.
#[derive(Debug, Clone)]
pub struct Unknown {
    /// The role the row carries, under which the record groups.
    pub role: Option<Role>,
}

impl Unknown {
    pub(super) fn parse(block: &Block) -> Self {
        Self { role: block.role }
    }
}

impl Agency for Unknown {}

impl Projection for Unknown {
    fn group_role(&self) -> Option<Role> {
        self.role
    }
}
