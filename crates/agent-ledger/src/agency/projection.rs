//! Block-owned projection into the neutral layer.
//!
//! WHAT a block says to the model is the block kind's own knowledge; the
//! central grouping pass owns only structure — contiguous-role grouping,
//! message boundaries, and the parts-versus-text group policy. This trait is
//! the projection's whole surface: four facts a kind states about itself,
//! consulted blindly by that pass.
//!
//! Shape choice — a SIBLING of [`Agency`](super::Agency), not more hooks on it:
//! agency answers the orchestration axis (*who owes my next move?*), projection
//! answers the representation axis (*what do I say to the model?*), and the two
//! evolve independently — the approval blocks are orchestration-loud and
//! model-invisible. Every hook defaults to invisible, so a pure-record kind
//! states nothing.
//!
//! The neutral [`ContentPart`] layer is the boundary language, and neutral to
//! wire belongs to the provider that speaks that wire. Two representation
//! layers per block, and this is the first of them.

use crate::block::OpaquePayload;

/// One piece of a message as the neutral layer holds it, before any provider
/// has translated it into its own wire form.
///
/// It lives beside the trait that produces it: a kind states its parts, the
/// grouping pass assembles them, and the provider layer that renders them into
/// vendor JSON arrives on top. Nothing here knows a vendor.
#[derive(Debug, Clone, PartialEq)]
pub enum ContentPart {
    /// Prose, in whatever markdown vocabulary the kind chose.
    Text {
        /// The text itself.
        text: String,
    },
    /// A first-class reasoning part — never stringified into
    /// [`Text`](Self::Text). Reasoning that arrives as text and leaves as text
    /// loses the provider continuity payload riding with it, and a replayed
    /// turn then fails vendor-side signature checks.
    Reasoning {
        /// The human-visible reasoning or summary.
        text: String,
        /// The provider-native continuity payload, absent for vendors without
        /// one.
        opaque: Option<OpaquePayload>,
    },
    /// The model's request for tool work.
    ToolUse {
        /// The provider's id for this call.
        id: String,
        /// The tool's registered name.
        name: String,
        /// The call's arguments, parsed where they parse.
        input: serde_json::Value,
    },
    /// A tool's outcome, answering a [`ToolUse`](Self::ToolUse) by id.
    ToolResult {
        /// The call this answers.
        tool_use_id: String,
        /// What the tool said — its output, or the reason it failed.
        content: String,
    },
    /// A user-authored image the model should see — never stringified into
    /// [`Text`](Self::Text): the model must read the real media, not an
    /// invented description of it.
    ///
    /// The bytes ride raw here; encoding them is the wire's business, and each
    /// provider that carries an image encodes it in its own shape. This is the
    /// only media variant on purpose: inline audio was verified to be silently
    /// dropped by the gateway path this layer serves, and a variant no wire can
    /// carry would be a silent no-op dressed as a feature (decision of
    /// 2026-08-24, `docs/slices/09-user-authored-media-wire.md`).
    Image {
        /// The image's MIME type, `image/png` and kin.
        mime: String,
        /// The raw image bytes.
        data: Vec<u8>,
    },
}

/// The four facts a block kind states about how it reaches the model.
///
/// Implemented by every kind. Every hook defaults to invisible, so a kind that
/// says nothing to the model writes an empty implementation and the grouping
/// pass steps over it.
pub trait Projection {
    /// The role under which this block groups into a message, or `None` —
    /// boundary-invisible: the grouping pass steps over it without opening a
    /// message.
    ///
    /// An unfinalized tail or a system-only block must never leak an empty
    /// message, which is what a boundary from a contentless block produces.
    /// Invisible is not transparent: a boundary-invisible block still ends the
    /// contiguous run before it — that consequence belongs to the grouping
    /// pass, not to this hook.
    fn group_role(&self) -> Option<crate::block::Role> {
        None
    }

    /// The neutral parts this block contributes when its group renders as
    /// native parts. `None` means invisible in parts mode.
    fn llm_parts(&self) -> Option<Vec<ContentPart>> {
        None
    }

    /// The markdown this block contributes when its group renders as joined
    /// text — also a transcript export's vocabulary. `None` means invisible in
    /// text mode.
    ///
    /// A kind may speak in one mode only: reasoning contributes a
    /// [`ContentPart::Reasoning`] and no text, so text-only groups drop
    /// reasoning. That asymmetry is pinned deliberately, not accidental.
    fn llm_text(&self) -> Option<String> {
        None
    }

    /// Whether this block's presence switches its whole group to native parts —
    /// true for the tool blocks, whose parts have no faithful text form.
    fn forces_parts(&self) -> bool {
        false
    }
}

// ─── The shared markdown vocabulary the kinds speak ──────────────────────
//
// One form per shape, in one place. A kind that formatted its own text inline
// would be a second place the form is decided, and the two drift the first time
// one of them is adjusted — which is exactly how a transcript export and a
// model prompt end up disagreeing about what the same block said.

/// Prose as the model reads it.
#[must_use]
pub fn render_text(content: &str) -> String {
    content.to_string()
}

/// A quoted span, `> `-prefixed per line. An empty quote renders empty rather
/// than as a bare marker.
#[must_use]
pub fn render_quote(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }
    text.lines()
        .map(|line| format!("> {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// A code snippet, fenced and language-tagged where a language is known.
#[must_use]
pub fn render_code(language: Option<&str>, content: &str) -> String {
    let lang = language.unwrap_or("");
    format!("```{lang}\n{content}\n```")
}

/// A tool's output as the model reads it.
#[must_use]
pub fn render_tool_result(content: &str) -> String {
    content.to_string()
}

/// A tool's failure as the model reads it.
#[must_use]
pub fn render_tool_error(error: &str) -> String {
    error.to_string()
}
