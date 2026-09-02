//! The block row as it comes out of the ledger, the role it carries, and the
//! content shapes stored beside it.
//!
//! A block is the only content unit: there is no message row anywhere in this
//! architecture. Everything a conversation is made of is a block, and the
//! machinery never branches on which kind it is — behavior lives on the kind
//! itself, in a later layer.

use serde::{Deserialize, Serialize};
use serde_json::Value;

// --- Role ---

/// Whose voice a block speaks in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// The human.
    User,
    /// The model.
    Assistant,
    /// The harness speaking to the model.
    System,
    /// A tool's output.
    Tool,
}

impl Role {
    /// The stored form of this role.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::System => "system",
            Self::Tool => "tool",
        }
    }
}

// --- Blocks ---

/// A block is the primary conversational unit. Each block carries its own
/// role and belongs to one or more conversations via the junction table.
/// The `block_type` field is the stored kind string (`text`, `streaming`,
/// `quote`, `code`, `tool_call`, `tool_result`, `thinking`, `status`).
/// The `fields` map carries the type-specific payload — serialized flat
/// alongside `id`, `role`, `block_type` and `created_at`.
///
/// The row header wins over the payload: a payload key named like one of the
/// header's own ([`RESERVED_FIELD_NAMES`]) is dropped from the serialized form
/// rather than written after it. The kind's payload is data the row carries, not
/// a place from which the row's identity can be rewritten.
#[derive(Debug, Clone)]
pub struct Block {
    /// Ledger row id, monotonic within a store.
    pub id: i64,
    /// The block's role, absent for blocks that speak in no voice.
    pub role: Option<Role>,
    /// The stored kind discriminator.
    pub block_type: String,
    /// Creation timestamp, in the store's text form.
    pub created_at: String,
    /// The dispatch identity (2026-08-22): the id of the block whose owed turn
    /// dispatched the stream this block is a product of — for every round of
    /// one tool conversation, the ORIGINAL summoning frontier, inherited
    /// across continuation rounds. `None` is the documented answer for
    /// everything that is not a turn's product: a message, a consumer append,
    /// an out-of-band tool call. Recorded at insert by the framework's own
    /// write paths; the public write surface never sets it.
    ///
    /// The id is the BLOCK ledger's id space, always: it resolves through
    /// [`Store::find_block`](crate::store::Store::find_block) and nothing
    /// else. A row surfaced from the metadata ledger
    /// ([`Store::list_metadata_blocks`](crate::store::Store::list_metadata_blocks))
    /// carries `None` here even though its anchoring is stored — its anchor
    /// names a metadata row, and one field cannot speak two id spaces without
    /// handing `find_block` a confidently wrong answer.
    ///
    /// The anchor names the turn's DISPATCH identity, never the newest author
    /// in the request: a message absorbed while a turn is open is answered by
    /// the turn the close dispatches, and that turn's products anchor on the
    /// PREVIOUS summoning frontier even though the dispatched request carries
    /// the absorbed text.
    pub dispatch_anchor: Option<i64>,
    /// The kind-specific payload, flattened into the serialized form.
    pub fields: serde_json::Map<String, Value>,
}

/// The keys the row header owns. A payload may not use them: the flattened form
/// is one map, and a second entry under one of these names is what a reader
/// takes as the row's identity — a text block would come back a different kind
/// with a different id, and nothing in the pipeline would report it.
///
/// The set is fixed rather than derived from the header actually written, so a
/// block whose role is absent is not a hole a payload can fill either — the
/// dispatch anchor included (2026-08-22): a payload cannot forge a turn
/// identity the header never recorded.
pub const RESERVED_FIELD_NAMES: [&str; 5] = ["id", "role", "type", "created_at", "dispatch_anchor"];

impl Serialize for Block {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("id", &self.id)?;
        if let Some(role) = &self.role {
            map.serialize_entry("role", role)?;
        }
        map.serialize_entry("type", &self.block_type)?;
        map.serialize_entry("created_at", &self.created_at)?;
        if let Some(anchor) = self.dispatch_anchor {
            map.serialize_entry("dispatch_anchor", &anchor)?;
        }
        for (k, v) in &self.fields {
            if RESERVED_FIELD_NAMES.contains(&k.as_str()) {
                tracing::warn!(
                    block_id = self.id,
                    field = k.as_str(),
                    "block payload carries a reserved field name; the row header wins and the payload entry is dropped"
                );
                continue;
            }
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

// Rendering a slice of blocks to text is deliberately absent here. Any such
// function has to decide which kinds contribute text, and this layer must not
// know: behavior lives on the kind. The hook belongs to the layer that owns
// block kinds, where each kind answers for its own text form.

// --- Reasoning continuity ---

/// The provider-native continuity payload for one reasoning block. Opaque to
/// the machinery: captured from the stream, replayed faithfully, never
/// interpreted. Shaped per vendor because the vendors' payloads are
/// structurally different — a flat `{format, data}` scalar cannot hold
/// `OpenRouter`'s multi-entry array verbatim.
///
/// It lives beside the block row because it is stored beside it: the store
/// writes it in the same transaction that finalizes the thinking block. The
/// provider layer that produces and replays it arrives in a later slice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpaquePayload {
    /// `encrypted_content` plus the server-assigned reasoning-item id
    /// (`rs_…`), both required for verbatim item replay.
    OpenAiResponses {
        /// The reasoning item's server-assigned id.
        item_id: String,
        /// The encrypted reasoning content, replayed as received.
        encrypted_content: String,
    },
    /// The thinking block's signature, echoed back as a native thinking block.
    Anthropic {
        /// The signature the provider issued for the block.
        signature: String,
    },
    /// The full `reasoning_details` entries, decomposed and order-preserving so
    /// the array can be rebuilt and format-gated per entry. Documented entry
    /// fields only — an undocumented extra field would be dropped.
    OpenRouter {
        /// The entries, in array order.
        entries: Vec<ReasoningDetailEntry>,
    },
    /// No extra payload: the thinking chunk is rebuilt from the block's own
    /// stored text, and the tag alone gates replay.
    Mistral,
}

/// One `reasoning_details` entry, relational — every datum its own field, so no
/// JSON blob is stored where columns will do.
///
/// `entry_type` is one of `reasoning.text`, `reasoning.summary` or
/// `reasoning.encrypted`; `content` holds that variant's text, summary or data;
/// `upstream_format` is the per-entry discriminator (for example
/// `google-gemini-v1`) that drives the replay filter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReasoningDetailEntry {
    /// Array order, preserved for verbatim rebuild.
    pub position: u32,
    /// Which kind of entry this is.
    pub entry_type: String,
    /// The provider's own id for the entry, when it issued one.
    pub entry_id: Option<String>,
    /// The per-entry format discriminator the replay filter reads.
    pub upstream_format: String,
    /// The entry's index within its upstream array, when it carried one.
    pub index: Option<u32>,
    /// The entry's payload — text, summary or encrypted data.
    pub content: String,
    /// A `reasoning.text` entry may carry one.
    pub signature: Option<String>,
}

/// The outcome of a tool call: what the ledger stores, what a read of a
/// resolved call answers, and what a backing system supplies when it settles a
/// deferred call through
/// [`Store::resolve_tool_call`](crate::store::Store::resolve_tool_call). One
/// vocabulary for one fact — which way the work came out, and the text the
/// model reads. The facts a resolver may NOT choose (the turn-ending stamp and
/// whether a failure was a refusal) are not here: the writes decide those.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ToolCallResult {
    /// The call ran and produced output.
    Success {
        /// What the tool returned.
        content: String,
    },
    /// The call failed.
    Error {
        /// Why it failed.
        error: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A payload key named like a header key must not reach the serialized form:
    /// flattening writes one map, so a second `id` or `type` is what a reader
    /// ends up believing. Without the guard a text block round-tripped as
    /// another kind, carrying another row's id, silently.
    #[test]
    fn a_payload_never_overrides_the_row_header() {
        let mut fields = serde_json::Map::new();
        fields.insert("id".into(), Value::from(9_999));
        fields.insert("type".into(), Value::from("tool_call"));
        fields.insert("role".into(), Value::from("assistant"));
        fields.insert("created_at".into(), Value::from("1999-12-31T23:59:59Z"));
        fields.insert("dispatch_anchor".into(), Value::from(7));
        fields.insert("content".into(), Value::from("hello"));

        let block = Block {
            id: 42,
            role: Some(Role::User),
            block_type: "text".into(),
            created_at: "2026-08-20T10:00:00Z".into(),
            dispatch_anchor: None,
            fields,
        };

        let value = serde_json::to_value(&block).unwrap();
        assert_eq!(
            value["id"],
            Value::from(42),
            "the row's id, not the payload's"
        );
        assert_eq!(value["type"], Value::from("text"));
        assert_eq!(value["role"], Value::from("user"));
        assert_eq!(value["created_at"], Value::from("2026-08-20T10:00:00Z"));
        assert_eq!(
            value["content"],
            Value::from("hello"),
            "a payload key of its own still travels"
        );
        assert!(
            !value.as_object().unwrap().contains_key("dispatch_anchor"),
            "a payload cannot forge a turn identity the header never recorded"
        );

        // The text form is where the collision actually happened: a duplicate
        // key parses back as the later one, so assert the bytes carry one.
        let text = serde_json::to_string(&block).unwrap();
        assert_eq!(text.matches("\"id\"").count(), 1, "one id in the wire form");
        assert_eq!(text.matches("\"type\"").count(), 1);
    }
}

// The draft-block shape is not carried here. Its only readers are the draft
// tables and the request surface, so the module that owns draft persistence
// decides its shape: it is [`crate::store::DraftBlock`], defined beside the
// tables it is read out of.
