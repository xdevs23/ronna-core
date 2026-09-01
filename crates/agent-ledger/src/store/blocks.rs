//! Loading blocks back out of the ledger: the one statement that reads a
//! block's header row together with whichever content table holds its payload.

use rusqlite::{Connection, OptionalExtension};
use serde_json::Value;

use crate::block::{Block, OpaquePayload, ReasoningDetailEntry, Role};

use super::block_content::parse_role;
use super::descriptors::{ContentDescriptor, overlay_consumer_content, read_quoted_text};
use super::tool_choice::decode_tool_names;
use super::{DomainGate, StoreError};

/// One statement joining every LIBRARY content table by name — the core kinds'
/// load path, kept literal on purpose and pinned byte-identical by test.
///
/// A kind this statement has no join for loads its header with an inert
/// payload; the second load step
/// ([`overlay_consumer_content`]) then fills in any kind a content-table
/// descriptor claims, reading the declared columns by name from the
/// descriptor's own table. A kind neither this statement nor a descriptor
/// knows stays inert, which is the documented fallback for a newer ledger read
/// by an older build.
///
/// 2026-08-22, in lockstep with the pinned literal: the header select gained
/// `dispatch_anchor` — a header column, not a content join, so every kind's
/// load carries it for free.
///
/// 2026-08-27, in the same lockstep: the date marker's join gained the three
/// nullable zone-and-minute columns. A column written and never selected here
/// is a column the projection can never speak.
///
/// 2026-08-30, the same lockstep again: the tool result's join gained the
/// turn-ending stamp, which decides whether the resolution asks the model for
/// anything at all.
///
/// 2026-08-31, the same lockstep once more: the ancestor reference arrived as
/// a join of its own, and the harness message joined the prose table the three
/// other text-shaped kinds already share — a kind absent from that IN list
/// loads its content empty, which is the same silence as a column never
/// selected.
///
/// 2026-09-01, the same lockstep again: the tool choice arrived as a join of
/// its own, carrying the one column that holds the recorded names.
pub(super) const BLOCKS_QUERY: &str = "SELECT
            b.id AS b_id, b.block_type AS b_type, b.created_at AS b_created_at, b.dispatch_anchor AS b_dispatch_anchor,
            bt.role AS bt_role, bt.content AS bt_content,
            bq.role AS bq_role, bq.start_block_id, bq.start_pos, bq.end_block_id, bq.end_pos,
            bc.role AS bc_role, bc.language AS bc_language, bc.content AS bc_content,
            btc.role AS btc_role, btc.tool_call_id AS btc_tool_call_id, btc.name AS btc_name, btc.input AS btc_input, btc.interactive AS btc_interactive,
            bstc.role AS bstc_role, bstc.tool_call_id AS bstc_tool_call_id, bstc.name AS bstc_name, bstc.input AS bstc_input,
            btr.tool_call_id AS btr_tool_call_id, btr.content AS btr_content, btr.ends_turn AS btr_ends_turn,
            bte.tool_call_id AS bte_tool_call_id, bte.error AS bte_error, bte.refusal AS bte_refusal,
            bth.role AS bth_role, bth.content AS bth_content, bth.title AS bth_title, bth.summary AS bth_summary,
            bth.opaque_kind AS bth_opaque_kind, bth.opaque_data AS bth_opaque_data, bth.opaque_item_id AS bth_opaque_item_id,
            bs.status AS bs_status, bs.subtitle AS bs_subtitle,
            bar.for_block_id AS bar_for_block_id,
            bad.for_block_id AS bad_for_block_id, bad.decision AS bad_decision, bad.system_reason AS bad_system_reason, bad.user_reason AS bad_user_reason,
            bdm.date AS bdm_date, bdm.tz_abbrev AS bdm_tz_abbrev, bdm.tz_name AS bdm_tz_name, bdm.written_at AS bdm_written_at,
            banc.ancestor_conversation_id AS banc_ancestor,
            btch.names AS btch_names
     FROM blocks b
     LEFT JOIN block_text bt ON bt.block_id = b.id AND b.block_type IN ('text', 'streaming', 'system_prompt', 'harness_message')
     LEFT JOIN block_quote bq ON bq.block_id = b.id AND b.block_type = 'quote'
     LEFT JOIN block_code bc ON bc.block_id = b.id AND b.block_type = 'code'
     LEFT JOIN block_tool_call btc ON btc.block_id = b.id AND b.block_type = 'tool_call'
     LEFT JOIN block_streaming_tool_call bstc ON bstc.block_id = b.id AND b.block_type = 'streaming_tool_call'
     LEFT JOIN block_tool_result btr ON btr.block_id = b.id AND b.block_type = 'tool_result'
     LEFT JOIN block_tool_error bte ON bte.block_id = b.id AND b.block_type = 'tool_error'
     LEFT JOIN block_thinking bth ON bth.block_id = b.id AND b.block_type IN ('thinking', 'streaming_thinking')
     LEFT JOIN block_status bs ON bs.block_id = b.id AND b.block_type = 'status'
     LEFT JOIN block_approval_request bar ON bar.block_id = b.id AND b.block_type = 'approval_request'
     LEFT JOIN block_approval_decision bad ON bad.block_id = b.id AND b.block_type = 'approval_decision'
     LEFT JOIN block_date_marker bdm ON bdm.block_id = b.id AND b.block_type = 'date_marker'
     LEFT JOIN block_ancestor_reference banc ON banc.block_id = b.id AND b.block_type = 'ancestor_reference'
     LEFT JOIN block_tool_choice btch ON btch.block_id = b.id AND b.block_type = 'tool_choice'";

/// The conversation's last block by junction order, or None when empty.
///
/// The ratchet asks this after every drive to decide the frontier, so it must
/// not cost a full ledger read: same join and ordering as the full list, taken
/// from the other end, one row.
pub(super) fn latest_block_for_conversation(
    conn: &Connection,
    descriptors: &'static [ContentDescriptor],
    gate: &DomainGate,
    conversation_id: i64,
) -> Result<Option<Block>, StoreError> {
    let mut stmt = conn.prepare(&format!(
        "{BLOCKS_QUERY}
         JOIN conversation_blocks cb ON cb.block_id = b.id
         WHERE cb.conversation_id = ?1
         ORDER BY cb.id DESC LIMIT 1"
    ))?;
    let mut rows = stmt.query_map([conversation_id], |row| Ok(row_to_block(row)))?;
    match rows.next() {
        Some(row) => {
            let mut latest = [row??];
            overlay_consumer_content(conn, descriptors, gate, &mut latest)?;
            let [block] = latest;
            Ok(Some(block))
        }
        None => Ok(None),
    }
}

pub(super) fn load_blocks_for_conversation(
    conn: &Connection,
    descriptors: &'static [ContentDescriptor],
    gate: &DomainGate,
    conversation_id: i64,
) -> Result<Vec<Block>, StoreError> {
    // Ledger position IS the junction order: strictly monotonic within a
    // conversation regardless of how forks share block rows. Deep-copied
    // blocks keep their source created_at, so a timestamp sort would misplace
    // them relative to fork-inserted rows.
    let mut stmt = conn.prepare(&format!(
        "{BLOCKS_QUERY}
         JOIN conversation_blocks cb ON cb.block_id = b.id
         WHERE cb.conversation_id = ?1
         ORDER BY cb.id"
    ))?;
    // A row that cannot be read is an error, never a block quietly left out of
    // the history: this ledger's premise is that replay is faithful, and a
    // shortened conversation is a lie the caller has no way to notice.
    let rows = stmt.query_map([conversation_id], |row| Ok(row_to_block(row)))?;
    let mut blocks = Vec::new();
    for row in rows {
        blocks.push(row??);
    }
    overlay_consumer_content(conn, descriptors, gate, &mut blocks)?;
    Ok(resolve_quotes(
        QuoteScope::new(conn, descriptors, gate, Some(conversation_id)),
        resolve_reasoning_payloads(conn, blocks),
    ))
}

pub(super) fn load_single_block(
    conn: &Connection,
    descriptors: &'static [ContentDescriptor],
    gate: &DomainGate,
    block_id: i64,
) -> Result<Option<Block>, StoreError> {
    let mut stmt = conn.prepare(&format!("{BLOCKS_QUERY} WHERE b.id = ?1"))?;
    let Some(block) = stmt
        .query_row([block_id], |row| Ok(row_to_block(row)))
        .optional()?
    else {
        return Ok(None);
    };
    let mut blocks = vec![block?];
    overlay_consumer_content(conn, descriptors, gate, &mut blocks)?;
    // No conversation is named here, so a quote resolves along whichever
    // conversation carries the quoting block.
    Ok(resolve_quotes(
        QuoteScope::new(conn, descriptors, gate, None),
        resolve_reasoning_payloads(conn, blocks),
    )
    .into_iter()
    .next())
}

fn col_opt<T: rusqlite::types::FromSql>(
    row: &rusqlite::Row<'_>,
    name: &str,
) -> rusqlite::Result<Option<T>> {
    row.get(name)
}

/// Read a column the block's content table declares NOT NULL.
///
/// The joins are outer, so a NULL here means one thing: the header row claims a
/// kind whose content row is not there. That is reported as
/// [`StoreError::MissingBlockContent`] naming the block and its kind. It used to
/// become an empty string for text and a dropped block for quotes — an invented
/// payload and a shortened history, both of them silent.
fn required<T: rusqlite::types::FromSql>(
    row: &rusqlite::Row<'_>,
    name: &str,
    block_id: i64,
    block_type: &str,
) -> Result<T, StoreError> {
    match row.get::<_, Option<T>>(name)? {
        Some(value) => Ok(value),
        None => Err(StoreError::MissingBlockContent {
            block_id,
            block_type: block_type.to_owned(),
        }),
    }
}

/// The same read for a string column, which is most of them.
fn required_str(
    row: &rusqlite::Row<'_>,
    name: &str,
    block_id: i64,
    block_type: &str,
) -> Result<String, StoreError> {
    required(row, name, block_id, block_type)
}

/// A block's payload as it comes off the row: whose voice it speaks in, and
/// the kind-specific fields.
type Payload = (Option<Role>, serde_json::Map<String, Value>);

fn row_to_block(row: &rusqlite::Row<'_>) -> Result<Block, StoreError> {
    let id: i64 = row.get("b_id")?;
    let block_type: String = row.get("b_type")?;
    let created_at: String = row.get("b_created_at")?;
    let dispatch_anchor: Option<i64> = row.get("b_dispatch_anchor")?;

    // Split where the source of a block's role differs: `content_payload` and
    // `tool_payload` read a role COLUMN off their content table, and
    // `structural_payload` does not — those kinds' voice is a fact about the
    // kind, and the last of its arms is where an unrecognised kind lands.
    let (role, fields) = match content_payload(row, id, &block_type)? {
        Some(payload) => payload,
        None => structural_payload(row, id, &block_type)?,
    };

    Ok(Block {
        id,
        role,
        block_type,
        created_at,
        dispatch_anchor,
        fields,
    })
}

/// The kinds whose content table carries their role and their fields.
/// `None` means this is not one of them.
fn content_payload(
    row: &rusqlite::Row<'_>,
    block_id: i64,
    block_type: &str,
) -> Result<Option<Payload>, StoreError> {
    let mut fields = serde_json::Map::new();
    let mut role: Option<Role> = None;

    match block_type {
        // Four kinds, one prose table: what they SAY is stored identically
        // and what they MEAN is each kind's own answer. A row of columns per
        // kind saying the same two things would be four places to keep in
        // step.
        "text" | "streaming" | "system_prompt" | "harness_message" => {
            role = parse_role(col_opt::<String>(row, "bt_role")?.as_deref());
            fields.insert(
                "content".into(),
                Value::String(required_str(row, "bt_content", block_id, block_type)?),
            );
        }
        "quote" => {
            role = parse_role(col_opt::<String>(row, "bq_role")?.as_deref());
            for name in ["start_block_id", "start_pos", "end_block_id", "end_pos"] {
                let value: i64 = required(row, name, block_id, block_type)?;
                fields.insert(name.into(), Value::Number(value.into()));
            }
            fields.insert("text".into(), Value::String(String::new()));
        }
        "code" => {
            role = parse_role(col_opt::<String>(row, "bc_role")?.as_deref());
            let lang: Option<String> = col_opt(row, "bc_language")?;
            fields.insert("language".into(), lang.map_or(Value::Null, Value::String));
            fields.insert(
                "content".into(),
                Value::String(required_str(row, "bc_content", block_id, block_type)?),
            );
        }
        "thinking" | "streaming_thinking" => {
            role = parse_role(col_opt::<String>(row, "bth_role")?.as_deref());
            fields.insert(
                "content".into(),
                Value::String(required_str(row, "bth_content", block_id, block_type)?),
            );
            // The display-only summary channel — surfaced for rendering, never
            // consumed by projection.
            if let Some(summary) = col_opt::<String>(row, "bth_summary")? {
                fields.insert("summary".into(), Value::String(summary));
            }
            // Raw opaque columns — reconstructed into one `opaque` field by
            // `resolve_reasoning_payloads`, which also fetches the multi-entry
            // sidecar rows (a row callback has no connection to query with).
            if let Some(kind) = col_opt::<String>(row, "bth_opaque_kind")? {
                fields.insert("opaque_kind".into(), Value::String(kind));
                if let Some(data) = col_opt::<String>(row, "bth_opaque_data")? {
                    fields.insert("opaque_data".into(), Value::String(data));
                }
                if let Some(item_id) = col_opt::<String>(row, "bth_opaque_item_id")? {
                    fields.insert("opaque_item_id".into(), Value::String(item_id));
                }
            }
        }
        "status" => {
            fields.insert(
                "status".into(),
                Value::String(required_str(row, "bs_status", block_id, block_type)?),
            );
            let subtitle: Option<String> = col_opt(row, "bs_subtitle")?;
            fields.insert(
                "subtitle".into(),
                subtitle.map_or(Value::Null, Value::String),
            );
        }
        _ => return tool_payload(row, block_id, block_type),
    }

    Ok(Some((role, fields)))
}

/// The tool-call family: the call, the streamed input tail it replaces, and the
/// two ways it resolves. `None` means this is not one of them.
fn tool_payload(
    row: &rusqlite::Row<'_>,
    block_id: i64,
    block_type: &str,
) -> Result<Option<Payload>, StoreError> {
    let mut fields = serde_json::Map::new();
    let mut role: Option<Role> = None;

    match block_type {
        "tool_call" => {
            role = parse_role(col_opt::<String>(row, "btc_role")?.as_deref());
            fields.insert(
                "tool_call_id".into(),
                Value::String(required_str(row, "btc_tool_call_id", block_id, block_type)?),
            );
            fields.insert(
                "name".into(),
                Value::String(required_str(row, "btc_name", block_id, block_type)?),
            );
            fields.insert(
                "input".into(),
                Value::String(required_str(row, "btc_input", block_id, block_type)?),
            );
            // The interactive stamp, so the block answers who owes its next
            // move from its own data on replay — never a tool-name match.
            fields.insert(
                "interactive".into(),
                Value::Bool(col_opt::<i64>(row, "btc_interactive")?.unwrap_or(0) != 0),
            );
        }
        "streaming_tool_call" => {
            role = parse_role(col_opt::<String>(row, "bstc_role")?.as_deref());
            fields.insert(
                "tool_call_id".into(),
                Value::String(required_str(
                    row,
                    "bstc_tool_call_id",
                    block_id,
                    block_type,
                )?),
            );
            fields.insert(
                "name".into(),
                Value::String(required_str(row, "bstc_name", block_id, block_type)?),
            );
            fields.insert(
                "input".into(),
                Value::String(required_str(row, "bstc_input", block_id, block_type)?),
            );
        }
        "tool_result" => {
            fields.insert(
                "tool_call_id".into(),
                Value::String(required_str(row, "btr_tool_call_id", block_id, block_type)?),
            );
            fields.insert(
                "content".into(),
                Value::String(required_str(row, "btr_content", block_id, block_type)?),
            );
            // The turn-ending stamp, so the resolution answers whether it asks
            // the model for anything from its own data on replay — never from
            // a tool-name match. Read optionally, the interactive stamp's
            // shape one arm above: the column is NOT NULL with a default, so
            // every row the widening step backfilled reads unstamped and
            // summons its continuation exactly as it always did. A missing
            // JOIN row cannot hide here either — the required reads above fail
            // that row loudly before this line runs.
            fields.insert(
                "ends_turn".into(),
                Value::Bool(col_opt::<i64>(row, "btr_ends_turn")?.unwrap_or(0) != 0),
            );
        }
        "tool_error" => {
            fields.insert(
                "tool_call_id".into(),
                Value::String(required_str(row, "bte_tool_call_id", block_id, block_type)?),
            );
            fields.insert(
                "error".into(),
                Value::String(required_str(row, "bte_error", block_id, block_type)?),
            );
            // The refusal fact, so the failure answers whether it counts
            // toward the forced turn end out of its own data and never out
            // of its wording. Read optionally, the ends-turn stamp's shape one
            // arm above: the column is NOT NULL with a default, so a row the
            // widening step backfilled reads as an ordinary failure.
            fields.insert(
                "refusal".into(),
                Value::Bool(col_opt::<i64>(row, "bte_refusal")?.unwrap_or(0) != 0),
            );
        }
        _ => return Ok(None),
    }

    Ok(Some((role, fields)))
}

/// The kinds whose role is not a column, and the fallback for a kind this
/// query does not know. A consumer kind lands in the fallback here and is
/// filled in by the descriptor overlay afterwards; a kind with no descriptor
/// either stays inert.
fn structural_payload(
    row: &rusqlite::Row<'_>,
    block_id: i64,
    block_type: &str,
) -> Result<Payload, StoreError> {
    let mut fields = serde_json::Map::new();
    let mut role: Option<Role> = None;

    match block_type {
        // Roleless in the row, and named by its own column: the
        // conversation this thread continues. NOT NULL in the schema, so a
        // present row always answers.
        "ancestor_reference" => {
            let ancestor: i64 = required(row, "banc_ancestor", block_id, block_type)?;
            fields.insert(
                "ancestor_conversation_id".into(),
                Value::Number(ancestor.into()),
            );
        }
        // Roleless in the row, and carrying the one list this schema holds in
        // a column: the tool names, serialized. Read back through the one
        // decoding of that form, the writer's own `decode_tool_names`, so the
        // kind is handed a list of strings and the other reader of a stored
        // row cannot answer differently. A column that does not hold
        // that list is a corrupt row, not an empty choice — the two mean
        // opposite things to the resolution — so it is reported instead of
        // resolving to nothing.
        "tool_choice" => {
            let stored = required_str(row, "btch_names", block_id, block_type)?;
            let names = decode_tool_names(&stored).map_err(|error| {
                StoreError::Other(format!(
                    "block {block_id} records a tool choice whose names do not parse: {error}"
                ))
            })?;
            fields.insert(
                "names".into(),
                Value::Array(names.into_iter().map(Value::String).collect()),
            );
        }
        // Roleless in the row — its grouping under the harness's voice is the
        // KIND's projection fact, not a stored column.
        "date_marker" => {
            fields.insert(
                "date".into(),
                Value::String(required_str(row, "bdm_date", block_id, block_type)?),
            );
            // The zone and the writing minute are each independently
            // nullable: a marker written before the columns existed carries
            // none of them, and a source that answered nothing wrote NULL.
            // Null travels as Null so the kind's projection can drop the
            // clause rather than print an empty one.
            for (field, column) in [
                ("tz_abbrev", "bdm_tz_abbrev"),
                ("tz_name", "bdm_tz_name"),
                ("written_at", "bdm_written_at"),
            ] {
                let value: Option<String> = col_opt(row, column)?;
                fields.insert(field.into(), value.map_or(Value::Null, Value::String));
            }
        }
        // The approval blocks are the human's — role user by nature, not by
        // column. Mechanically load-bearing: the fork's group walk reads this
        // RAW role, so role User is what keeps approval blocks inside the
        // surrounding user turn's group boundary.
        "approval_request" => {
            role = Some(Role::User);
            let for_block_id: i64 = required(row, "bar_for_block_id", block_id, block_type)?;
            fields.insert("for_block_id".into(), Value::Number(for_block_id.into()));
        }
        "approval_decision" => {
            role = Some(Role::User);
            let for_block_id: i64 = required(row, "bad_for_block_id", block_id, block_type)?;
            fields.insert("for_block_id".into(), Value::Number(for_block_id.into()));
            fields.insert(
                "decision".into(),
                Value::String(required_str(row, "bad_decision", block_id, block_type)?),
            );
            let system_reason: Option<String> = col_opt(row, "bad_system_reason")?;
            fields.insert(
                "system_reason".into(),
                system_reason.map_or(Value::Null, Value::String),
            );
            let user_reason: Option<String> = col_opt(row, "bad_user_reason")?;
            fields.insert(
                "user_reason".into(),
                user_reason.map_or(Value::Null, Value::String),
            );
        }
        // A kind this statement has no join for at all. A descriptor-claimed
        // kind is filled in by the overlay step after this query; anything
        // else stays inert. Empty content here is not an invented payload:
        // there is no content row to have missed, because nothing selected
        // one.
        _ => {
            fields.insert("content".into(), Value::String(String::new()));
        }
    }

    Ok((role, fields))
}

/// Reconstruct each thinking block's stored [`OpaquePayload`] from the raw
/// `opaque_*` columns — plus the `block_reasoning_detail` sidecar for the
/// multi-entry variant — replacing them with a single `opaque` field the
/// provider layer deserializes on the next turn.
fn resolve_reasoning_payloads(conn: &Connection, mut blocks: Vec<Block>) -> Vec<Block> {
    for block in &mut blocks {
        let Some(Value::String(kind)) = block.fields.remove("opaque_kind") else {
            continue;
        };
        let data = match block.fields.remove("opaque_data") {
            Some(Value::String(s)) => Some(s),
            _ => None,
        };
        let item_id = match block.fields.remove("opaque_item_id") {
            Some(Value::String(s)) => Some(s),
            _ => None,
        };

        let payload = match kind.as_str() {
            "openai_responses" => {
                if let (Some(item_id), Some(encrypted_content)) = (item_id, data) {
                    Some(OpaquePayload::OpenAiResponses {
                        item_id,
                        encrypted_content,
                    })
                } else {
                    tracing::warn!(
                        block_id = block.id,
                        "openai_responses payload missing item id or data"
                    );
                    None
                }
            }
            "anthropic" => data.map(|signature| OpaquePayload::Anthropic { signature }),
            "openrouter" => Some(OpaquePayload::OpenRouter {
                entries: load_reasoning_details(conn, block.id),
            }),
            "mistral" => Some(OpaquePayload::Mistral),
            other => {
                tracing::warn!(
                    block_id = block.id,
                    kind = other,
                    "unknown opaque_kind — payload dropped"
                );
                None
            }
        };

        if let Some(payload) = payload
            && let Ok(value) = serde_json::to_value(&payload)
        {
            block.fields.insert("opaque".into(), value);
        }
    }
    blocks
}

/// Fetch a block's sidecar rows in position order — the verbatim rebuild order
/// for the replayed entry array.
fn load_reasoning_details(conn: &Connection, block_id: i64) -> Vec<ReasoningDetailEntry> {
    conn.prepare(
        "SELECT position, entry_type, entry_id, upstream_format, idx, content, signature
         FROM block_reasoning_detail WHERE block_id = ?1 ORDER BY position",
    )
    .and_then(|mut stmt| {
        let rows: Vec<ReasoningDetailEntry> = stmt
            .query_map([block_id], |row| {
                Ok(ReasoningDetailEntry {
                    position: u32::try_from(row.get::<_, i64>(0)?).unwrap_or(0),
                    entry_type: row.get(1)?,
                    entry_id: row.get(2)?,
                    upstream_format: row.get(3)?,
                    index: row
                        .get::<_, Option<i64>>(4)?
                        .and_then(|i| u32::try_from(i).ok()),
                    content: row.get(5)?,
                    signature: row.get(6)?,
                })
            })?
            .filter_map(Result::ok)
            .collect();
        Ok(rows)
    })
    .unwrap_or_default()
}

/// What a quote resolves against.
///
/// A quote's text is not one lookup but four facts held together: the
/// connection, the conversation whose visibility bounds the span, the
/// descriptor set that says which consumer kinds carry quotable text and in
/// which column, and the domain gate those descriptor reads answer to. The
/// three call sites — the block loader, the drafts preview and the fork's
/// target collection — each build one of these and hand it over whole, so
/// none of them can supply three of the four and quietly resolve differently
/// from the other two.
#[derive(Clone, Copy)]
pub(super) struct QuoteScope<'a> {
    conn: &'a Connection,
    descriptors: &'a [ContentDescriptor],
    gate: &'a DomainGate,
    /// The conversation the quote is read from, or `None` when the caller was
    /// handed a bare block id.
    conversation_id: Option<i64>,
}

impl<'a> QuoteScope<'a> {
    pub(super) fn new(
        conn: &'a Connection,
        descriptors: &'a [ContentDescriptor],
        gate: &'a DomainGate,
        conversation_id: Option<i64>,
    ) -> Self {
        Self {
            conn,
            descriptors,
            gate,
            conversation_id,
        }
    }

    /// The same scope read from another conversation — how the single-block
    /// load, handed no conversation, resolves along whichever one carries the
    /// quoting block.
    fn within(self, conversation_id: Option<i64>) -> Self {
        Self {
            conversation_id,
            ..self
        }
    }

    /// The stored type strings whose descriptors declare quotable text. This
    /// is compile-time descriptor data, so span membership never depends on
    /// runtime state.
    fn quotable_kinds(self) -> Vec<&'a str> {
        self.descriptors
            .iter()
            .filter(|d| d.quoted_text_column.is_some())
            .flat_map(|d| d.kinds.iter().copied())
            .collect()
    }
}

fn resolve_quotes(scope: QuoteScope<'_>, mut blocks: Vec<Block>) -> Vec<Block> {
    for block in &mut blocks {
        if block.block_type == "quote" {
            let scope = scope.within(
                scope
                    .conversation_id
                    .or_else(|| conversation_of(scope.conn, block.id)),
            );
            let field = |name: &str| block.fields.get(name).and_then(Value::as_i64).unwrap_or(0);
            let text = resolve_quote_text(
                scope,
                field("start_block_id"),
                field("start_pos"),
                field("end_block_id"),
                field("end_pos"),
            );
            block.fields.insert("text".into(), Value::String(text));
        }
    }
    blocks
}

/// One conversation a block hangs in, for callers that were handed a block id
/// and no conversation. A junction-shared block answers with one of them.
fn conversation_of(conn: &Connection, block_id: i64) -> Option<i64> {
    conn.query_row(
        "SELECT conversation_id FROM conversation_blocks WHERE block_id = ?1 LIMIT 1",
        [block_id],
        |row| row.get(0),
    )
    .optional()
    .ok()
    .flatten()
}

/// Resolve quote text from a block range.
///
/// Collects text content from every text-carrying block the range covers, then
/// applies the character offsets on the first and last.
///
/// **The range covers only what the quoting conversation can see** — its own
/// blocks and the detached ones a fork cloned for it. Block ids are global and
/// every conversation writes into the same sequence, so a bare id range picks
/// up whatever any other conversation happened to append between the endpoints
/// — another conversation's text, spliced into a quote nobody wrote that way.
/// [`quoted_text_blocks`] holds that membership rule for both quote sites.
pub(super) fn resolve_quote_text(
    scope: QuoteScope<'_>,
    start_block_id: i64,
    start_pos: i64,
    end_block_id: i64,
    end_pos: i64,
) -> String {
    if start_block_id == end_block_id {
        // Single-block quote — just a substring. No range, so nothing to walk,
        // and no membership rule to apply: exactly as it has always been for
        // the library's own text.
        let full = single_quoted_text(scope, start_block_id);
        let s = usize::try_from(start_pos.max(0)).unwrap_or(0);
        let e = usize::try_from(end_pos.max(0)).unwrap_or(0).min(full.len());
        return if s < e {
            full.chars().skip(s).take(e - s).collect()
        } else {
            String::new()
        };
    }

    let parts = quoted_text_blocks(scope, start_block_id, end_block_id);

    let mut result = String::new();
    for (id, text) in &parts {
        if *id == start_block_id {
            let s = usize::try_from(start_pos.max(0)).unwrap_or(0);
            result.push_str(&text.chars().skip(s).collect::<String>());
        } else if *id == end_block_id {
            let e = usize::try_from(end_pos.max(0)).unwrap_or(0).min(text.len());
            result.push_str(&text.chars().take(e).collect::<String>());
        } else {
            result.push_str(text);
        }
    }
    result
}

/// Every text-carrying block a quote range covers, in ledger order, with its
/// content.
///
/// This is the one place that decides what "between these two blocks" means,
/// and both quote sites — the resolved text and the fork's target collection —
/// go through it for every RANGE, so the two can never answer differently
/// there. A single-block quote is the one asymmetry: resolution takes the
/// membership-free substring path above, while the fork's collection still
/// walks through here — a shape the library has always had for its own text.
///
/// **Text-carrying means a `block_text` row OR a declaration.** The library's
/// own text kinds store their content in `block_text`; a consumer kind carries
/// quotable text when its descriptor names the column
/// ([`ContentDescriptor::quoted_text_column`]), and only then. That
/// declaration is compile-time data, so what a span covers is a static fact:
/// a kind that declares nothing is not a member, which is exactly the walk as
/// it stood before consumer kinds could be quoted at all, and a kind that
/// declares one IS a member even while its domain gate is shut. Membership and
/// text are decided separately on purpose — the fork's deep copy takes this
/// walk as its copy set, so a span that shrank when a consumer migration
/// failed would make a fork copy a different set of blocks depending on
/// runtime health.
///
/// **A block is in the range when it lies between the endpoints AND belongs to
/// the quoting conversation** — where belonging has two shapes, because a quote
/// has two:
///
///   - It hangs in the quoting conversation's junction. That is ordinary
///     history, and another conversation's blocks are excluded by it: an
///     interloper appending between the endpoints hangs in ITS junction, not
///     this one's, so it cannot be spliced into a quote nobody wrote that way.
///   - It hangs in no junction at all. The deep copy a new thread makes clones
///     quote targets as detached rows on purpose, so that deleting the source
///     cannot cascade them away; a detached block belongs to whoever quotes it
///     and to nobody else.
///
/// Both at once is the normal case after a fork, not an exotic one: the copy
/// junctions the group's own blocks and detaches the targets outside it, so one
/// range routinely covers some of each. Admitting only one kind returns half
/// the quoted text.
///
/// The order is the block id, which is the ledger order — every junction append
/// points at a just-created block and copies preserve order, so ids ascend
/// along junction order in every conversation.
///
/// One residual is accepted here, and it is that **a detached block carries no
/// owner today**: two deep copies running concurrently could interleave the
/// detached ids they write, and a range would then reach across into the other
/// copy's clone. (A block whose conversation was deleted is detached the same
/// way until collection sweeps it, so it is the same gap, not a second one.)
/// Stage 3's content-table descriptors are where an owner column would land;
/// until then the fork's single transaction is what keeps its clones
/// consecutive.
pub(super) fn quoted_text_blocks(
    scope: QuoteScope<'_>,
    start_block_id: i64,
    end_block_id: i64,
) -> Vec<(i64, String)> {
    let mut members = span_members(scope, start_block_id, end_block_id);
    fill_declared_text(scope, &mut members);
    members
        .into_iter()
        .map(|member| (member.id, member.text.unwrap_or_default()))
        .collect()
}

/// One block a quote span covers: its id, its stored type, and its
/// `block_text` content where it has one.
///
/// `text: None` is "no `block_text` row", which is the question the descriptor
/// half of the resolution answers — and, once that has had its turn, is the
/// absence that resolves to the empty string.
struct SpanMember {
    id: i64,
    block_type: String,
    text: Option<String>,
}

/// The span's membership walk: the endpoints, the quoting conversation's
/// visibility, and the text-carrying rule, in one statement.
fn span_members(scope: QuoteScope<'_>, start_block_id: i64, end_block_id: i64) -> Vec<SpanMember> {
    // Conversation ids start at 1, so a caller holding no conversation passes 0
    // and the junction half of the rule matches nothing — leaving the detached
    // half standing alone, which is the whole answer available to it.
    let quoting = scope.conversation_id.unwrap_or(0);
    let quotable_kinds = scope.quotable_kinds();
    // Bound, never interpolated: a stored type string is consumer-supplied and
    // is not checked as an identifier anywhere. The list is empty for a
    // core-only store, and `IN ()` is legal in this engine — it matches
    // nothing, which is precisely the answer a store with no declaration owes.
    let kind_placeholders = (0..quotable_kinds.len())
        .map(|i| format!("?{}", i + 4))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT b.id, b.block_type, bt.content
             FROM blocks b
             LEFT JOIN block_text bt ON bt.block_id = b.id
             WHERE b.id >= ?2 AND b.id <= ?3
               AND (bt.block_id IS NOT NULL OR b.block_type IN ({kind_placeholders}))
               AND (
                   EXISTS (
                       SELECT 1 FROM conversation_blocks cb
                       WHERE cb.block_id = b.id AND cb.conversation_id = ?1
                   )
                   OR NOT EXISTS (
                       SELECT 1 FROM conversation_blocks cb WHERE cb.block_id = b.id
                   )
               )
             ORDER BY b.id"
    );

    let mut params: Vec<rusqlite::types::Value> =
        vec![quoting.into(), start_block_id.into(), end_block_id.into()];
    params.extend(
        quotable_kinds
            .iter()
            .map(|kind| rusqlite::types::Value::Text((*kind).to_owned())),
    );

    scope
        .conn
        .prepare(&sql)
        .and_then(|mut stmt| {
            let rows: Vec<SpanMember> = stmt
                .query_map(rusqlite::params_from_iter(params.iter()), |row| {
                    Ok(SpanMember {
                        id: row.get(0)?,
                        block_type: row.get(1)?,
                        text: row.get(2)?,
                    })
                })?
                .filter_map(Result::ok)
                .collect();
            Ok(rows)
        })
        .unwrap_or_default()
}

/// The `block_text`-less members' text, read from whichever descriptor claims
/// their kind through its declared quotable column.
///
/// **A closed domain gate declines the read.** A failed consumer migration
/// leaves that domain's schema in doubt, and the store's standing discipline
/// is that nothing runs raw against it; a quote is display enrichment, so
/// declining leaves the member's text absent — the empty resolution the
/// projection renders as nothing — instead of failing every load of every
/// conversation that happens to hold such a quote. The member stays in the
/// span either way: membership is the declaration's answer, not the gate's.
fn fill_declared_text(scope: QuoteScope<'_>, members: &mut [SpanMember]) {
    for descriptor in scope.descriptors {
        if descriptor.quoted_text_column.is_none() {
            continue;
        }
        let targets: Vec<usize> = members
            .iter()
            .enumerate()
            .filter(|(_, member)| {
                member.text.is_none() && descriptor.kinds.contains(&member.block_type.as_str())
            })
            .map(|(index, _)| index)
            .collect();
        if targets.is_empty() || scope.gate.ensure(descriptor.domain).is_err() {
            continue;
        }

        let ids: Vec<i64> = targets.iter().map(|&index| members[index].id).collect();
        // A read that fails leaves the text absent, which is how every other
        // failure this resolver can meet already answers.
        let found = read_quoted_text(scope.conn, descriptor, &ids).unwrap_or_default();
        for index in targets {
            members[index].text = found.get(&members[index].id).cloned();
        }
    }
}

/// The single-block path's text: one block's `block_text` content, or its
/// declared quotable column where it has no `block_text` row.
///
/// It runs the same two-source rule [`fill_declared_text`] holds, on a span of
/// one, so the single-block and range paths can never disagree about where a
/// kind's text comes from. What it does NOT run is the membership rule: a
/// single-block quote names its block outright, exactly as it always has for
/// the library's own text.
fn single_quoted_text(scope: QuoteScope<'_>, block_id: i64) -> String {
    let Some(member) = scope
        .conn
        .query_row(
            "SELECT b.id, b.block_type, bt.content
             FROM blocks b
             LEFT JOIN block_text bt ON bt.block_id = b.id
             WHERE b.id = ?1",
            [block_id],
            |row| {
                Ok(SpanMember {
                    id: row.get(0)?,
                    block_type: row.get(1)?,
                    text: row.get(2)?,
                })
            },
        )
        .optional()
        .ok()
        .flatten()
    else {
        return String::new();
    };

    let mut members = [member];
    fill_declared_text(scope, &mut members);
    let [member] = members;
    member.text.unwrap_or_default()
}
