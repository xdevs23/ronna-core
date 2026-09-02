//! Block persistence: appending to the ledger and mutating the streaming tail.
//!
//! The module name is historical and the architecture has no message row —
//! everything here persists **blocks**.

use rusqlite::{Connection, OptionalExtension, params};

use crate::agency::{LeafKind, SystemPrompt};
use crate::block::{Block, OpaquePayload, Role};
use crate::types::InputBlock;

use super::blocks::{
    latest_block_for_conversation, load_blocks_for_conversation, load_single_block,
};
use super::descriptors::ContentDescriptor;
use super::{Store, StoreError, now_iso8601, transact};

/// Where one ledger append lands: the conversation, and — through the
/// crate-only constructor — the dispatch anchor the row's header records.
///
/// This is the anchor's one route into a write (2026-08-22). Every insert
/// method takes `impl Into<BlockDestination>`, and the two forms are the two
/// writer surfaces: a bare conversation id converts to the anchor-less
/// destination, which is all the public write surface can express, while the
/// anchored constructor is crate-internal — the framework's own paths (the
/// streaming reader's per-turn seam, the interrupt's status, the metadata
/// worker's response) are the only writers that can record an anchor. One
/// method per operation, no anchored siblings.
#[derive(Clone, Copy, Debug)]
pub struct BlockDestination {
    conversation_id: i64,
    dispatch_anchor: Option<i64>,
}

impl From<i64> for BlockDestination {
    /// The public form: a bare conversation id, recording no anchor.
    fn from(conversation_id: i64) -> Self {
        Self {
            conversation_id,
            dispatch_anchor: None,
        }
    }
}

impl BlockDestination {
    /// The framework's form: a destination that records the given turn's
    /// dispatch anchor on the inserted header.
    pub(crate) fn anchored(conversation_id: i64, dispatch_anchor: Option<i64>) -> Self {
        Self {
            conversation_id,
            dispatch_anchor,
        }
    }

    /// The conversation this destination appends to.
    pub(crate) fn conversation_id(self) -> i64 {
        self.conversation_id
    }

    /// The dispatch anchor this destination records, `None` for the public
    /// form. For the metadata ledger's own insert, which writes no `blocks`
    /// header and therefore reads the destination directly.
    pub(crate) fn dispatch_anchor(self) -> Option<i64> {
        self.dispatch_anchor
    }
}

/// One junction row's own two facts: a block, and the conversation it is
/// joined to. Read together because the change hook names the ROW, and the row
/// is where both answers live — deriving the conversation from the block
/// afterwards is what let a shared block's change land on the wrong one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinedBlock {
    /// The conversation the row joins the block to.
    pub conversation_id: i64,
    /// The block joined.
    pub block_id: i64,
}

/// The facts a tool call carries into the ledger, grouped because they travel
/// together: a call's identity, what it asks for, and whether it is answered in
/// the chat rather than out of band.
#[derive(Debug, Clone)]
pub struct ToolCallInsert {
    /// The provider's id for this call.
    pub tool_call_id: String,
    /// The tool's registered name.
    pub name: String,
    /// The call's arguments, as the provider serialized them.
    pub input: String,
    /// Whether a human answers this call in the conversation. Stamped at insert
    /// so the block answers who owes its next move from its own data on replay,
    /// never from a tool-name match.
    pub interactive: bool,
}

/// Decompose a payload into the `block_thinking` opaque columns: the variant
/// tag, the scalar payload (encrypted content or signature), and the reasoning
/// item id. The multi-entry variant's entries live in the
/// `block_reasoning_detail` sidecar instead of a scalar column.
fn opaque_columns(
    opaque: Option<&OpaquePayload>,
) -> (Option<&'static str>, Option<&str>, Option<&str>) {
    match opaque {
        None => (None, None, None),
        Some(OpaquePayload::OpenAiResponses {
            item_id,
            encrypted_content,
        }) => (
            Some("openai_responses"),
            Some(encrypted_content),
            Some(item_id),
        ),
        Some(OpaquePayload::Anthropic { signature }) => (Some("anthropic"), Some(signature), None),
        Some(OpaquePayload::OpenRouter { .. }) => (Some("openrouter"), None, None),
        Some(OpaquePayload::Mistral) => (Some("mistral"), None, None),
    }
}

/// The library's ephemeral streaming block types — with any kinds a descriptor
/// declares ephemeral, the ONLY rows a finalization may delete. Block storage
/// is append-only once promoted; streaming blocks are the documented
/// exception: they are replaced by their final blocks.
pub(super) const STREAMING_TYPES: &[&str] =
    &["streaming", "streaming_thinking", "streaming_tool_call"];

/// The SQL `IN` list of every ephemeral block type: the library's literal
/// union plus the kinds the descriptors declare ephemeral. With no ephemeral
/// descriptors the output is byte-identical to the literal the library always
/// used, and a test pins that.
pub(super) fn ephemeral_types_sql(descriptors: &[ContentDescriptor]) -> String {
    let names = STREAMING_TYPES
        .iter()
        .copied()
        .chain(
            descriptors
                .iter()
                .filter(|d| d.ephemeral)
                .flat_map(|d| d.kinds.iter().copied()),
        )
        .map(|name| format!("'{name}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("({names})")
}

/// Delete one ephemeral block by id, type-guarded so this seam can never touch
/// a committed block. Cascades to the content table and the junction rows.
pub(super) fn delete_streaming_counterpart(
    conn: &Connection,
    descriptors: &[ContentDescriptor],
    block_id: i64,
) -> Result<bool, StoreError> {
    let deleted = conn.execute(
        &format!(
            "DELETE FROM blocks WHERE id = ?1 AND block_type IN {}",
            ephemeral_types_sql(descriptors)
        ),
        params![block_id],
    )?;
    Ok(deleted > 0)
}

/// Insert a block row and link it to a conversation. Returns the block id.
///
/// **A block is three rows** — the header here, the junction row here, and the
/// content row the caller writes next — so every caller runs all three inside
/// ONE transaction. Autocommit statements do not survive a failure between
/// them: the junction insert alone can be refused (the single-system-prompt
/// trigger fires exactly there), and what that left behind was a committed
/// header with no junction row and no content, a block belonging to nothing
/// that no query can even name.
///
/// The destination's `dispatch_anchor` is the header's dispatch identity
/// (2026-08-22): the block whose owed turn dispatched the stream this block
/// is a product of, `None` for everything else. Every writer passes it
/// through this one helper as part of its [`BlockDestination`], so the
/// per-writer-class rule — the reader's per-turn seam, the copy at the
/// resolution write, the actor's own at the interrupt, NULL on the public
/// write surface — has exactly one place it lands in the header.
pub(super) fn insert_block(
    conn: &Connection,
    dest: impl Into<BlockDestination>,
    block_type: &str,
) -> Result<i64, StoreError> {
    let dest = dest.into();
    let now = now_iso8601();
    conn.execute(
        "INSERT INTO blocks (block_type, created_at, dispatch_anchor) VALUES (?1, ?2, ?3)",
        params![block_type, now, dest.dispatch_anchor],
    )?;
    let block_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO conversation_blocks (conversation_id, block_id) VALUES (?1, ?2)",
        params![dest.conversation_id, block_id],
    )?;
    Ok(block_id)
}

/// One block's recorded dispatch anchor — the copy source for the writer class
/// that answers a call. Results, errors and the approval chain copy the anchor
/// from the block they resolve AT the resolution write, off the durable row,
/// which is what makes a restart-recovered round correct for free: no
/// in-memory state has to survive to the write.
pub(super) fn anchor_of(conn: &Connection, block_id: i64) -> Result<Option<i64>, rusqlite::Error> {
    conn.query_row(
        "SELECT dispatch_anchor FROM blocks WHERE id = ?1",
        [block_id],
        |row| row.get(0),
    )
}

impl Store {
    // ─── Block queries ───────────────────────────────────────────────────

    /// Every block in a conversation, in ledger order.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn list_blocks(&self, conversation_id: i64) -> Result<Vec<Block>, StoreError> {
        #[cfg(test)]
        self.count_ledger_load();
        let descriptors = self.descriptors;
        let gate = self.gate.clone();
        self.run(move |conn| {
            load_blocks_for_conversation(conn, descriptors, &gate, conversation_id)
        })
        .await
    }

    /// The conversation's last block by junction order — the frontier's one
    /// read. One row instead of the whole ledger.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn latest_block(&self, conversation_id: i64) -> Result<Option<Block>, StoreError> {
        let descriptors = self.descriptors;
        let gate = self.gate.clone();
        self.run(move |conn| {
            latest_block_for_conversation(conn, descriptors, &gate, conversation_id)
        })
        .await
    }

    /// One block by id, whichever conversations it belongs to.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn find_block(&self, block_id: i64) -> Result<Option<Block>, StoreError> {
        let descriptors = self.descriptors;
        let gate = self.gate.clone();
        self.run(move |conn| load_single_block(conn, descriptors, &gate, block_id))
            .await
    }

    /// How many blocks a conversation holds.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn block_count(&self, conversation_id: i64) -> Result<usize, StoreError> {
        self.run(move |conn| {
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM conversation_blocks WHERE conversation_id = ?1",
                [conversation_id],
                |row| row.get(0),
            )?;
            Ok(usize::try_from(count).unwrap_or(0))
        })
        .await
    }

    // ─── Typed block insertion ───────────────────────────────────────────

    /// Append a committed text block. Part of the public write surface, which
    /// never records a dispatch anchor.
    ///
    /// # Errors
    ///
    /// If the insert fails or the store's actor has stopped.
    pub async fn insert_text_block(
        &self,
        conversation_id: i64,
        role: Role,
        content: String,
    ) -> Result<i64, StoreError> {
        self.run(move |conn| {
            transact(conn, |tx| {
                let block_id = insert_block(tx, conversation_id, "text")?;
                tx.execute(
                    "INSERT INTO block_text (block_id, role, content) VALUES (?1, ?2, ?3)",
                    params![block_id, role.as_str(), content],
                )?;
                Ok(block_id)
            })
        })
        .await
    }

    /// Finalize a streamed text: insert the final `text` block and delete its
    /// streaming counterpart in ONE transaction — the literal atomic replace
    /// that keeps streaming blocks ephemeral. `replaces_streaming = None`
    /// covers the finalization events that never opened a streaming tail.
    ///
    /// The streaming reader appends through an anchored [`BlockDestination`],
    /// carrying the anchor its per-turn seam holds; a bare conversation id is
    /// the public, anchor-less form.
    ///
    /// # Errors
    ///
    /// If the transaction fails or the store's actor has stopped.
    pub async fn insert_final_text_block(
        &self,
        dest: impl Into<BlockDestination>,
        role: Role,
        content: String,
        replaces_streaming: Option<i64>,
    ) -> Result<i64, StoreError> {
        let dest = dest.into();
        let descriptors = self.descriptors;
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let block_id = insert_block(&tx, dest, "text")?;
            tx.execute(
                "INSERT INTO block_text (block_id, role, content) VALUES (?1, ?2, ?3)",
                params![block_id, role.as_str(), content],
            )?;
            if let Some(streaming_id) = replaces_streaming {
                delete_streaming_counterpart(&tx, descriptors, streaming_id)?;
            }
            tx.commit()?;
            Ok(block_id)
        })
        .await
    }

    /// Discard one streaming block that can never finalize — a duplicate start
    /// event's orphan, an empty-input call, an empty thinking tail.
    /// Type-guarded: committed blocks are unreachable through this seam.
    ///
    /// # Errors
    ///
    /// If the delete fails or the store's actor has stopped.
    pub async fn discard_streaming_block(&self, block_id: i64) -> Result<bool, StoreError> {
        let descriptors = self.descriptors;
        self.run(move |conn| delete_streaming_counterpart(conn, descriptors, block_id))
            .await
    }

    /// Append the conversation's system prompt block. The content is the
    /// caller's: prompts are a consumer's own words, and this library has none.
    ///
    /// **The prompt is the head of the ledger, or it is refused** (2026-09-02).
    /// A system prompt joins a conversation that holds no row yet; a
    /// conversation already holding any block takes none, and a conversation
    /// already holding a prompt takes no second one. This is the whole
    /// statement of the rule, and every other place that mentions it points
    /// here.
    ///
    /// The SCHEMA is what holds it, not this door, because a prompt reaches a
    /// conversation through the junction from several directions — a fork
    /// copying its source's rows among them — and a check in front of one door
    /// leaves the others open. So the refusal is the database's own sentence,
    /// carried to the caller typed as [`StoreError::Rejected`]: one rule, one
    /// place it is stated, one class a caller acts on.
    ///
    /// A caller replacing a conversation's prompt appends the new one to a
    /// fresh conversation and clones the history behind it — see
    /// [`clone_join_rows_after`](Store::clone_join_rows_after).
    ///
    /// # Errors
    ///
    /// [`StoreError::Rejected`] if the conversation holds any block already, a
    /// system prompt among them; whatever the write fails with otherwise; or
    /// [`StoreError::ActorStopped`] if the store's actor has stopped.
    pub async fn insert_system_prompt(
        &self,
        conversation_id: i64,
        content: String,
    ) -> Result<i64, StoreError> {
        self.run(move |conn| {
            transact(conn, |tx| {
                let block_id = insert_block(tx, conversation_id, SystemPrompt::KINDS[0])?;
                tx.execute(
                    "INSERT INTO block_text (block_id, role, content) VALUES (?1, 'system', ?2)",
                    params![block_id, content],
                )?;
                Ok(block_id)
            })
        })
        .await
    }

    /// Open a streaming text tail for the model's next emission. The live tail
    /// is a turn product too: the streaming reader appends through an anchored
    /// [`BlockDestination`], and a bare conversation id records no anchor.
    ///
    /// # Errors
    ///
    /// If the insert fails or the store's actor has stopped.
    pub async fn insert_streaming_block(
        &self,
        dest: impl Into<BlockDestination>,
        role: Role,
    ) -> Result<i64, StoreError> {
        let dest = dest.into();
        self.run(move |conn| {
            transact(conn, |tx| {
                let block_id = insert_block(tx, dest, "streaming")?;
                tx.execute(
                    "INSERT INTO block_text (block_id, role, content) VALUES (?1, ?2, '')",
                    params![block_id, role.as_str()],
                )?;
                Ok(block_id)
            })
        })
        .await
    }

    /// Append an empty committed thinking block.
    ///
    /// # Errors
    ///
    /// If the insert fails or the store's actor has stopped.
    pub async fn insert_thinking_block(
        &self,
        conversation_id: i64,
        role: Role,
    ) -> Result<i64, StoreError> {
        self.run(move |conn| {
            transact(conn, |tx| {
                let block_id = insert_block(tx, conversation_id, "thinking")?;
                tx.execute(
                    "INSERT INTO block_thinking (block_id, role, content) VALUES (?1, ?2, '')",
                    params![block_id, role.as_str()],
                )?;
                Ok(block_id)
            })
        })
        .await
    }

    /// Open a streaming thinking tail. The streaming reader appends through an
    /// anchored [`BlockDestination`]; a bare conversation id records no anchor.
    ///
    /// # Errors
    ///
    /// If the insert fails or the store's actor has stopped.
    pub async fn insert_streaming_thinking_block(
        &self,
        dest: impl Into<BlockDestination>,
        role: Role,
    ) -> Result<i64, StoreError> {
        let dest = dest.into();
        self.run(move |conn| {
            transact(conn, |tx| {
                let block_id = insert_block(tx, dest, "streaming_thinking")?;
                tx.execute(
                    "INSERT INTO block_thinking (block_id, role, content) VALUES (?1, ?2, '')",
                    params![block_id, role.as_str()],
                )?;
                Ok(block_id)
            })
        })
        .await
    }

    /// Finalize a thinking block with its content, its display-only summary —
    /// the second reasoning channel, never projected — and, when captured, its
    /// opaque continuity payload, all in ONE transaction with the block insert.
    /// Blocks are immutable once promoted, so the payload rides the
    /// finalization INSERT; there is no UPDATE path.
    ///
    /// `replaces_streaming` is the streamed tail this final replaces, deleted
    /// in the SAME transaction. The streaming reader's finalization appends
    /// through an anchored [`BlockDestination`]; a bare conversation id
    /// records no anchor.
    ///
    /// # Errors
    ///
    /// If the transaction fails or the store's actor has stopped.
    pub async fn insert_thinking_block_with_content(
        &self,
        dest: impl Into<BlockDestination>,
        role: Role,
        content: String,
        summary: Option<String>,
        opaque: Option<OpaquePayload>,
        replaces_streaming: Option<i64>,
    ) -> Result<i64, StoreError> {
        let dest = dest.into();
        let descriptors = self.descriptors;
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let block_id = insert_block(&tx, dest, "thinking")?;
            let (kind, data, item_id) = opaque_columns(opaque.as_ref());
            tx.execute(
                "INSERT INTO block_thinking (block_id, role, content, summary, opaque_kind, opaque_data, opaque_item_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![block_id, role.as_str(), content, summary, kind, data, item_id],
            )?;
            if let Some(OpaquePayload::OpenRouter { entries }) = &opaque {
                for entry in entries {
                    tx.execute(
                        "INSERT INTO block_reasoning_detail
                             (block_id, position, entry_type, entry_id, upstream_format, idx, content, signature)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                        params![
                            block_id,
                            entry.position,
                            entry.entry_type,
                            entry.entry_id,
                            entry.upstream_format,
                            entry.index,
                            entry.content,
                            entry.signature,
                        ],
                    )?;
                }
            }
            if let Some(streaming_id) = replaces_streaming {
                delete_streaming_counterpart(&tx, descriptors, streaming_id)?;
            }
            tx.commit()?;
            Ok(block_id)
        })
        .await
    }

    /// Append a committed tool call. `replaces_streaming` is the streamed input
    /// tail this final call replaces, deleted in the SAME transaction.
    ///
    /// The streamed call's finalization appends through an anchored
    /// [`BlockDestination`] — the fact the dispatch-identity slice exists for:
    /// a consumer holding a call block reads its summoning message in one
    /// step, round one and round ten alike. A bare conversation id records no
    /// anchor.
    ///
    /// # Errors
    ///
    /// If the transaction fails or the store's actor has stopped.
    pub async fn insert_tool_call_block(
        &self,
        dest: impl Into<BlockDestination>,
        role: Role,
        call: ToolCallInsert,
        replaces_streaming: Option<i64>,
    ) -> Result<i64, StoreError> {
        let dest = dest.into();
        let descriptors = self.descriptors;
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let block_id = insert_block(&tx, dest, "tool_call")?;
            tx.execute(
                "INSERT INTO block_tool_call (block_id, role, tool_call_id, name, input, interactive)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    block_id,
                    role.as_str(),
                    call.tool_call_id,
                    call.name,
                    call.input,
                    call.interactive
                ],
            )?;
            if let Some(streaming_id) = replaces_streaming {
                delete_streaming_counterpart(&tx, descriptors, streaming_id)?;
            }
            tx.commit()?;
            Ok(block_id)
        })
        .await
    }

    /// Open a streaming tool-call tail whose arguments arrive in deltas. The
    /// streaming reader appends through an anchored [`BlockDestination`]; a
    /// bare conversation id records no anchor.
    ///
    /// # Errors
    ///
    /// If the insert fails or the store's actor has stopped.
    pub async fn insert_streaming_tool_call_block(
        &self,
        dest: impl Into<BlockDestination>,
        role: Role,
        tool_call_id: String,
        name: String,
    ) -> Result<i64, StoreError> {
        let dest = dest.into();
        self.run(move |conn| {
            transact(conn, |tx| {
                let block_id = insert_block(tx, dest, "streaming_tool_call")?;
                tx.execute(
                    "INSERT INTO block_streaming_tool_call (block_id, role, tool_call_id, name, input) VALUES (?1, ?2, ?3, ?4, '')",
                    params![block_id, role.as_str(), tool_call_id, name],
                )?;
                Ok(block_id)
            })
        })
        .await
    }

    /// Append an arguments delta to a streaming tool call.
    ///
    /// # Errors
    ///
    /// If the transaction fails or the store's actor has stopped.
    pub async fn append_to_streaming_tool_call(
        &self,
        block_id: i64,
        input_delta: String,
        updated_at: String,
    ) -> Result<(), StoreError> {
        self.run(move |conn| {
            let tx = conn.transaction()?;
            tx.execute(
                "UPDATE block_streaming_tool_call SET input = input || ?1 WHERE block_id = ?2",
                params![input_delta, block_id],
            )?;
            tx.execute(
                "UPDATE blocks SET updated_at = ?1 WHERE id = ?2",
                params![updated_at, block_id],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// The arguments accumulated so far on a streaming tool call.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn get_streaming_tool_call_input(
        &self,
        block_id: i64,
    ) -> Result<Option<String>, StoreError> {
        self.run(move |conn| {
            conn.query_row(
                "SELECT input FROM block_streaming_tool_call WHERE block_id = ?1",
                [block_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
        })
        .await
    }

    /// Read both reasoning channels off a thinking block: the verbatim content
    /// and the display-only summary, the latter `None` when the provider never
    /// streamed one.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn get_thinking_block_channels(
        &self,
        block_id: i64,
    ) -> Result<Option<(String, Option<String>)>, StoreError> {
        self.run(move |conn| {
            conn.query_row(
                "SELECT content, summary FROM block_thinking WHERE block_id = ?1",
                [block_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(Into::into)
        })
        .await
    }

    /// Append a summary delta to a streaming thinking block's `summary`
    /// channel. `COALESCE` turns the column's NULL — no summary yet — into the
    /// empty accumulation base on the first delta.
    ///
    /// # Errors
    ///
    /// If the transaction fails or the store's actor has stopped.
    pub async fn append_to_thinking_summary(
        &self,
        block_id: i64,
        text: String,
        updated_at: String,
    ) -> Result<(), StoreError> {
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let rows = tx.execute(
                "UPDATE block_thinking SET summary = COALESCE(summary, '') || ?1 WHERE block_id = ?2",
                params![text, block_id],
            )?;
            if rows > 0 {
                tx.execute(
                    "UPDATE blocks SET updated_at = ?1 WHERE id = ?2",
                    params![updated_at, block_id],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    // A tool result or error enters the ledger through exactly one door each:
    // the conditional resolution writes in the tool-call module
    // (`complete_tool_call_block`, `fail_tool_call_block`), keyed on the call
    // block id. An unconditional insert here would be a second door past the
    // one-outcome-per-call condition.

    /// Append a status block. The reader's abnormal-stop statuses and the
    /// interrupt's status append through an anchored [`BlockDestination`] —
    /// the turn the status is about is the turn it anchors on; a bare
    /// conversation id records no anchor.
    ///
    /// # Errors
    ///
    /// If the insert fails or the store's actor has stopped.
    pub async fn insert_status_block(
        &self,
        dest: impl Into<BlockDestination>,
        status: String,
        subtitle: Option<String>,
    ) -> Result<i64, StoreError> {
        let dest = dest.into();
        self.run(move |conn| {
            transact(conn, |tx| {
                let block_id = insert_block(tx, dest, "status")?;
                tx.execute(
                    "INSERT INTO block_status (block_id, status, subtitle) VALUES (?1, ?2, ?3)",
                    params![block_id, status, subtitle],
                )?;
                Ok(block_id)
            })
        })
        .await
    }

    /// Replace a committed tool call's arguments, stamping the block's
    /// `updated_at`.
    ///
    /// # Errors
    ///
    /// If the update fails or the store's actor has stopped.
    pub async fn update_tool_call_block_input(
        &self,
        block_id: i64,
        input: String,
    ) -> Result<(), StoreError> {
        self.run(move |conn| {
            transact(conn, |tx| {
                let now = now_iso8601();
                tx.execute(
                    "UPDATE block_tool_call SET input = ?1 WHERE block_id = ?2",
                    params![input, block_id],
                )?;
                tx.execute(
                    "UPDATE blocks SET updated_at = ?1 WHERE id = ?2",
                    params![now, block_id],
                )?;
                Ok(())
            })
        })
        .await
    }

    // ─── User block insertion (from the composer) ────────────────────────

    /// Append the human's blocks, preceded by the day's date marker when the
    /// date has changed.
    ///
    /// # Errors
    ///
    /// If the transaction fails or the store's actor has stopped.
    pub async fn insert_user_blocks(
        &self,
        conversation_id: i64,
        blocks: Vec<InputBlock>,
    ) -> Result<Vec<i64>, StoreError> {
        self.insert_user_blocks_dated(
            conversation_id,
            blocks,
            super::date_markers::DateStamp::now_local(),
        )
        .await
    }

    /// The injectable-date seam behind [`insert_user_blocks`]: one atomic
    /// transaction that runs the date marker's change detection BEFORE the user
    /// blocks land. Production always routes through [`insert_user_blocks`]
    /// with the stamp built from now; tests drive midnight crossings and zone
    /// changes deterministically.
    pub(crate) async fn insert_user_blocks_dated(
        &self,
        conversation_id: i64,
        blocks: Vec<InputBlock>,
        stamp: super::date_markers::DateStamp,
    ) -> Result<Vec<i64>, StoreError> {
        self.run(move |conn| {
            let tx = conn.transaction()?;
            super::date_markers::ensure_date_marker(&tx, conversation_id, &stamp)?;
            let mut ids = Vec::with_capacity(blocks.len());
            for block in &blocks {
                ids.push(super::conversations::insert_input_block(
                    &tx,
                    conversation_id,
                    block,
                )?);
            }
            tx.commit()?;
            Ok(ids)
        })
        .await
    }

    /// Append text to the newest block of a given role and type in a
    /// conversation. Returns whether one was found.
    ///
    /// # Errors
    ///
    /// If the update fails or the store's actor has stopped.
    pub async fn append_to_latest_block(
        &self,
        conversation_id: i64,
        role: Role,
        block_type: String,
        text: String,
        updated_at: String,
    ) -> Result<bool, StoreError> {
        self.run(move |conn| {
            transact(conn, |tx| {
                let table = ContentTable::for_block_type(&block_type);
                let rows = tx.execute(
                    &format!(
                        "UPDATE {} SET content = content || ?1
                         WHERE block_id = ({})",
                        table.name(),
                        latest_block_selector(table, false)
                    ),
                    params![text, conversation_id, role.as_str(), block_type],
                )?;
                if rows > 0 {
                    tx.execute(
                        &format!(
                            "UPDATE blocks SET updated_at = ?1
                             WHERE id = ({})",
                            latest_block_selector(table, false)
                        ),
                        params![updated_at, conversation_id, role.as_str(), block_type],
                    )?;
                }
                Ok(rows > 0)
            })
        })
        .await
    }

    /// Append text to one block by id. `table` is one of the library's own
    /// content tables — statically known at every call site, never built from
    /// input.
    ///
    /// # Errors
    ///
    /// If the transaction fails or the store's actor has stopped.
    pub async fn append_to_block_by_id(
        &self,
        block_id: i64,
        table: &'static str,
        text: String,
        updated_at: String,
    ) -> Result<(), StoreError> {
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let rows = tx.execute(
                &format!("UPDATE {table} SET content = content || ?1 WHERE block_id = ?2"),
                params![text, block_id],
            )?;
            if rows > 0 {
                tx.execute(
                    "UPDATE blocks SET updated_at = ?1 WHERE id = ?2",
                    params![updated_at, block_id],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Read one block's accumulated content out of `table`. Same static-table
    /// contract as [`Store::append_to_block_by_id`].
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn get_block_content(
        &self,
        block_id: i64,
        table: &'static str,
    ) -> Result<Option<String>, StoreError> {
        self.run(move |conn| {
            conn.query_row(
                &format!("SELECT content FROM {table} WHERE block_id = ?1"),
                [block_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
        })
        .await
    }

    /// Replace the newest matching block's text outright. `before_ts` bounds
    /// the search to blocks created no later than it.
    ///
    /// The bound applies to BOTH statements this runs — the content and the
    /// `updated_at` stamp — because they are meant to be about the same block.
    /// Dropping it from the stamp made the two select different blocks the
    /// moment a newer one existed: the older block's text changed and the newer
    /// block was marked as having changed.
    ///
    /// # Errors
    ///
    /// If the update fails or the store's actor has stopped.
    pub async fn set_latest_block_text(
        &self,
        conversation_id: i64,
        role: Role,
        block_type: String,
        text: String,
        updated_at: String,
        before_ts: Option<String>,
    ) -> Result<(), StoreError> {
        self.run(move |conn| {
            transact(conn, |tx| {
                let table = ContentTable::for_block_type(&block_type);
                let bounded = before_ts.is_some();
                let selector = latest_block_selector(table, bounded);

                let content_sql = format!(
                    "UPDATE {} SET content = ?1 WHERE block_id = ({selector})",
                    table.name()
                );
                let stamp_sql =
                    format!("UPDATE blocks SET updated_at = ?1 WHERE id = ({selector})");

                // One parameter list, bound to both statements: the selector
                // reads the same parameters in each, so the two cannot pick
                // different blocks.
                if let Some(before) = &before_ts {
                    tx.execute(
                        &content_sql,
                        params![text, conversation_id, role.as_str(), block_type, before],
                    )?;
                    tx.execute(
                        &stamp_sql,
                        params![
                            updated_at,
                            conversation_id,
                            role.as_str(),
                            block_type,
                            before
                        ],
                    )?;
                } else {
                    tx.execute(
                        &content_sql,
                        params![text, conversation_id, role.as_str(), block_type],
                    )?;
                    tx.execute(
                        &stamp_sql,
                        params![updated_at, conversation_id, role.as_str(), block_type],
                    )?;
                }
                Ok(())
            })
        })
        .await
    }

    // ─── Block watcher support ───────────────────────────────────────────

    /// One junction row, read by the id the change hook announced: the
    /// conversation and the block it joins, from the SAME row.
    ///
    /// `None` when that row is gone by read time — a delete, or an insert its
    /// transaction rolled back. The hook announces row changes, not commits,
    /// so this is ordinary and it attributes nothing: there is no conversation
    /// to name and no block to name it for.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn joined_block(&self, junction_id: i64) -> Result<Option<JoinedBlock>, StoreError> {
        self.run(move |conn| {
            conn.query_row(
                "SELECT conversation_id, block_id FROM conversation_blocks WHERE id = ?1",
                [junction_id],
                |row| {
                    Ok(JoinedBlock {
                        conversation_id: row.get(0)?,
                        block_id: row.get(1)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
        })
        .await
    }

    /// EVERY conversation a block is joined to, in junction order.
    ///
    /// A block is shared by every fork that inherited it, so "which
    /// conversation" has no single answer and asking for one was the defect
    /// (2026-09-01): a `LIMIT 1` without an order let the index hand back the
    /// oldest join, and a fork's own writes were announced to the conversation
    /// it was forked FROM. A change to a block is a change for each
    /// conversation that reads it, so each is told.
    ///
    /// Empty is normal, not a fault: a header and its content row land before
    /// the junction row inside one flow, and a draft's blocks are joined to
    /// nothing by design. The junction row's own change carries the
    /// announcement.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn conversations_for_block(&self, block_id: i64) -> Result<Vec<i64>, StoreError> {
        self.run(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT conversation_id FROM conversation_blocks WHERE block_id = ?1 ORDER BY id",
            )?;
            let rows = stmt.query_map([block_id], |row| row.get::<_, i64>(0))?;
            Ok(rows.collect::<Result<Vec<i64>, _>>()?)
        })
        .await
    }

    // ─── Cleanup ─────────────────────────────────────────────────────────

    /// Restart-clean: delete every still-streaming block for a conversation.
    ///
    /// Targets only the ephemeral types — the transient `streaming`,
    /// `streaming_thinking` and `streaming_tool_call` types plus any kinds a
    /// descriptor declares ephemeral: the uncommitted partials a dropped
    /// stream left behind. Committed `text`, `thinking` and `tool_call` blocks
    /// and all `tool_result` and `tool_error` rows are untouched, so a
    /// regenerated turn starts from the last committed block with no
    /// duplication. Returns the number removed. Deleting the `blocks` rows
    /// cascades to their content tables and junction rows.
    ///
    /// # Errors
    ///
    /// If the delete fails or the store's actor has stopped.
    pub async fn delete_streaming_blocks(&self, conversation_id: i64) -> Result<u64, StoreError> {
        let descriptors = self.descriptors;
        self.run(move |conn| {
            let deleted = conn.execute(
                &format!(
                    "DELETE FROM blocks
                     WHERE block_type IN {}
                       AND id IN (
                           SELECT block_id FROM conversation_blocks WHERE conversation_id = ?1
                       )",
                    ephemeral_types_sql(descriptors)
                ),
                params![conversation_id],
            )?;
            tracing::debug!(
                conversation_id,
                deleted,
                "deleted streaming blocks (restart-clean)"
            );
            Ok(u64::try_from(deleted).unwrap_or(0))
        })
        .await
    }

    /// Delete every orphaned block — a block NOTHING points at. See
    /// `orphan_block_predicate` in this module for the rule, which is written
    /// once and is what every site here means by the word.
    ///
    /// Collection repeats until a pass takes nothing. One reference can only be
    /// released by collecting the block that carries it — a tool result points
    /// at the call it answers, so the call becomes collectable only once the
    /// result is gone — so a single pass would leave a tail behind that the
    /// caller has no way to know about. Each pass strictly shrinks the table,
    /// so this ends.
    ///
    /// # Errors
    ///
    /// If the delete fails or the store's actor has stopped.
    pub async fn gc_orphan_blocks(&self) -> Result<u64, StoreError> {
        let descriptors = self.descriptors;
        let gate = self.gate.clone();
        self.run(move |conn| {
            // The predicate reads every declared reference table, so a domain
            // whose migrations failed refuses collection with that failure
            // rather than running over schema in doubt.
            gate.ensure_each(descriptors.iter().map(|d| d.domain))?;
            transact(conn, |tx| {
                let sql = format!(
                    "DELETE FROM blocks WHERE {}",
                    orphan_block_predicate(descriptors)
                );
                let mut collected: u64 = 0;
                loop {
                    let deleted = tx.execute(&sql, [])?;
                    if deleted == 0 {
                        return Ok(collected);
                    }
                    collected += u64::try_from(deleted).unwrap_or(0);
                }
            })
        })
        .await
    }
}

/// Every column in the library's schema that points AT a block, other than a
/// block's own two rows (its junction row's `block_id` and its content row's
/// `block_id`, both of which carry `ON DELETE CASCADE` and go with it).
///
/// A block named by any of these is in use by something else, which is the
/// whole of what [`orphan_block_predicate`] asks. A new reference column joins
/// this list and the rule follows it — there is nowhere else to change.
const BLOCK_REFERENCES: &[(&str, &[&str])] = &[
    // The junction: what links a block to a conversation at all.
    ("conversation_blocks", &["block_id"]),
    // A quote's two endpoints.
    ("block_quote", &["start_block_id", "end_block_id"]),
    // The approval chain: the block a request covers, the request a decision
    // answers.
    ("block_approval_request", &["for_block_id"]),
    ("block_approval_decision", &["for_block_id"]),
    // A resolved call points back at the call it resolves.
    ("block_tool_result", &["source_block_id"]),
    ("block_tool_error", &["source_block_id"]),
    // The second ledger's rows name the block they were derived from.
    ("metadata", &["source_block_id"]),
    // The self-referential arm (2026-08-22, in lockstep with the pinned
    // predicate literal): a turn product's header names its summoning
    // frontier through `dispatch_anchor`, and an identity that can dangle is
    // the refuted shape-walk wearing a column — so an anchored-at block is a
    // referenced block, and fork-then-delete leaves the anchor target loadable
    // until its last anchorer is collected.
    ("blocks", &["dispatch_anchor"]),
];

/// **What makes a block an orphan, written once.**
///
/// A block is an orphan when NOTHING points at it: no junction row links it to
/// a conversation, and no other row names it through any column in
/// [`BLOCK_REFERENCES`].
///
/// The junction half alone is not the rule, and reading it as the rule broke
/// collection outright. A new thread's deep copy clones its quote targets as
/// junction-less rows on purpose — that is what keeps a fork's quotes readable
/// after the source conversation is deleted — so "no junction row" describes a
/// block that is very much in use. And the references have no delete rule while
/// foreign keys are on, so the delete did not skip such a block: it aborted the
/// whole statement on it, and once one existed, nothing in the database was ever
/// collected again.
///
/// Quotes are where that was found, and every reference column is the same
/// defect wearing a different name — a fork that copies an approval block into a
/// new thread leaves it pointing at a block of the source conversation, which is
/// the identical shape. The rule is stated over the references, not over one of
/// them.
///
/// Built as a SQL predicate over `blocks` so the definition and the statement
/// that acts on it cannot drift apart. The predicate is the library's literal
/// reference union plus every reference column the descriptors declare — a
/// consumer table pointing at a block keeps that block alive exactly as a
/// quote's endpoint does. With no descriptors the output is byte-identical to
/// the core-only form, and a test pins that.
pub(super) fn orphan_block_predicate(descriptors: &[ContentDescriptor]) -> String {
    BLOCK_REFERENCES
        .iter()
        .map(|(table, columns)| {
            let names = columns
                .iter()
                .map(|column| format!("r.{column} = blocks.id"))
                .collect::<Vec<_>>()
                .join(" OR ");
            format!("NOT EXISTS (SELECT 1 FROM {table} r WHERE {names})")
        })
        .chain(
            descriptors
                .iter()
                .flat_map(|d| d.reference_columns.iter())
                .map(|reference| {
                    // Descriptor-supplied identifiers are quoted everywhere they
                    // enter generated SQL; this arm was the one site left bare,
                    // and a keyword-named reference column killed collection
                    // outright with a syntax error.
                    format!(
                        "NOT EXISTS (SELECT 1 FROM {} r WHERE r.{} = blocks.id)",
                        super::descriptors::quoted(reference.table),
                        super::descriptors::quoted(reference.column)
                    )
                }),
        )
        .collect::<Vec<_>>()
        .join("\n    AND ")
}
/// The content tables a streamed-into block accumulates in.
///
/// A type and not a string because the name is interpolated into SQL: the set
/// of tables that can ever reach [`latest_block_selector`] is these two, and
/// the compiler is what says so. Nothing derived from caller input can name a
/// third.
#[derive(Clone, Copy)]
enum ContentTable {
    Text,
    Thinking,
}

impl ContentTable {
    /// Which table a block type accumulates in.
    fn for_block_type(block_type: &str) -> Self {
        match block_type {
            "thinking" | "streaming_thinking" => Self::Thinking,
            _ => Self::Text,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Text => "block_text",
            Self::Thinking => "block_thinking",
        }
    }
}

/// The subquery selecting the newest block of a role and type in a
/// conversation. Written once because several statements need exactly it, and
/// copies of one selector are chances for them to disagree.
///
/// # The caller owns parameters 1 and 5
///
/// This is a fragment embedded in someone else's statement, and it reads that
/// statement's parameters by NUMBER:
///
///   - `?1` is the embedding statement's own value — the text to write, the
///     stamp to set — and is never read here.
///   - `?2` conversation id, `?3` role, `?4` block type.
///   - `?5` is the `created_at` upper bound, read only when `before_ts` is set,
///     and it must then be bound by every statement this fragment goes into.
///
/// A caller that reorders its parameters silently changes what this selects.
/// Both statements of one operation pass the same `before_ts` for the same
/// reason: a fragment that reads different parameters in two statements is two
/// different selectors wearing one name.
fn latest_block_selector(table: ContentTable, before_ts: bool) -> String {
    let ts_filter = if before_ts {
        "AND b.created_at <= ?5"
    } else {
        ""
    };
    format!(
        "SELECT b.id FROM blocks b
         JOIN conversation_blocks cb ON cb.block_id = b.id
         JOIN {} ct ON ct.block_id = b.id
         WHERE cb.conversation_id = ?2 AND ct.role = ?3 AND b.block_type = ?4
           {ts_filter}
         ORDER BY b.created_at DESC, b.id DESC LIMIT 1",
        table.name()
    )
}

/// Round-trip of the opaque reasoning payloads: the finalization INSERT writes
/// the columns (and the sidecar) in one transaction; `list_blocks` reconstructs
/// the exact variant onto the block's `opaque` field; junction-shared
/// continuations see the same payload with zero copy code.
#[cfg(test)]
mod reasoning_payload_tests {
    use super::{OpaquePayload, Role, Store};
    use crate::block::ReasoningDetailEntry;

    async fn fixture() -> (Store, i64) {
        let store = Store::in_memory().unwrap();
        let conv = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();
        (store, conv)
    }

    async fn read_back(store: &Store, conv: i64, block_id: i64) -> OpaquePayload {
        let blocks = store.list_blocks(conv).await.unwrap();
        let block = blocks
            .iter()
            .find(|b| b.id == block_id)
            .expect("block listed");
        assert_eq!(block.block_type, "thinking");
        assert!(
            !block.fields.contains_key("opaque_kind"),
            "raw columns are resolved away, never leaked"
        );
        serde_json::from_value(block.fields["opaque"].clone()).expect("payload deserializes")
    }

    fn multi_entry_payload() -> OpaquePayload {
        OpaquePayload::OpenRouter {
            entries: vec![
                ReasoningDetailEntry {
                    position: 0,
                    entry_type: "reasoning.text".into(),
                    entry_id: Some("rd_0".into()),
                    upstream_format: "anthropic-claude-v1".into(),
                    index: Some(0),
                    content: "step one".into(),
                    signature: Some("sig-1".into()),
                },
                ReasoningDetailEntry {
                    position: 1,
                    entry_type: "reasoning.encrypted".into(),
                    entry_id: None,
                    upstream_format: "google-gemini-v1".into(),
                    index: None,
                    content: "AAAA".into(),
                    signature: None,
                },
            ],
        }
    }

    /// Every scalar variant round-trips through its columns byte-exactly.
    #[tokio::test]
    async fn scalar_payload_variants_round_trip() {
        let (store, conv) = fixture().await;
        for payload in [
            OpaquePayload::OpenAiResponses {
                item_id: "rs_1".into(),
                encrypted_content: "gAAAA".into(),
            },
            OpaquePayload::Anthropic {
                signature: "sig-xyz".into(),
            },
            OpaquePayload::Mistral,
        ] {
            let id = store
                .insert_thinking_block_with_content(
                    conv,
                    Role::Assistant,
                    "thought".into(),
                    None,
                    Some(payload.clone()),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(read_back(&store, conv, id).await, payload);
        }
    }

    /// The multi-entry variant round-trips through the relational sidecar —
    /// entries come back in position order with every field intact.
    #[tokio::test]
    async fn multi_entry_payload_round_trips_through_ordered_sidecar() {
        let (store, conv) = fixture().await;
        let payload = multi_entry_payload();
        let id = store
            .insert_thinking_block_with_content(
                conv,
                Role::Assistant,
                "thought".into(),
                None,
                Some(payload.clone()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(read_back(&store, conv, id).await, payload);
    }

    /// A payload-less thinking block — the common case — reads back with no
    /// `opaque` field at all: a NULL kind means no continuity payload.
    #[tokio::test]
    async fn payload_less_block_has_no_opaque_field() {
        let (store, conv) = fixture().await;
        let id = store
            .insert_thinking_block_with_content(
                conv,
                Role::Assistant,
                "plain".into(),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let blocks = store.list_blocks(conv).await.unwrap();
        let block = blocks.iter().find(|b| b.id == id).unwrap();
        assert!(!block.fields.contains_key("opaque"));
    }

    /// Junction share: a rerun continuation shares the payload-bearing thinking
    /// block via junction rows — the fork reads the SAME row, sidecar included,
    /// with zero copy code.
    #[tokio::test]
    async fn rerun_continuation_shares_payload_via_junction() {
        use crate::store::{Continuation, ModelOverride};
        use crate::types::InputBlock;

        let (store, conv) = fixture().await;
        store
            .insert_text_block(conv, Role::User, "question".into())
            .await
            .unwrap();
        let payload = multi_entry_payload();
        let thinking_id = store
            .insert_thinking_block_with_content(
                conv,
                Role::Assistant,
                "prior reasoning".into(),
                None,
                Some(payload.clone()),
                None,
            )
            .await
            .unwrap();
        store
            .insert_text_block(conv, Role::Assistant, "answer".into())
            .await
            .unwrap();
        let anchor = store
            .insert_user_blocks(
                conv,
                vec![InputBlock::Text {
                    content: "again?".into(),
                }],
            )
            .await
            .unwrap()[0];

        let fork = store
            .fork_continuation(conv, anchor, Continuation::Rerun, ModelOverride::default())
            .await
            .unwrap();

        let fork_blocks = store.list_blocks(fork).await.unwrap();
        let shared = fork_blocks
            .iter()
            .find(|b| b.id == thinking_id)
            .expect("the fork shares the very same thinking block row");
        let read: OpaquePayload = serde_json::from_value(shared.fields["opaque"].clone()).unwrap();
        assert_eq!(
            read, payload,
            "payload and sidecar intact through the junction share"
        );
    }
}
