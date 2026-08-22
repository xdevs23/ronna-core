//! Conversation rows, the processed cursor, and forking.

use rusqlite::{OptionalExtension, params};
use tracing::warn;

use crate::types::InputBlock;

use super::block_cloner::BlockCloner;
use super::messages::insert_block;
use super::models::resolve_model;
use super::{Store, StoreError, transact};

/// The model a conversation runs on, denormalized for reading.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConversationModel {
    /// The model row's id.
    pub id: i64,
    /// The provider's own identifier for the model.
    pub external_id: String,
    /// The name a human reads.
    pub display_name: String,
    /// Who trained it.
    pub vendor: String,
    /// The provider instance it is reached through.
    pub provider_id: String,
    /// That instance's name, falling back to its id.
    pub provider_name: String,
}

/// One conversation row, with its model and its latest derived title.
///
/// The title is not a column: it is folded out of the metadata ledger at read
/// time, because a fact that can be derived is not stored.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Conversation {
    /// The conversation's id.
    pub id: i64,
    /// The conversation this one forked from, if any.
    pub parent_id: Option<i64>,
    /// The latest title the metadata ledger derived, if any.
    pub title: Option<String>,
    /// The model this conversation runs on.
    pub model: ConversationModel,
    /// The selected reasoning level as its canonical key, or `None` to defer to
    /// the provider's own default. Stored as the key: the typed level is the
    /// provider layer's word, and the store does not need to know it to keep it.
    pub reasoning: Option<String>,
    /// When the conversation was created.
    pub created_at: String,
}

/// A point where one conversation's history splits into forks.
#[derive(Debug, Clone, serde::Serialize)]
pub struct BranchPoint {
    /// The last block the forks share with their parent.
    pub block_id: i64,
    /// How many forks branch off it.
    pub branch_count: i64,
}

/// Grouped model override for fork operations.
///
/// When both `provider_id` and `external_id` are `Some`, the pair is applied;
/// otherwise the source conversation's model is inherited.
#[derive(Debug, Clone, Default)]
pub struct ModelOverride {
    /// The provider instance to switch to.
    pub provider_id: Option<String>,
    /// The model to switch to, within that provider.
    pub external_id: Option<String>,
    /// The switched-to model's display name; defaults to its external id.
    pub display_name: Option<String>,
    /// The switched-to model's vendor.
    pub vendor: Option<String>,
    /// Reasoning level. `None` inherits from the source, `Some(empty)` defers
    /// to the provider's default, `Some(non-empty)` sets the level.
    pub reasoning: Option<String>,
}

/// How the fork should carry the target user-message group forward.
pub enum Continuation {
    /// The group is shared verbatim with the source via junction rows. No
    /// blocks are inserted.
    Rerun,
    /// The group is cut off before the user's first block and replaced by these
    /// blocks — the composer's edited payload.
    Edit(Vec<InputBlock>),
    /// The fork has no parent. The group's blocks are deep-copied into fresh
    /// `blocks` rows. Quote-target blocks referenced by quotes in the group are
    /// also duplicated as *detached* blocks — no junction row — so deleting the
    /// source conversation cannot orphan the quotes.
    NewThread {
        /// The new thread's system prompt, appended ahead of everything else.
        /// `None` starts the thread without one.
        ///
        /// It is a parameter because a prompt is a consumer's own words: the
        /// code this was extracted from read a constant here, and that constant
        /// stayed with the application that wrote it.
        system_prompt: Option<String>,
    },
}

impl Store {
    /// Create a conversation on a model, resolving the model row if it is new.
    ///
    /// # Errors
    ///
    /// If the insert fails or the store's actor has stopped.
    pub async fn create_conversation(
        &self,
        provider_id: String,
        external_id: String,
        display_name: String,
        vendor: String,
    ) -> Result<i64, StoreError> {
        // Resolving the model can itself append a row, so the model and the
        // conversation that names it land together or not at all.
        self.run(move |conn| {
            transact(conn, |tx| {
                let model_id =
                    resolve_model(tx, &provider_id, &external_id, &display_name, &vendor)?;
                tx.execute(
                    "INSERT INTO conversations (model_id) VALUES (?1)",
                    [model_id],
                )?;
                Ok(tx.last_insert_rowid())
            })
        })
        .await
    }

    /// One conversation by id.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn find_conversation(&self, id: i64) -> Result<Option<Conversation>, StoreError> {
        self.run(move |conn| {
            conn.prepare(&format!("{CONVERSATION_SELECT} WHERE c.id = ?1"))?
                .query_row([id], row_to_conversation)
                .optional()
                .map_err(Into::into)
        })
        .await
    }

    /// Every conversation, newest first.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn list_conversations(&self) -> Result<Vec<Conversation>, StoreError> {
        self.run(|conn| {
            let mut stmt =
                conn.prepare(&format!("{CONVERSATION_SELECT} ORDER BY c.created_at DESC"))?;
            let rows = stmt
                .query_map([], row_to_conversation)?
                .filter_map(|r| {
                    r.map_err(|e| warn!(error = %e, "skipping corrupted conversation"))
                        .ok()
                })
                .collect();
            Ok(rows)
        })
        .await
    }

    /// The forks of one conversation, oldest first.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn list_branches(&self, parent_id: i64) -> Result<Vec<Conversation>, StoreError> {
        self.run(move |conn| {
            let mut stmt = conn.prepare(&format!(
                "{CONVERSATION_SELECT} WHERE c.parent_id = ?1 ORDER BY c.created_at"
            ))?;
            let rows = stmt
                .query_map([parent_id], row_to_conversation)?
                .filter_map(|r| {
                    r.map_err(|e| warn!(error = %e, "skipping corrupted conversation"))
                        .ok()
                })
                .collect();
            Ok(rows)
        })
        .await
    }

    /// Set or clear a conversation's reasoning level key.
    ///
    /// # Errors
    ///
    /// If the update fails or the store's actor has stopped.
    pub async fn set_conversation_reasoning(
        &self,
        id: i64,
        reasoning: Option<String>,
    ) -> Result<(), StoreError> {
        self.run(move |conn| {
            conn.execute(
                "UPDATE conversations SET reasoning = ?1 WHERE id = ?2",
                params![reasoning, id],
            )?;
            Ok(())
        })
        .await
    }

    /// Delete a conversation. Its junction rows, drafts and metadata cascade;
    /// blocks shared with a fork survive and are collected by
    /// [`gc_orphan_blocks`](Store::gc_orphan_blocks) once nothing links them.
    ///
    /// Dispatch anchors pointing INTO the deleted conversation are nulled in
    /// the same transaction (2026-08-22): a fork's kept cross-conversation
    /// anchor is a reference the collector honors, so left standing it would
    /// keep the deleted conversation's blocks alive forever — deletion that
    /// cannot delete — and let a provenance read cross into a conversation
    /// that no longer exists. Null is the documented answer a reader already
    /// folds; an anchor whose target is junction-shared with a SURVIVING
    /// conversation stays, because its target remains that conversation's
    /// readable history.
    ///
    /// # Errors
    ///
    /// If the delete fails or the store's actor has stopped.
    pub async fn delete_conversation(&self, id: i64) -> Result<(), StoreError> {
        self.run(move |conn| {
            transact(conn, |tx| {
                tx.execute(
                    "UPDATE blocks SET dispatch_anchor = NULL
                     WHERE dispatch_anchor IN (
                         SELECT cb.block_id FROM conversation_blocks cb
                         WHERE cb.conversation_id = ?1
                           AND NOT EXISTS (
                               SELECT 1 FROM conversation_blocks survivor
                               WHERE survivor.block_id = cb.block_id
                                 AND survivor.conversation_id != ?1
                           )
                     )",
                    [id],
                )?;
                tx.execute("DELETE FROM conversations WHERE id = ?1", [id])?;
                Ok(())
            })
        })
        .await
    }

    // ─── The processed cursor ───────────────────────────────────────────

    /// How far the ratchet has CONFIRMED driving this conversation's ledger.
    /// 0 means nothing is confirmed — the drive re-derives from the start.
    ///
    /// # Errors
    ///
    /// If the conversation does not exist, or the store's actor has stopped.
    pub async fn cursor(&self, conversation_id: i64) -> Result<i64, StoreError> {
        self.run(move |conn| {
            conn.query_row(
                "SELECT last_processed_block_id FROM conversations WHERE id = ?1",
                [conversation_id],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .await
    }

    /// Advance the processed cursor — the ONE mutable single-field write on a
    /// conversation's orchestration state; block storage stays append-only and
    /// immutable.
    ///
    /// # This write wakes the loop that made it — ruled, 2026-08-21
    ///
    /// The row lives in `conversations`, which the change hook names, so
    /// confirming progress announces a change and the drive loop ticks again.
    /// The ruling, made with the actor slice as promised here: the scheduler
    /// TOLERATES the edge rather than filtering the wake. The cost is one
    /// extra no-op tick per confirm and it is bounded — the extra tick
    /// re-reads, confirms nothing new, writes nothing, and therefore announces
    /// nothing, so the edge feeds back exactly once and rests. The test
    /// `actor::tests::cursor_confirm_wakes_once_and_converges_to_rest` pins
    /// that convergence. Filtering was rejected because the wake path is
    /// deliberately kind- and column-blind: a filter would need the scheduler
    /// to sort table columns into "orchestration" and "content", a
    /// distinction nothing else in the machinery draws, and the first new
    /// column would silently fall on the wrong side of it.
    ///
    /// # Errors
    ///
    /// If the update fails or the store's actor has stopped.
    pub async fn update_cursor(
        &self,
        conversation_id: i64,
        block_id: i64,
    ) -> Result<(), StoreError> {
        self.run(move |conn| {
            conn.execute(
                "UPDATE conversations SET last_processed_block_id = ?1 WHERE id = ?2",
                params![block_id, conversation_id],
            )?;
            Ok(())
        })
        .await
    }

    // ─── Forking ────────────────────────────────────────────────────────

    /// Fork a conversation, inheriting its history up to and including
    /// `up_to_block_id` through junction rows.
    ///
    /// # Errors
    ///
    /// If the source or the block does not exist, if the transaction fails, or
    /// if the store's actor has stopped.
    pub async fn fork_conversation(
        &self,
        source_id: i64,
        up_to_block_id: i64,
        model: ModelOverride,
    ) -> Result<i64, StoreError> {
        self.run(move |conn| {
            let model_id = resolve_model_for_fork(conn, source_id, &model)?;
            let reasoning = resolve_reasoning_for_fork(conn, source_id, &model)?;

            let tx = conn.transaction()?;
            let fork_id =
                insert_conversation(&tx, Some(source_id), model_id, reasoning.as_deref())?;
            copy_junction_up_to(&tx, source_id, fork_id, up_to_block_id)?;
            confirm_inherited_history(&tx, source_id, fork_id)?;
            tx.commit()?;
            Ok(fork_id)
        })
        .await
    }

    /// Fork a conversation carrying a user-message group forward via the given
    /// [`Continuation`]. Runs entirely in one transaction, so the scheduler sees
    /// a single atomic change through the row change hook.
    ///
    /// `block_id` identifies *any* block inside the target user message; the
    /// group bounds are walked internally, contiguous by role.
    ///
    /// # Errors
    ///
    /// If the anchor block is not in the source conversation, if the
    /// transaction fails, or if the store's actor has stopped.
    pub async fn fork_continuation(
        &self,
        source_id: i64,
        block_id: i64,
        continuation: Continuation,
        model: ModelOverride,
    ) -> Result<i64, StoreError> {
        let descriptors = self.descriptors;
        let gate = self.gate.clone();
        self.run(move |conn| {
            let model_id = resolve_model_for_fork(conn, source_id, &model)?;
            let reasoning = resolve_reasoning_for_fork(conn, source_id, &model)?;

            let tx = conn.transaction()?;

            let group = find_group_bounds(&tx, descriptors, &gate, source_id, block_id)?;

            let new_id = match &continuation {
                Continuation::Rerun => {
                    let new_id =
                        insert_conversation(&tx, Some(source_id), model_id, reasoning.as_deref())?;
                    copy_junction_up_to(&tx, source_id, new_id, group.last_block_id)?;
                    confirm_inherited_history(&tx, source_id, new_id)?;
                    new_id
                }
                Continuation::Edit(blocks) => {
                    let new_id =
                        insert_conversation(&tx, Some(source_id), model_id, reasoning.as_deref())?;
                    copy_junction_before(&tx, source_id, new_id, group.first_block_id)?;
                    confirm_inherited_history(&tx, source_id, new_id)?;
                    // The edited message is fresh user content owing a turn, so
                    // it gets the same date change-detection as any append — an
                    // edit resubmitted a day after the source was last written
                    // must correct the inherited date, not carry it stale.
                    super::date_markers::ensure_date_marker(
                        &tx,
                        new_id,
                        &super::date_markers::today_local(),
                    )?;
                    for block in blocks {
                        insert_input_block(&tx, new_id, block)?;
                    }
                    new_id
                }
                Continuation::NewThread { system_prompt } => {
                    // Nothing is junction-inherited from the source — the
                    // prompt and the deep-copied group are fork-authored and
                    // never driven by anyone, so nothing is confirmed: the
                    // cursor stays 0 and the first drive derives the whole
                    // (tiny) ledger.
                    let new_id = insert_conversation(&tx, None, model_id, reasoning.as_deref())?;
                    if let Some(prompt) = system_prompt {
                        insert_system_prompt_block(&tx, new_id, prompt)?;
                    }
                    // The deep-copied group is the fresh thread's first turn,
                    // owed to the model today — precede it with the date marker
                    // so the model knows the date on turn one (same intent as
                    // the append path; the thread has no inherited marker).
                    super::date_markers::ensure_date_marker(
                        &tx,
                        new_id,
                        &super::date_markers::today_local(),
                    )?;
                    deep_copy_group_into(&tx, descriptors, source_id, new_id, &group)?;
                    new_id
                }
            };

            tx.commit()?;
            Ok(new_id)
        })
        .await
    }

    /// Detach a conversation from its parent, leaving its inherited blocks in
    /// place.
    ///
    /// # Errors
    ///
    /// If the update fails or the store's actor has stopped.
    pub async fn unlink_conversation(&self, id: i64) -> Result<(), StoreError> {
        self.run(move |conn| {
            conn.execute(
                "UPDATE conversations SET parent_id = NULL WHERE id = ?1",
                [id],
            )?;
            Ok(())
        })
        .await
    }

    /// Where this conversation's forks split off, and how many split at each
    /// point.
    ///
    /// # Errors
    ///
    /// If the query fails or the store's actor has stopped.
    pub async fn branch_points(
        &self,
        conversation_id: i64,
    ) -> Result<Vec<BranchPoint>, StoreError> {
        self.run(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT fork_cb.block_id, COUNT(*) as branch_count
                 FROM conversations c
                 JOIN (
                     SELECT cb.conversation_id, MAX(cb.id) as max_junction_id
                     FROM conversation_blocks cb
                     WHERE cb.conversation_id IN (SELECT id FROM conversations WHERE parent_id = ?1)
                     AND cb.block_id IN (SELECT block_id FROM conversation_blocks WHERE conversation_id = ?1)
                     GROUP BY cb.conversation_id
                 ) last_shared ON last_shared.conversation_id = c.id
                 JOIN conversation_blocks fork_cb ON fork_cb.id = last_shared.max_junction_id
                 WHERE c.parent_id = ?1
                 GROUP BY fork_cb.block_id",
            )?;
            let rows = stmt
                .query_map([conversation_id], |row| {
                    Ok(BranchPoint {
                        block_id: row.get(0)?,
                        branch_count: row.get(1)?,
                    })
                })?
                .filter_map(|r| {
                    r.map_err(|e| warn!(error = %e, "skipping corrupted branch point")).ok()
                })
                .collect();
            Ok(rows)
        })
        .await
    }
}

// ─── Fork helpers ───────────────────────────────────────────────────────

/// The block ids that form one role-contiguous run in a conversation's ledger.
///
/// The walk that fills it is role-blind: it takes the anchor block's role,
/// whatever that is, and extends while the neighbours carry the same one. A
/// user message is what the fork paths anchor on, so that is the run they get —
/// but nothing here is limited to the user's voice.
struct GroupBounds {
    first_block_id: i64,
    last_block_id: i64,
    block_ids: Vec<i64>,
}

fn resolve_model_for_fork(
    conn: &rusqlite::Connection,
    source_id: i64,
    model: &ModelOverride,
) -> Result<i64, StoreError> {
    match (&model.provider_id, &model.external_id) {
        (Some(pid), Some(eid)) => {
            let name = model.display_name.as_deref().unwrap_or(eid);
            let vendor = model.vendor.as_deref().unwrap_or("");
            resolve_model(conn, pid, eid, name, vendor)
        }
        _ => Ok(conn.query_row(
            "SELECT model_id FROM conversations WHERE id = ?1",
            [source_id],
            |row| row.get(0),
        )?),
    }
}

/// Resolve a fork override against the source's stored value, with the shared
/// `None` = inherit, `Some(empty)` = clear, `Some(non-empty)` = set semantics.
fn resolve_override_for_fork(
    conn: &rusqlite::Connection,
    source_id: i64,
    column: &str,
    override_value: Option<&String>,
) -> Result<Option<String>, StoreError> {
    match override_value {
        Some(v) if v.is_empty() => Ok(None),
        Some(v) => Ok(Some(v.clone())),
        None => Ok(conn.query_row(
            &format!("SELECT {column} FROM conversations WHERE id = ?1"),
            [source_id],
            |row| row.get(0),
        )?),
    }
}

fn resolve_reasoning_for_fork(
    conn: &rusqlite::Connection,
    source_id: i64,
    model: &ModelOverride,
) -> Result<Option<String>, StoreError> {
    resolve_override_for_fork(conn, source_id, "reasoning", model.reasoning.as_ref())
}

/// Walk the source conversation to find the role-contiguous group containing
/// `anchor_block_id`. The load runs the descriptor overlay, so a consumer
/// block's role places it in its group exactly as a library block's does.
fn find_group_bounds(
    conn: &rusqlite::Connection,
    descriptors: &'static [super::descriptors::ContentDescriptor],
    gate: &super::DomainGate,
    source_id: i64,
    anchor_block_id: i64,
) -> Result<GroupBounds, StoreError> {
    let blocks = super::blocks::load_blocks_for_conversation(conn, descriptors, gate, source_id)?;
    let idx = blocks
        .iter()
        .position(|b| b.id == anchor_block_id)
        .ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    let target_role = blocks[idx].role;

    let mut start = idx;
    while start > 0 && blocks[start - 1].role == target_role {
        start -= 1;
    }
    let mut end = idx;
    while end + 1 < blocks.len() && blocks[end + 1].role == target_role {
        end += 1;
    }

    let block_ids: Vec<i64> = blocks[start..=end].iter().map(|b| b.id).collect();
    Ok(GroupBounds {
        first_block_id: blocks[start].id,
        last_block_id: blocks[end].id,
        block_ids,
    })
}

fn insert_conversation(
    conn: &rusqlite::Connection,
    parent_id: Option<i64>,
    model_id: i64,
    reasoning: Option<&str>,
) -> Result<i64, StoreError> {
    conn.execute(
        "INSERT INTO conversations (parent_id, model_id, reasoning) VALUES (?1, ?2, ?3)",
        params![parent_id, model_id, reasoning],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Initialize a fork's processed cursor to
/// **min(the source's confirmed cursor, the last inherited ledger position)**:
/// inherited history is confirmed only insofar as the SOURCE confirmed it.
///
/// Stamping past a block the source never drove to done is a skip-ahead — a
/// dangling call would be declared confirmed and render as an unpaired tool use
/// on the wire forever. Capped, the fork's first drive re-drives from the
/// source's frontier and heals anything the source had not settled, exactly as
/// the source itself would.
///
/// Called at the point in the fork transaction where the inherited junction
/// rows end; derived from what the transaction actually junctioned, never
/// assumed. No junction rows — or a never-driven source — leaves the default 0,
/// re-deriving from the start. Comparing raw block ids is sound because every
/// junction append points at a just-created block (and copies preserve order),
/// so ids ascend along junction order in every conversation.
fn confirm_inherited_history(
    conn: &rusqlite::Connection,
    source_id: i64,
    conversation_id: i64,
) -> Result<(), StoreError> {
    let inherited_tail: Option<i64> = conn
        .query_row(
            "SELECT block_id FROM conversation_blocks
             WHERE conversation_id = ?1 ORDER BY id DESC LIMIT 1",
            [conversation_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(inherited_tail) = inherited_tail else {
        return Ok(());
    };
    let source_cursor: i64 = conn.query_row(
        "SELECT last_processed_block_id FROM conversations WHERE id = ?1",
        [source_id],
        |row| row.get(0),
    )?;
    let confirmed = inherited_tail.min(source_cursor);
    if confirmed > 0 {
        conn.execute(
            "UPDATE conversations SET last_processed_block_id = ?1 WHERE id = ?2",
            params![confirmed, conversation_id],
        )?;
    }
    Ok(())
}

/// Copy source junction rows up to and including `last_block_id`.
fn copy_junction_up_to(
    conn: &rusqlite::Connection,
    source_id: i64,
    dst_id: i64,
    last_block_id: i64,
) -> Result<(), StoreError> {
    let cutoff: i64 = conn.query_row(
        "SELECT id FROM conversation_blocks WHERE conversation_id = ?1 AND block_id = ?2",
        (source_id, last_block_id),
        |row| row.get(0),
    )?;
    conn.execute(
        "INSERT INTO conversation_blocks (conversation_id, block_id)
         SELECT ?1, block_id FROM conversation_blocks
         WHERE conversation_id = ?2 AND id <= ?3
         ORDER BY id",
        (dst_id, source_id, cutoff),
    )?;
    Ok(())
}

/// Copy source junction rows strictly before `first_block_id`.
fn copy_junction_before(
    conn: &rusqlite::Connection,
    source_id: i64,
    dst_id: i64,
    first_block_id: i64,
) -> Result<(), StoreError> {
    let cutoff: i64 = conn.query_row(
        "SELECT id FROM conversation_blocks WHERE conversation_id = ?1 AND block_id = ?2",
        (source_id, first_block_id),
        |row| row.get(0),
    )?;
    conn.execute(
        "INSERT INTO conversation_blocks (conversation_id, block_id)
         SELECT ?1, block_id FROM conversation_blocks
         WHERE conversation_id = ?2 AND id < ?3
         ORDER BY id",
        (dst_id, source_id, cutoff),
    )?;
    Ok(())
}

/// Append one composer block to a conversation, returning its block id. The
/// user-block append path and the fork's edit path both land here: one insert,
/// one place.
pub(super) fn insert_input_block(
    conn: &rusqlite::Connection,
    conversation_id: i64,
    block: &InputBlock,
) -> Result<i64, StoreError> {
    match block {
        InputBlock::Text { content } => {
            let id = insert_block(conn, conversation_id, "text")?;
            conn.execute(
                "INSERT INTO block_text (block_id, role, content) VALUES (?1, 'user', ?2)",
                params![id, content],
            )?;
            Ok(id)
        }
        InputBlock::Quote {
            start_block_id,
            start_pos,
            end_block_id,
            end_pos,
        } => {
            let id = insert_block(conn, conversation_id, "quote")?;
            conn.execute(
                "INSERT INTO block_quote
                    (block_id, role, start_block_id, start_pos, end_block_id, end_pos)
                 VALUES (?1, 'user', ?2, ?3, ?4, ?5)",
                params![id, start_block_id, start_pos, end_block_id, end_pos],
            )?;
            Ok(id)
        }
    }
}

/// Append a new thread's system prompt. Called from inside the fork
/// transaction, so its header, junction and content rows are already atomic.
fn insert_system_prompt_block(
    conn: &rusqlite::Connection,
    conversation_id: i64,
    prompt: &str,
) -> Result<(), StoreError> {
    let id = insert_block(conn, conversation_id, "system_prompt")?;
    conn.execute(
        "INSERT INTO block_text (block_id, role, content) VALUES (?1, 'system', ?2)",
        params![id, prompt],
    )?;
    Ok(())
}

/// For each quote block in the group, collect the ids of the text-content
/// blocks its range touches that are *not* already in the group. These are the
/// blocks that must be deep-copied as detached rows for a new thread.
///
/// The range is the one the SOURCE conversation's ledger describes — the same
/// walk that resolves the quote's text, so what gets copied and what gets read
/// are the same blocks. Taking a bare id range instead would drag in whatever
/// another conversation appended between the endpoints.
///
/// **The ids come back in ascending source order, which is the order they must
/// be cloned in.** A quote's endpoints are rewritten to the clones' ids, and a
/// range only reads forwards, so the clones have to ascend the way their
/// sources do. Collected in first-seen order, two overlapping quotes in one
/// group hand back ids the second quote reaches backwards over — its rewritten
/// range comes out inverted and reads back empty.
fn collect_quote_targets(
    conn: &rusqlite::Connection,
    source_id: i64,
    group_block_ids: &[i64],
) -> Result<Vec<i64>, StoreError> {
    let group: std::collections::HashSet<i64> = group_block_ids.iter().copied().collect();
    let mut seen: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut ordered: Vec<i64> = Vec::new();

    for &block_id in group_block_ids {
        let block_type: String = conn.query_row(
            "SELECT block_type FROM blocks WHERE id = ?1",
            [block_id],
            |row| row.get(0),
        )?;
        if block_type != "quote" {
            continue;
        }
        let (sb, eb): (i64, i64) = conn.query_row(
            "SELECT start_block_id, end_block_id FROM block_quote WHERE block_id = ?1",
            [block_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let covered = super::blocks::quoted_text_blocks(conn, Some(source_id), sb, eb);
        for (id, _) in covered {
            if !group.contains(&id) && seen.insert(id) {
                ordered.push(id);
            }
        }
    }
    ordered.sort_unstable();
    Ok(ordered)
}

/// Deep-copy a user-message group into a fresh conversation. Group members are
/// cloned and linked to `dst_id`; every text block reachable through quotes in
/// the group is cloned as a *detached* block (no junction row), so the new
/// thread's quotes stay resolvable even if the source is deleted.
fn deep_copy_group_into(
    conn: &rusqlite::Connection,
    descriptors: &'static [super::descriptors::ContentDescriptor],
    source_id: i64,
    dst_id: i64,
    group: &GroupBounds,
) -> Result<(), StoreError> {
    let detached_targets = collect_quote_targets(conn, source_id, &group.block_ids)?;
    let mut cloner = BlockCloner::new(conn, descriptors);

    // Detached first — populates the remap so any quote in the group below
    // picks up the rewritten start and end ids when it is cloned. In ascending
    // source order, so the clones ascend the same way their sources do and
    // every rewritten range still runs forwards.
    for target in detached_targets {
        cloner.clone_detached(target)?;
    }
    for &block_id in &group.block_ids {
        cloner.clone_linked(block_id, dst_id)?;
    }
    Ok(())
}

// ─── Row mapping ─────────────────────────────────────────────────────────

/// The conversation projection, written once. Callers append their own `WHERE`
/// or `ORDER BY`; two copies of one SELECT are two chances for them to drift.
const CONVERSATION_SELECT: &str = "SELECT c.id, c.parent_id, c.created_at,
            m.id, m.external_id, m.display_name, m.vendor, m.provider_id,
            COALESCE(p.name, m.provider_id),
            c.reasoning,
            latest_title.content
     FROM conversations c
     JOIN models m ON m.id = c.model_id
     LEFT JOIN provider_instances p ON p.id = m.provider_id
     LEFT JOIN metadata latest_title
       ON latest_title.conversation_id = c.id
      AND latest_title.meta_type = 'title_response'
      AND latest_title.id = (
          SELECT MAX(mt.id) FROM metadata mt
          WHERE mt.conversation_id = c.id AND mt.meta_type = 'title_response'
      )";

fn row_to_conversation(row: &rusqlite::Row<'_>) -> rusqlite::Result<Conversation> {
    Ok(Conversation {
        id: row.get(0)?,
        parent_id: row.get(1)?,
        created_at: row.get(2)?,
        title: row.get(10)?,
        model: ConversationModel {
            id: row.get(3)?,
            external_id: row.get(4)?,
            display_name: row.get(5)?,
            vendor: row.get(6)?,
            provider_id: row.get(7)?,
            provider_name: row.get(8)?,
        },
        reasoning: row.get(9)?,
    })
}
