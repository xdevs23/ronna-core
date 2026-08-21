//! Typed representation of a single block's content row.
//!
//! Blocks in the schema are two-part: a header row in `blocks` (id,
//! `block_type`, `created_at`) plus a content row in a per-type table
//! (`block_text`, `block_quote`, `block_code`, …). [`BlockContent`] captures
//! just the content-row side as a typed value — read, rewrite, write — so that
//! any operation that needs to move or copy a block has one abstraction to
//! reach for instead of open-coding the per-type SQL.

use std::collections::HashMap;

use rusqlite::{Connection, params};

use crate::block::Role;

use super::StoreError;

/// Content payload of a block, decoupled from its `blocks` header row.
///
/// Only the variants forking currently needs are modelled. More can be added
/// incrementally as further flows adopt this type.
///
/// The approval kinds are in here because the fork walks GROUPS, not kinds: the
/// two approval blocks carry role user precisely so a group walk keeps them
/// inside the surrounding user turn, which means every deep copy of such a turn
/// arrives here holding one.
pub(super) enum BlockContent {
    Text {
        role: Option<Role>,
        content: String,
    },
    Quote {
        role: Option<Role>,
        start_block_id: i64,
        start_pos: i64,
        end_block_id: i64,
        end_pos: i64,
    },
    Code {
        role: Option<Role>,
        language: Option<String>,
        content: String,
    },
    ApprovalRequest {
        for_block_id: i64,
    },
    ApprovalDecision {
        for_block_id: i64,
        decision: String,
        system_reason: Option<String>,
        user_reason: Option<String>,
    },
}

impl BlockContent {
    /// Read the content row for `block_id` given its `block_type`.
    pub(super) fn read(
        conn: &Connection,
        block_id: i64,
        block_type: &str,
    ) -> Result<Self, StoreError> {
        match block_type {
            "text" | "streaming" | "system_prompt" => {
                let (role, content): (Option<String>, String) = conn.query_row(
                    "SELECT role, content FROM block_text WHERE block_id = ?1",
                    [block_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                Ok(Self::Text {
                    role: parse_role(role.as_deref()),
                    content,
                })
            }
            "quote" => {
                let (role, sb, sp, eb, ep): (Option<String>, i64, i64, i64, i64) = conn.query_row(
                    "SELECT role, start_block_id, start_pos, end_block_id, end_pos
                         FROM block_quote WHERE block_id = ?1",
                    [block_id],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )?;
                Ok(Self::Quote {
                    role: parse_role(role.as_deref()),
                    start_block_id: sb,
                    start_pos: sp,
                    end_block_id: eb,
                    end_pos: ep,
                })
            }
            "code" => {
                let (role, language, content): (Option<String>, Option<String>, String) = conn
                    .query_row(
                        "SELECT role, language, content FROM block_code WHERE block_id = ?1",
                        [block_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )?;
                Ok(Self::Code {
                    role: parse_role(role.as_deref()),
                    language,
                    content,
                })
            }
            "approval_request" => {
                let for_block_id: i64 = conn.query_row(
                    "SELECT for_block_id FROM block_approval_request WHERE block_id = ?1",
                    [block_id],
                    |row| row.get(0),
                )?;
                Ok(Self::ApprovalRequest { for_block_id })
            }
            "approval_decision" => {
                let (for_block_id, decision, system_reason, user_reason): (
                    i64,
                    String,
                    Option<String>,
                    Option<String>,
                ) = conn.query_row(
                    "SELECT for_block_id, decision, system_reason, user_reason
                         FROM block_approval_decision WHERE block_id = ?1",
                    [block_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )?;
                Ok(Self::ApprovalDecision {
                    for_block_id,
                    decision,
                    system_reason,
                    user_reason,
                })
            }
            other => {
                // The kind is genuinely outside what this type models — which
                // is a fact about this type, not a malformed statement. It used
                // to be reported as `InvalidQuery`, which told the reader the
                // SQL was wrong and sent them looking at the wrong thing.
                Err(StoreError::UnsupportedBlockKind {
                    block_type: other.to_owned(),
                    operation: "BlockContent::read",
                })
            }
        }
    }

    /// Insert this content as the payload for `new_block_id`.
    pub(super) fn write(&self, conn: &Connection, new_block_id: i64) -> Result<(), StoreError> {
        match self {
            Self::Text { role, content } => {
                conn.execute(
                    "INSERT INTO block_text (block_id, role, content) VALUES (?1, ?2, ?3)",
                    params![new_block_id, role.map(|r| r.as_str()), content],
                )?;
            }
            Self::Quote {
                role,
                start_block_id,
                start_pos,
                end_block_id,
                end_pos,
            } => {
                conn.execute(
                    "INSERT INTO block_quote
                        (block_id, role, start_block_id, start_pos, end_block_id, end_pos)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        new_block_id,
                        role.map(|r| r.as_str()),
                        start_block_id,
                        start_pos,
                        end_block_id,
                        end_pos,
                    ],
                )?;
            }
            Self::Code {
                role,
                language,
                content,
            } => {
                conn.execute(
                    "INSERT INTO block_code (block_id, role, language, content) VALUES (?1, ?2, ?3, ?4)",
                    params![new_block_id, role.map(|r| r.as_str()), language, content],
                )?;
            }
            Self::ApprovalRequest { for_block_id } => {
                conn.execute(
                    "INSERT INTO block_approval_request (block_id, for_block_id) VALUES (?1, ?2)",
                    params![new_block_id, for_block_id],
                )?;
            }
            Self::ApprovalDecision {
                for_block_id,
                decision,
                system_reason,
                user_reason,
            } => {
                conn.execute(
                    "INSERT INTO block_approval_decision
                        (block_id, for_block_id, decision, system_reason, user_reason)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        new_block_id,
                        for_block_id,
                        decision,
                        system_reason,
                        user_reason
                    ],
                )?;
            }
        }
        Ok(())
    }

    /// Rewrite any internal block-id references through `remap`. A variant with
    /// no such reference is a no-op.
    pub(super) fn remap(&mut self, remap: &HashMap<i64, i64>) {
        let references: Vec<&mut i64> = match self {
            Self::Quote {
                start_block_id,
                end_block_id,
                ..
            } => vec![start_block_id, end_block_id],
            // The block an approval covers, and the request a decision answers:
            // when the copy took that block along, the copy must point at its
            // own, exactly as a quote does with its target.
            Self::ApprovalRequest { for_block_id }
            | Self::ApprovalDecision { for_block_id, .. } => vec![for_block_id],
            Self::Text { .. } | Self::Code { .. } => Vec::new(),
        };
        for reference in references {
            if let Some(&copied) = remap.get(reference) {
                *reference = copied;
            }
        }
    }
}

/// The stored role string back to its type. An unrecognised or absent value is
/// no role at all — a block that speaks in no voice.
pub(super) fn parse_role(role: Option<&str>) -> Option<Role> {
    match role {
        Some("user") => Some(Role::User),
        Some("assistant") => Some(Role::Assistant),
        Some("system") => Some(Role::System),
        Some("tool") => Some(Role::Tool),
        _ => None,
    }
}
