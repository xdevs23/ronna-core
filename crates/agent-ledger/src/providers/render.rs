//! The block-to-neutral pass: grouping and message boundaries, nothing else.
//!
//! WHAT a block contributes to the model is the block kind's own knowledge, so
//! this module never branches on a block type. It owns exactly the structure
//! that is not per-kind: contiguous grouping by the kind-stated role fact, the
//! parts-versus-text group policy, and the blank-line join between text
//! contributions.
//!
//! The markdown vocabulary the kinds speak lives with the kinds, in the
//! behavior layer, and is re-exported here rather than restated. A second copy
//! of `render_quote` is how a transcript export and a model prompt end up
//! disagreeing about what the same block said.

use crate::agency::{BlockKind, Projection};
use crate::block::{Block, Role};

use super::types::{Message, MessageContent, MessageRole};

// The shared markdown vocabulary, at the vantage point a provider imports from.
// It is defined beside the block kinds that speak it, because a kind stating
// its own text form is the whole reason this layer never has to.
pub use crate::agency::{
    render_code, render_quote, render_text, render_tool_error, render_tool_result,
};

/// Group contiguous blocks by their kind-stated role into neutral messages.
///
/// This is the conversion every vendor module starts from. Each block is parsed
/// to its typed kind ONCE; everything after that is blind hook consultation, so
/// adding a block kind never touches this function.
#[must_use]
pub fn blocks_to_messages(blocks: &[Block]) -> Vec<Message> {
    let kinds: Vec<BlockKind> = blocks.iter().map(BlockKind::from_block).collect();
    let mut messages = Vec::new();
    let mut i = 0;
    while i < kinds.len() {
        let Some(role) = kinds[i].group_role() else {
            i += 1;
            continue;
        };
        let start = i;
        while i < kinds.len() && kinds[i].group_role() == Some(role) {
            i += 1;
        }
        messages.push(Message {
            role: message_role(role),
            content: group_content(&kinds[start..i]),
        });
    }
    messages
}

/// Collapse a ledger role onto the three voices a model wire has.
///
/// A tool speaks in the user's turn because no wire has a tool voice. Doing it
/// once, here, is what lets every vendor module take the collapse as given.
fn message_role(role: Role) -> MessageRole {
    match role {
        Role::System => MessageRole::System,
        Role::Assistant => MessageRole::Assistant,
        Role::User | Role::Tool => MessageRole::User,
    }
}

/// One same-role group's content — the central parts-versus-text policy.
///
/// A block whose parts have no text form switches the whole group to native
/// parts; otherwise the text contributions join as markdown. The policy is
/// structure. What each block contributes is the kind's own answer.
fn group_content(group: &[BlockKind]) -> MessageContent {
    if group.iter().any(Projection::forces_parts) {
        MessageContent::Parts(
            group
                .iter()
                .filter_map(Projection::llm_parts)
                .flatten()
                .collect(),
        )
    } else {
        text_content(group)
    }
}

/// The text-mode join: empty contributions are dropped, the rest join with a
/// blank line.
///
/// A kind with no text form simply answers `None` — the pinned asymmetry where
/// a text-only group drops reasoning lives on the kinds, not here.
fn text_content(group: &[BlockKind]) -> MessageContent {
    let parts: Vec<String> = group
        .iter()
        .filter_map(Projection::llm_text)
        .filter(|s| !s.is_empty())
        .collect();

    MessageContent::Text(parts.join("\n\n"))
}

/// Render a run of blocks into a single text content through each kind's
/// text-mode contribution.
#[must_use]
pub fn render_blocks_to_text(blocks: &[Block]) -> MessageContent {
    text_content(&blocks.iter().map(BlockKind::from_block).collect::<Vec<_>>())
}

/// The single-group call shape, kept as a test seam so the group-policy pins
/// read at the level they are about. It routes through the SAME
/// `group_content` the production pass uses, so it cannot drift away from what
/// it claims to be testing.
#[cfg(test)]
fn render_group(blocks: &[Block]) -> MessageContent {
    group_content(&blocks.iter().map(BlockKind::from_block).collect::<Vec<_>>())
}

/// Render an entire ledger into one copyable markdown transcript, grouped by
/// role with labelled sections.
///
/// It groups on the block's stored role rather than the kind's projection role,
/// because a transcript is a record of who said what, not of what the model was
/// told.
#[must_use]
pub fn render_conversation(blocks: &[Block]) -> String {
    let mut sections = Vec::new();
    let mut i = 0;
    while i < blocks.len() {
        let Some(role) = blocks[i].role else {
            i += 1;
            continue;
        };
        let start = i;
        while i < blocks.len() && blocks[i].role == Some(role) {
            i += 1;
        }
        let label = match role {
            Role::User => "You",
            Role::Assistant => "Assistant",
            Role::Tool => "Tool",
            // The harness's own words are not part of a transcript of the
            // conversation.
            Role::System => continue,
        };
        let MessageContent::Text(body) = render_blocks_to_text(&blocks[start..i]) else {
            continue;
        };
        if !body.is_empty() {
            sections.push(format!("**{label}:**\n{body}"));
        }
    }
    sections.join("\n\n---\n\n")
}

#[cfg(test)]
mod tests;
