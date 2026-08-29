//! Block cloning with reference rewriting.
//!
//! [`BlockCloner`] deep-copies blocks across conversations while accumulating a
//! remap table that lets later clones rewrite internal references (today only
//! quote start and end ids). Callers pick per clone whether the copy is linked
//! to a conversation via a junction row or left detached — a block row
//! reachable only by id, with no junction entry.

use std::collections::HashMap;

use rusqlite::{Connection, params};

use super::block_content::BlockContent;
use super::descriptors::{ContentDescriptor, clone_consumer_content, descriptor_for_kind};
use super::{DomainGate, StoreError};

pub(super) struct BlockCloner<'c> {
    conn: &'c Connection,
    descriptors: &'static [ContentDescriptor],
    /// The consumer domains' health. A clone of a descriptor-claimed kind
    /// writes into that descriptor's own table, so it answers to the same gate
    /// every other descriptor-path read and write does.
    gate: &'c DomainGate,
    remap: HashMap<i64, i64>,
}

impl<'c> BlockCloner<'c> {
    pub(super) fn new(
        conn: &'c Connection,
        descriptors: &'static [ContentDescriptor],
        gate: &'c DomainGate,
    ) -> Self {
        Self {
            conn,
            descriptors,
            gate,
            remap: HashMap::new(),
        }
    }

    /// Clone a block into a fresh row with no junction linkage.
    ///
    /// The cloned row is reachable only via its new id — as the target of a
    /// quote's `start_block_id` or `end_block_id`, say — so deleting any
    /// conversation cannot cascade it away.
    ///
    /// A row made here has no junction row BY DESIGN and must survive
    /// collection. That is why the one definition of an orphan —
    /// `orphan_block_predicate` in the block module — asks whether ANYTHING
    /// points at a block, the junction rows and every reference column alike:
    /// this seam and the collector have to mean the same thing by the word, or
    /// one of them deletes what the other is relying on.
    pub(super) fn clone_detached(&mut self, src_block_id: i64) -> Result<i64, StoreError> {
        self.clone_inner(src_block_id, None)
    }

    /// Clone a block and link it to `conversation_id` via a junction row.
    pub(super) fn clone_linked(
        &mut self,
        src_block_id: i64,
        conversation_id: i64,
    ) -> Result<i64, StoreError> {
        self.clone_inner(src_block_id, Some(conversation_id))
    }

    fn clone_inner(&mut self, src_block_id: i64, link_to: Option<i64>) -> Result<i64, StoreError> {
        let (block_type, created_at, src_anchor): (String, String, Option<i64>) =
            self.conn.query_row(
                "SELECT block_type, created_at, dispatch_anchor FROM blocks WHERE id = ?1",
                [src_block_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
        // The dispatch anchor is a real reference and rides the same remap as
        // every other one (2026-08-22, in lockstep with the header insert):
        // rewritten to the clone's id where the anchor's target was cloned,
        // kept by reference where it was not — and the kept reference holds
        // its source block alive through the collector's reference union, so
        // fork-then-delete leaves no dangling anchor.
        let dispatch_anchor =
            src_anchor.map(|anchor| self.remap.get(&anchor).copied().unwrap_or(anchor));

        // A descriptor-claimed kind is copied generically from its declared
        // columns; the library's own kinds keep the typed content path,
        // untouched. A consumer row's declared reference columns are resolved
        // through the same remap the core kinds use: rewritten to the clone's
        // id where the referenced block was cloned, kept by reference where it
        // was not — and the kept reference holds its source block alive
        // through the collector's reference predicate.
        let descriptor = descriptor_for_kind(self.descriptors, &block_type);
        // Consulted BEFORE the header row goes in, so a clone under a failed
        // consumer migration writes nothing at all rather than a header whose
        // content row never follows. A quote can now span a consumer kind, so
        // the fork's deep copy of a quote target reaches this path with a row
        // it must copy out of a schema in doubt — and the answer is the same
        // one every descriptor read gives: refuse loudly with the migration
        // failure, never a raw write.
        if let Some(descriptor) = descriptor {
            self.gate.ensure(descriptor.domain)?;
        }
        let core_content = if descriptor.is_some() {
            None
        } else {
            let mut content = BlockContent::read(self.conn, src_block_id, &block_type)?;
            content.remap(&self.remap);
            Some(content)
        };

        self.conn.execute(
            "INSERT INTO blocks (block_type, created_at, dispatch_anchor) VALUES (?1, ?2, ?3)",
            params![block_type, created_at, dispatch_anchor],
        )?;
        let new_block_id = self.conn.last_insert_rowid();

        if let Some(descriptor) = descriptor {
            clone_consumer_content(
                self.conn,
                self.descriptors,
                descriptor,
                src_block_id,
                new_block_id,
                &block_type,
                &self.remap,
            )?;
        } else if let Some(content) = core_content {
            content.write(self.conn, new_block_id)?;
        }
        self.remap.insert(src_block_id, new_block_id);

        if let Some(conversation_id) = link_to {
            self.conn.execute(
                "INSERT INTO conversation_blocks (conversation_id, block_id) VALUES (?1, ?2)",
                params![conversation_id, new_block_id],
            )?;
        }

        Ok(new_block_id)
    }
}
