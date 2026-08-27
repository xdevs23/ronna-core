//! The library's own schema, and the counter that applies it.
//!
//! # Why this is one step and not a history
//!
//! This sequence was rewritten, not copied. In the code it was extracted from,
//! one ordered array under one `user_version` counter created both that
//! application's tables and the runtime's, and individual steps did both in a
//! single indivisible statement — so the array could not be split by
//! partitioning it. What is here is the library's own sequence, containing only
//! the library's own tables.
//!
//! The rewrite collapses fifteen historical steps into one because **this
//! library has no installed base**: there is no database anywhere carrying an
//! older shape, so there is no upgrade path to preserve, and one is not
//! invented here. The next reader will wonder where the history went, which is
//! why this paragraph exists. The `user_version` mechanism itself stays, and
//! it earned its keep on 2026-08-22: the library now has an installed base (the
//! first consumer's ledgers), so the dispatch-anchor column arrived as step
//! two, and every later change to the shipped schema arrives the same way.
//!
//! Six tests that proved the old array's *upgrade* steps could not come across
//! with it: they replayed historical on-disk shapes that never existed here.
//! The facts those steps established are still asserted, against the schema a
//! fresh database gets.
//!
//! # The consumer's own tables
//!
//! A consumer does not append to [`MIGRATIONS`]. It calls
//! [`domain_migrate`](super::domain_migrate) with its own domain name and its
//! own statements; the store tracks that domain's version in the
//! `domain_migrations` table, beside this counter and independent of it, and
//! holds every query for that domain until its migrations have run. That is the
//! seam — the library's schema and a consumer's schema advance on separate
//! counters and neither can stall the other.

use rusqlite::Connection;

/// The library's schema, one entry per version. Applied in order; entry `i`
/// becomes `user_version` `i + 1`.
///
/// Design rules the schema holds to:
///   - No JSON blobs. Every datum has its own column.
///   - Blocks are the primary conversational unit, each carrying its own role.
///   - Content lives in typed block content tables, one per block type.
///   - The junction table (`conversation_blocks`) enables forking at block
///     granularity.
///   - Drafts are separate from blocks: mutable versus immutable.
///   - Range-based quoting: start and end block plus character positions.
///   - Derived, never stored. A fact that can be folded from the ledger is not
///     a column — which is why the conversation row carries no state, no
///     inactive reason and no title. One recorded exception (2026-08-22): the
///     dispatch anchor in v2, an insert-time decision whose derivation from
///     stored shape was adversarially refuted — see the v2 entry.
const MIGRATIONS: &[&str] = &[
    // v1: the block-first relational schema.
    "
    -- Provider registry: identity only, no config.
    CREATE TABLE IF NOT EXISTS provider_instances (
        id         TEXT PRIMARY KEY,
        type       TEXT NOT NULL,
        name       TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    -- Models: normalized metadata, survives a provider delisting one.
    CREATE TABLE IF NOT EXISTS models (
        id           INTEGER PRIMARY KEY,
        external_id  TEXT NOT NULL,
        display_name TEXT NOT NULL,
        vendor       TEXT NOT NULL DEFAULT '',
        provider_id  TEXT NOT NULL,
        created_at   TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(external_id, provider_id)
    );

    -- Conversations.
    --
    -- last_processed_block_id is the processed cursor — the ONE mutable
    -- single-field write on a conversation's orchestration state; blocks stay
    -- append-only. 0 = nothing confirmed: a conversation re-derives from the
    -- start, so every drive is idempotent.
    --
    -- last_processed_metadata_id is the metadata ledger's own cursor. One
    -- machinery, two ledgers: each ledger carries its own cursor and the two
    -- never interact.
    --
    -- reasoning is NULL to defer to the provider's own default, otherwise a
    -- canonical level key.
    CREATE TABLE IF NOT EXISTS conversations (
        id                         INTEGER PRIMARY KEY,
        parent_id                  INTEGER REFERENCES conversations(id) ON DELETE SET NULL,
        model_id                   INTEGER NOT NULL REFERENCES models(id),
        reasoning                  TEXT,
        last_processed_block_id    INTEGER NOT NULL DEFAULT 0,
        last_processed_metadata_id INTEGER NOT NULL DEFAULT 0,
        created_at                 TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_conversations_parent ON conversations(parent_id);

    -- Blocks: the primary conversational unit.
    CREATE TABLE IF NOT EXISTS blocks (
        id         INTEGER PRIMARY KEY,
        block_type TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT
    );

    -- Junction: conversation to block. Forking shares rows instead of copying.
    CREATE TABLE IF NOT EXISTS conversation_blocks (
        id              INTEGER PRIMARY KEY,
        conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
        block_id        INTEGER NOT NULL REFERENCES blocks(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_conversation_blocks_conv  ON conversation_blocks(conversation_id);
    CREATE INDEX IF NOT EXISTS idx_conversation_blocks_block ON conversation_blocks(block_id);

    -- Block content tables: one per block type, each keyed by the block id.
    CREATE TABLE IF NOT EXISTS block_text (
        block_id INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
        role     TEXT NOT NULL,
        content  TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS block_quote (
        block_id       INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
        role           TEXT NOT NULL,
        start_block_id INTEGER NOT NULL REFERENCES blocks(id),
        start_pos      INTEGER NOT NULL,
        end_block_id   INTEGER NOT NULL REFERENCES blocks(id),
        end_pos        INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS block_code (
        block_id INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
        role     TEXT NOT NULL,
        language TEXT,
        content  TEXT NOT NULL
    );

    -- interactive is stamped at insert so the block answers who owes its next
    -- move from its own data on replay, never from a tool-name match.
    CREATE TABLE IF NOT EXISTS block_tool_call (
        block_id     INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
        role         TEXT NOT NULL,
        tool_call_id TEXT NOT NULL,
        name         TEXT NOT NULL,
        input        TEXT NOT NULL,
        interactive  INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS block_streaming_tool_call (
        block_id     INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
        role         TEXT NOT NULL,
        tool_call_id TEXT NOT NULL,
        name         TEXT NOT NULL,
        input        TEXT NOT NULL
    );

    -- source_block_id points at the tool_call block this answers: a model's
    -- tool_call_id can repeat, the block id cannot, so matching goes by id.
    CREATE TABLE IF NOT EXISTS block_tool_result (
        block_id        INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
        tool_call_id    TEXT NOT NULL,
        content         TEXT NOT NULL,
        source_block_id INTEGER REFERENCES blocks(id)
    );

    CREATE TABLE IF NOT EXISTS block_tool_error (
        block_id        INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
        tool_call_id    TEXT NOT NULL,
        error           TEXT NOT NULL,
        source_block_id INTEGER REFERENCES blocks(id)
    );

    -- Both thinking and streaming_thinking rows live here, so one set of
    -- columns covers streaming accumulation and the finalized block.
    --
    -- content is the verbatim reasoning. summary is the display-only channel
    -- some providers stream instead of verbatim text: the two are un-conflated
    -- on the wire and stay separate at rest, and summary NEVER enters
    -- projection — replay rides the opaque columns exclusively.
    --
    -- The opaque_* columns are the continuity payload, all nullable: a NULL
    -- opaque_kind is a reasoning block with no payload, the common case.
    CREATE TABLE IF NOT EXISTS block_thinking (
        block_id       INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
        role           TEXT NOT NULL,
        title          TEXT,
        content        TEXT NOT NULL,
        summary        TEXT,
        opaque_kind    TEXT,
        opaque_data    TEXT,
        opaque_item_id TEXT
    );

    -- The multi-entry continuity payload, held relationally, one row per entry
    -- and order-preserving for verbatim rebuild and format-gated replay.
    CREATE TABLE IF NOT EXISTS block_reasoning_detail (
        block_id        INTEGER NOT NULL REFERENCES blocks(id) ON DELETE CASCADE,
        position        INTEGER NOT NULL,
        entry_type      TEXT NOT NULL,
        entry_id        TEXT,
        upstream_format TEXT NOT NULL,
        idx             INTEGER,
        content         TEXT NOT NULL,
        signature       TEXT,
        PRIMARY KEY (block_id, position)
    );

    CREATE TABLE IF NOT EXISTS block_status (
        block_id INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
        status   TEXT NOT NULL,
        subtitle TEXT
    );

    -- The approval chain. for_block_id points at the covered block; who denied
    -- is structural — system_reason versus user_reason, two fields, never a
    -- string plus a flag.
    CREATE TABLE IF NOT EXISTS block_approval_request (
        block_id     INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
        for_block_id INTEGER NOT NULL REFERENCES blocks(id)
    );

    CREATE TABLE IF NOT EXISTS block_approval_decision (
        block_id      INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
        for_block_id  INTEGER NOT NULL REFERENCES blocks(id),
        decision      TEXT NOT NULL,
        system_reason TEXT,
        user_reason   TEXT
    );

    -- A ledger-true record of the local date at the moment user blocks landed.
    CREATE TABLE IF NOT EXISTS block_date_marker (
        block_id INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
        date     TEXT NOT NULL
    );

    -- At most one system_prompt block per conversation.
    CREATE TRIGGER IF NOT EXISTS trg_unique_system_prompt
         BEFORE INSERT ON conversation_blocks
         WHEN (SELECT block_type FROM blocks WHERE id = NEW.block_id) = 'system_prompt'
     BEGIN
         SELECT RAISE(ABORT, 'conversation already has a system prompt')
         WHERE EXISTS (
             SELECT 1 FROM conversation_blocks cb
             JOIN blocks b ON b.id = cb.block_id
             WHERE cb.conversation_id = NEW.conversation_id
               AND b.block_type = 'system_prompt'
         );
     END;

    -- Drafts: mutable composer state, one per conversation.
    CREATE TABLE IF NOT EXISTS drafts (
        id              INTEGER PRIMARY KEY,
        conversation_id INTEGER NOT NULL UNIQUE REFERENCES conversations(id) ON DELETE CASCADE,
        updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS draft_blocks (
        id         INTEGER PRIMARY KEY,
        draft_id   INTEGER NOT NULL REFERENCES drafts(id) ON DELETE CASCADE,
        position   INTEGER NOT NULL,
        block_type TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_draft_blocks_draft ON draft_blocks(draft_id, position);

    CREATE TABLE IF NOT EXISTS draft_block_text (
        block_id INTEGER PRIMARY KEY REFERENCES draft_blocks(id) ON DELETE CASCADE,
        content  TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS draft_block_quote (
        block_id       INTEGER PRIMARY KEY REFERENCES draft_blocks(id) ON DELETE CASCADE,
        start_block_id INTEGER NOT NULL,
        start_pos      INTEGER NOT NULL,
        end_block_id   INTEGER NOT NULL,
        end_pos        INTEGER NOT NULL
    );

    -- The second ledger: derived conversation properties (titles, summaries,
    -- tags), driven by the same machinery as blocks and cursored separately.
    CREATE TABLE IF NOT EXISTS metadata (
        id              INTEGER PRIMARY KEY,
        conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
        meta_type       TEXT NOT NULL,
        source_block_id INTEGER REFERENCES blocks(id),
        content         TEXT,
        created_at      TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_metadata_conversation_type
        ON metadata(conversation_id, meta_type);

    -- Persistent file attachments with sparse download tracking.
    CREATE TABLE IF NOT EXISTS attachments (
        id         TEXT PRIMARY KEY,
        url        TEXT,
        filename   TEXT NOT NULL,
        mime       TEXT NOT NULL,
        total_size INTEGER NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS attachment_headers (
        attachment_id TEXT NOT NULL REFERENCES attachments(id) ON DELETE CASCADE,
        name          TEXT NOT NULL,
        value         TEXT NOT NULL,
        PRIMARY KEY (attachment_id, name)
    );

    CREATE TABLE IF NOT EXISTS attachment_ranges (
        attachment_id TEXT NOT NULL REFERENCES attachments(id) ON DELETE CASCADE,
        start         INTEGER NOT NULL,
        end           INTEGER NOT NULL,
        PRIMARY KEY (attachment_id, start)
    );

    CREATE INDEX IF NOT EXISTS idx_attachment_ranges_id
        ON attachment_ranges(attachment_id);
    ",
    // v2 (2026-08-22): the dispatch anchor — one nullable header column naming
    // the block whose owed turn dispatched this block's stream, plus its
    // counterpart on the second ledger, where a response row anchors on the
    // request row of its own ledger. Written by the framework's own insert
    // paths only; NULL for everything that is not a turn's product. This is a
    // deliberate exception to "derived, never stored": reconstructing the
    // summoning frontier from stored shape was adversarially refuted at the
    // first consumer (three proven escalations closed the question), so the
    // dispatch decision is recorded at the one moment it is known.
    "
    ALTER TABLE blocks ADD COLUMN dispatch_anchor INTEGER REFERENCES blocks(id);
    ALTER TABLE metadata ADD COLUMN dispatch_anchor INTEGER REFERENCES metadata(id);
    ",
    // v3 (2026-08-27): what the date marker records beside the date — the
    // platform's zone abbreviation, the IANA zone name, and the wall-clock
    // minute the marker was written. Three nullable columns, each written by
    // its own source and each NULL when that source answers nothing; every
    // marker that predates them reads back all-NULL and projects exactly the
    // line it always did. A step, not an edit to v1's CREATE TABLE: editing
    // the shipped statement would pass every fresh-database test and strand
    // every store that already exists.
    "
    ALTER TABLE block_date_marker ADD COLUMN tz_abbrev  TEXT;
    ALTER TABLE block_date_marker ADD COLUMN tz_name    TEXT;
    ALTER TABLE block_date_marker ADD COLUMN written_at TEXT;
    ",
];

/// Apply every unapplied step, advancing `user_version` as each lands.
///
/// Re-running is a no-op: a step whose version the counter already carries is
/// skipped, so this is called unconditionally on every open.
///
/// # Errors
///
/// Returns the database's error if a step or the counter update fails.
pub(super) fn run(conn: &Connection) -> rusqlite::Result<()> {
    let current: u32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    tracing::info!(
        current,
        migrations_count = MIGRATIONS.len(),
        "migrations: checking"
    );

    for (i, sql) in MIGRATIONS.iter().enumerate() {
        let version = u32::try_from(i + 1).unwrap_or(u32::MAX);
        if version > current {
            conn.execute_batch(sql)?;
            conn.pragma_update(None, "user_version", version)?;
            tracing::info!(version, "applied migration");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Connection, MIGRATIONS, run};

    fn migrated() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        run(&conn).unwrap();
        conn
    }

    fn version(conn: &Connection) -> u32 {
        conn.pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap()
    }

    fn table_columns(conn: &Connection, table: &str) -> Vec<String> {
        conn.prepare(&format!("PRAGMA table_info({table})"))
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    #[test]
    fn applies_all_migrations() {
        let conn = migrated();
        assert_eq!(version(&conn), u32::try_from(MIGRATIONS.len()).unwrap());
    }

    #[test]
    fn idempotent() {
        let conn = migrated();
        run(&conn).unwrap();
        assert_eq!(version(&conn), u32::try_from(MIGRATIONS.len()).unwrap());
    }

    /// A fresh database carries the three nullable opaque columns on
    /// `block_thinking` and the `block_reasoning_detail` sidecar with its
    /// `(block_id, position)` primary key.
    #[test]
    fn opaque_columns_and_the_reasoning_detail_sidecar_are_present() {
        let conn = migrated();

        let thinking = table_columns(&conn, "block_thinking");
        for col in ["opaque_kind", "opaque_data", "opaque_item_id"] {
            assert!(
                thinking.contains(&col.to_string()),
                "block_thinking has {col}"
            );
        }

        let sidecar = table_columns(&conn, "block_reasoning_detail");
        assert_eq!(
            sidecar,
            vec![
                "block_id",
                "position",
                "entry_type",
                "entry_id",
                "upstream_format",
                "idx",
                "content",
                "signature",
            ]
        );

        // (block_id, position) is the primary key — a duplicate position for
        // the same block must be rejected.
        conn.execute_batch(
            "INSERT INTO blocks (block_type) VALUES ('thinking');
             INSERT INTO block_reasoning_detail (block_id, position, entry_type, upstream_format, content)
                 VALUES (1, 0, 'reasoning.text', 'anthropic-claude-v1', 'x');",
        )
        .unwrap();
        let dup = conn.execute(
            "INSERT INTO block_reasoning_detail (block_id, position, entry_type, upstream_format, content)
                 VALUES (1, 0, 'reasoning.text', 'anthropic-claude-v1', 'y')",
            [],
        );
        assert!(
            dup.is_err(),
            "duplicate (block_id, position) violates the PK"
        );
    }

    /// The orchestration schema: the processed cursor on conversations, the
    /// interactive stamp on tool calls, and the approval content tables.
    #[test]
    fn the_cursor_the_interactive_stamp_and_the_approval_tables_are_present() {
        let conn = migrated();

        assert!(
            table_columns(&conn, "conversations").contains(&"last_processed_block_id".to_string())
        );
        assert!(table_columns(&conn, "block_tool_call").contains(&"interactive".to_string()));
        assert_eq!(
            table_columns(&conn, "block_approval_request"),
            vec!["block_id", "for_block_id"]
        );
        assert_eq!(
            table_columns(&conn, "block_approval_decision"),
            vec![
                "block_id",
                "for_block_id",
                "decision",
                "system_reason",
                "user_reason"
            ]
        );
    }

    /// The metadata ledger's own cursor, distinct from the conversation's.
    #[test]
    fn the_metadata_cursor_coexists_with_the_block_cursor() {
        let conn = migrated();

        let columns = table_columns(&conn, "conversations");
        assert!(columns.contains(&"last_processed_metadata_id".to_string()));
        assert!(
            columns.contains(&"last_processed_block_id".to_string()),
            "the two cursors coexist as separate columns"
        );
    }

    /// A populated v1 database — the shipped shape — upgrades in place: every
    /// later step runs, the existing rows survive, and every pre-column row
    /// reads back a NULL anchor. The library's first real upgrade, so the
    /// version gate is exercised against data, not just a fresh file.
    #[test]
    fn a_populated_v1_database_upgrades_to_v2_in_place() {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        conn.execute_batch(MIGRATIONS[0]).unwrap();
        conn.pragma_update(None, "user_version", 1).unwrap();
        conn.execute("INSERT INTO blocks (block_type) VALUES ('text')", [])
            .unwrap();

        run(&conn).unwrap();
        assert_eq!(version(&conn), u32::try_from(MIGRATIONS.len()).unwrap());
        let anchor: Option<i64> = conn
            .query_row("SELECT dispatch_anchor FROM blocks WHERE id = 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(anchor, None, "a pre-column row is not a turn's product");
    }

    /// v2: the dispatch anchor rides the block header, and the second ledger
    /// carries its own — both nullable, both defaulting to NULL, so every row
    /// that predates the column reads back as "not a turn's product".
    #[test]
    fn the_dispatch_anchor_columns_are_present_and_nullable() {
        let conn = migrated();
        assert!(table_columns(&conn, "blocks").contains(&"dispatch_anchor".to_string()));
        assert!(table_columns(&conn, "metadata").contains(&"dispatch_anchor".to_string()));

        // A pre-column row shape — the insert names no anchor — lands as NULL.
        conn.execute("INSERT INTO blocks (block_type) VALUES ('text')", [])
            .unwrap();
        let anchor: Option<i64> = conn
            .query_row("SELECT dispatch_anchor FROM blocks WHERE id = 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(anchor, None);
    }

    /// The date marker's content table, with the zone and minute columns v3
    /// added beside the date.
    #[test]
    fn the_date_marker_table_is_present() {
        let conn = migrated();
        assert_eq!(
            table_columns(&conn, "block_date_marker"),
            vec!["block_id", "date", "tz_abbrev", "tz_name", "written_at"]
        );
    }

    /// A populated store carrying date markers from before v3 gains the three
    /// columns in place: the marker rows survive, and every one of them reads
    /// back all-NULL — the shape whose projected line is unchanged. Same
    /// precedent as the v1-to-v2 upgrade above, against the data the step
    /// actually widens.
    #[test]
    fn a_populated_store_with_markers_gains_the_zone_columns_in_place() {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        for sql in &MIGRATIONS[..2] {
            conn.execute_batch(sql).unwrap();
        }
        conn.pragma_update(None, "user_version", 2).unwrap();
        conn.execute_batch(
            "INSERT INTO blocks (block_type) VALUES ('date_marker');
             INSERT INTO block_date_marker (block_id, date) VALUES (1, '2026-08-26');",
        )
        .unwrap();

        run(&conn).unwrap();
        assert_eq!(version(&conn), u32::try_from(MIGRATIONS.len()).unwrap());

        let row: (String, Option<String>, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT date, tz_abbrev, tz_name, written_at FROM block_date_marker
                 WHERE block_id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            row,
            ("2026-08-26".to_owned(), None, None, None),
            "the existing marker survives, knowing nothing it was never told"
        );

        // And the widened row writes through the same table afterwards.
        conn.execute_batch(
            "INSERT INTO blocks (block_type) VALUES ('date_marker');
             INSERT INTO block_date_marker (block_id, date, tz_abbrev, tz_name, written_at)
                 VALUES (2, '2026-08-27', 'CEST', 'Europe/Berlin', '22:41');",
        )
        .unwrap();
        let name: Option<String> = conn
            .query_row(
                "SELECT tz_name FROM block_date_marker WHERE block_id = 2",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(name.as_deref(), Some("Europe/Berlin"));
    }

    /// The display-only summary channel sits beside content and the opaque
    /// columns rather than sharing one of them.
    #[test]
    fn the_thinking_summary_is_its_own_column() {
        let conn = migrated();

        let columns = table_columns(&conn, "block_thinking");
        assert!(columns.contains(&"summary".to_string()));
        assert!(
            columns.contains(&"content".to_string())
                && columns.contains(&"opaque_kind".to_string()),
            "summary joins content and opaque as separate channels"
        );
    }
}
