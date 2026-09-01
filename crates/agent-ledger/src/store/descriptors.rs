//! The consumer's content-table seam: descriptors, the configured open, and
//! the generic read, write, copy and teardown paths they drive.
//!
//! A block kind defined outside this library stores its content in a table of
//! its own. A [`ContentDescriptor`] is that table's contract with the store:
//! which stored type strings live there, which columns carry the content and
//! as what declared type, which columns elsewhere point at blocks through it,
//! and whether its kinds are ephemeral. The store validates every descriptor
//! at open — against the schema the domain migrations just created — and from
//! then on the descriptor drives loading, appending, forking, the collector's
//! reference predicate, the ephemeral sweep and the change-hook allowlist,
//! with no further registration anywhere.
//!
//! # Descriptors are durable facts
//!
//! The configured open records every descriptor's table and kinds in a
//! store-owned registry table. From then on, opening that database without
//! descriptors covering the registered tables fails loudly
//! ([`StoreError::MissingDescriptors`]): a database reopened without its
//! descriptors is a different ledger — consumer blocks would render as empty
//! content and the collector would abort on their references — so the open
//! refuses instead of misreading.
//!
//! # A failed open can leave the disk migrated, and that is safe
//!
//! The configured open runs the domain migrations BEFORE descriptor
//! validation, so an open that fails validation leaves the migrations applied
//! and their version counters advanced. That state is documented as safe by
//! idempotency rather than prevented: every domain step is version-gated —
//! a step is either applied and counted or neither — so the corrected reopen
//! finds the applied versions counted, skips them, and validates against the
//! exact schema the first attempt built. Preventing it instead would demand
//! down-migrations, a contract the seam deliberately does not have; the
//! registry rows are only written after validation passes, so a failed open
//! registers nothing.
//!
//! # One internal asymmetry, decided 2026-08-21
//!
//! The library's own kinds do NOT load through descriptors: their statements —
//! the one block query, the typed content reads and writes, the reference
//! union, the sweep's type list — are kept literal and byte-identical, and a
//! test pins them against the literals. Consumer kinds take the second step
//! described here. Rejected alternatives: migrating the core kinds onto
//! descriptors now (rewrites proven statements for no behavioral gain; left
//! open for a later stage), and a runtime registry of boxed kinds (two classes
//! of the same thing, which this architecture exists to avoid). The ported path
//! keeps its fidelity; the new path is a declared seam.
//!
//! Amended 2026-08-22: "byte-identical" pins the statements against SILENT
//! drift, not against deliberate schema growth — the dispatch-anchor column
//! joined the block query's header select and the reference union's
//! self-referential arm, and both pinned literals moved in lockstep with the
//! statements, each carrying its dated note.

use std::collections::{HashMap, HashSet};

use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{Connection, OptionalExtension};
use serde_json::Value;

use crate::block::{Block, Role};

use super::block_content::parse_role;
use super::{DomainGate, Store, StoreError, transact};

/// One column, named with its table — how a descriptor points at a reference
/// column that lives outside the descriptor's own declaration order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnRef {
    /// The table the column lives in. Always a descriptor's table — its own or
    /// another descriptor's, never a library table: the library's own
    /// reference columns are the literal reference union's business, and a
    /// [`ColumnRef`] naming one would put a second owner behind it.
    pub table: &'static str,
    /// The column's name.
    pub column: &'static str,
}

/// What a declared content column stores, and therefore how the read maps the
/// stored value back into a block field. The write refuses a field the
/// declared type does not describe, and the open refuses a declared type the
/// underlying column's affinity cannot hold losslessly — so what goes in is
/// what comes out, booleans included.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    /// A string, stored as SQL text.
    Text,
    /// A whole number, stored as an SQL integer.
    Integer,
    /// A floating-point number, stored as an SQL real.
    Real,
    /// A boolean, stored as the integers 0 and 1 and read back as a boolean —
    /// never surfaced as a number.
    Boolean,
    /// An arbitrary JSON value, stored serialized in a text column and parsed
    /// back on read. This is the one declared type that admits nesting; under
    /// any other, a nested value is refused at the write.
    Json,
}

/// One declared content column: its name and its declared type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Column {
    /// The column's name in the table.
    pub name: &'static str,
    /// The declared type the read and write map through.
    pub ty: ColumnType,
}

impl Column {
    /// A declared column, by name and type.
    #[must_use]
    pub const fn new(name: &'static str, ty: ColumnType) -> Self {
        Self { name, ty }
    }
}

/// A consumer kind's content-table contract with the store.
///
/// The table itself is created by the consumer's domain migrations, handed to
/// [`Store::open_with`] alongside the descriptors; the two arrive together and
/// are checked against each other before any query is served.
///
/// # The table's shape
///
/// - `block_id INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE` is
///   the key. It is implicit — never listed in `columns` — and every part of
///   it is checked at open. The `INTEGER PRIMARY KEY` must really be the rowid
///   alias on a rowid table: the change hook announces rowids, and only the
///   alias makes the announced rowid BE the block id; a `WITHOUT ROWID` table
///   would fire no change hook at all and is refused. The cascade is what lets
///   the collector delete a header row and take the content row with it,
///   exactly as every library content table does — a key without it aborts the
///   collector's DELETE instead of following it.
/// - A column named `role` is the block's voice, declared
///   [`ColumnType::Text`]. Declared in `columns`, it is written from the role
///   argument of [`Store::append_consumer_block`] and read back into the
///   block's `role` — never into `fields`.
/// - Every other declared column is a content field, written from and read
///   into the block's `fields` by name, mapped through its declared
///   [`ColumnType`]. One column per datum; nested JSON is refused everywhere
///   but under [`ColumnType::Json`]. An omitted field is stored as NULL and
///   stays absent on read — it does not come back as a present null.
/// - The names `id`, `type`, `created_at` and `block_id` are the row header's
///   own and are refused at open: a payload entry under one of them would
///   shadow the block's identity.
#[derive(Debug, Clone, Copy)]
pub struct ContentDescriptor {
    /// The content table this descriptor owns.
    pub table: &'static str,
    /// The domain whose migrations create and advance this table — the same
    /// name the consumer's [`DomainMigrations`] carries. Descriptor reads and
    /// writes run under this domain, so a failed migration for it answers
    /// them with [`StoreError::MigrationFailed`] exactly as
    /// [`domain_run`](super::domain_run) is answered, instead of running raw
    /// against a schema in doubt. `"core"` is the library's own and is
    /// refused.
    pub domain: &'static str,
    /// The stored type strings whose content rows live in that table.
    pub kinds: &'static [&'static str],
    /// The content columns, each read by name into the block's fields through
    /// its declared type.
    pub columns: &'static [Column],
    /// Columns in descriptor tables that reference blocks(id) through this
    /// kind — the garbage collector's reference predicate extends over these,
    /// and the fork remaps them wherever the referenced block was cloned. Each
    /// must name a descriptor's table (this one's or another's), never a
    /// library table.
    pub reference_columns: &'static [ColumnRef],
    /// Ephemeral kinds: deleted by the finalization that replaces them, never
    /// a cursor anchor. Joins the streaming teardown sweep.
    ///
    /// **This flag mirrors a fact the kind itself owns**: the kind's
    /// [`Agency::durable`](crate::agency::Agency::durable) is the single
    /// source of the row-lifetime fact, and this flag must answer its exact
    /// negation, or the cursor anchors on rows that finalization deletes —
    /// the ephemeral-pin regression. The agreement is the conformance check's
    /// job:
    /// [`check_descriptor_durability`](crate::agency::check_descriptor_durability)
    /// asserts the two agree for every kind a descriptor set declares, and a
    /// consumer's conformance tests run it over their set.
    pub ephemeral: bool,
    /// The one declared column holding this kind's quotable text, if the kind
    /// has one. `None` — the default a kind takes by saying nothing — means
    /// the kind's content cannot be quoted: a quote spanning it resolves to
    /// the empty string, which the projection renders as nothing.
    ///
    /// A quote block stores a span reference and resolves it to text at
    /// store-read time, before any kind is parsed, so the resolver cannot ask
    /// the kind what it says. This declaration is how a kind answers that
    /// question in advance, and it is the ONLY thing that makes a consumer
    /// kind reachable by a quote.
    ///
    /// **The declaration is what a span covers, and the gate is what it
    /// reads.** A kind naming a column here joins the quote range walk as a
    /// member — a compile-time fact, so what a span covers never depends on
    /// runtime state, and the fork's deep copy (which copies exactly the
    /// span's members) is decided the same way. The column's TEXT, by
    /// contrast, is read under the descriptor's domain gate like every other
    /// descriptor-path read: with that domain's migrations in a failed state
    /// the resolver declines the read and the quote resolves empty, rather
    /// than running raw against a schema in doubt.
    ///
    /// Validated at open: the name must be one of this descriptor's own
    /// [`columns`](Self::columns), declared [`ColumnType::Text`] — by
    /// variant, so [`ColumnType::Json`] is refused even though the store
    /// keeps JSON in a text column — never the `role` column, and never on an
    /// [`ephemeral`](Self::ephemeral) kind, whose rows finalization deletes
    /// out from under every quote of them.
    pub quoted_text_column: Option<&'static str>,
}

/// One domain's migrations, submitted at open. Entry `i` of `sqls` is that
/// domain's version `i + 1`, tracked in the store's `domain_migrations` table
/// exactly as a call to [`domain_migrate`](super::domain_migrate) would track
/// it — the configured open runs them before any query is served, which is
/// what lets descriptor validation read the schema they create.
///
/// One entry per domain: a second `DomainMigrations` naming the same domain
/// would be silently swallowed by the version counter (its steps re-count the
/// first entry's versions), so the open refuses the duplicate loudly, naming
/// the domain.
#[derive(Debug, Clone)]
pub struct DomainMigrations {
    /// The domain whose schema these advance. `"core"` is the library's own and
    /// is refused.
    pub domain: &'static str,
    /// The migration steps, in order.
    pub sqls: Vec<&'static str>,
}

/// What a consumer hands [`Store::open_with`]: its content-table descriptors
/// and the domain migrations that create their tables. The empty configuration
/// is exactly [`Store::open`]'s core-only form.
#[derive(Debug, Clone, Default)]
pub struct StoreConfig {
    /// The consumer's content-table descriptors.
    pub descriptors: &'static [ContentDescriptor],
    /// The consumer's domain migrations, run at open before validation.
    pub domain_migrations: Vec<DomainMigrations>,
}

/// The column name that carries a block's voice in a content table.
const ROLE_COLUMN: &str = "role";

/// Column names a descriptor may not declare: the row header's field names
/// ([`RESERVED_FIELD_NAMES`](crate::block::RESERVED_FIELD_NAMES), except
/// `role` — the one header fact a content table legitimately carries, as its
/// voice column) and the content table's own key. This literal moves in
/// lockstep with `RESERVED_FIELD_NAMES` (2026-08-22, when `dispatch_anchor`
/// joined both): a header name missing here is a column the open-time
/// validation ACCEPTS and the serializer then silently drops, so the
/// lockstep is asserted by
/// [`tests::reserved_columns_mirror_the_header_field_names`].
const RESERVED_COLUMNS: &[&str] = &["id", "type", "created_at", "dispatch_anchor", "block_id"];

/// How many block ids one batch read binds at a time. The engine's parameter
/// ceiling is finite (999 by default), a bound the literal core statements
/// never approach because they bind no id lists — so the overlay chunks its
/// `IN` list well below it. Small under test, so a handful of rows crosses a
/// chunk boundary and the seam stays exercised.
const READ_CHUNK: usize = if cfg!(test) { 3 } else { 500 };

/// The library's own content tables keyed by block id. Every one of them
/// declares `block_id INTEGER PRIMARY KEY`, so a row change hook's rowid IS
/// the block id — the fact the runtime's block watcher relies on, and the
/// shape a descriptor's table must share (checked at open).
pub(super) const CORE_CONTENT_TABLES: &[&str] = &[
    "block_text",
    "block_thinking",
    "block_code",
    "block_quote",
    "block_tool_call",
    "block_streaming_tool_call",
    "block_tool_result",
    "block_tool_error",
    "block_status",
    "block_approval_request",
    "block_approval_decision",
    "block_date_marker",
    "block_ancestor_reference",
];

/// Ledger tables the change hook announces whose rowid is NOT a block id: the
/// header table (rowid = block id, but it is not a content table), the
/// junction, the conversation rows, the thinking block's multi-row sidecar and
/// the second ledger. They join the hook's allowlist beside the content tables
/// and are deliberately not part of [`Store::content_tables`].
pub(super) const STRUCTURAL_CHANGE_TABLES: &[&str] = &[
    "blocks",
    "conversation_blocks",
    "conversations",
    "block_reasoning_detail",
    "metadata",
];

/// The stored type strings the library itself claims — the block kinds plus
/// the metadata ledger's two `meta_type` values, which surface as block types
/// on the same parse path. A descriptor claiming one of these would put two
/// owners behind one string.
///
/// A literal, and deliberately so — it is the namespace CLAIM, not a mirror of
/// any statement — but a test cross-checks it against the kinds the core block
/// query and the metadata read actually serve, so drift between the claim and
/// the served set goes red.
const CORE_KINDS: &[&str] = &[
    "text",
    "streaming",
    "system_prompt",
    "quote",
    "code",
    "thinking",
    "streaming_thinking",
    "tool_call",
    "streaming_tool_call",
    "tool_result",
    "tool_error",
    "status",
    "approval_request",
    "approval_decision",
    "date_marker",
    "title_request",
    "title_response",
    "ancestor_reference",
    "harness_message",
];

/// The effective content-table list: the library's own content tables followed
/// by every descriptor's. This is the ONE list — the change-hook allowlist and
/// the runtime's block watcher both read it, and nothing else names a content
/// table.
pub(super) fn effective_content_tables(descriptors: &[ContentDescriptor]) -> Vec<&'static str> {
    CORE_CONTENT_TABLES
        .iter()
        .copied()
        .chain(descriptors.iter().map(|d| d.table))
        .collect()
}

/// The row change hook's allowlist: the structural ledger tables plus the
/// effective content-table list. A change to a table not named here wakes
/// nothing — which is why a descriptor's table joins the moment the store
/// opens, and why the rule "every table whose rows carry ledger content is
/// announced" can no longer drift from the load path: both are built from the
/// same descriptors.
///
/// Left off deliberately, because their rows are not ledger content: drafts
/// (mutable composer state), attachments and their sidecars, the provider
/// and model registries, and the store's own tracking tables (the migration
/// counters and the descriptor registry).
pub(super) fn change_hook_tables(descriptors: &[ContentDescriptor]) -> Vec<&'static str> {
    STRUCTURAL_CHANGE_TABLES
        .iter()
        .copied()
        .chain(effective_content_tables(descriptors))
        .collect()
}

/// The descriptor that owns a stored type string, if any.
pub(super) fn descriptor_for_kind<'d>(
    descriptors: &'d [ContentDescriptor],
    kind: &str,
) -> Option<&'d ContentDescriptor> {
    descriptors.iter().find(|d| d.kinds.contains(&kind))
}

/// How many descriptors a set of descriptor slices holds in total — the
/// length of what [`concat_descriptors`] produces, evaluable in a const
/// context so the derive can size the concatenated array from the composed
/// kinds' own declarations.
#[must_use]
pub const fn descriptor_count(sets: &[&[ContentDescriptor]]) -> usize {
    let mut total = 0;
    let mut i = 0;
    while i < sets.len() {
        total += sets[i].len();
        i += 1;
    }
    total
}

/// Concatenate descriptor slices into one array at compile time, in order —
/// the composing enum's descriptor set, built from each composed kind's own
/// declaration so no second list of tables exists anywhere. `N` must be
/// [`descriptor_count`] of the same sets; the derive infers it from the
/// annotated array type, and a mismatch fails the build.
///
/// # Panics
///
/// At compile time, if `N` differs from the sets' total — unreachable through
/// the derive, which computes both from the same sets.
#[must_use]
pub const fn concat_descriptors<const N: usize>(
    sets: &[&[ContentDescriptor]],
) -> [ContentDescriptor; N] {
    let placeholder = ContentDescriptor {
        table: "",
        domain: "",
        kinds: &[],
        columns: &[],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: None,
    };
    let mut out = [placeholder; N];
    let mut at = 0;
    let mut i = 0;
    while i < sets.len() {
        let mut j = 0;
        while j < sets[i].len() {
            out[at] = sets[i][j];
            at += 1;
            j += 1;
        }
        i += 1;
    }
    assert!(
        at == N,
        "concat_descriptors was given an N that is not the sets' total"
    );
    out
}

/// A descriptor-supplied name as a quoted SQL identifier.
///
/// Every descriptor-supplied name is interpolated into generated SQL, and the
/// identifier check alone does not make that safe: `is_identifier` rightly
/// admits SQL keywords (`order`, `group` are fine column names), and unquoted
/// they change the statement's parse. The check bars quote characters, so no
/// escaping is needed here — the quoting is the whole job.
pub(super) fn quoted(name: &str) -> String {
    format!("\"{name}\"")
}

// ─── Open-time validation ────────────────────────────────────────────────

/// The engine's column affinity, derived from a declared type per its own
/// rules — what decides whether a [`ColumnType`] can live in a column
/// losslessly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Affinity {
    Text,
    Integer,
    Real,
    Numeric,
    Blob,
}

/// The affinity a declared SQL type carries, per the engine's five rules.
fn affinity_of(declared: &str) -> Affinity {
    let t = declared.to_ascii_uppercase();
    if t.contains("INT") {
        Affinity::Integer
    } else if t.contains("CHAR") || t.contains("CLOB") || t.contains("TEXT") {
        Affinity::Text
    } else if t.is_empty() || t.contains("BLOB") {
        Affinity::Blob
    } else if t.contains("REAL") || t.contains("FLOA") || t.contains("DOUB") {
        Affinity::Real
    } else {
        Affinity::Numeric
    }
}

/// Whether a column of this affinity holds the declared type without
/// converting it. BLOB affinity holds nothing: a stored blob has no field
/// form, so it is refused here at open instead of erroring on first read.
fn affinity_holds(ty: ColumnType, affinity: Affinity) -> bool {
    match ty {
        ColumnType::Text | ColumnType::Json => affinity == Affinity::Text,
        ColumnType::Integer | ColumnType::Boolean => {
            matches!(affinity, Affinity::Integer | Affinity::Numeric)
        }
        ColumnType::Real => matches!(affinity, Affinity::Real | Affinity::Numeric),
    }
}

/// One column as `PRAGMA table_info` reports it: name, declared type, and its
/// position in the primary key (0 = not part of it).
struct TableColumn {
    name: String,
    declared_type: String,
    pk_position: i64,
}

/// Check every descriptor against the schema the migrations just created and
/// against each other. Runs at open, after the migrations and before the store
/// serves anything, so a misdeclared descriptor fails the open loudly instead
/// of loading empty payloads silently. This is the conformance kit's
/// table-existence check, made mechanical.
///
/// `core_tables` is the snapshot of what the library's own migrations create —
/// taken from a pristine schema, so it is correct by construction and a
/// descriptor cannot claim a library table.
pub(super) fn validate(
    conn: &Connection,
    descriptors: &[ContentDescriptor],
    core_tables: &HashSet<String>,
) -> Result<(), StoreError> {
    let descriptor_tables: HashSet<&str> = descriptors.iter().map(|d| d.table).collect();
    let mut claimed_tables: HashSet<&str> = HashSet::new();
    let mut claimed_kinds: HashSet<&str> = HashSet::new();

    for descriptor in descriptors {
        let fail = |reason: String| {
            Err(StoreError::InvalidDescriptor {
                table: descriptor.table.to_owned(),
                reason,
            })
        };

        // Every name a descriptor declares is interpolated into SQL, so each
        // must be a plain identifier before anything else is asked about it.
        // (Generated SQL quotes them too — the identifier check is what makes
        // the quoting escape-free.)
        for name in [descriptor.table, descriptor.domain]
            .into_iter()
            .chain(descriptor.columns.iter().map(|c| c.name))
            .chain(descriptor.kinds.iter().copied())
            .chain(
                descriptor
                    .reference_columns
                    .iter()
                    .flat_map(|r| [r.table, r.column]),
            )
        {
            if !is_identifier(name) {
                return fail(format!("'{name}' is not a plain identifier"));
            }
        }

        if descriptor.domain == super::CORE_DOMAIN {
            return fail(format!(
                "domain '{}' is the library's own; a descriptor's tables live under \
                 a consumer domain",
                super::CORE_DOMAIN
            ));
        }
        if core_tables.contains(descriptor.table) {
            return fail("the table collides with a library table".into());
        }
        if !claimed_tables.insert(descriptor.table) {
            return fail("another descriptor already owns the table".into());
        }
        if !table_exists(conn, descriptor.table)? {
            return fail("the table does not exist — the domain migrations must create it".into());
        }

        let shape = table_shape(conn, descriptor.table)?;
        validate_key(conn, descriptor, &shape)?;
        validate_columns(descriptor, &shape)?;

        for kind in descriptor.kinds {
            if CORE_KINDS.contains(kind) {
                return fail(format!("kind '{kind}' collides with a library kind"));
            }
            if !claimed_kinds.insert(kind) {
                return fail(format!("kind '{kind}' is claimed by another descriptor"));
            }
        }

        validate_references(conn, descriptor, &descriptor_tables)?;
    }
    Ok(())
}

/// The declared columns against the table's real shape: reserved names,
/// existence, the role column's text form, and — for every declared type —
/// that the column's affinity holds it losslessly. BLOB affinity (a declared
/// BLOB or a typeless column) holds nothing and is refused here at open,
/// instead of erroring on first read. The quotable-text declaration is checked
/// here too, against the same declared columns.
fn validate_columns(
    descriptor: &ContentDescriptor,
    shape: &[TableColumn],
) -> Result<(), StoreError> {
    let fail = |reason: String| {
        Err(StoreError::InvalidDescriptor {
            table: descriptor.table.to_owned(),
            reason,
        })
    };

    for column in descriptor.columns {
        if RESERVED_COLUMNS.contains(&column.name) {
            return fail(format!(
                "column '{}' collides with the row header's own field names",
                column.name
            ));
        }
        let Some(existing) = shape.iter().find(|c| c.name == column.name) else {
            return fail(format!(
                "declared column '{}' does not exist in the table",
                column.name
            ));
        };
        if column.name == ROLE_COLUMN && column.ty != ColumnType::Text {
            return fail(
                "the role column carries the block's voice as text; declare it \
                 ColumnType::Text"
                    .into(),
            );
        }
        let affinity = affinity_of(&existing.declared_type);
        if affinity == Affinity::Blob {
            return fail(format!(
                "column '{}' has BLOB affinity (declared '{}'), which has no field \
                 form — declare the column with a type the field mapping can hold",
                column.name, existing.declared_type
            ));
        }
        if !affinity_holds(column.ty, affinity) {
            return fail(format!(
                "column '{}' is declared {:?} but the table gives it '{}' \
                 ({affinity:?} affinity), which cannot hold that type losslessly",
                column.name, column.ty, existing.declared_type
            ));
        }
    }

    validate_quotable_column(descriptor, &fail)?;
    Ok(())
}

/// The quotable-text declaration against the declaration it points into.
///
/// A wrong declaration is refused at open rather than left to answer quotes
/// with nonsense: the quote resolver reads this column raw into the quoted
/// span, so a name that resolves to nothing, to a serialized payload, to a
/// role word, or to a row finalization is about to delete is a mistake in the
/// descriptor and nowhere else.
fn validate_quotable_column(
    descriptor: &ContentDescriptor,
    fail: &impl Fn(String) -> Result<(), StoreError>,
) -> Result<(), StoreError> {
    let Some(quotable) = descriptor.quoted_text_column else {
        return Ok(());
    };

    let Some(column) = descriptor.columns.iter().find(|c| c.name == quotable) else {
        return fail(format!(
            "quoted_text_column '{quotable}' is not one of the descriptor's declared \
             columns — a quote reads the column through this declaration, so it must \
             name one the descriptor already declares"
        ));
    };
    if quotable == ROLE_COLUMN {
        return fail(
            "quoted_text_column names the role column, which carries the block's \
             voice — a quote of it would resolve to the literal role word, never to \
             what the block said"
                .into(),
        );
    }
    // By VARIANT, not by affinity: the store keeps JSON in a text column, so
    // an affinity check would admit ColumnType::Json and splice a serialized
    // payload into quoted text.
    if column.ty != ColumnType::Text {
        return fail(format!(
            "quoted_text_column '{quotable}' is declared {:?}; quotable text is \
             ColumnType::Text",
            column.ty
        ));
    }
    if descriptor.ephemeral {
        return fail(format!(
            "quoted_text_column '{quotable}' is declared on an ephemeral kind, whose \
             rows finalization deletes — every quote of it would dangle by design"
        ));
    }
    Ok(())
}

/// The declared reference columns: each lives in a descriptor's table — this
/// one's or another descriptor's — never a library table. The library's own
/// reference columns belong to the literal reference union, and a `ColumnRef`
/// naming a library table would make the collector's generated predicate
/// reference a schema it does not own; the one proven consequence was a
/// predicate that disabled collection for good.
fn validate_references(
    conn: &Connection,
    descriptor: &ContentDescriptor,
    descriptor_tables: &HashSet<&str>,
) -> Result<(), StoreError> {
    let fail = |reason: String| {
        Err(StoreError::InvalidDescriptor {
            table: descriptor.table.to_owned(),
            reason,
        })
    };

    for reference in descriptor.reference_columns {
        if !descriptor_tables.contains(reference.table) {
            return fail(format!(
                "reference column {}.{} names a table no descriptor owns — a \
                 reference column lives in a descriptor's table, never a library \
                 table",
                reference.table, reference.column
            ));
        }
        if !table_exists(conn, reference.table)? {
            return fail(format!(
                "reference column {}.{} names a table that does not exist",
                reference.table, reference.column
            ));
        }
        if !table_shape(conn, reference.table)?
            .iter()
            .any(|c| c.name == reference.column)
        {
            return fail(format!(
                "reference column {}.{} does not exist",
                reference.table, reference.column
            ));
        }
    }
    Ok(())
}

/// The key checks the doc comment on [`ContentDescriptor`] swears by, each
/// verified against the schema pragmas rather than assumed:
///
/// - `block_id` exists and IS the rowid alias — the single `INTEGER PRIMARY
///   KEY` — because the change hook announces rowids and the block watcher
///   reads them as block ids.
/// - The table is a rowid table: a `WITHOUT ROWID` table fires no change hook
///   ever, so a store built on one would accept every write and wake nothing.
/// - `block_id` cascades from `blocks(id)`, because an uncascaded key aborts
///   the collector's DELETE and a missing key strands content rows.
fn validate_key(
    conn: &Connection,
    descriptor: &ContentDescriptor,
    shape: &[TableColumn],
) -> Result<(), StoreError> {
    let fail = |reason: String| {
        Err(StoreError::InvalidDescriptor {
            table: descriptor.table.to_owned(),
            reason,
        })
    };

    let Some(block_id) = shape.iter().find(|c| c.name == "block_id") else {
        return fail("the table has no block_id column to key content rows by".into());
    };
    let pk_count = shape.iter().filter(|c| c.pk_position > 0).count();
    if pk_count != 1
        || block_id.pk_position != 1
        || !block_id.declared_type.eq_ignore_ascii_case("integer")
    {
        return fail(format!(
            "block_id must be the table's one INTEGER PRIMARY KEY — the rowid alias, \
             so the change hook's announced rowid IS the block id — but the table \
             declares it '{}' with {pk_count} primary key column(s)",
            block_id.declared_type
        ));
    }
    if is_without_rowid(conn, descriptor.table)? {
        return fail(
            "the table is WITHOUT ROWID, and the row change hook never fires for such \
             a table — no write to it would ever wake the scheduler; declare it as an \
             ordinary rowid table"
                .into(),
        );
    }
    if !block_id_cascades(conn, descriptor.table)? {
        return fail(
            "block_id must be declared REFERENCES blocks(id) ON DELETE CASCADE — \
             without the cascade, the first header row deleted out from under a \
             content row aborts the whole statement, and collection stops for good"
                .into(),
        );
    }
    Ok(())
}

fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool, StoreError> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [table],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

/// Whether the table was declared `WITHOUT ROWID`, per the schema's own table
/// list pragma (`wr` is 1 for such a table).
fn is_without_rowid(conn: &Connection, table: &str) -> Result<bool, StoreError> {
    let wr: i64 = conn.query_row(
        "SELECT wr FROM pragma_table_list(?1) WHERE schema = 'main'",
        [table],
        |row| row.get(0),
    )?;
    Ok(wr != 0)
}

/// Whether `table`'s `block_id` carries the contract's delete rule: a foreign
/// key to `blocks` whose delete action is `CASCADE`. `PRAGMA foreign_key_list`
/// answers per declared key — column 2 is the referenced table, column 3 the
/// referencing column, column 6 the delete action — and a table with no
/// foreign key at all yields no rows, which fails the same check: an
/// uncascaded key aborts the collector's DELETE, and a missing key strands the
/// content row for a reused block id to inherit.
fn block_id_cascades(conn: &Connection, table: &str) -> Result<bool, StoreError> {
    let mut stmt = conn.prepare(&format!("PRAGMA foreign_key_list({})", quoted(table)))?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let referenced: String = row.get(2)?;
        let from: String = row.get(3)?;
        let on_delete: String = row.get(6)?;
        if from == "block_id" && referenced == "blocks" && on_delete == "CASCADE" {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The table's columns as the schema declares them.
fn table_shape(conn: &Connection, table: &str) -> Result<Vec<TableColumn>, StoreError> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", quoted(table)))?;
    let columns = stmt
        .query_map([], |row| {
            Ok(TableColumn {
                name: row.get(1)?,
                declared_type: row.get(2)?,
                pk_position: row.get(5)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(columns)
}

// ─── The descriptor registry ─────────────────────────────────────────────

/// Fail the open when the database's descriptor registry names content tables
/// the supplied descriptor set does not cover. Runs before the consumer's
/// migrations: a database that was created with descriptors NEEDS them on
/// every open — read without them it is a different ledger, rendering consumer
/// blocks as empty content and aborting collection on their references.
pub(super) fn check_registry(
    conn: &Connection,
    descriptors: &[ContentDescriptor],
) -> Result<(), StoreError> {
    let mut stmt =
        conn.prepare("SELECT table_name, kinds FROM content_descriptors ORDER BY table_name")?;
    let registered = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    // The registry is the database's own statement of BOTH facts a reopen must
    // reproduce: which tables carry descriptor content, and which kinds live in
    // each. Comparing tables alone let a reopen rename a kind and read the same
    // rows back as empty core content — the exact misread the registry exists
    // to refuse.
    let supplied: std::collections::HashMap<&str, String> = descriptors
        .iter()
        .map(|d| {
            let kinds = serde_json::to_string(d.kinds).unwrap_or_default();
            (d.table, kinds)
        })
        .collect();
    let tables: Vec<String> = registered
        .into_iter()
        .filter(|(table, kinds)| supplied.get(table.as_str()) != Some(kinds))
        .map(|(table, _)| table)
        .collect();
    if tables.is_empty() {
        Ok(())
    } else {
        Err(StoreError::MissingDescriptors { tables })
    }
}

/// Record every descriptor's table and kinds durably, making the registry the
/// database's own statement that these tables carry descriptor-driven content.
/// Called only after validation passed, so a failed open registers nothing.
pub(super) fn record_registry(
    conn: &Connection,
    descriptors: &[ContentDescriptor],
) -> Result<(), StoreError> {
    for descriptor in descriptors {
        let kinds = serde_json::to_string(descriptor.kinds)
            .map_err(|e| StoreError::Other(format!("descriptor kinds do not serialize: {e}")))?;
        conn.execute(
            "INSERT INTO content_descriptors (table_name, domain, kinds)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(table_name) DO UPDATE
                 SET domain = excluded.domain, kinds = excluded.kinds",
            rusqlite::params![descriptor.table, descriptor.domain, kinds],
        )?;
    }
    Ok(())
}

// ─── The consumer load path ──────────────────────────────────────────────

/// The second load step: for every block whose kind a descriptor claims,
/// replace the query's inert fallback payload with the declared columns, read
/// by name from the descriptor's table in one batch per descriptor. A claimed
/// block with no content row is [`StoreError::MissingBlockContent`], exactly
/// as it is for a library kind — faithful replay does not depend on who
/// defined the kind.
///
/// Each descriptor's read runs under its domain's gate: with that domain's
/// migrations in a failed state, the read answers with the migration failure
/// instead of running raw against a schema in doubt — and only when the ledger
/// actually holds one of the descriptor's kinds, so a failed domain does not
/// poison reads that never touch its tables.
pub(super) fn overlay_consumer_content(
    conn: &Connection,
    descriptors: &[ContentDescriptor],
    gate: &DomainGate,
    blocks: &mut [Block],
) -> Result<(), StoreError> {
    for descriptor in descriptors {
        let targets: Vec<usize> = blocks
            .iter()
            .enumerate()
            .filter(|(_, b)| descriptor.kinds.contains(&b.block_type.as_str()))
            .map(|(index, _)| index)
            .collect();
        if targets.is_empty() {
            continue;
        }
        gate.ensure(descriptor.domain)?;

        let ids: Vec<i64> = targets.iter().map(|&index| blocks[index].id).collect();
        let mut rows = read_content_rows(conn, descriptor, &ids)?;
        for index in targets {
            let block = &mut blocks[index];
            let Some((role, fields)) = rows.remove(&block.id) else {
                return Err(StoreError::MissingBlockContent {
                    block_id: block.id,
                    block_type: block.block_type.clone(),
                });
            };
            block.role = role;
            block.fields = fields;
        }
    }
    Ok(())
}

/// One batch read of a descriptor's table: block id to (role, fields), the
/// `IN` list chunked below the engine's parameter ceiling.
#[allow(clippy::type_complexity)]
fn read_content_rows(
    conn: &Connection,
    descriptor: &ContentDescriptor,
    ids: &[i64],
) -> Result<HashMap<i64, (Option<Role>, serde_json::Map<String, Value>)>, StoreError> {
    let mut column_list = String::new();
    for column in descriptor.columns {
        column_list.push_str(", ");
        column_list.push_str(&quoted(column.name));
    }

    let mut out = HashMap::new();
    for chunk in ids.chunks(READ_CHUNK) {
        let placeholders = vec!["?"; chunk.len()].join(", ");
        let sql = format!(
            "SELECT block_id{column_list} FROM {} WHERE block_id IN ({placeholders})",
            quoted(descriptor.table)
        );

        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(chunk.iter()))?;
        while let Some(row) = rows.next()? {
            let block_id: i64 = row.get(0)?;
            let mut role: Option<Role> = None;
            let mut fields = serde_json::Map::new();
            for (offset, column) in descriptor.columns.iter().enumerate() {
                let index = offset + 1;
                if column.name == ROLE_COLUMN {
                    role = parse_role(row.get::<_, Option<String>>(index)?.as_deref());
                } else if let Some(value) =
                    column_to_field(descriptor, column, row.get_ref(index)?)?
                {
                    fields.insert(column.name.to_owned(), value);
                }
            }
            out.insert(block_id, (role, fields));
        }
    }
    Ok(out)
}

/// One batch read of a descriptor's declared quotable column: block id to
/// text, for the quote resolver.
///
/// A descriptor with no [`quoted_text_column`](ContentDescriptor::quoted_text_column)
/// answers with nothing, which is how a kind that never declared one resolves
/// empty without a special case anywhere upstream. A row whose column is NULL
/// answers with the empty string through `COALESCE` — the shape an erased
/// message takes, and the reason erasure needs no special case here either. A
/// block with no row at all is simply absent from the map, exactly as a
/// missing `block_text` row is.
///
/// The caller consults the domain gate before calling: this runs raw, so
/// nothing may call it for a domain whose migrations are in a failed state.
pub(super) fn read_quoted_text(
    conn: &Connection,
    descriptor: &ContentDescriptor,
    ids: &[i64],
) -> Result<HashMap<i64, String>, StoreError> {
    let Some(column) = descriptor.quoted_text_column else {
        return Ok(HashMap::new());
    };

    let mut out = HashMap::new();
    for chunk in ids.chunks(READ_CHUNK) {
        let placeholders = vec!["?"; chunk.len()].join(", ");
        let sql = format!(
            "SELECT block_id, COALESCE({}, '') FROM {} WHERE block_id IN ({placeholders})",
            quoted(column),
            quoted(descriptor.table)
        );
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(chunk.iter()))?;
        while let Some(row) = rows.next()? {
            out.insert(row.get(0)?, row.get(1)?);
        }
    }
    Ok(out)
}

/// A stored column value as a block field, mapped through the column's
/// declared type. `None` is an omitted field: a NULL column is skipped on
/// read, so what was not written does not come back as a present null.
fn column_to_field(
    descriptor: &ContentDescriptor,
    column: &Column,
    value: ValueRef<'_>,
) -> Result<Option<Value>, StoreError> {
    let mismatch = |held: &str| {
        Err(StoreError::Other(format!(
            "column '{}' of table '{}' is declared {:?} but holds {held}",
            column.name, descriptor.table, column.ty
        )))
    };
    if matches!(value, ValueRef::Null) {
        return Ok(None);
    }
    let mapped = match (column.ty, value) {
        (ColumnType::Text, ValueRef::Text(text)) => {
            Value::String(String::from_utf8_lossy(text).into_owned())
        }
        (ColumnType::Integer, ValueRef::Integer(i)) => Value::Number(i.into()),
        (ColumnType::Boolean, ValueRef::Integer(i)) => Value::Bool(i != 0),
        (ColumnType::Real, ValueRef::Integer(i)) => {
            #[allow(clippy::cast_precision_loss)]
            let f = i as f64;
            return number_from_f64(descriptor, column, f).map(Some);
        }
        (ColumnType::Real, ValueRef::Real(f)) => {
            return number_from_f64(descriptor, column, f).map(Some);
        }
        (ColumnType::Json, ValueRef::Text(text)) => serde_json::from_slice(text).map_err(|e| {
            StoreError::Other(format!(
                "column '{}' of table '{}' is declared Json but does not parse: {e}",
                column.name, descriptor.table
            ))
        })?,
        (_, ValueRef::Blob(_)) => return mismatch("a BLOB, which has no field form"),
        (_, ValueRef::Integer(_)) => return mismatch("an integer"),
        (_, ValueRef::Real(_)) => return mismatch("a real"),
        (_, ValueRef::Text(_)) => return mismatch("text"),
        (_, ValueRef::Null) => unreachable!("NULL returns above"),
    };
    Ok(Some(mapped))
}

/// A finite float as a JSON number, with the non-finite case named.
fn number_from_f64(
    descriptor: &ContentDescriptor,
    column: &Column,
    f: f64,
) -> Result<Value, StoreError> {
    serde_json::Number::from_f64(f)
        .map(Value::Number)
        .ok_or_else(|| {
            StoreError::Other(format!(
                "column '{}' of table '{}' holds a non-finite number",
                column.name, descriptor.table
            ))
        })
}

/// A block field as a column value, checked against the column's declared
/// type. A `Null` field is stored as NULL — the same absence an omitted field
/// gets. Nested JSON is refused everywhere but under [`ColumnType::Json`]: the
/// schema rule is one column per datum, for consumer tables no less than the
/// library's.
fn field_to_column(
    descriptor: &ContentDescriptor,
    column: &Column,
    value: &Value,
) -> Result<SqlValue, StoreError> {
    let mismatch = || {
        Err(StoreError::Other(format!(
            "field '{}' for table '{}' does not fit its declared type {:?}",
            column.name, descriptor.table, column.ty
        )))
    };
    if value.is_null() {
        return Ok(SqlValue::Null);
    }
    match (column.ty, value) {
        (ColumnType::Text, Value::String(s)) => Ok(SqlValue::Text(s.clone())),
        (ColumnType::Integer, Value::Number(n)) => match n.as_i64() {
            Some(i) => Ok(SqlValue::Integer(i)),
            None => mismatch(),
        },
        (ColumnType::Real, Value::Number(n)) => match n.as_f64() {
            Some(f) => Ok(SqlValue::Real(f)),
            None => mismatch(),
        },
        (ColumnType::Boolean, Value::Bool(b)) => Ok(SqlValue::Integer(i64::from(*b))),
        (ColumnType::Json, any) => serde_json::to_string(any).map(SqlValue::Text).map_err(|e| {
            StoreError::Other(format!(
                "field '{}' for table '{}' does not serialize: {e}",
                column.name, descriptor.table
            ))
        }),
        _ => mismatch(),
    }
}

// ─── The consumer write and copy paths ───────────────────────────────────

impl Store {
    /// Append a consumer kind's block: header, junction and content row in one
    /// transaction, the content row driven by the kind's descriptor. This is
    /// the one write path for kinds defined outside the library; the library's
    /// own kinds keep their typed inserts.
    ///
    /// `fields` maps declared column names to values, each checked against the
    /// column's declared type. A declared column with no field is written as
    /// NULL, so the table's own constraints decide loudly whether it was
    /// required. The role travels as its own argument and is written to the
    /// table's `role` column.
    ///
    /// `replaces_streaming` is the ephemeral tail this block finalizes,
    /// deleted in the SAME transaction — the identical atomic replace the
    /// library's own finalizing inserts carry, and the only way an ephemeral
    /// consumer kind keeps its "deleted by the finalization that replaces it"
    /// promise. The delete is type-guarded: a committed block is unreachable
    /// through it.
    ///
    /// The write runs under the descriptor's own domain, so a failed migration
    /// for that domain answers it with [`StoreError::MigrationFailed`] exactly
    /// as [`domain_run`](super::domain_run) is answered.
    ///
    /// The date-marker discipline runs here for a USER-VOICED append, and only
    /// for one: an append whose role is [`Role::User`] carries the marker's
    /// change detection in its own transaction, ordered before the block, so
    /// the day a member speaks on is on the wire exactly as the library's own
    /// group appends put it there.
    ///
    /// Amended 2026-08-27. The recorded reasoning this replaces — that the
    /// discipline "deliberately does NOT run here" — was written about the
    /// composer's finalizing inserts and the approval chain, which detect the
    /// day change once per submitted group and keep their skip. It silently
    /// decided for every consumer as well, and a consumer landing chat
    /// messages through this path is the group-append seam, whatever the
    /// argument list looks like. Non-user appends — context notes, reports,
    /// anything role-less or assistant-voiced — still never trip it.
    ///
    /// One consequence of that amendment, recorded here at the seam that
    /// causes it rather than left to be re-discovered: a marker carries NO
    /// role, so it BREAKS a role-contiguous run. The fork's group walk
    /// (`conversations::find_group_bounds`) therefore stops at it — a fork
    /// anchored on the day's first user-voiced consumer append copies that
    /// append without the user blocks that came before the marker, where
    /// before this amendment it copied them too. Every such split is a day
    /// boundary, since that is the only thing that trips a marker; the blocks
    /// on the far side belong to a different day's turn. The slice that made
    /// this change does not name the split among its residuals — it is stated
    /// here, and pinned by
    /// `a_marker_splits_a_user_run_at_the_days_first_append`.
    ///
    /// # Errors
    ///
    /// [`StoreError::UnsupportedBlockKind`] if no descriptor claims `kind`;
    /// [`StoreError::MigrationFailed`] if the descriptor's domain is in a
    /// failed-migration state; an error if a field names an undeclared column
    /// or does not fit its declared type, if a role is given without a
    /// declared `role` column, if the transaction fails — a refused junction
    /// insert, or a refused marker, rolls back the header with it — or if the
    /// store's actor has stopped.
    pub async fn append_consumer_block(
        &self,
        conversation_id: i64,
        role: Option<Role>,
        kind: &'static str,
        fields: serde_json::Map<String, Value>,
        replaces_streaming: Option<i64>,
    ) -> Result<i64, StoreError> {
        self.append_consumer_block_stamped(
            conversation_id,
            role,
            kind,
            fields,
            replaces_streaming,
            super::date_markers::DateStamp::now_local(),
        )
        .await
    }

    /// The injectable-stamp seam behind [`Store::append_consumer_block`],
    /// mirroring [`Store::insert_user_blocks_dated`] behind its own public
    /// method: production passes the stamp built from now, tests drive
    /// midnight, zone changes and the NULL cases deterministically.
    pub(crate) async fn append_consumer_block_stamped(
        &self,
        conversation_id: i64,
        role: Option<Role>,
        kind: &'static str,
        fields: serde_json::Map<String, Value>,
        replaces_streaming: Option<i64>,
        stamp: super::date_markers::DateStamp,
    ) -> Result<i64, StoreError> {
        let descriptors = self.descriptors;
        let Some(descriptor) = descriptor_for_kind(descriptors, kind) else {
            return Err(StoreError::UnsupportedBlockKind {
                block_type: kind.to_owned(),
                operation: "Store::append_consumer_block",
            });
        };

        super::domain_run(&self.tx, descriptor.domain, move |conn| {
            if role.is_some() && !descriptor.columns.iter().any(|c| c.name == ROLE_COLUMN) {
                return Err(StoreError::Other(format!(
                    "kind '{kind}' carries a role but table '{}' declares no role column",
                    descriptor.table
                )));
            }
            for name in fields.keys() {
                if name == ROLE_COLUMN {
                    return Err(StoreError::Other(
                        "the role travels as its own argument, never as a field".into(),
                    ));
                }
                if !descriptor.columns.iter().any(|c| c.name == name) {
                    return Err(StoreError::Other(format!(
                        "field '{name}' is not a declared column of table '{}'",
                        descriptor.table
                    )));
                }
            }

            transact(conn, |tx| {
                // A user-voiced append is the day's first word as much as a
                // composed group is, so it runs the same change detection —
                // inside this transaction and BEFORE the block, which is what
                // puts the marker ahead of the message in junction order.
                if role == Some(Role::User) {
                    super::date_markers::ensure_date_marker(tx, conversation_id, &stamp)?;
                }
                // The public consumer write path never sets a dispatch
                // anchor: the anchor is written by the framework's own paths
                // only, so a consumer block is never a turn's product by its
                // own claim.
                let block_id = super::messages::insert_block(tx, conversation_id, kind)?;

                let mut names: Vec<String> = vec![quoted("block_id")];
                let mut values: Vec<SqlValue> = vec![SqlValue::Integer(block_id)];
                for column in descriptor.columns {
                    names.push(quoted(column.name));
                    if column.name == ROLE_COLUMN {
                        values.push(
                            role.map_or(SqlValue::Null, |r| SqlValue::Text(r.as_str().to_owned())),
                        );
                    } else {
                        values.push(match fields.get(column.name) {
                            Some(value) => field_to_column(descriptor, column, value)?,
                            None => SqlValue::Null,
                        });
                    }
                }
                let placeholders = (1..=names.len())
                    .map(|i| format!("?{i}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                tx.execute(
                    &format!(
                        "INSERT INTO {} ({}) VALUES ({placeholders})",
                        quoted(descriptor.table),
                        names.join(", ")
                    ),
                    rusqlite::params_from_iter(values.iter()),
                )?;
                if let Some(streaming_id) = replaces_streaming {
                    super::messages::delete_streaming_counterpart(tx, descriptors, streaming_id)?;
                }
                Ok(block_id)
            })
        })
        .await
    }
}

/// Deep-copy one consumer content row onto a fresh block id, generically from
/// the declared columns — the fork path for a kind the library never heard of.
///
/// The copy carries the declared content columns plus every reference column
/// any descriptor aims at this table, and each reference is resolved through
/// the cloner's id map exactly as a core reference is: remapped to the clone's
/// id where the referenced block was cloned, kept by reference where it was
/// not — the core detached-target semantics, under which the collector's
/// reference predicate keeps the still-referenced source block alive.
pub(super) fn clone_consumer_content(
    conn: &Connection,
    descriptors: &[ContentDescriptor],
    descriptor: &ContentDescriptor,
    src_block_id: i64,
    new_block_id: i64,
    block_type: &str,
    remap: &HashMap<i64, i64>,
) -> Result<(), StoreError> {
    let mut names: Vec<&str> = descriptor.columns.iter().map(|c| c.name).collect();
    let mut reference_names: HashSet<&str> = HashSet::new();
    for reference in descriptors.iter().flat_map(|d| d.reference_columns.iter()) {
        if reference.table == descriptor.table {
            reference_names.insert(reference.column);
            if !names.contains(&reference.column) {
                names.push(reference.column);
            }
        }
    }

    let select = names
        .iter()
        .map(|n| quoted(n))
        .collect::<Vec<_>>()
        .join(", ");
    let row: Option<Vec<SqlValue>> = conn
        .query_row(
            &format!(
                "SELECT {select} FROM {} WHERE block_id = ?1",
                quoted(descriptor.table)
            ),
            [src_block_id],
            |row| {
                (0..names.len())
                    .map(|i| row.get::<_, SqlValue>(i))
                    .collect()
            },
        )
        .optional()?;
    let Some(mut values) = row else {
        return Err(StoreError::MissingBlockContent {
            block_id: src_block_id,
            block_type: block_type.to_owned(),
        });
    };

    for (name, value) in names.iter().zip(values.iter_mut()) {
        if reference_names.contains(name)
            && let SqlValue::Integer(id) = value
            && let Some(&cloned) = remap.get(id)
        {
            *value = SqlValue::Integer(cloned);
        }
    }

    let mut insert_names: Vec<String> = vec![quoted("block_id")];
    insert_names.extend(names.iter().map(|n| quoted(n)));
    let placeholders = (1..=insert_names.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut params: Vec<SqlValue> = vec![SqlValue::Integer(new_block_id)];
    params.extend(values);
    conn.execute(
        &format!(
            "INSERT INTO {} ({}) VALUES ({placeholders})",
            quoted(descriptor.table),
            insert_names.join(", ")
        ),
        rusqlite::params_from_iter(params.iter()),
    )?;
    Ok(())
}

/// The descriptor seam's tests: the core statements pinned byte-identical to
/// their literals, and a test descriptor carried through its whole lifecycle —
/// created by its domain migration, validated at open, loaded, woken on,
/// forked, collected and torn down — plus every loud refusal at open and at
/// the write path.
#[cfg(test)]
mod tests {
    use serde_json::{Map, json};

    use super::super::date_markers::DateStamp;
    use super::super::{
        Continuation, ModelOverride, StoreConfig, domain_migrate, domain_run, temp_dir,
    };
    use super::*;
    use crate::block::Role;

    // ─── The core kinds' statements, pinned against the literals ─────────

    /// The one block query, exactly as the library's own kinds use it. A
    /// difference here means the core load path was touched without this pin
    /// moving in lockstep: consumer kinds load through the overlay, never
    /// through an edited join list.
    ///
    /// Updated 2026-08-22 with the statement: the header select gained
    /// `dispatch_anchor` — a header column on `blocks`, not a content join,
    /// so the join list is untouched and every kind carries the anchor for
    /// free.
    ///
    /// Updated 2026-08-27 with the statement: the date marker's existing join
    /// gained three selected columns — the zone abbreviation, the IANA name
    /// and the writing minute — with the join list itself untouched.
    ///
    /// Updated 2026-08-30 with the statement: the tool result's existing join
    /// gained the turn-ending stamp, with the join list itself untouched.
    ///
    /// Updated 2026-08-31 with the statement: the ancestor reference added one
    /// join and one selected column, and the harness message was added to the
    /// prose table's kind list — the first change to the join list since it
    /// was pinned.
    ///
    /// Updated 2026-09-01 with the statement: the tool error's existing join
    /// gained the refusal fact, with the join list itself untouched.
    const PINNED_BLOCKS_QUERY: &str = "SELECT
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
            banc.ancestor_conversation_id AS banc_ancestor
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
     LEFT JOIN block_ancestor_reference banc ON banc.block_id = b.id AND b.block_type = 'ancestor_reference'";

    /// The collector's reference union for a core-only store, spelled out.
    ///
    /// Updated 2026-08-22 with the union: the self-referential
    /// `dispatch_anchor` arm joined it — an anchored-at block is a referenced
    /// block, which is what keeps a fork-then-delete anchor loadable instead
    /// of dangling.
    const PINNED_ORPHAN_PREDICATE: &str = "NOT EXISTS (SELECT 1 FROM conversation_blocks r WHERE r.block_id = blocks.id)\n    AND NOT EXISTS (SELECT 1 FROM block_quote r WHERE r.start_block_id = blocks.id OR r.end_block_id = blocks.id)\n    AND NOT EXISTS (SELECT 1 FROM block_approval_request r WHERE r.for_block_id = blocks.id)\n    AND NOT EXISTS (SELECT 1 FROM block_approval_decision r WHERE r.for_block_id = blocks.id)\n    AND NOT EXISTS (SELECT 1 FROM block_tool_result r WHERE r.source_block_id = blocks.id)\n    AND NOT EXISTS (SELECT 1 FROM block_tool_error r WHERE r.source_block_id = blocks.id)\n    AND NOT EXISTS (SELECT 1 FROM metadata r WHERE r.source_block_id = blocks.id)\n    AND NOT EXISTS (SELECT 1 FROM blocks r WHERE r.dispatch_anchor = blocks.id)";

    /// The change-hook allowlist a core-only store carried before descriptors
    /// existed, as the set it always was.
    const PINNED_HOOK_TABLES: &[&str] = &[
        "blocks",
        "conversation_blocks",
        "conversations",
        "block_text",
        "block_thinking",
        "block_reasoning_detail",
        "block_code",
        "block_quote",
        "block_tool_call",
        "block_streaming_tool_call",
        "block_tool_result",
        "block_tool_error",
        "block_status",
        "block_approval_request",
        "block_approval_decision",
        "block_date_marker",
        "block_ancestor_reference",
        "metadata",
    ];

    #[test]
    fn the_core_statements_are_byte_identical_to_the_literals() {
        assert_eq!(
            super::super::blocks::BLOCKS_QUERY,
            PINNED_BLOCKS_QUERY,
            "the core kinds' load statement is untouched"
        );
        assert_eq!(
            super::super::messages::orphan_block_predicate(&[]),
            PINNED_ORPHAN_PREDICATE,
            "the core-only reference union is untouched"
        );
        assert_eq!(
            super::super::messages::ephemeral_types_sql(&[]),
            "('streaming', 'streaming_thinking', 'streaming_tool_call')",
            "the core-only ephemeral type list is untouched"
        );

        let hook: std::collections::HashSet<&str> = change_hook_tables(&[]).into_iter().collect();
        let pinned: std::collections::HashSet<&str> = PINNED_HOOK_TABLES.iter().copied().collect();
        assert_eq!(hook, pinned, "the core-only hook allowlist is unchanged");
    }

    // ─── The name tables, cross-checked against what is actually served ──

    /// Every stored type string the pinned block query serves, extracted from
    /// the statement itself: the strings between single quotes are exactly the
    /// type names in its join conditions.
    fn kinds_served_by_the_block_query() -> std::collections::HashSet<&'static str> {
        super::super::blocks::BLOCKS_QUERY
            .split('\'')
            .enumerate()
            .filter(|(i, _)| i % 2 == 1)
            .map(|(_, s)| s)
            .collect()
    }

    /// `CORE_KINDS` is the library's namespace claim, and this is what keeps
    /// the literal honest: every kind the core block query serves is claimed,
    /// every claimed kind resolves to a real typed kind (never the inert
    /// fallback), the claimed kinds the query does not serve are exactly
    /// the metadata ledger's, which surface through the metadata read path —
    /// and the parse chain's own claim (`BlockKind::CLAIMED_KINDS`, the const
    /// the derive checks consumer leaves against) is the same nineteen
    /// strings. Any drift between the claims and what the code serves goes
    /// red here.
    #[test]
    fn core_kinds_match_what_the_core_paths_actually_serve() {
        use crate::agency::{BlockKind, FromBlock};

        let served = kinds_served_by_the_block_query();
        let claimed: std::collections::HashSet<&str> = CORE_KINDS.iter().copied().collect();
        let parse_claim: std::collections::HashSet<&str> =
            BlockKind::CLAIMED_KINDS.iter().copied().collect();
        assert_eq!(
            parse_claim, claimed,
            "the parse chain claims exactly the library's namespace claim"
        );
        assert_eq!(
            BlockKind::CLAIMED_KINDS.len(),
            19,
            "one claim per core kind, no duplicates"
        );
        for kind in &served {
            assert!(
                claimed.contains(kind),
                "the block query serves '{kind}' but CORE_KINDS does not claim it"
            );
        }

        for kind in CORE_KINDS {
            let block = Block {
                id: 0,
                role: None,
                block_type: (*kind).to_owned(),
                created_at: String::new(),
                dispatch_anchor: None,
                fields: Map::new(),
            };
            let parsed = BlockKind::from_block(&block);
            assert!(
                !matches!(parsed, BlockKind::Unknown(_)),
                "CORE_KINDS claims '{kind}' but the library resolves it to the inert fallback"
            );
            if !served.contains(kind) {
                assert!(
                    matches!(
                        parsed,
                        BlockKind::MetadataTitleRequest(_) | BlockKind::MetadataTitleResponse(_)
                    ),
                    "'{kind}' is claimed, unserved by the block query, and not a metadata \
                     kind — nothing serves it"
                );
            }
        }
    }

    // ─── The ephemeral flag and the kind's durable(), one fact ───────────

    /// The core kinds' durable ephemerality, written as descriptors so the
    /// coherence check runs over them. The two fixtures cannot rot: the test
    /// below pins the ephemeral half to the sweep's own type list and the
    /// union to `CORE_KINDS`.
    static CORE_DURABLE_KIND_NAMES: &[&str] = &[
        "text",
        "system_prompt",
        "quote",
        "code",
        "thinking",
        "tool_call",
        "tool_result",
        "tool_error",
        "status",
        "approval_request",
        "approval_decision",
        "date_marker",
        "title_request",
        "title_response",
        "ancestor_reference",
        "harness_message",
    ];

    static CORE_SET_AS_DESCRIPTORS: &[ContentDescriptor] = &[
        ContentDescriptor {
            table: "core_durable_check",
            domain: "core_check",
            kinds: CORE_DURABLE_KIND_NAMES,
            columns: &[],
            reference_columns: &[],
            ephemeral: false,
            quoted_text_column: None,
        },
        ContentDescriptor {
            table: "core_ephemeral_check",
            domain: "core_check",
            kinds: super::super::messages::STREAMING_TYPES,
            columns: &[],
            reference_columns: &[],
            ephemeral: true,
            quoted_text_column: None,
        },
    ];

    /// The coherence check over the core set: for every core kind, the store's
    /// ephemerality (the sweep's type list) and the kind's `durable()` are the
    /// one fact they claim to be. The fixture is pinned to the real lists
    /// first, so it cannot drift into checking a set of its own invention.
    #[test]
    fn the_core_set_declares_durability_coherently() {
        let ephemeral: std::collections::HashSet<&str> =
            CORE_SET_AS_DESCRIPTORS[1].kinds.iter().copied().collect();
        let sweep: std::collections::HashSet<&str> = super::super::messages::STREAMING_TYPES
            .iter()
            .copied()
            .collect();
        assert_eq!(
            ephemeral, sweep,
            "the ephemeral fixture IS the sweep's list"
        );

        let all: std::collections::HashSet<&str> = CORE_SET_AS_DESCRIPTORS
            .iter()
            .flat_map(|d| d.kinds.iter().copied())
            .collect();
        let claimed: std::collections::HashSet<&str> = CORE_KINDS.iter().copied().collect();
        assert_eq!(all, claimed, "the fixture covers exactly the core kinds");

        crate::agency::check_descriptor_durability::<crate::agency::BlockKind>(
            CORE_SET_AS_DESCRIPTORS,
        )
        .expect("every core kind's durable() negates its ephemerality");
    }

    /// The check catches the real defect: a kind whose typed side never
    /// declared itself ephemeral (here, anything resolving to the inert
    /// fallback answers durable) against a descriptor that did — the exact
    /// cursor-anchor regression, named per kind with both values.
    #[test]
    fn the_durability_check_names_a_kind_whose_declarations_disagree() {
        let err = crate::agency::check_descriptor_durability::<crate::agency::BlockKind>(
            NOTE_DESCRIPTORS,
        )
        .expect_err("note_draft is declared ephemeral but resolves durable");
        assert!(err.contains("note_draft"), "names the kind: {err}");
        assert!(
            err.contains("true") && err.contains("false"),
            "names both values: {err}"
        );
    }

    /// And a consumer kind whose two declarations agree passes.
    #[test]
    fn the_durability_check_passes_a_coherent_consumer_kind() {
        struct NoteKind {
            ephemeral: bool,
        }
        impl crate::agency::Agency for NoteKind {
            fn durable(&self) -> bool {
                !self.ephemeral
            }
        }
        impl crate::agency::FromBlock for NoteKind {
            const CLAIMED_KINDS: &'static [&'static str] = &["note", "note_draft"];

            fn from_block(block: &Block) -> Self {
                Self {
                    ephemeral: block.block_type == "note_draft",
                }
            }
        }
        crate::agency::check_descriptor_durability::<NoteKind>(NOTE_DESCRIPTORS)
            .expect("both declarations agree for every note kind");
    }

    // ─── The test descriptor and its schema ──────────────────────────────

    static NOTE_DESCRIPTORS: &[ContentDescriptor] = &[
        ContentDescriptor {
            table: "block_note",
            domain: "notes",
            kinds: &["note"],
            columns: &[
                Column::new("role", ColumnType::Text),
                Column::new("body", ColumnType::Text),
                Column::new("about_block_id", ColumnType::Integer),
            ],
            reference_columns: &[ColumnRef {
                table: "block_note",
                column: "about_block_id",
            }],
            ephemeral: false,
            quoted_text_column: None,
        },
        ContentDescriptor {
            table: "block_note_draft",
            domain: "notes",
            kinds: &["note_draft"],
            columns: &[Column::new("body", ColumnType::Text)],
            reference_columns: &[],
            ephemeral: true,
            quoted_text_column: None,
        },
    ];

    const NOTE_SCHEMA: &str = "
        CREATE TABLE block_note (
            block_id       INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
            role           TEXT,
            body           TEXT NOT NULL,
            about_block_id INTEGER REFERENCES blocks(id)
        );
        CREATE TABLE block_note_draft (
            block_id INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
            body     TEXT NOT NULL
        );";

    fn note_migrations() -> Vec<super::DomainMigrations> {
        vec![super::DomainMigrations {
            domain: "notes",
            sqls: vec![NOTE_SCHEMA],
        }]
    }

    fn note_config() -> StoreConfig {
        StoreConfig {
            descriptors: NOTE_DESCRIPTORS,
            domain_migrations: note_migrations(),
        }
    }

    fn configured_store() -> Store {
        Store::in_memory_with(note_config()).unwrap()
    }

    async fn make_conv(s: &Store) -> i64 {
        s.create_conversation("p1".into(), "model".into(), "model".into(), String::new())
            .await
            .unwrap()
    }

    fn note_fields(body: &str, about: Option<i64>) -> Map<String, Value> {
        let mut fields = Map::new();
        fields.insert("body".into(), Value::String(body.into()));
        if let Some(id) = about {
            fields.insert("about_block_id".into(), Value::Number(id.into()));
        }
        fields
    }

    // ─── The lifecycle, each step pinned ─────────────────────────────────

    /// Created and validated: the configured open ran the domain migration,
    /// the descriptors passed validation, and the store's one content-table
    /// list — the one the change hook and the block watcher read — names the
    /// consumer tables after the library's own.
    #[tokio::test]
    async fn the_domain_migration_creates_the_tables_the_open_validates() {
        let s = configured_store();

        let created: i64 = s
            .run(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE type = 'table' AND name IN ('block_note', 'block_note_draft')",
                    [],
                    |row| row.get(0),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(created, 2);

        let tables = s.content_tables();
        assert_eq!(&tables[..CORE_CONTENT_TABLES.len()], CORE_CONTENT_TABLES);
        assert!(tables.contains(&"block_note"));
        assert!(tables.contains(&"block_note_draft"));
    }

    /// Loaded: the write lands three rows, and every read path — the list,
    /// the single-block read and the frontier's one-row read — decodes the
    /// declared columns by name, with the role column carried as the block's
    /// voice and never as a field.
    #[tokio::test]
    async fn a_consumer_block_loads_through_its_descriptor() {
        let s = configured_store();
        let conv = make_conv(&s).await;

        let target = s
            .insert_text_block(conv, Role::Assistant, "the finding".into())
            .await
            .unwrap();
        let note = s
            .append_consumer_block(
                conv,
                Some(Role::User),
                "note",
                note_fields("remember this", Some(target)),
                None,
            )
            .await
            .unwrap();

        let blocks = s.list_blocks(conv).await.unwrap();
        let loaded = blocks.iter().find(|b| b.id == note).unwrap();
        assert_eq!(loaded.block_type, "note");
        assert_eq!(loaded.role, Some(Role::User));
        assert_eq!(loaded.fields["body"], "remember this");
        assert_eq!(loaded.fields["about_block_id"], target);
        assert!(
            !loaded.fields.contains_key("role"),
            "the voice is the block's role, not a payload field"
        );

        let single = s.find_block(note).await.unwrap().unwrap();
        assert_eq!(single.fields["body"], "remember this");

        let latest = s.latest_block(conv).await.unwrap().unwrap();
        assert_eq!(latest.id, note);
        assert_eq!(latest.fields["body"], "remember this");
    }

    /// A ledger past one chunk boundary loads whole: the overlay's `IN` list
    /// is chunked (three ids per chunk under test), and every block on both
    /// sides of the boundary gets its payload.
    #[tokio::test]
    async fn a_batch_read_past_the_chunk_boundary_loads_every_block() {
        let s = configured_store();
        let conv = make_conv(&s).await;

        let count = READ_CHUNK * 2 + 1;
        let mut ids = Vec::new();
        for i in 0..count {
            ids.push(
                s.append_consumer_block(
                    conv,
                    Some(Role::User),
                    "note",
                    note_fields(&format!("note {i}"), None),
                    None,
                )
                .await
                .unwrap(),
            );
        }

        let blocks = s.list_blocks(conv).await.unwrap();
        let notes: Vec<_> = blocks.iter().filter(|b| b.block_type == "note").collect();
        assert_eq!(notes.len(), count, "every chunk's blocks came back");
        for (i, id) in ids.iter().enumerate() {
            let block = notes.iter().find(|b| b.id == *id).unwrap();
            assert_eq!(block.fields["body"], format!("note {i}"));
        }
    }

    /// A claimed kind whose content row is missing is the same loud error a
    /// library kind gets — never an empty payload.
    #[tokio::test]
    async fn a_missing_consumer_content_row_is_an_error_naming_the_block() {
        let s = configured_store();
        let conv = make_conv(&s).await;

        let headerless = s
            .run(move |conn| {
                conn.execute("INSERT INTO blocks (block_type) VALUES ('note')", [])?;
                let id = conn.last_insert_rowid();
                conn.execute(
                    "INSERT INTO conversation_blocks (conversation_id, block_id)
                     VALUES (?1, ?2)",
                    rusqlite::params![conv, id],
                )?;
                Ok(id)
            })
            .await
            .unwrap();

        match s.list_blocks(conv).await {
            Err(StoreError::MissingBlockContent {
                block_id,
                block_type,
            }) => {
                assert_eq!(block_id, headerless);
                assert_eq!(block_type, "note");
            }
            other => panic!("expected a missing-content error, got {other:?}"),
        }
    }

    /// Woken on: a consumer append announces its content table through the
    /// change log — the same wakeup a library table gets, because the hook's
    /// allowlist is built from the same list.
    #[tokio::test]
    async fn a_consumer_write_wakes_the_change_log() {
        let s = configured_store();
        let conv = make_conv(&s).await;

        let consumer = s.changes.consumer();
        let _ = consumer.drain();

        s.append_consumer_block(
            conv,
            Some(Role::User),
            "note",
            note_fields("wake up", None),
            None,
        )
        .await
        .unwrap();

        let announced: std::collections::HashSet<String> =
            consumer.drain().into_iter().map(|c| c.table).collect();
        for table in ["blocks", "conversation_blocks", "block_note"] {
            assert!(
                announced.contains(table),
                "a consumer append announces {table}; announced: {announced:?}"
            );
        }
    }

    /// The write/read round trip is lossless through every declared type: a
    /// boolean comes back a boolean, JSON comes back parsed, and an omitted
    /// field stays absent — never a present null.
    static PROBE_DESCRIPTORS: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_probe",
        domain: "probes",
        kinds: &["probe"],
        columns: &[
            Column::new("t", ColumnType::Text),
            Column::new("i", ColumnType::Integer),
            Column::new("r", ColumnType::Real),
            Column::new("flag", ColumnType::Boolean),
            Column::new("j", ColumnType::Json),
        ],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: None,
    }];

    const PROBE_SCHEMA: &str = "
        CREATE TABLE block_probe (
            block_id INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
            t        TEXT,
            i        INTEGER,
            r        REAL,
            flag     BOOLEAN,
            j        TEXT
        );";

    fn probe_config() -> StoreConfig {
        StoreConfig {
            descriptors: PROBE_DESCRIPTORS,
            domain_migrations: vec![super::DomainMigrations {
                domain: "probes",
                sqls: vec![PROBE_SCHEMA],
            }],
        }
    }

    #[tokio::test]
    async fn every_declared_type_round_trips_and_an_omitted_field_stays_absent() {
        let s = Store::in_memory_with(probe_config()).unwrap();
        let conv = make_conv(&s).await;

        let mut fields = Map::new();
        fields.insert("t".into(), Value::String("plain".into()));
        fields.insert("i".into(), json!(42));
        fields.insert("r".into(), json!(1.5));
        fields.insert("flag".into(), Value::Bool(true));
        fields.insert("j".into(), json!({"a": [1, 2], "b": "nested"}));
        let full = s
            .append_consumer_block(conv, None, "probe", fields, None)
            .await
            .unwrap();

        let mut sparse = Map::new();
        sparse.insert("flag".into(), Value::Bool(false));
        let partial = s
            .append_consumer_block(conv, None, "probe", sparse, None)
            .await
            .unwrap();

        let blocks = s.list_blocks(conv).await.unwrap();
        let full = blocks.iter().find(|b| b.id == full).unwrap();
        assert_eq!(full.fields["t"], json!("plain"));
        assert_eq!(full.fields["i"], json!(42));
        assert_eq!(full.fields["r"], json!(1.5));
        assert_eq!(
            full.fields["flag"],
            Value::Bool(true),
            "a boolean reads back as a boolean, not a number"
        );
        assert_eq!(full.fields["j"], json!({"a": [1, 2], "b": "nested"}));

        let partial = blocks.iter().find(|b| b.id == partial).unwrap();
        assert_eq!(partial.fields["flag"], Value::Bool(false));
        for omitted in ["t", "i", "r", "j"] {
            assert!(
                !partial.fields.contains_key(omitted),
                "the omitted field '{omitted}' stays absent, never a present null"
            );
        }
    }

    /// A field that does not fit its declared type is refused at the write —
    /// the declared type is a contract, not a hint.
    #[tokio::test]
    async fn a_field_outside_its_declared_type_is_refused() {
        let s = Store::in_memory_with(probe_config()).unwrap();
        let conv = make_conv(&s).await;

        let mut fields = Map::new();
        fields.insert("i".into(), Value::String("not a number".into()));
        assert!(
            s.append_consumer_block(conv, None, "probe", fields, None)
                .await
                .is_err(),
            "a string is refused where an integer is declared"
        );

        let mut fields = Map::new();
        fields.insert("t".into(), json!({"nested": true}));
        assert!(
            s.append_consumer_block(conv, None, "probe", fields, None)
                .await
                .is_err(),
            "nested JSON is refused outside a Json column"
        );
    }

    /// A keyword-named column round-trips safely: `order` passes the
    /// identifier check on purpose, and the generated SQL quotes every
    /// descriptor-supplied identifier, so the keyword never reaches the
    /// parser bare.
    static KEYWORD_DESCRIPTORS: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_keyword",
        domain: "keywords",
        kinds: &["keyword_note"],
        columns: &[Column::new("order", ColumnType::Integer)],
        // The keyword-named column doubles as a declared reference, so the
        // collector's generated predicate carries it — the one site a bare
        // keyword identifier used to kill collection with a syntax error.
        reference_columns: &[ColumnRef {
            table: "block_keyword",
            column: "order",
        }],
        ephemeral: false,
        quoted_text_column: None,
    }];

    const KEYWORD_SCHEMA: &str = "
        CREATE TABLE block_keyword (
            block_id INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
            \"order\" INTEGER NOT NULL
        );";

    #[tokio::test]
    async fn a_keyword_named_column_round_trips_safely() {
        let s = Store::in_memory_with(StoreConfig {
            descriptors: KEYWORD_DESCRIPTORS,
            domain_migrations: vec![super::DomainMigrations {
                domain: "keywords",
                sqls: vec![KEYWORD_SCHEMA],
            }],
        })
        .unwrap();
        let conv = make_conv(&s).await;

        let mut fields = Map::new();
        fields.insert("order".into(), json!(7));
        let id = s
            .append_consumer_block(conv, None, "keyword_note", fields, None)
            .await
            .unwrap();

        let block = s.find_block(id).await.unwrap().unwrap();
        assert_eq!(block.fields["order"], json!(7));

        // The one bare identifier a verifier found lived in the orphan
        // predicate: a keyword-named REFERENCE column opened fine, wrote
        // fine, and then killed collection forever with a syntax error.
        // Collection over this schema must simply work.
        s.gc_orphan_blocks().await.unwrap();
    }

    /// The same keyword table under a RENAMED kind — the registry must refuse
    /// a reopen carrying it.
    static RENAMED: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_keyword",
        domain: "keywords",
        kinds: &["keyword_memo"],
        columns: &[Column::new("order", ColumnType::Integer)],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: None,
    }];

    /// The registry refuses a reopen whose descriptors rename a kind, not only
    /// one that drops a table. Comparing tables alone let the same rows read
    /// back as empty core content under a renamed kind — the misread the
    /// registry exists to refuse.
    #[tokio::test]
    async fn the_registry_refuses_a_reopen_with_a_renamed_kind() {
        let dir = temp_dir("descriptor-registry-kinds");
        let path = dir.join("ledger.sqlite3");
        {
            let s = Store::open_with(
                &path,
                StoreConfig {
                    descriptors: KEYWORD_DESCRIPTORS,
                    domain_migrations: vec![super::DomainMigrations {
                        domain: "keywords",
                        sqls: vec![KEYWORD_SCHEMA],
                    }],
                },
            )
            .unwrap();
            let conv = make_conv(&s).await;
            let mut fields = Map::new();
            fields.insert("order".into(), json!(1));
            s.append_consumer_block(conv, None, "keyword_note", fields, None)
                .await
                .unwrap();
        }

        let err = Store::open_with(
            &path,
            StoreConfig {
                descriptors: RENAMED,
                domain_migrations: vec![],
            },
        )
        .err()
        .expect("a renamed kind must refuse the reopen");
        assert!(
            matches!(err, StoreError::MissingDescriptors { ref tables }
                if tables == &vec!["block_keyword".to_string()]),
            "the refusal names the table whose registered kinds differ: {err:?}"
        );
    }

    /// Forked: a new thread deep-copies a consumer block generically from its
    /// declared columns — fresh row, same payload. A declared reference column
    /// whose target was NOT cloned is kept by reference, the core
    /// detached-target semantics: the copy still names the source block, and
    /// the collector's reference predicate keeps that block alive.
    #[tokio::test]
    async fn a_fork_deep_copies_a_consumer_block_from_its_declared_columns() {
        let s = configured_store();
        let conv = make_conv(&s).await;

        let target = s
            .insert_text_block(conv, Role::Assistant, "quoted target".into())
            .await
            .unwrap();
        s.insert_text_block(conv, Role::User, "context".into())
            .await
            .unwrap();
        let note = s
            .append_consumer_block(
                conv,
                Some(Role::User),
                "note",
                note_fields("keep", Some(target)),
                None,
            )
            .await
            .unwrap();

        let thread = s
            .fork_continuation(
                conv,
                note,
                Continuation::NewThread {
                    system_prompt: None,
                },
                ModelOverride::default(),
            )
            .await
            .unwrap();

        let blocks = s.list_blocks(thread).await.unwrap();
        let copied = blocks
            .iter()
            .find(|b| b.block_type == "note")
            .expect("the consumer block came across");
        assert_ne!(copied.id, note, "a deep copy is a fresh row");
        assert_eq!(copied.role, Some(Role::User));
        assert_eq!(copied.fields["body"], "keep");
        assert_eq!(
            copied.fields["about_block_id"], target,
            "an uncloned target is kept by reference, the detached-target semantics"
        );
        // The user-voiced append tripped a marker between the earlier user
        // text and the note, and a marker carries no role — so the note's
        // group starts AT the note, and the text before it belongs to the
        // turn on the marker's far side. The neighbouring remap test covers a
        // group with more than one block in it.
        assert!(
            !blocks.iter().any(|b| b.block_type == "text"),
            "nothing from before the marker was in the group"
        );
        assert_eq!(
            blocks.iter().filter(|b| b.block_type == "note").count(),
            1,
            "the group is the note alone"
        );
    }

    /// The other shape: a declared reference column whose target WAS cloned is
    /// remapped to the clone's id, exactly as a quote's endpoints are — copied
    /// verbatim it would point back into the source conversation, and deleting
    /// the source would strand the fork's reference.
    #[tokio::test]
    async fn a_fork_remaps_a_reference_whose_target_was_cloned() {
        use crate::types::InputBlock;

        let s = configured_store();
        let conv = make_conv(&s).await;

        let context = s
            .insert_user_blocks(
                conv,
                vec![InputBlock::Text {
                    content: "context".into(),
                }],
            )
            .await
            .unwrap()[0];
        let note = s
            .append_consumer_block(
                conv,
                Some(Role::User),
                "note",
                note_fields("about the context", Some(context)),
                None,
            )
            .await
            .unwrap();

        let thread = s
            .fork_continuation(
                conv,
                note,
                Continuation::NewThread {
                    system_prompt: None,
                },
                ModelOverride::default(),
            )
            .await
            .unwrap();

        let blocks = s.list_blocks(thread).await.unwrap();
        let copied_context = blocks
            .iter()
            .find(|b| b.block_type == "text" && b.fields["content"] == "context")
            .expect("the context came across in the group");
        let copied_note = blocks
            .iter()
            .find(|b| b.block_type == "note")
            .expect("the note came across");
        assert_ne!(copied_context.id, context, "the context is a fresh clone");
        assert_eq!(
            copied_note.fields["about_block_id"], copied_context.id,
            "the cloned note points at the cloned context, not the source's"
        );
    }

    /// Collected: the collector's reference predicate extends over the
    /// declared reference columns — a block a consumer row points at survives
    /// collection exactly as a quoted block does, and is taken the moment
    /// nothing points at it any more.
    #[tokio::test]
    async fn gc_extends_over_a_declared_reference_column() {
        let s = configured_store();
        let kept = make_conv(&s).await;
        let gone = make_conv(&s).await;

        let target = s
            .insert_text_block(gone, Role::Assistant, "pointed at".into())
            .await
            .unwrap();
        s.append_consumer_block(
            kept,
            Some(Role::User),
            "note",
            note_fields("still points", Some(target)),
            None,
        )
        .await
        .unwrap();

        s.delete_conversation(gone).await.unwrap();
        assert_eq!(
            s.gc_orphan_blocks().await.unwrap(),
            0,
            "the only orphan is referenced through the declared column, and is spared"
        );
        assert!(s.find_block(target).await.unwrap().is_some());

        s.delete_conversation(kept).await.unwrap();
        assert_eq!(
            s.gc_orphan_blocks().await.unwrap(),
            3,
            "the note and the marker its user voice tripped, then the target \
             the note's cascade released"
        );
        assert!(s.find_block(target).await.unwrap().is_none());
    }

    /// A legal cross-descriptor reference: descriptor A's `ColumnRef` names a
    /// column in descriptor B's table, and the collector's predicate extends
    /// over it — the referenced block survives collection until the reference
    /// is gone.
    static CROSS_REF_DESCRIPTORS: &[ContentDescriptor] = &[
        ContentDescriptor {
            table: "block_pin",
            domain: "pins",
            kinds: &["pin"],
            columns: &[Column::new("target_block_id", ColumnType::Integer)],
            reference_columns: &[],
            ephemeral: false,
            quoted_text_column: None,
        },
        ContentDescriptor {
            table: "block_label",
            domain: "pins",
            kinds: &["label"],
            columns: &[Column::new("body", ColumnType::Text)],
            reference_columns: &[ColumnRef {
                table: "block_pin",
                column: "target_block_id",
            }],
            ephemeral: false,
            quoted_text_column: None,
        },
    ];

    const CROSS_REF_SCHEMA: &str = "
        CREATE TABLE block_pin (
            block_id        INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
            target_block_id INTEGER
        );
        CREATE TABLE block_label (
            block_id INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
            body     TEXT
        );";

    fn cross_ref_config() -> StoreConfig {
        StoreConfig {
            descriptors: CROSS_REF_DESCRIPTORS,
            domain_migrations: vec![super::DomainMigrations {
                domain: "pins",
                sqls: vec![CROSS_REF_SCHEMA],
            }],
        }
    }

    #[tokio::test]
    async fn a_cross_descriptor_reference_holds_blocks_back_from_gc() {
        let s = Store::in_memory_with(cross_ref_config()).unwrap();
        let kept = make_conv(&s).await;
        let gone = make_conv(&s).await;

        let target = s
            .insert_text_block(gone, Role::Assistant, "held by a pin".into())
            .await
            .unwrap();
        let mut fields = Map::new();
        fields.insert("target_block_id".into(), Value::Number(target.into()));
        s.append_consumer_block(kept, None, "pin", fields, None)
            .await
            .unwrap();

        s.delete_conversation(gone).await.unwrap();
        assert_eq!(
            s.gc_orphan_blocks().await.unwrap(),
            0,
            "the cross-descriptor reference spares the target"
        );
        assert!(s.find_block(target).await.unwrap().is_some());

        s.delete_conversation(kept).await.unwrap();
        assert_eq!(
            s.gc_orphan_blocks().await.unwrap(),
            2,
            "the pin, then the target its removal released"
        );
    }

    /// Torn down: a kind declared ephemeral joins the streaming teardown sweep
    /// and the type-guarded discard, and a committed consumer kind is
    /// unreachable through either.
    #[tokio::test]
    async fn a_declared_ephemeral_joins_the_teardown_sweep() {
        let s = configured_store();
        let conv = make_conv(&s).await;

        let committed = s
            .append_consumer_block(
                conv,
                Some(Role::User),
                "note",
                note_fields("stays", None),
                None,
            )
            .await
            .unwrap();
        let draft = s
            .append_consumer_block(
                conv,
                None,
                "note_draft",
                note_fields("half-typed", None),
                None,
            )
            .await
            .unwrap();
        let core_tail = s
            .insert_streaming_block(conv, Role::Assistant)
            .await
            .unwrap();

        let deleted = s.delete_streaming_blocks(conv).await.unwrap();
        assert_eq!(deleted, 2, "the declared ephemeral and the core tail");
        assert!(s.find_block(committed).await.unwrap().is_some());
        assert!(s.find_block(draft).await.unwrap().is_none());
        assert!(s.find_block(core_tail).await.unwrap().is_none());

        let second_draft = s
            .append_consumer_block(conv, None, "note_draft", note_fields("again", None), None)
            .await
            .unwrap();
        assert!(s.discard_streaming_block(second_draft).await.unwrap());
        assert!(
            !s.discard_streaming_block(committed).await.unwrap(),
            "a committed kind is unreachable through the ephemeral seam"
        );
    }

    /// A consumer finalization replaces its ephemeral tail in the SAME
    /// transaction — the atomic replace every core finalizing insert carries,
    /// now available to the consumer write path.
    #[tokio::test]
    async fn a_consumer_finalization_replaces_its_ephemeral_tail_atomically() {
        let s = configured_store();
        let conv = make_conv(&s).await;

        let draft = s
            .append_consumer_block(conv, None, "note_draft", note_fields("half", None), None)
            .await
            .unwrap();
        let note = s
            .append_consumer_block(
                conv,
                Some(Role::User),
                "note",
                note_fields("whole", None),
                Some(draft),
            )
            .await
            .unwrap();

        assert!(
            s.find_block(draft).await.unwrap().is_none(),
            "the ephemeral tail is gone with the finalization that replaced it"
        );
        assert!(s.find_block(note).await.unwrap().is_some());
        assert_eq!(
            kinds_of(&s, conv).await,
            vec!["date_marker", "note"],
            "one committed block remains, behind the marker its user voice tripped"
        );
    }

    // ─── Descriptors as durable facts ────────────────────────────────────

    /// The proven scenario, now failing loudly: a database created with
    /// descriptors refuses a plain reopen instead of rendering consumer
    /// blocks as empty content; reopening WITH the descriptors works; and a
    /// fresh core-only database is unaffected.
    #[tokio::test]
    async fn a_database_with_descriptors_refuses_a_plain_reopen() {
        let dir = temp_dir("descriptor-registry");
        let db = dir.join("ledger.sqlite3");

        let conv = {
            let s = Store::open_with(&db, note_config()).unwrap();
            let conv = make_conv(&s).await;
            s.append_consumer_block(
                conv,
                Some(Role::User),
                "note",
                note_fields("durable fact", None),
                None,
            )
            .await
            .unwrap();
            conv
        };

        match Store::open(&db) {
            Err(StoreError::MissingDescriptors { tables }) => {
                assert!(tables.contains(&"block_note".to_owned()), "{tables:?}");
                assert!(
                    tables.contains(&"block_note_draft".to_owned()),
                    "{tables:?}"
                );
            }
            Ok(_) => panic!("a plain open of a descriptor database must refuse"),
            Err(other) => panic!("expected the missing-descriptors refusal, got {other:?}"),
        }

        let s = Store::open_with(&db, note_config()).unwrap();
        let blocks = s.list_blocks(conv).await.unwrap();
        assert_eq!(
            blocks.len(),
            2,
            "the marker the append tripped, then the note"
        );
        assert_eq!(blocks[1].fields["body"], "durable fact");
        drop(s);

        let core_only = dir.join("core.sqlite3");
        let s = Store::open(&core_only).unwrap();
        make_conv(&s).await;
        drop(s);
        Store::open(&core_only).expect("a core-only database reopens plainly");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A failed open leaves the disk migrated — documented as safe by
    /// idempotency: the corrected reopen finds the versions counted, skips
    /// the applied steps, and validates against the same schema.
    #[tokio::test]
    async fn a_fix_descriptors_then_reopen_recovers_a_failed_open() {
        let dir = temp_dir("descriptor-idempotency");
        let db = dir.join("ledger.sqlite3");

        match Store::open_with(
            &db,
            StoreConfig {
                descriptors: UNDECLARED_COLUMN,
                domain_migrations: note_migrations(),
            },
        ) {
            Err(StoreError::InvalidDescriptor { .. }) => {}
            Ok(_) => panic!("the misdeclared open must fail"),
            Err(other) => panic!("expected an invalid-descriptor error, got {other:?}"),
        }

        // The migrations landed; the corrected descriptors reopen cleanly.
        let s = Store::open_with(&db, note_config()).unwrap();
        let conv = make_conv(&s).await;
        s.append_consumer_block(
            conv,
            Some(Role::User),
            "note",
            note_fields("recovered", None),
            None,
        )
        .await
        .unwrap();
        drop(s);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ─── The domain-aware path ───────────────────────────────────────────

    /// After a failed consumer migration, descriptor reads and writes answer
    /// with the migration failure — exactly the answer `domain_run` gives —
    /// instead of running raw against a schema in doubt; and a corrected
    /// migration lifts the refusal.
    #[tokio::test]
    async fn a_failed_consumer_migration_refuses_descriptor_reads_and_writes() {
        let s = configured_store();
        let conv = make_conv(&s).await;
        s.append_consumer_block(
            conv,
            Some(Role::User),
            "note",
            note_fields("already here", None),
            None,
        )
        .await
        .unwrap();

        assert!(
            domain_migrate(
                &s.tx(),
                "notes",
                vec![NOTE_SCHEMA, "CREATE TABLE broken (;"],
            )
            .await
            .is_err()
        );

        match s
            .append_consumer_block(
                conv,
                Some(Role::User),
                "note",
                note_fields("no", None),
                None,
            )
            .await
        {
            Err(StoreError::MigrationFailed {
                domain, version, ..
            }) => {
                assert_eq!(domain, "notes");
                assert_eq!(version, 2);
            }
            other => panic!("the write answers with the migration failure, got {other:?}"),
        }

        match s.list_blocks(conv).await {
            Err(StoreError::MigrationFailed { domain, .. }) => assert_eq!(domain, "notes"),
            other => panic!("the read answers with the migration failure, got {other:?}"),
        }

        // The same answer domain_run gives, for the same domain.
        match domain_run(&s.tx(), "notes", |_| Ok(())).await {
            Err(StoreError::MigrationFailed { domain, .. }) => assert_eq!(domain, "notes"),
            other => panic!("domain_run answers identically, got {other:?}"),
        }

        // A corrected migration lifts the refusal.
        domain_migrate(
            &s.tx(),
            "notes",
            vec![
                NOTE_SCHEMA,
                "CREATE TABLE notes_extra (id INTEGER PRIMARY KEY);",
            ],
        )
        .await
        .unwrap();
        assert_eq!(
            s.list_blocks(conv).await.unwrap().len(),
            2,
            "the marker and the note the first append wrote together"
        );
        s.append_consumer_block(
            conv,
            Some(Role::User),
            "note",
            note_fields("healed", None),
            None,
        )
        .await
        .unwrap();
    }

    // ─── The loud refusals ───────────────────────────────────────────────

    fn open_fails_with(config: StoreConfig) -> (String, String) {
        match Store::in_memory_with(config) {
            Ok(_) => panic!("the open must fail"),
            Err(StoreError::InvalidDescriptor { table, reason }) => (table, reason),
            Err(other) => panic!("expected an invalid-descriptor error, got {other:?}"),
        }
    }

    /// Each misdeclared shape the open must refuse, as module-level fixtures.
    static MISSING_TABLE: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_absent",
        domain: "absentia",
        kinds: &["absent"],
        columns: &[],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: None,
    }];

    static COLLIDING_COLUMN: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_note",
        domain: "notes",
        kinds: &["note"],
        columns: &[Column::new("type", ColumnType::Text)],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: None,
    }];

    static UNDECLARED_COLUMN: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_note",
        domain: "notes",
        kinds: &["note"],
        columns: &[Column::new("no_such_column", ColumnType::Text)],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: None,
    }];

    static ANCHOR_COLUMN: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_note",
        domain: "notes",
        kinds: &["note"],
        columns: &[Column::new("dispatch_anchor", ColumnType::Integer)],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: None,
    }];

    static CORE_KIND_COLLISION: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_note",
        domain: "notes",
        kinds: &["text"],
        columns: &[Column::new("body", ColumnType::Text)],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: None,
    }];

    static CORE_TABLE_COLLISION: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_text",
        domain: "notes",
        kinds: &["note"],
        columns: &[],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: None,
    }];

    static CORE_DOMAIN_CLAIM: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_note",
        domain: "core",
        kinds: &["note"],
        columns: &[Column::new("body", ColumnType::Text)],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: None,
    }];

    static CORE_TABLE_REFERENCE: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_note",
        domain: "notes",
        kinds: &["note"],
        columns: &[Column::new("body", ColumnType::Text)],
        reference_columns: &[ColumnRef {
            table: "metadata",
            column: "source_block_id",
        }],
        ephemeral: false,
        quoted_text_column: None,
    }];

    /// Two shapes of the same broken key: a `block_id` whose foreign key has
    /// no delete rule, and a `block_id` with no foreign key at all.
    const LOOSE_SCHEMA: &str = "
        CREATE TABLE block_loose_note (
            block_id INTEGER PRIMARY KEY REFERENCES blocks(id),
            body     TEXT
        );
        CREATE TABLE block_keyless_note (
            block_id INTEGER PRIMARY KEY,
            body     TEXT
        );";

    fn loose_migrations() -> Vec<super::DomainMigrations> {
        vec![super::DomainMigrations {
            domain: "loose_notes",
            sqls: vec![LOOSE_SCHEMA],
        }]
    }

    static UNCASCADED_KEY: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_loose_note",
        domain: "loose_notes",
        kinds: &["loose_note"],
        columns: &[Column::new("body", ColumnType::Text)],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: None,
    }];

    static KEYLESS: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_keyless_note",
        domain: "loose_notes",
        kinds: &["keyless_note"],
        columns: &[Column::new("body", ColumnType::Text)],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: None,
    }];

    static KIND_CLAIMED_TWICE: &[ContentDescriptor] = &[
        ContentDescriptor {
            table: "block_note",
            domain: "notes",
            kinds: &["note"],
            columns: &[Column::new("body", ColumnType::Text)],
            reference_columns: &[],
            ephemeral: false,
            quoted_text_column: None,
        },
        ContentDescriptor {
            table: "block_note_draft",
            domain: "notes",
            kinds: &["note"],
            columns: &[Column::new("body", ColumnType::Text)],
            reference_columns: &[],
            ephemeral: true,
            quoted_text_column: None,
        },
    ];

    #[tokio::test]
    async fn open_with_refuses_a_descriptor_naming_a_missing_table() {
        let (table, reason) = open_fails_with(StoreConfig {
            descriptors: MISSING_TABLE,
            domain_migrations: Vec::new(),
        });
        assert_eq!(table, "block_absent");
        assert!(
            reason.contains("does not exist"),
            "loud about what: {reason}"
        );
    }

    #[tokio::test]
    async fn open_with_refuses_a_colliding_or_missing_column() {
        let (_, reason) = open_fails_with(StoreConfig {
            descriptors: COLLIDING_COLUMN,
            domain_migrations: note_migrations(),
        });
        assert!(reason.contains("collides with the row header"), "{reason}");

        let (_, reason) = open_fails_with(StoreConfig {
            descriptors: UNDECLARED_COLUMN,
            domain_migrations: note_migrations(),
        });
        assert!(reason.contains("does not exist in the table"), "{reason}");

        // The header column this slice added is refused at open like the
        // rest of the header set: accepted, it would load into the payload
        // beside the header's own column and the serializer would silently
        // drop it — data loss with no open-time rejection.
        let (_, reason) = open_fails_with(StoreConfig {
            descriptors: ANCHOR_COLUMN,
            domain_migrations: note_migrations(),
        });
        assert!(reason.contains("collides with the row header"), "{reason}");
    }

    /// The lockstep behind [`RESERVED_COLUMNS`]: every header field name is a
    /// refused column except `role` — the one header fact a content table
    /// legitimately carries as its voice column — and the content table's own
    /// key joins the set. A header name missing from the refusal set is a
    /// column open-validation accepts and serialization then silently drops.
    #[test]
    fn reserved_columns_mirror_the_header_field_names() {
        for name in crate::block::RESERVED_FIELD_NAMES {
            if name == "role" {
                assert!(
                    !RESERVED_COLUMNS.contains(&name),
                    "the voice column stays declarable"
                );
                continue;
            }
            assert!(
                RESERVED_COLUMNS.contains(&name),
                "header field '{name}' is missing from RESERVED_COLUMNS — the two literals move in lockstep"
            );
        }
        assert!(RESERVED_COLUMNS.contains(&"block_id"));
    }

    #[tokio::test]
    async fn open_with_refuses_collisions_with_the_core_set_and_between_descriptors() {
        let (_, reason) = open_fails_with(StoreConfig {
            descriptors: CORE_KIND_COLLISION,
            domain_migrations: note_migrations(),
        });
        assert!(reason.contains("collides with a library kind"), "{reason}");

        let (_, reason) = open_fails_with(StoreConfig {
            descriptors: CORE_TABLE_COLLISION,
            domain_migrations: Vec::new(),
        });
        assert!(reason.contains("collides with a library table"), "{reason}");

        let (_, reason) = open_fails_with(StoreConfig {
            descriptors: KIND_CLAIMED_TWICE,
            domain_migrations: note_migrations(),
        });
        assert!(reason.contains("claimed by another descriptor"), "{reason}");

        let (_, reason) = open_fails_with(StoreConfig {
            descriptors: CORE_DOMAIN_CLAIM,
            domain_migrations: note_migrations(),
        });
        assert!(reason.contains("library's own"), "{reason}");
    }

    /// A reference column may live in a descriptor's table — its own or
    /// another descriptor's — never a library table: a `ColumnRef` naming one
    /// would extend the collector's generated predicate over schema the
    /// literal reference union owns, and the proven consequence was a
    /// predicate that disabled collection for good.
    #[tokio::test]
    async fn open_with_refuses_a_reference_into_a_library_table() {
        let (_, reason) = open_fails_with(StoreConfig {
            descriptors: CORE_TABLE_REFERENCE,
            domain_migrations: note_migrations(),
        });
        assert!(
            reason.contains("never a library table"),
            "loud about the rule: {reason}"
        );
    }

    /// The contract's delete rule is enforced, not assumed: a `block_id` that
    /// does not cascade would abort the collector's DELETE the first time a
    /// header row fell — the exact failure the reference union's doc records —
    /// so the open refuses it, with the foreign key or without one.
    #[tokio::test]
    async fn open_with_refuses_a_block_id_that_does_not_cascade() {
        let (table, reason) = open_fails_with(StoreConfig {
            descriptors: UNCASCADED_KEY,
            domain_migrations: loose_migrations(),
        });
        assert_eq!(table, "block_loose_note");
        assert!(reason.contains("ON DELETE CASCADE"), "{reason}");

        let (table, reason) = open_fails_with(StoreConfig {
            descriptors: KEYLESS,
            domain_migrations: loose_migrations(),
        });
        assert_eq!(table, "block_keyless_note");
        assert!(reason.contains("ON DELETE CASCADE"), "{reason}");
    }

    /// The three shapes of a table the change hook cannot serve, each refused
    /// at open with the requirement named: a `block_id` that is not the rowid
    /// alias, a `WITHOUT ROWID` table (which fires no change hook ever), and a
    /// BLOB-affinity column (which has no field form and used to error on
    /// first read instead).
    const MISKEYED_SCHEMA: &str = "
        CREATE TABLE block_textkey_note (
            block_id TEXT PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
            body     TEXT
        );
        CREATE TABLE block_norowid_note (
            block_id INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
            body     TEXT
        ) WITHOUT ROWID;
        CREATE TABLE block_blob_note (
            block_id INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
            body     BLOB
        );";

    fn miskeyed_migrations() -> Vec<super::DomainMigrations> {
        vec![super::DomainMigrations {
            domain: "miskeyed_notes",
            sqls: vec![MISKEYED_SCHEMA],
        }]
    }

    static TEXT_KEY: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_textkey_note",
        domain: "miskeyed_notes",
        kinds: &["textkey_note"],
        columns: &[Column::new("body", ColumnType::Text)],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: None,
    }];

    static NO_ROWID: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_norowid_note",
        domain: "miskeyed_notes",
        kinds: &["norowid_note"],
        columns: &[Column::new("body", ColumnType::Text)],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: None,
    }];

    static BLOB_COLUMN: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_blob_note",
        domain: "miskeyed_notes",
        kinds: &["blob_note"],
        columns: &[Column::new("body", ColumnType::Text)],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: None,
    }];

    #[tokio::test]
    async fn open_with_refuses_a_block_id_that_is_not_the_rowid_alias() {
        let (table, reason) = open_fails_with(StoreConfig {
            descriptors: TEXT_KEY,
            domain_migrations: miskeyed_migrations(),
        });
        assert_eq!(table, "block_textkey_note");
        assert!(reason.contains("INTEGER PRIMARY KEY"), "{reason}");
        assert!(reason.contains("rowid alias"), "{reason}");
    }

    #[tokio::test]
    async fn open_with_refuses_a_without_rowid_table() {
        let (table, reason) = open_fails_with(StoreConfig {
            descriptors: NO_ROWID,
            domain_migrations: miskeyed_migrations(),
        });
        assert_eq!(table, "block_norowid_note");
        assert!(reason.contains("WITHOUT ROWID"), "{reason}");
    }

    #[tokio::test]
    async fn open_with_refuses_a_blob_affinity_column() {
        let (table, reason) = open_fails_with(StoreConfig {
            descriptors: BLOB_COLUMN,
            domain_migrations: miskeyed_migrations(),
        });
        assert_eq!(table, "block_blob_note");
        assert!(reason.contains("BLOB"), "{reason}");
    }

    /// A declared type the column's affinity cannot hold is refused at open:
    /// here a Json field over an INTEGER column, whose affinity would mangle
    /// the serialized text.
    static AFFINITY_MISMATCH: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_note",
        domain: "notes",
        kinds: &["note"],
        columns: &[Column::new("about_block_id", ColumnType::Json)],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: None,
    }];

    #[tokio::test]
    async fn open_with_refuses_a_declared_type_the_affinity_cannot_hold() {
        let (_, reason) = open_fails_with(StoreConfig {
            descriptors: AFFINITY_MISMATCH,
            domain_migrations: note_migrations(),
        });
        assert!(
            reason.contains("cannot hold that type"),
            "loud about the affinity: {reason}"
        );
    }

    /// A second `DomainMigrations` for one domain is an open-time error naming
    /// the domain — silently re-counting the first entry's versions is how it
    /// used to be skipped.
    #[tokio::test]
    async fn open_with_refuses_a_domain_submitted_twice() {
        let mut migrations = note_migrations();
        migrations.extend(note_migrations());
        match Store::in_memory_with(StoreConfig {
            descriptors: NOTE_DESCRIPTORS,
            domain_migrations: migrations,
        }) {
            Err(StoreError::Other(message)) => {
                assert!(message.contains("notes"), "names the domain: {message}");
                assert!(message.contains("twice"), "{message}");
            }
            Ok(_) => panic!("the duplicate domain must refuse the open"),
            Err(other) => panic!("expected the duplicate-domain refusal, got {other:?}"),
        }
    }

    /// The write is one transaction: a content row that fails to go in takes
    /// the header and junction rows down with it.
    ///
    /// The failure is injected as a trigger whose body names a table that does
    /// not exist, so the content INSERT fails at prepare with `SQLITE_ERROR` —
    /// an operational failure, the only kind a test may provoke here. A
    /// constraint violation would be impossible state and would end the
    /// process instead of returning (see [`super::integrity`]).
    #[tokio::test]
    async fn a_refused_consumer_write_leaves_no_residue_rows() {
        let s = configured_store();
        let conv = make_conv(&s).await;

        let counts = |s: &Store| {
            let s = s.clone();
            async move {
                s.run(|conn| {
                    let count = |sql: &str| -> Result<i64, StoreError> {
                        Ok(conn.query_row(sql, [], |row| row.get(0))?)
                    };
                    Ok((
                        count("SELECT COUNT(*) FROM blocks")?,
                        count("SELECT COUNT(*) FROM conversation_blocks")?,
                        count("SELECT COUNT(*) FROM block_note")?,
                    ))
                })
                .await
                .unwrap()
            }
        };

        let before = counts(&s).await;
        s.run(|conn| {
            conn.execute_batch(
                "CREATE TRIGGER refuse_the_note BEFORE INSERT ON block_note
                 BEGIN INSERT INTO no_such_table_for_the_injected_failure VALUES (1); END",
            )?;
            Ok(())
        })
        .await
        .unwrap();

        // The content insert is refused after the header and junction rows
        // were written.
        assert!(
            s.append_consumer_block(
                conv,
                Some(Role::User),
                "note",
                note_fields("body", None),
                None
            )
            .await
            .is_err()
        );
        assert_eq!(
            counts(&s).await,
            before,
            "no header, no junction, no content"
        );
    }

    /// The write path's own refusals: an unclaimed kind, an undeclared field,
    /// and a role handed to a kind whose table declares no voice.
    #[tokio::test]
    async fn the_write_path_refuses_what_no_descriptor_declares() {
        let s = configured_store();
        let conv = make_conv(&s).await;

        match s
            .append_consumer_block(conv, None, "mystery", Map::new(), None)
            .await
        {
            Err(StoreError::UnsupportedBlockKind {
                block_type,
                operation,
            }) => {
                assert_eq!(block_type, "mystery");
                assert_eq!(operation, "Store::append_consumer_block");
            }
            other => panic!("expected an unsupported-kind error, got {other:?}"),
        }

        let mut undeclared = note_fields("body is fine", None);
        undeclared.insert("extra".into(), Value::String("not a column".into()));
        assert!(
            s.append_consumer_block(conv, Some(Role::User), "note", undeclared, None)
                .await
                .is_err(),
            "a field naming an undeclared column is refused"
        );

        assert!(
            s.append_consumer_block(
                conv,
                Some(Role::User),
                "note_draft",
                note_fields("draft", None),
                None,
            )
            .await
            .is_err(),
            "a role without a declared role column is refused"
        );
    }

    // ─── The date-marker discipline on the consumer write path ───────────

    /// The blocks of a conversation as stored type strings, in junction order.
    async fn kinds_of(s: &Store, conv: i64) -> Vec<String> {
        s.list_blocks(conv)
            .await
            .unwrap()
            .into_iter()
            .map(|b| b.block_type)
            .collect()
    }

    /// A user-voiced consumer append trips the marker, and the marker lands
    /// BEFORE the block it rides with — the ordering promise, which is only
    /// keepable from inside the append's own transaction. A second append on
    /// the same day adds nothing.
    #[tokio::test]
    async fn a_user_voiced_consumer_append_trips_the_marker_before_its_block() {
        let s = configured_store();
        let conv = make_conv(&s).await;

        s.append_consumer_block_stamped(
            conv,
            Some(Role::User),
            "note",
            note_fields("a member speaks", None),
            None,
            DateStamp::date_only("2026-08-27"),
        )
        .await
        .unwrap();
        assert_eq!(kinds_of(&s, conv).await, vec!["date_marker", "note"]);

        s.append_consumer_block_stamped(
            conv,
            Some(Role::User),
            "note",
            note_fields("again, same day", None),
            None,
            DateStamp::date_only("2026-08-27"),
        )
        .await
        .unwrap();
        assert_eq!(
            kinds_of(&s, conv).await,
            vec!["date_marker", "note", "note"],
            "same day, no second marker"
        );

        s.append_consumer_block_stamped(
            conv,
            Some(Role::User),
            "note",
            note_fields("next day", None),
            None,
            DateStamp::date_only("2026-08-28"),
        )
        .await
        .unwrap();
        assert_eq!(
            kinds_of(&s, conv).await,
            vec!["date_marker", "note", "note", "date_marker", "note"],
            "midnight crossed — the fresh marker precedes the new block"
        );

        let dates: Vec<String> = s
            .list_blocks(conv)
            .await
            .unwrap()
            .iter()
            .filter(|b| b.block_type == "date_marker")
            .map(|b| b.fields["date"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(dates, vec!["2026-08-27", "2026-08-28"]);
    }

    /// Everything that is not user-voiced still skips it: an assistant-voiced
    /// append and a role-less one — context notes, palette rows, reports —
    /// write no marker, on a fresh conversation where one would otherwise be
    /// free.
    #[tokio::test]
    async fn a_non_user_consumer_append_never_trips_the_marker() {
        let s = configured_store();

        let conv = make_conv(&s).await;
        s.append_consumer_block_stamped(
            conv,
            Some(Role::Assistant),
            "note",
            note_fields("the assistant's own record", None),
            None,
            DateStamp::date_only("2026-08-27"),
        )
        .await
        .unwrap();
        assert_eq!(kinds_of(&s, conv).await, vec!["note"]);

        let roleless = make_conv(&s).await;
        s.append_consumer_block_stamped(
            roleless,
            None,
            "note",
            note_fields("a context note", None),
            None,
            DateStamp::date_only("2026-08-27"),
        )
        .await
        .unwrap();
        assert_eq!(kinds_of(&s, roleless).await, vec!["note"]);
    }

    /// The marker the consumer path writes carries the whole stamp, and it is
    /// the same change detection the library's own seams run — a stamped
    /// consumer append after a library group append on the same day adds
    /// nothing, and the zone rule holds across the two paths.
    #[tokio::test]
    async fn the_consumer_path_shares_the_one_change_detection() {
        let s = configured_store();
        let conv = make_conv(&s).await;

        s.insert_user_blocks_dated(
            conv,
            vec![crate::types::InputBlock::Text {
                content: "composed".into(),
            }],
            DateStamp::zoned("2026-08-27", Some("Europe/Berlin")),
        )
        .await
        .unwrap();
        s.append_consumer_block_stamped(
            conv,
            Some(Role::User),
            "note",
            note_fields("landed", None),
            None,
            DateStamp::zoned("2026-08-27", Some("Europe/Berlin")),
        )
        .await
        .unwrap();
        assert_eq!(
            kinds_of(&s, conv).await,
            vec!["date_marker", "text", "note"],
            "one day, one marker, whichever path wrote it"
        );

        s.append_consumer_block_stamped(
            conv,
            Some(Role::User),
            "note",
            note_fields("moved", None),
            None,
            DateStamp::zoned("2026-08-27", Some("Europe/Lisbon")),
        )
        .await
        .unwrap();
        assert_eq!(
            kinds_of(&s, conv).await,
            vec!["date_marker", "text", "note", "date_marker", "note"],
            "the zone changed knowably — the consumer path detects it too"
        );
    }

    /// What the consumer path stores is what the read path serves: the full
    /// stamp round-trips through the block query onto the marker's fields.
    #[tokio::test]
    async fn the_consumer_path_writes_the_whole_stamp() {
        let s = configured_store();
        let conv = make_conv(&s).await;

        s.append_consumer_block_stamped(
            conv,
            Some(Role::User),
            "note",
            note_fields("a member speaks", None),
            None,
            DateStamp {
                date: "2026-08-27".into(),
                tz_abbrev: Some("CEST".into()),
                tz_name: Some("Europe/Berlin".into()),
                written_at: Some("22:41".into()),
            },
        )
        .await
        .unwrap();

        let blocks = s.list_blocks(conv).await.unwrap();
        let marker = &blocks[0];
        assert_eq!(marker.block_type, "date_marker");
        assert_eq!(marker.fields["date"], "2026-08-27");
        assert_eq!(marker.fields["tz_abbrev"], "CEST");
        assert_eq!(marker.fields["tz_name"], "Europe/Berlin");
        assert_eq!(marker.fields["written_at"], "22:41");
    }

    /// The grouping consequence of running the discipline here, pinned as its
    /// own statement instead of as a side effect of another test's
    /// assertions. A marker is role-less, so it BREAKS a role-contiguous run:
    /// a fork anchored on the day's FIRST user-voiced consumer append leaves
    /// the user blocks before the marker behind, where before this change it
    /// carried them. Later the same day no marker stands between them and the
    /// run is whole again — so every split is a day boundary. The slice does
    /// not name this among its residuals; the disagreement is recorded on
    /// `append_consumer_block` and pinned here.
    #[tokio::test]
    async fn a_marker_splits_a_user_run_at_the_days_first_append() {
        let s = configured_store();
        let conv = make_conv(&s).await;

        s.insert_user_blocks_dated(
            conv,
            vec![crate::types::InputBlock::Text {
                content: "yesterday's words".into(),
            }],
            DateStamp::date_only("2026-08-26"),
        )
        .await
        .unwrap();
        let first_of_the_day = s
            .append_consumer_block_stamped(
                conv,
                Some(Role::User),
                "note",
                note_fields("the new day's first word", None),
                None,
                DateStamp::date_only("2026-08-27"),
            )
            .await
            .unwrap();
        assert_eq!(
            kinds_of(&s, conv).await,
            vec!["date_marker", "text", "date_marker", "note"],
            "the fresh marker sits between yesterday's user text and today's append"
        );

        let split = s
            .fork_continuation(
                conv,
                first_of_the_day,
                Continuation::NewThread {
                    system_prompt: None,
                },
                ModelOverride::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            kinds_of(&s, split).await,
            vec!["date_marker", "note"],
            "the group walk stopped at the marker: yesterday's user text stayed behind"
        );

        let later_that_day = s
            .append_consumer_block_stamped(
                conv,
                Some(Role::User),
                "note",
                note_fields("later the same day", None),
                None,
                DateStamp::date_only("2026-08-27"),
            )
            .await
            .unwrap();
        let whole = s
            .fork_continuation(
                conv,
                later_that_day,
                Continuation::NewThread {
                    system_prompt: None,
                },
                ModelOverride::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            kinds_of(&s, whole).await,
            vec!["date_marker", "note", "note"],
            "no marker between them — the user run is unbroken and both came across"
        );
    }

    /// A marker inside a forked group is COPIED, not refused. The fork walks
    /// role-contiguous groups, and a marker is role-less, so any role-less
    /// block beside one shares its group — a shape this path has always been
    /// able to produce and now produces routinely. Before the marker's own
    /// clone arm existed this fork failed with
    /// `UnsupportedBlockKind{block_type:"date_marker"}`, an error on ordinary
    /// data.
    #[tokio::test]
    async fn a_fork_whose_group_holds_a_marker_copies_the_marker() {
        let s = configured_store();
        let conv = make_conv(&s).await;

        let note = s
            .append_consumer_block_stamped(
                conv,
                None,
                "note",
                note_fields("a role-less context note", None),
                None,
                DateStamp::date_only("2026-08-27"),
            )
            .await
            .unwrap();
        s.append_consumer_block_stamped(
            conv,
            Some(Role::User),
            "note",
            note_fields("a member speaks", None),
            None,
            DateStamp::date_only("2026-08-27"),
        )
        .await
        .unwrap();
        assert_eq!(
            kinds_of(&s, conv).await,
            vec!["note", "date_marker", "note"],
            "the marker landed role-less, directly after the role-less note"
        );

        let thread = s
            .fork_continuation(
                conv,
                note,
                Continuation::NewThread {
                    system_prompt: None,
                },
                ModelOverride::default(),
            )
            .await
            .unwrap();

        assert_eq!(
            kinds_of(&s, thread).await,
            vec!["date_marker", "note", "date_marker"],
            "the fresh thread's own marker, then the role-less group whole — \
             the marker inside it copied with the rest, not refused"
        );
        let blocks = s.list_blocks(thread).await.unwrap();
        assert_eq!(
            blocks[2].fields["date"], "2026-08-27",
            "the copied marker carries the date it recorded, not today's"
        );
    }

    // ─── Quote reach: the declared column a quote resolves through ───────

    /// A consumer kind that carries quotable text, plus a second declared
    /// column the fork's clone must carry across untouched. `body` is
    /// deliberately nullable: an erased row is a null text column, and the
    /// resolver must answer that with the empty string and no special case.
    static REMARK_DESCRIPTORS: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_remark",
        domain: "remarks",
        kinds: &["remark"],
        columns: &[
            Column::new("role", ColumnType::Text),
            Column::new("body", ColumnType::Text),
            Column::new("origin", ColumnType::Text),
        ],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: Some("body"),
    }];

    const REMARK_SCHEMA: &str = "
        CREATE TABLE block_remark (
            block_id INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
            role     TEXT,
            body     TEXT,
            origin   TEXT
        );";

    fn remark_migrations() -> Vec<super::DomainMigrations> {
        vec![super::DomainMigrations {
            domain: "remarks",
            sqls: vec![REMARK_SCHEMA],
        }]
    }

    fn remark_store() -> Store {
        Store::in_memory_with(StoreConfig {
            descriptors: REMARK_DESCRIPTORS,
            domain_migrations: remark_migrations(),
        })
        .unwrap()
    }

    fn remark_fields(body: &str, origin: &str) -> Map<String, Value> {
        let mut fields = Map::new();
        fields.insert("body".into(), Value::String(body.into()));
        fields.insert("origin".into(), Value::String(origin.into()));
        fields
    }

    /// A remark in its own voice — role-less, so it trips no date marker and
    /// the pins below say what they mean about the blocks they name.
    async fn append_remark(s: &Store, conv: i64, body: &str, origin: &str) -> i64 {
        s.append_consumer_block(conv, None, "remark", remark_fields(body, origin), None)
            .await
            .unwrap()
    }

    async fn quote_of(s: &Store, conv: i64, start: i64, from: i64, end: i64, to: i64) -> i64 {
        s.insert_user_blocks(
            conv,
            vec![crate::types::InputBlock::Quote {
                start_block_id: start,
                start_pos: from,
                end_block_id: end,
                end_pos: to,
            }],
        )
        .await
        .unwrap()[0]
    }

    async fn loaded_quote(s: &Store, conv: i64) -> Block {
        s.list_blocks(conv)
            .await
            .unwrap()
            .into_iter()
            .find(|b| b.block_type == "quote")
            .expect("the conversation holds its quote")
    }

    /// What a block contributes to a model request, through the real
    /// projection fold rather than a re-implementation of it.
    fn projected(block: &Block) -> String {
        match crate::providers::render_blocks_to_text::<crate::agency::BlockKind>(
            std::slice::from_ref(block),
        ) {
            crate::providers::MessageContent::Text(text) => text,
            parts @ crate::providers::MessageContent::Parts(_) => {
                panic!("a quote contributes text, got {parts:?}")
            }
        }
    }

    /// AC2 and AC3: a quote of a declared consumer kind resolves that kind's
    /// column, sliced by CHARACTER offsets — the multibyte span slices between
    /// characters, never through one — and reaches the model as a rendered
    /// quote. Before the declaration existed, the resolver read `block_text`
    /// alone and this came back empty.
    #[tokio::test]
    async fn a_quote_of_a_declared_consumer_kind_resolves_its_column() {
        let s = remark_store();
        let conv = make_conv(&s).await;
        let remark = append_remark(&s, conv, "grüße — a naïve ☕ note", "somewhere").await;
        quote_of(&s, conv, remark, 2, remark, 7).await;

        let quote = loaded_quote(&s, conv).await;
        assert_eq!(
            quote.fields["text"].as_str().unwrap(),
            "üße —",
            "the slice runs from character 2 to character 7, not from byte 2 to byte 7"
        );
        assert_eq!(projected(&quote), "> üße —");
    }

    /// AC4, the conversation half: the membership rule holds for consumer
    /// blocks exactly as it does for the library's own text. Block ids are
    /// global, so another conversation's remark landing between the endpoints
    /// would otherwise be spliced into a quote nobody wrote that way.
    #[tokio::test]
    async fn a_consumer_range_quote_reads_only_its_own_conversation() {
        let s = remark_store();
        let quoting = make_conv(&s).await;
        let other = make_conv(&s).await;

        let first = append_remark(&s, quoting, "the beginning", "here").await;
        append_remark(&s, other, "INTRUDER", "elsewhere").await;
        let last = append_remark(&s, quoting, " and the end", "here").await;
        quote_of(&s, quoting, first, 4, last, 8).await;

        let quote = loaded_quote(&s, quoting).await;
        assert_eq!(quote.fields["text"].as_str().unwrap(), "beginning and the");
    }

    /// AC4, the date-marker half: a marker sitting inside a quoted span
    /// contributes nothing, and does so for a stated reason rather than by
    /// luck — no descriptor can claim the `date_marker` kind (the core-kinds
    /// collision refusal), so no descriptor declares quotable text for it and
    /// the widened walk never admits it.
    #[tokio::test]
    async fn a_date_marker_inside_a_quoted_span_contributes_nothing() {
        let s = remark_store();
        let conv = make_conv(&s).await;

        let first = s
            .append_consumer_block_stamped(
                conv,
                Some(Role::User),
                "remark",
                remark_fields("said today", "here"),
                None,
                DateStamp::date_only("2026-08-27"),
            )
            .await
            .unwrap();
        let last = s
            .append_consumer_block_stamped(
                conv,
                Some(Role::User),
                "remark",
                remark_fields(" said tomorrow", "here"),
                None,
                DateStamp::date_only("2026-08-28"),
            )
            .await
            .unwrap();
        assert_eq!(
            kinds_of(&s, conv).await,
            vec!["date_marker", "remark", "date_marker", "remark"],
            "the midnight crossing put a marker inside the span about to be quoted"
        );

        quote_of(&s, conv, first, 0, last, 14).await;
        let quote = loaded_quote(&s, conv).await;
        assert_eq!(
            quote.fields["text"].as_str().unwrap(),
            "said today said tomorrow",
            "the marker between the two remarks is not a member of the span"
        );
    }

    /// AC5, first way: a consumer kind that declares no quotable column
    /// resolves empty and renders as nothing — today's behaviour, now stated
    /// by the declaration instead of falling out of the resolver's reach.
    #[tokio::test]
    async fn a_quote_of_an_undeclared_consumer_kind_resolves_empty() {
        let s = configured_store();
        let conv = make_conv(&s).await;
        let note = s
            .append_consumer_block(
                conv,
                None,
                "note",
                note_fields("never quotable", None),
                None,
            )
            .await
            .unwrap();
        quote_of(&s, conv, note, 0, note, 5).await;

        let quote = loaded_quote(&s, conv).await;
        assert_eq!(quote.fields["text"].as_str().unwrap(), "");
        assert_eq!(projected(&quote), "", "an empty quote renders as nothing");
    }

    /// AC5, second way: erasure nulls the declared column, and the quote of
    /// the erased row resolves empty through `COALESCE` — no erasure special
    /// case anywhere in the resolver, which is what keeps the semantics free.
    #[tokio::test]
    async fn a_quote_of_an_erased_row_resolves_empty() {
        let s = remark_store();
        let conv = make_conv(&s).await;
        let remark = append_remark(&s, conv, "about to be erased", "somewhere").await;
        quote_of(&s, conv, remark, 0, remark, 5).await;
        assert_eq!(
            loaded_quote(&s, conv).await.fields["text"]
                .as_str()
                .unwrap(),
            "about",
            "it resolves before the erasure, so the pin is about the erasure"
        );

        // The consumer's own erasure pass, standing in for it: the text
        // column goes null and the row stays.
        s.run(move |conn| {
            conn.execute(
                "UPDATE block_remark SET body = NULL WHERE block_id = ?1",
                [remark],
            )?;
            Ok(())
        })
        .await
        .unwrap();

        let quote = loaded_quote(&s, conv).await;
        assert_eq!(quote.fields["text"].as_str().unwrap(), "");
        assert_eq!(projected(&quote), "");
    }

    /// AC5, third way: a quote whose target is not there at all resolves
    /// empty. The draft preview is where a dangling reference can actually be
    /// written — its quote rows carry no foreign key — and it is one of the
    /// three sites the widened resolver serves.
    #[tokio::test]
    async fn a_quote_of_a_missing_block_resolves_empty() {
        let s = remark_store();
        let conv = make_conv(&s).await;
        let remark = append_remark(&s, conv, "the only block there is", "somewhere").await;

        s.save_draft(
            conv,
            vec![crate::types::InputBlock::Quote {
                start_block_id: remark + 10_000,
                start_pos: 0,
                end_block_id: remark + 10_000,
                end_pos: 5,
            }],
        )
        .await
        .unwrap();

        match &s.load_draft(conv).await.unwrap()[..] {
            [super::super::drafts::DraftBlock::Quote { text, .. }] => {
                assert_eq!(text, "", "a target that does not exist resolves empty");
                assert!(crate::agency::render_quote(text).is_empty());
            }
            other => panic!("the draft holds one quote, got {other:?}"),
        }
    }

    /// AC5, fourth way, and the no-raw-read proof: a closed domain gate
    /// resolves the quote empty WITHOUT touching the descriptor's table.
    ///
    /// The order is the one the store's lifecycle permits — the quotable
    /// block is appended while the domain is healthy, the gate is then closed
    /// through the established failed-migrate pattern, and only then is the
    /// table taken out from under it with test-owned SQL.
    ///
    /// The proof runs in two steps, because the resolver answers in a bare
    /// `String` and therefore swallows a read failure exactly as it swallows
    /// every other one: an error alone would be invisible, so absence alone
    /// cannot prove a read was skipped.
    ///
    ///   1. The table is DROPPED: the resolution is empty and the call still
    ///      returns, which is the decision that a closed gate must not fail a
    ///      whole conversation load over a quote.
    ///   2. The table is put back holding text a read WOULD find: the
    ///      resolution is still empty, and that is what proves no read ran —
    ///      a resolver that touched the table would answer with what it found
    ///      there.
    ///
    /// The resolver is called directly because that is the only place its
    /// decline can be observed: the public load of a conversation holding a
    /// junctioned consumer block already fails whole at the load's own gate
    /// consult, so the decline matters for detached targets and nothing else.
    #[tokio::test]
    async fn a_closed_domain_gate_resolves_a_quote_empty_without_reading_the_table() {
        let s = remark_store();
        let conv = make_conv(&s).await;
        let remark = append_remark(&s, conv, "said while healthy", "somewhere").await;

        assert!(
            domain_migrate(
                &s.tx(),
                "remarks",
                vec![REMARK_SCHEMA, "CREATE TABLE broken (;"],
            )
            .await
            .is_err(),
            "the second step fails and the gate records it"
        );

        let resolve = async || {
            let descriptors = s.descriptors;
            let gate = s.gate.clone();
            s.run(move |conn| {
                Ok(super::super::blocks::resolve_quote_text(
                    super::super::blocks::QuoteScope::new(conn, descriptors, &gate, Some(conv)),
                    remark,
                    0,
                    remark,
                    5,
                ))
            })
            .await
            .expect("the resolver declines rather than failing the load")
        };

        s.run(|conn| {
            conn.execute_batch("DROP TABLE block_remark")?;
            Ok(())
        })
        .await
        .unwrap();
        let text = resolve().await;
        assert_eq!(text, "", "the closed gate joins the absence list");
        assert!(crate::agency::render_quote(&text).is_empty());

        s.run(move |conn| {
            conn.execute_batch(REMARK_SCHEMA)?;
            conn.execute(
                "INSERT INTO block_remark (block_id, body) VALUES (?1, 'READ ANYWAY')",
                [remark],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(
            resolve().await,
            "",
            "a resolver that read the table would answer with what is in it"
        );
    }

    /// AC6: every wrong quotable declaration is refused at descriptor open,
    /// each with its own named reason. A declaration that survives open would
    /// otherwise fail quietly at quote time, one resolved quote at a time.
    #[tokio::test]
    async fn open_with_refuses_a_wrong_quotable_declaration() {
        let (_, reason) = open_fails_with(StoreConfig {
            descriptors: QUOTABLE_UNDECLARED,
            domain_migrations: remark_migrations(),
        });
        assert!(
            reason.contains("is not one of the descriptor's declared"),
            "{reason}"
        );

        let (_, reason) = open_fails_with(StoreConfig {
            descriptors: QUOTABLE_NON_TEXT,
            domain_migrations: remark_migrations(),
        });
        assert!(reason.contains("is declared Integer"), "{reason}");

        // JSON lives in a text column, so an affinity check would admit it:
        // the refusal is by declared VARIANT, and this is the pin that says so.
        let (_, reason) = open_fails_with(StoreConfig {
            descriptors: QUOTABLE_JSON,
            domain_migrations: remark_migrations(),
        });
        assert!(reason.contains("is declared Json"), "{reason}");

        let (_, reason) = open_fails_with(StoreConfig {
            descriptors: QUOTABLE_ROLE,
            domain_migrations: remark_migrations(),
        });
        assert!(reason.contains("names the role column"), "{reason}");

        let (_, reason) = open_fails_with(StoreConfig {
            descriptors: QUOTABLE_EPHEMERAL,
            domain_migrations: remark_migrations(),
        });
        assert!(reason.contains("ephemeral kind"), "{reason}");
    }

    static QUOTABLE_UNDECLARED: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_remark",
        domain: "remarks",
        kinds: &["remark"],
        columns: &[Column::new("body", ColumnType::Text)],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: Some("origin"),
    }];

    static QUOTABLE_NON_TEXT: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_remark",
        domain: "remarks",
        kinds: &["remark"],
        columns: &[Column::new("body", ColumnType::Integer)],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: Some("body"),
    }];

    static QUOTABLE_JSON: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_remark",
        domain: "remarks",
        kinds: &["remark"],
        columns: &[Column::new("body", ColumnType::Json)],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: Some("body"),
    }];

    static QUOTABLE_ROLE: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_remark",
        domain: "remarks",
        kinds: &["remark"],
        columns: &[Column::new("role", ColumnType::Text)],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: Some("role"),
    }];

    static QUOTABLE_EPHEMERAL: &[ContentDescriptor] = &[ContentDescriptor {
        table: "block_remark",
        domain: "remarks",
        kinds: &["remark"],
        columns: &[Column::new("body", ColumnType::Text)],
        reference_columns: &[],
        ephemeral: true,
        quoted_text_column: Some("body"),
    }];

    /// AC7: the loader, the drafts preview and the fork all resolve the same
    /// span to the same text — one resolver, three callers, no second
    /// decision. The fork's clone is checked in the same pin: the copied row
    /// lands in the consumer's own table carrying EVERY declared column, which
    /// is what puts it where the consumer's person-keyed erasure already walks.
    /// (That erasure's reach over clones is the consumer's own pin, not this
    /// one's.)
    #[tokio::test]
    async fn all_three_quote_sites_resolve_a_consumer_span_identically() {
        let s = remark_store();
        let conv = make_conv(&s).await;
        let remark = append_remark(&s, conv, "what was said earlier", "somewhere").await;
        quote_of(&s, conv, remark, 5, remark, 14).await;

        let loaded = loaded_quote(&s, conv).await;
        assert_eq!(loaded.fields["text"].as_str().unwrap(), "was said ");

        s.save_draft(
            conv,
            vec![crate::types::InputBlock::Quote {
                start_block_id: remark,
                start_pos: 5,
                end_block_id: remark,
                end_pos: 14,
            }],
        )
        .await
        .unwrap();
        match &s.load_draft(conv).await.unwrap()[..] {
            [super::super::drafts::DraftBlock::Quote { text, .. }] => {
                assert_eq!(text, "was said ", "the preview resolves what the load does");
            }
            other => panic!("the draft holds one quote, got {other:?}"),
        }

        let thread = s
            .fork_continuation(
                conv,
                loaded.id,
                Continuation::NewThread {
                    system_prompt: None,
                },
                ModelOverride::default(),
            )
            .await
            .unwrap();

        // The source goes away entirely: the fork's quote reads its own
        // detached clone or it reads nothing.
        s.delete_conversation(conv).await.unwrap();
        s.gc_orphan_blocks().await.unwrap();

        let forked = loaded_quote(&s, thread).await;
        assert_eq!(
            forked.fields["text"].as_str().unwrap(),
            "was said ",
            "the fork resolves what the loader and the preview did"
        );

        let clone_id = forked.fields["start_block_id"].as_i64().unwrap();
        assert_ne!(
            clone_id, remark,
            "the fork copied the remark rather than pointing at it"
        );
        let cloned = s
            .find_block(clone_id)
            .await
            .unwrap()
            .expect("the clone outlived the source");
        assert_eq!(cloned.block_type, "remark");
        assert_eq!(
            cloned.fields["body"],
            Value::String("what was said earlier".into())
        );
        assert_eq!(
            cloned.fields["origin"],
            Value::String("somewhere".into()),
            "every declared column rode to the clone, not just the quotable one"
        );
    }

    /// AC7, the fork's gate consult, pinned through the ONLY shape that
    /// reaches it. A junctioned consumer block fails the fork earlier, at the
    /// source load's own gate consult; a DETACHED one is invisible to that
    /// load and arrives at the clone as a quote target. Before this slice the
    /// clone ran raw there, because no consumer row could ever be a quote
    /// target — so this pin passes only with the consult in place.
    #[tokio::test]
    async fn a_fork_of_a_detached_consumer_quote_target_refuses_under_a_closed_gate() {
        let s = remark_store();
        let conv = make_conv(&s).await;
        let remark = append_remark(&s, conv, "quoted, then detached", "somewhere").await;
        let quote = quote_of(&s, conv, remark, 0, remark, 6).await;

        // Detached: the remark keeps its row and leaves the junction, which is
        // what hides it from the fork's source load and hands it to the clone.
        s.detach_block(conv, remark).await.unwrap();
        assert!(
            !kinds_of(&s, conv).await.contains(&"remark".to_owned()),
            "the source load no longer sees a consumer block, so its own gate consult stays quiet"
        );

        assert!(
            domain_migrate(
                &s.tx(),
                "remarks",
                vec![REMARK_SCHEMA, "CREATE TABLE broken (;"],
            )
            .await
            .is_err()
        );

        match s
            .fork_continuation(
                conv,
                quote,
                Continuation::NewThread {
                    system_prompt: None,
                },
                ModelOverride::default(),
            )
            .await
        {
            Err(StoreError::MigrationFailed { domain, .. }) => assert_eq!(domain, "remarks"),
            other => {
                panic!("the fork must refuse loudly with the migration failure, got {other:?}")
            }
        }

        let clones: i64 = s
            .run(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM blocks WHERE block_type = 'remark'",
                    [],
                    |row| row.get(0),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(
            clones, 1,
            "nothing was written raw — the source remark stands alone"
        );
    }

    /// The membership rule's other consequence for the fork, chosen and
    /// stated: a declared block whose content row is MISSING is damaged data,
    /// and it now enters the fork's copy set and fails the fork loudly with
    /// [`StoreError::MissingBlockContent`]. The walk's old `JOIN` dropped such
    /// a block on the floor, so the fork quietly produced a thread whose quote
    /// had lost part of its span. Loud beats silent.
    #[tokio::test]
    async fn a_fork_over_a_declared_block_with_no_content_row_fails_loudly() {
        let s = remark_store();
        let conv = make_conv(&s).await;

        // A header claiming the declared kind, detached and with no content
        // row behind it — the damage this pin is about.
        let damaged = s
            .run(|conn| {
                conn.execute("INSERT INTO blocks (block_type) VALUES ('remark')", [])?;
                Ok(conn.last_insert_rowid())
            })
            .await
            .unwrap();
        let quote = quote_of(&s, conv, damaged, 0, damaged, 4).await;

        match s
            .fork_continuation(
                conv,
                quote,
                Continuation::NewThread {
                    system_prompt: None,
                },
                ModelOverride::default(),
            )
            .await
        {
            Err(StoreError::MissingBlockContent {
                block_id,
                block_type,
            }) => {
                assert_eq!(block_id, damaged);
                assert_eq!(block_type, "remark");
            }
            other => panic!("the fork must name the damaged block, got {other:?}"),
        }
    }
}
