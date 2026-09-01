//! Persistence for the ledger.
//!
//! One connection, owned by one thread, reached through closures. Every write
//! in the library funnels through that single writer, so "who is writing right
//! now" is never a question anyone has to answer: the actor is, one operation
//! at a time, in the order the operations arrived.
//!
//! The store also feeds the reactive change log: the database's row change hook
//! pushes every relevant row change into a [`ChangeLog`], which is what wakes
//! the scheduler. A change to a table the hook does not name wakes nothing.
//!
//! ```no_run
//! # async fn example() -> Result<(), agent_ledger::store::StoreError> {
//! use agent_ledger::block::Role;
//! use agent_ledger::store::Store;
//!
//! let store = Store::in_memory()?;
//! let conversation = store
//!     .create_conversation("local".into(), "a-model".into(), "A Model".into(), String::new())
//!     .await?;
//! store.insert_text_block(conversation, Role::User, "hello".into()).await?;
//! assert_eq!(store.list_blocks(conversation).await?.len(), 1);
//! # Ok(())
//! # }
//! ```

mod approvals;
mod attachments;
mod block_cloner;
mod block_content;
mod blocks;
mod compaction;
mod conversations;
mod date_markers;
mod descriptors;
mod drafts;
mod integrity;
mod messages;
mod metadata;
mod migrations;
mod models;
mod providers;
mod tool_calls;

use std::path::Path;
use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension};
use tokio::sync::{mpsc, oneshot};

use crate::reactivity::ChangeLog;

pub use attachments::{Attachment, ByteRange};
pub use compaction::{
    CompactedThread, ConsumerRecord, LedgerCut, TemporaryConversation, TemporaryFork,
};
pub use conversations::{
    BranchPoint, Continuation, Conversation, ConversationModel, ModelOverride,
};
pub use date_markers::ClockReading;
pub use descriptors::{
    Column, ColumnRef, ColumnType, ContentDescriptor, DomainMigrations, StoreConfig,
    concat_descriptors, descriptor_count,
};
pub use drafts::DraftBlock;
pub use integrity::IntegrityCheck;
pub use messages::{BlockDestination, JoinedBlock, ToolCallInsert};
pub use models::ModelEntry;
pub use providers::ProviderInstance;

/// Everything that can go wrong reaching the store.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The database refused the statement.
    #[error("database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// The actor thread is gone, so nothing can be run against the connection
    /// any more.
    #[error("store actor stopped")]
    ActorStopped,
    /// A block header exists with no row in the content table its kind stores
    /// into. The ledger's premise is faithful replay, so this is reported
    /// rather than papered over with empty content or a dropped block.
    #[error("block {block_id} of kind '{block_type}' has no content row")]
    MissingBlockContent {
        /// The header row's id.
        block_id: i64,
        /// The kind the header claims, which is what says where its content
        /// should have been.
        block_type: String,
    },
    /// An operation that maps a kind to its content table was handed a kind it
    /// has no mapping for. Not a malformed statement — a kind outside what the
    /// operation covers.
    #[error("block kind '{block_type}' has no content mapping in {operation}")]
    UnsupportedBlockKind {
        /// The kind that was asked for.
        block_type: String,
        /// The operation that has no mapping for it.
        operation: &'static str,
    },
    /// A content-table descriptor failed its open-time check: its table is
    /// missing, a declared column is missing or collides with a reserved name,
    /// or its table or a kind collides with another descriptor or the library's
    /// own set. The open fails loudly instead of leaving a kind that would load
    /// empty payloads silently.
    #[error("descriptor for table '{table}' is invalid: {reason}")]
    InvalidDescriptor {
        /// The table the failing descriptor names.
        table: String,
        /// What the check found.
        reason: String,
    },
    /// The database needs its descriptors: it was created with content-table
    /// descriptors this open does not supply. Read without them it is a
    /// different ledger — consumer blocks render as empty content and the
    /// collector aborts on their references — so the open refuses instead of
    /// misreading. Reopen with [`Store::open_with`] and the descriptor set
    /// that covers the named tables.
    #[error(
        "the database needs its descriptors: its registry names content tables this \
         open does not cover: {tables:?}"
    )]
    MissingDescriptors {
        /// The registered content tables the supplied descriptor set does not
        /// cover.
        tables: Vec<String>,
    },
    /// A domain's migrations failed, so its tables are in an unknown state and
    /// every query for that domain is refused with this instead of being run or
    /// parked. Naming the step that failed is the whole point: the domain stays
    /// refused until a corrected migration is submitted and succeeds.
    #[error("domain '{domain}' migration {version} failed: {reason}")]
    MigrationFailed {
        /// The domain whose schema is in doubt.
        domain: String,
        /// The one-based step that failed.
        version: i64,
        /// What the database said.
        reason: String,
    },
    /// A rule this library enforces above the schema — an already-decided
    /// approval, say — refused the write.
    #[error("{0}")]
    Other(String),
}

/// An operation sent to the store's actor thread. Constructed by
/// [`domain_run`] and [`domain_migrate`]; it travels the channel [`StoreTx`]
/// names, which is why it is public at all.
///
/// Both variants are `non_exhaustive` (2026-09-01) so that stays true from
/// outside this library as well: a hand-rolled `Query` could hold a closure
/// that ignores the [`IntegrityCheck`] it is handed, and the guarantee that
/// every answer is judged would then rest on a convention a consumer cannot
/// see. Construction through the two doors is the guarantee.
pub enum StoreOp {
    /// A domain submits its migrations. The actor runs them and signals
    /// completion through `done`. Every query for that domain waits until it
    /// fires.
    #[non_exhaustive]
    Migration {
        /// The domain whose schema this advances.
        domain: &'static str,
        /// Its statements, in order; entry `i` is that domain's version `i + 1`.
        sqls: Vec<&'static str>,
        /// Signalled once they have all run.
        done: oneshot::Sender<Result<(), StoreError>>,
    },
    /// A domain's query. Runs immediately if that domain's migrations have
    /// completed, is deferred until they do, and is refused outright if they
    /// failed.
    #[non_exhaustive]
    Query {
        /// The domain whose tables it reads or writes.
        domain: &'static str,
        /// The work, handed the connection.
        f: QueryFn,
    },
}

/// One piece of work for the actor.
///
/// It is handed the connection when its domain is ready, and a
/// [`StoreError::MigrationFailed`] instead when that domain's migrations
/// failed: a query whose tables may not exist is answered, never run and never
/// left parked.
///
/// The second argument is the actor thread's [`IntegrityCheck`] (2026-09-01).
/// Every answer passes through it here, where the error is still a typed
/// `rusqlite` failure and still on this thread, so a database in a state the
/// design forbids takes the process down before its answer can be acted on.
/// [`domain_run`] does this for every query the library and its consumers
/// send.
pub type QueryFn = Box<dyn FnOnce(Result<&mut Connection, StoreError>, &IntegrityCheck) + Send>;

/// A handle on the store actor's channel. A consumer's own tables hold one of
/// these to send closures to the same actor thread — one writer, whoever is
/// writing.
pub type StoreTx = mpsc::UnboundedSender<StoreOp>;

/// The ledger's storage: one connection behind one writer, plus the change log
/// that announces what it wrote.
#[derive(Clone)]
pub struct Store {
    tx: StoreTx,
    /// The consumer's content-table descriptors, empty for a core-only store.
    /// They drive the consumer load, write, fork, collection and teardown
    /// paths; the library's own kinds never consult them.
    descriptors: &'static [ContentDescriptor],
    /// The effective content-table list — see [`Store::content_tables`].
    content_tables: Arc<[&'static str]>,
    /// The consumer domains' health, shared with the actor: descriptor-path
    /// reads and writes consult it so a failed consumer migration answers
    /// them with [`StoreError::MigrationFailed`] instead of running raw.
    gate: DomainGate,
    /// Fires whenever a relevant table changes, carrying the action, the table
    /// and the row id per change. Backed by the database's own row change hook.
    pub changes: ChangeLog,
}

impl Store {
    /// Clone the actor channel's sender, for a consumer's own tables to submit
    /// their migrations and queries through.
    #[must_use]
    pub fn tx(&self) -> StoreTx {
        self.tx.clone()
    }

    /// Open a store at a database location, core kinds only.
    ///
    /// **A location and nothing else.** No configuration directory, no
    /// provider, no model, no import of anything a product happens to keep
    /// beside its database: the code this was extracted from read a
    /// product-specific file here on every open, and that import went back to
    /// the product it belongs to.
    ///
    /// # Errors
    ///
    /// If the database cannot be opened or its migrations fail.
    pub fn open(db_path: &Path) -> Result<Self, StoreError> {
        Self::open_with(db_path, StoreConfig::default())
    }

    /// Open a store at a database location with a consumer's configuration:
    /// its content-table descriptors and the domain migrations that create
    /// their tables.
    ///
    /// The two arrive together on purpose. The library's migrations and the
    /// consumer's run before any query is served, then every descriptor is
    /// validated against the schema they produced — the table exists, every
    /// declared column exists, nothing collides with another descriptor or the
    /// library's own set — and the open fails loudly otherwise. A kind whose
    /// table was never wired up therefore cannot exist quietly.
    ///
    /// # Errors
    ///
    /// If the database cannot be opened, if any migrations fail, or if a
    /// descriptor fails validation ([`StoreError::InvalidDescriptor`]).
    pub fn open_with(db_path: &Path, config: StoreConfig) -> Result<Self, StoreError> {
        let conn = Connection::open(db_path)?;
        Self::init(conn, config)
    }

    /// Open a store held entirely in memory, core kinds only. Nothing touches
    /// the disk, which is what every test in this library uses.
    ///
    /// # Errors
    ///
    /// If the database cannot be created or its migrations fail.
    pub fn in_memory() -> Result<Self, StoreError> {
        Self::in_memory_with(StoreConfig::default())
    }

    /// Open a store held entirely in memory with a consumer's configuration —
    /// [`Store::open_with`]'s contract, without a disk. This is what a
    /// consumer's tests use for the same reason the library's own do: fast,
    /// parallel, nothing shared.
    ///
    /// # Errors
    ///
    /// If the database cannot be created, if any migrations fail, or if a
    /// descriptor fails validation ([`StoreError::InvalidDescriptor`]).
    pub fn in_memory_with(config: StoreConfig) -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory()?;
        Self::init(conn, config)
    }

    /// The effective content-table list: the library's own block content
    /// tables followed by every configured descriptor's table.
    ///
    /// One list, one owner. The row change hook's allowlist is built from it
    /// and the runtime's block watcher filters by it, so a descriptor's table
    /// wakes the same machinery a library table does — and the two consumers
    /// of the list cannot drift apart, because neither keeps a copy.
    #[must_use]
    pub fn content_tables(&self) -> &[&'static str] {
        &self.content_tables
    }

    fn init(mut conn: Connection, config: StoreConfig) -> Result<Self, StoreError> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "busy_timeout", 5000)?;
        conn.pragma_update(None, "cache_size", -8000)?;

        migrations::run(&conn)?;
        ensure_tracking_tables(&conn)?;

        // What the library's own migrations create, snapshotted from a
        // pristine schema — the collision set descriptor validation checks
        // tables against, correct by construction with no literal to rot.
        let core_tables = core_table_snapshot()?;

        let StoreConfig {
            descriptors,
            domain_migrations,
        } = config;

        // Descriptors are durable facts: a database created with them refuses
        // an open that does not supply them, before anything else consumer-side
        // runs.
        descriptors::check_registry(&conn, descriptors)?;

        // The consumer's own migrations, before anything can query: the
        // descriptors are validated against the schema these create, so they
        // cannot run later. Each domain advances on its own version row,
        // exactly as a call to `domain_migrate` would advance it — and one
        // entry per domain, because a second entry's steps would silently
        // re-count the first's versions and never run.
        let mut premigrated: std::collections::HashSet<&'static str> =
            std::collections::HashSet::new();
        premigrated.insert(CORE_DOMAIN);
        for migration in domain_migrations {
            if migration.domain == CORE_DOMAIN {
                return Err(StoreError::Other(format!(
                    "domain '{CORE_DOMAIN}' is the library's own; a consumer domain needs a name of its own"
                )));
            }
            if premigrated.contains(migration.domain) {
                return Err(StoreError::Other(format!(
                    "domain '{}' is submitted twice in one StoreConfig — one \
                     DomainMigrations per domain, or the second entry's steps are \
                     silently skipped by the version counter",
                    migration.domain
                )));
            }
            run_domain_migrations(&mut conn, migration.domain, &migration.sqls)
                .map_err(|failure| failure.error(migration.domain))?;
            premigrated.insert(migration.domain);
        }

        descriptors::validate(&conn, descriptors, &core_tables)?;
        descriptors::record_registry(&conn, descriptors)?;

        // A descriptor's domain is ready the moment its schema validated: its
        // tables exist in the shape the descriptor declares, which is the fact
        // the migration gate stands for. Without this, a reopened database
        // whose migrations were all applied on an earlier open would park
        // descriptor queries forever behind migrations nobody resubmits.
        for descriptor in descriptors {
            premigrated.insert(descriptor.domain);
        }

        let content_tables: Arc<[&'static str]> =
            descriptors::effective_content_tables(descriptors).into();
        let hook_tables = descriptors::change_hook_tables(descriptors);

        // The hook's installation is checked, not assumed: a store whose hook
        // never attached would accept every write and wake nothing, and the
        // symptom of that is a scheduler that simply never ticks — the hardest
        // possible thing to trace back to here.
        //
        // WHAT THIS HOOK ANNOUNCES IS A PROMPT TO RE-READ, NEVER A RECORD OF
        // WHAT LANDED. The database fires it per ROW CHANGE, not per commit, so
        // a transaction that later rolls back has already announced every row it
        // touched, and the rowid it announced may name a row that no longer
        // exists — or, after a rollback and a later insert, a different row
        // entirely. That is deliberate and is the architecture's position: an
        // event is a wakeup, and truth is the durable state a consumer re-reads
        // for itself. A consumer that treats a ChangeEvent as evidence a write
        // landed is wrong at the first rollback.
        let mut hook_installed = Ok(());
        let changes = ChangeLog::new(|push| {
            hook_installed = conn.update_hook(Some(
                move |action: rusqlite::hooks::Action, _: &str, table: &str, rowid: i64| {
                    if hook_tables.contains(&table) {
                        push(action as i32, table, rowid);
                    }
                },
            ));
        });
        hook_installed?;

        let gate = DomainGate::default();
        let actor_gate = gate.clone();
        let (tx, rx) = mpsc::unbounded_channel();
        // A panic on this thread is the same fact the integrity check acts on:
        // the one writer is gone and nothing it was in the middle of is
        // known to have finished. Letting the thread die would merely close
        // the channel, and every caller would read `ActorStopped` — an
        // ordinary-looking error for a process that can no longer write
        // anything. Abort instead (2026-09-01).
        std::thread::spawn(move || {
            let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                Self::actor(rx, conn, premigrated, &actor_gate);
            }));
            if run.is_err() {
                integrity::abort_on_impossible_state("the store's actor thread panicked");
            }
        });

        Ok(Self {
            tx,
            descriptors,
            content_tables,
            gate,
            changes,
        })
    }

    /// The actor loop — owns the connection and executes operations
    /// sequentially. A domain's migrations run before any of its queries, and a
    /// domain whose migrations failed answers every query with that failure.
    ///
    /// `premigrated` names the domains `init` migrated before this thread
    /// started: always the library's own, plus every domain the configured
    /// open ran for the consumer.
    fn actor(
        mut rx: mpsc::UnboundedReceiver<StoreOp>,
        mut conn: Connection,
        premigrated: std::collections::HashSet<&'static str>,
        gate: &DomainGate,
    ) {
        use std::collections::{HashMap, HashSet, VecDeque};

        let mut migrated: HashSet<&'static str> = premigrated;
        let mut deferred: HashMap<&'static str, VecDeque<QueryFn>> = HashMap::new();
        // The right to judge an answer lives here and nowhere else: one
        // thread, one check, under every caller.
        let integrity = IntegrityCheck::new();

        while let Some(op) = rx.blocking_recv() {
            match op {
                StoreOp::Migration { domain, sqls, done } => {
                    let result = run_domain_migrations(&mut conn, domain, &sqls);

                    match result {
                        Ok(()) => {
                            migrated.insert(domain);
                            // A corrected retry clears the refusal: the failure
                            // is a state of the schema, not a life sentence.
                            gate.clear(domain);
                            let _ = done.send(Ok(()));
                            // Drain whatever queued up while this domain waited.
                            for f in deferred.remove(domain).unwrap_or_default() {
                                f(Ok(&mut conn), &integrity);
                            }
                        }
                        Err(failure) => {
                            let _ = done.send(Err(failure.error(domain)));
                            // Everything that queued behind the migration is
                            // answered with the failure. Running it would let
                            // queries loose on a schema whose state is unknown,
                            // and dropping it would park its caller forever.
                            for f in deferred.remove(domain).unwrap_or_default() {
                                f(Err(failure.error(domain)), &integrity);
                            }
                            gate.record(domain, failure);
                        }
                    }
                }
                StoreOp::Query { domain, f } => {
                    if let Some(refusal) = gate.failure(domain) {
                        f(Err(refusal), &integrity);
                    } else if migrated.contains(domain) {
                        f(Ok(&mut conn), &integrity);
                    } else {
                        deferred.entry(domain).or_default().push_back(f);
                    }
                }
            }
        }
    }

    /// Send a closure to the actor and await its result.
    ///
    /// # Errors
    ///
    /// Whatever the closure returns, or [`StoreError::ActorStopped`] if the
    /// actor thread is gone.
    pub async fn run<F, R>(&self, f: F) -> Result<R, StoreError>
    where
        F: FnOnce(&mut Connection) -> Result<R, StoreError> + Send + 'static,
        R: Send + 'static,
    {
        domain_run(&self.tx, CORE_DOMAIN, f).await
    }
}

/// The domain name the library's own tables live under.
const CORE_DOMAIN: &str = "core";

// The change hook's allowlist is built by `descriptors::change_hook_tables`:
// the structural ledger tables plus the effective content-table list, which is
// also what `Store::content_tables` exposes. The rule it holds to: **every
// table whose rows carry ledger content is announced.** A content table left
// off would leave a consumer waking on the header and junction rows of the
// same transaction, seeing the content only by the order the rows happen to be
// written in — which is not a guarantee anything states.

/// The migration step that failed for a domain, kept behind the
/// [`DomainGate`] so every query for that domain can be answered with the
/// same error.
#[derive(Clone)]
struct FailedMigration {
    version: i64,
    reason: String,
}

impl FailedMigration {
    fn error(&self, domain: &str) -> StoreError {
        StoreError::MigrationFailed {
            domain: domain.to_owned(),
            version: self.version,
            reason: self.reason.clone(),
        }
    }
}

/// The consumer domains' failed-migration state, shared between the actor —
/// which records and clears it as migrations run — and the store's
/// descriptor-path operations, which consult it before touching a
/// descriptor's tables.
///
/// This is what routes descriptor reads and writes through the domain-aware
/// discipline: the tables a descriptor drives are migrated under the
/// consumer's domain, so a read or write of them while that domain's
/// migrations are in a failed state answers with
/// [`StoreError::MigrationFailed`] — the exact answer [`domain_run`] gives —
/// instead of running raw against a schema in doubt. The checks run inside
/// closures on the actor thread, which processes operations in order, so a
/// failure recorded by an earlier migration op is always visible to a later
/// query's check.
#[derive(Clone, Default)]
pub(crate) struct DomainGate {
    failures: Arc<std::sync::Mutex<std::collections::HashMap<&'static str, FailedMigration>>>,
}

impl DomainGate {
    fn lock(
        &self,
    ) -> std::sync::MutexGuard<'_, std::collections::HashMap<&'static str, FailedMigration>> {
        self.failures.lock().expect("domain gate lock poisoned")
    }

    /// Record a domain's migration failure; queries for it are refused with it
    /// until a corrected migration clears it.
    fn record(&self, domain: &'static str, failure: FailedMigration) {
        self.lock().insert(domain, failure);
    }

    /// A corrected migration lifts the refusal.
    fn clear(&self, domain: &'static str) {
        self.lock().remove(domain);
    }

    /// The refusal a domain currently carries, if any.
    fn failure(&self, domain: &str) -> Option<StoreError> {
        self.lock().get(domain).map(|f| f.error(domain))
    }

    /// Refuse when the domain is in a failed-migration state.
    pub(crate) fn ensure(&self, domain: &str) -> Result<(), StoreError> {
        match self.failure(domain) {
            Some(refusal) => Err(refusal),
            None => Ok(()),
        }
    }

    /// Refuse when any of the given domains is in a failed-migration state.
    pub(crate) fn ensure_each<'d, I>(&self, domains: I) -> Result<(), StoreError>
    where
        I: IntoIterator<Item = &'d str>,
    {
        for domain in domains {
            self.ensure(domain)?;
        }
        Ok(())
    }
}

/// The store's own bookkeeping tables, created idempotently at open beside
/// the core migrations: the per-domain migration counter and the descriptor
/// registry that makes descriptors durable facts of the database.
fn ensure_tracking_tables(conn: &Connection) -> Result<(), StoreError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS domain_migrations (
            domain TEXT PRIMARY KEY,
            version INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS content_descriptors (
            table_name TEXT PRIMARY KEY,
            domain     TEXT NOT NULL,
            kinds      TEXT NOT NULL
        );",
    )?;
    Ok(())
}

/// Every table the library's own migrations (plus the tracking tables)
/// create, as a snapshot of `sqlite_master` — the set a descriptor's table
/// may not collide with.
///
/// Taken from a throwaway in-memory schema rather than the connection being
/// opened, deliberately: after the core migrations, and before any consumer
/// migration ever touches it, which is the only moment the snapshot means
/// "the library's tables and nothing else". The opened database itself offers
/// no such moment on a reopen — the consumer's tables from earlier opens are
/// already in ITS `sqlite_master`, and a snapshot there would claim them as
/// the library's. Correct by construction, with no hand-mirrored literal to
/// rot.
fn core_table_snapshot() -> Result<std::collections::HashSet<String>, StoreError> {
    let conn = Connection::open_in_memory()?;
    migrations::run(&conn)?;
    ensure_tracking_tables(&conn)?;
    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type = 'table'")?;
    let tables = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<std::collections::HashSet<_>, _>>()?;
    Ok(tables)
}

/// Apply one domain's migrations, tracking its version in `domain_migrations`.
///
/// Each step runs in a transaction of its own **with its version bump inside
/// it**, so a step is either applied and counted or neither. A multi-statement
/// step that fails halfway used to leave its earlier statements applied under
/// an unchanged version, and the retry then met the tables the first attempt
/// had created — a domain bricked by its own recovery path.
fn run_domain_migrations(
    conn: &mut Connection,
    domain: &'static str,
    sqls: &[&'static str],
) -> Result<(), FailedMigration> {
    let current_version = domain_version(conn, domain).map_err(|e| FailedMigration {
        version: 0,
        reason: e.to_string(),
    })?;

    for (i, sql) in sqls.iter().enumerate() {
        let version = i64::try_from(i + 1).unwrap_or(i64::MAX);
        if version > current_version {
            apply_domain_step(conn, domain, version, sql).map_err(|e| FailedMigration {
                version,
                reason: e.to_string(),
            })?;
            tracing::info!(domain, version, "applied domain migration");
        }
    }
    Ok(())
}

/// The version `domain_migrations` carries for a domain, creating that table on
/// first use. An unknown domain is at version 0 — a legal absence, written as
/// the `Option` the read answers.
fn domain_version(conn: &Connection, domain: &'static str) -> rusqlite::Result<i64> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS domain_migrations (
            domain TEXT PRIMARY KEY,
            version INTEGER NOT NULL DEFAULT 0
        );",
    )?;

    Ok(conn
        .query_row(
            "SELECT version FROM domain_migrations WHERE domain = ?1",
            [domain],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0))
}

/// One migration step and its version bump, in one transaction.
fn apply_domain_step(
    conn: &mut Connection,
    domain: &'static str,
    version: i64,
    sql: &str,
) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    tx.execute_batch(sql)?;
    tx.execute(
        "INSERT INTO domain_migrations (domain, version) VALUES (?1, ?2)
         ON CONFLICT(domain) DO UPDATE SET version = excluded.version",
        rusqlite::params![domain, version],
    )?;
    tx.commit()
}

/// Run a closure inside a transaction. Commits on `Ok`, rolls back on `Err`.
///
/// # A rollback does not un-announce
///
/// The row change hook fires while the statements run, not when the transaction
/// commits. A transaction that rolls back has therefore already pushed a
/// [`ChangeEvent`](crate::reactivity::ChangeEvent) for every row it touched, and
/// those rows are gone. Nothing here suppresses that, on purpose: an event is a
/// prompt to re-derive from durable state, so a wakeup for a write that never
/// landed costs one re-read that finds nothing — while buffering events until
/// commit would cost the wakeup that a consumer's correctness depends on.
///
/// # Errors
///
/// Whatever the closure returns, or the database's error if the transaction
/// cannot be opened or committed.
pub fn transact<F, T>(conn: &mut Connection, f: F) -> Result<T, StoreError>
where
    F: FnOnce(&rusqlite::Transaction<'_>) -> Result<T, StoreError>,
{
    let tx = conn.transaction()?;
    let result = f(&tx)?;
    tx.commit()?;
    Ok(result)
}

// ─── The consumer's seam ─────────────────────────────────────────────────

/// Submit a query for a specific domain. Waits behind that domain's migrations
/// if they have not completed yet.
///
/// This is how a consumer's own tables share the single writer: same
/// connection, same ordering, its own migration counter.
///
/// # Errors
///
/// Whatever the closure returns, [`StoreError::MigrationFailed`] if this
/// domain's migrations failed, or [`StoreError::ActorStopped`] if the actor
/// thread is gone.
///
/// # Aborts
///
/// The answer is judged on the actor thread before it travels back
/// (2026-09-01): a database in a state the design forbids — a violated
/// constraint, a corrupt or misused file, a missing row a query guarantees —
/// ends the process here. See [`IntegrityCheck`]. This is why no caller of
/// this function classifies a database error, and why none can.
pub async fn domain_run<F, R>(tx: &StoreTx, domain: &'static str, f: F) -> Result<R, StoreError>
where
    F: FnOnce(&mut Connection) -> Result<R, StoreError> + Send + 'static,
    R: Send + 'static,
{
    let (resp_tx, resp_rx) = oneshot::channel();
    tx.send(StoreOp::Query {
        domain,
        f: Box::new(move |target, integrity| {
            let answer = match target {
                Ok(conn) => f(conn),
                Err(migration_failure) => Err(migration_failure),
            };
            // Before the answer leaves this thread, and so before anything
            // can act on it.
            integrity.judge(&answer);
            let _ = resp_tx.send(answer);
        }),
    })
    .map_err(|_| StoreError::ActorStopped)?;
    resp_rx.await.map_err(|_| StoreError::ActorStopped)?
}

/// Submit migrations for a domain. Returns once they have all executed on the
/// actor thread.
///
/// Queries for this domain submitted **before** this call wait for it; queries
/// submitted after it proceed normally. The library's own schema advances on
/// the database's `user_version`; a domain advances on its own row in
/// `domain_migrations`, and neither counter can stall the other.
///
/// A step that fails is rolled back whole, and from then on every query for
/// that domain — the ones already waiting and the ones still to arrive — is
/// answered with [`StoreError::MigrationFailed`]. Submitting corrected
/// migrations that succeed lifts the refusal.
///
/// # Errors
///
/// [`StoreError::MigrationFailed`] if a step fails, or
/// [`StoreError::ActorStopped`] if the actor thread is gone.
pub async fn domain_migrate(
    tx: &StoreTx,
    domain: &'static str,
    sqls: Vec<&'static str>,
) -> Result<(), StoreError> {
    let (done_tx, done_rx) = oneshot::channel();
    tx.send(StoreOp::Migration {
        domain,
        sqls,
        done: done_tx,
    })
    .map_err(|_| StoreError::ActorStopped)?;
    done_rx.await.map_err(|_| StoreError::ActorStopped)?
}

/// The clock every ledger stamp is written from: the machine's local time,
/// carried as a fixed numeric offset so the instant survives the string.
///
/// One clock, one home. A fold that compares stamps against "now" reads THIS,
/// never a second clock of its own — a window measured against UTC while the
/// rows are stamped in local time is off by the offset, silently, and only
/// where the offset is not zero.
pub(crate) fn now_instant() -> chrono::DateTime<chrono::FixedOffset> {
    chrono::Local::now().fixed_offset()
}

/// An ISO 8601 timestamp with the local timezone offset, for example
/// `2026-03-01T19:17:09.524+02:00`.
pub(crate) fn now_iso8601() -> String {
    now_instant().to_rfc3339_opts(chrono::SecondsFormat::Millis, false)
}

/// Read a stored stamp back as the instant it names — the inverse of
/// [`now_iso8601`], and the only place the stored form is parsed.
///
/// Offset-aware, never lexical: two stamps written either side of a daylight
/// saving change carry different offsets, and comparing them as strings sorts
/// them by the wall-clock digits rather than by time.
///
/// `None` for anything that is not the written form. Every production insert
/// stamps through [`now_iso8601`], so the one shape that lands here is the
/// schema's own `datetime('now')` column default — a space-separated UTC
/// string, reachable only by a fixture writing a row without a stamp. Reading
/// that as unparseable rather than as "just now" is deliberate: a caller
/// folding a trailing window over the stamps leaves an unreadable row OUT of
/// the window instead of holding one forever.
pub(crate) fn parse_stamp(stamp: &str) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    chrono::DateTime::parse_from_rfc3339(stamp).ok()
}

/// A directory of this test process's own, named so two tests never share
/// one — where a suite that needs a store on DISK puts it (the in-memory
/// store is what every other test uses, and it cannot be reopened).
///
/// One definition for the whole crate: two test modules were writing the same
/// eight lines, and two more were about to.
#[cfg(test)]
pub(crate) fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "agent-ledger-{label}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The store's own tests.
///
/// Two tests of the source's twenty-two did not come across: they exercised the
/// tables for tracked projects and for sessions, and neither table exists here
/// — both belong to the application this was extracted from, not to the ledger.
/// Nothing they asserted applies to a library table.
#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::time::{Duration, sleep, timeout};

    use super::*;
    use crate::block::Role;
    use crate::reactive;
    use crate::types::InputBlock;

    fn store() -> Store {
        Store::in_memory().unwrap()
    }

    async fn make_conv(s: &Store, provider_id: &str, model: &str) -> i64 {
        s.create_conversation(
            provider_id.into(),
            model.into(),
            model.into(),
            String::new(),
        )
        .await
        .unwrap()
    }

    fn text(content: &str) -> InputBlock {
        InputBlock::Text {
            content: content.into(),
        }
    }

    #[tokio::test]
    async fn provider_instances_crud() {
        let s = store();
        s.save_provider_instance(ProviderInstance {
            id: "p1".into(),
            provider_type: "anthropic".into(),
            name: "Test".into(),
        })
        .await
        .unwrap();

        assert_eq!(s.list_provider_instances().await.unwrap().len(), 1);

        s.save_provider_instance(ProviderInstance {
            id: "p1".into(),
            provider_type: "openai".into(),
            name: "Updated".into(),
        })
        .await
        .unwrap();

        let instances = s.list_provider_instances().await.unwrap();
        assert_eq!(instances.len(), 1);
        assert_eq!(instances[0].name, "Updated");
        assert_eq!(
            s.find_provider_instance("p1".into())
                .await
                .unwrap()
                .unwrap()
                .provider_type,
            "openai"
        );

        s.delete_provider_instance("p1".into()).await.unwrap();
        assert!(s.list_provider_instances().await.unwrap().is_empty());
    }

    /// Eight tasks hitting one store at once. The actor serializes them, so
    /// every write lands and none of them collide.
    #[tokio::test]
    async fn concurrent_access() {
        let s = Arc::new(store());
        let mut handles = Vec::new();
        for i in 0..8 {
            let store = Arc::clone(&s);
            handles.push(tokio::spawn(async move {
                let conv = make_conv(&store, "p1", &format!("model-{i}")).await;
                store
                    .insert_text_block(conv, Role::User, format!("hello {i}"))
                    .await
                    .unwrap();
                store.list_conversations().await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(s.list_conversations().await.unwrap().len(), 8);
    }

    #[tokio::test]
    async fn conversations_crud() {
        let s = store();
        let c1 = make_conv(&s, "p1", "gpt-4").await;
        let c2 = make_conv(&s, "p1", "claude-sonnet").await;

        let convs = s.list_conversations().await.unwrap();
        assert_eq!(convs.len(), 2);

        let conv = s.find_conversation(c1).await.unwrap().unwrap();
        assert_eq!(conv.model.external_id, "gpt-4");

        s.delete_conversation(c1).await.unwrap();
        assert_eq!(s.list_conversations().await.unwrap().len(), 1);
        let _ = c2;
    }

    #[tokio::test]
    async fn blocks_crud() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;

        let b1 = s
            .insert_text_block(c1, Role::User, "hello".into())
            .await
            .unwrap();
        let b2 = s
            .insert_text_block(c1, Role::Assistant, "hi there".into())
            .await
            .unwrap();

        let greeting = s.find_block(b1).await.unwrap().unwrap();
        assert_eq!(greeting.role, Some(Role::User));
        assert_eq!(greeting.block_type, "text");
        assert_eq!(greeting.fields["content"].as_str().unwrap(), "hello");

        let answer = s.find_block(b2).await.unwrap().unwrap();
        assert_eq!(answer.role, Some(Role::Assistant));

        let blocks = s.list_blocks(c1).await.unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(s.block_count(c1).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn delete_streaming_blocks_keeps_committed() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;
        // A second conversation whose streaming block must survive — the delete
        // is scoped per conversation.
        let c2 = make_conv(&s, "p1", "model").await;

        // Committed blocks that MUST survive a restart-clean.
        let committed_text = s
            .insert_text_block(c1, Role::Assistant, "committed".into())
            .await
            .unwrap();
        let committed_thinking = s
            .insert_thinking_block_with_content(
                c1,
                Role::Assistant,
                "reasoned".into(),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let committed_tool_call = s
            .insert_tool_call_block(
                c1,
                Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call_1".into(),
                    name: "search".into(),
                    input: "{}".into(),
                    interactive: false,
                },
                None,
            )
            .await
            .unwrap();
        let tool_result = s
            .complete_tool_call_block(
                c1,
                "call_1".into(),
                "result text".into(),
                committed_tool_call,
            )
            .await
            .unwrap()
            .expect("the call is unresolved");

        // Uncommitted streaming partials left by a dropped stream.
        let _streaming_text = s.insert_streaming_block(c1, Role::Assistant).await.unwrap();
        let _streaming_thinking = s
            .insert_streaming_thinking_block(c1, Role::Assistant)
            .await
            .unwrap();
        let _streaming_tool = s
            .insert_streaming_tool_call_block(c1, Role::Assistant, "call_2".into(), "edit".into())
            .await
            .unwrap();

        // Another conversation's streaming block — must be untouched.
        let other_streaming = s.insert_streaming_block(c2, Role::Assistant).await.unwrap();

        assert_eq!(s.list_blocks(c1).await.unwrap().len(), 7);

        let deleted = s.delete_streaming_blocks(c1).await.unwrap();
        assert_eq!(deleted, 3, "exactly the 3 streaming blocks are removed");

        // The committed blocks and the tool result survive.
        let remaining = s.list_blocks(c1).await.unwrap();
        let remaining_ids: Vec<i64> = remaining.iter().map(|b| b.id).collect();
        assert_eq!(remaining.len(), 4);
        assert!(remaining_ids.contains(&committed_text));
        assert!(remaining_ids.contains(&committed_thinking));
        assert!(remaining_ids.contains(&committed_tool_call));
        assert!(remaining_ids.contains(&tool_result));
        assert!(
            remaining
                .iter()
                .all(|b| !b.block_type.starts_with("streaming"))
        );

        // The other conversation's streaming block is untouched.
        assert!(s.find_block(other_streaming).await.unwrap().is_some());
        assert_eq!(s.list_blocks(c2).await.unwrap().len(), 1);

        // Idempotent: a second call deletes nothing.
        assert_eq!(s.delete_streaming_blocks(c1).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn block_level_operations() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;
        let now = now_iso8601();

        s.insert_thinking_block(c1, Role::Assistant).await.unwrap();
        s.insert_text_block(c1, Role::Assistant, String::new())
            .await
            .unwrap();

        s.append_to_latest_block(
            c1,
            Role::Assistant,
            "thinking".into(),
            "hmm".into(),
            now.clone(),
        )
        .await
        .unwrap();
        s.append_to_latest_block(
            c1,
            Role::Assistant,
            "thinking".into(),
            " let me think".into(),
            now.clone(),
        )
        .await
        .unwrap();
        s.append_to_latest_block(
            c1,
            Role::Assistant,
            "text".into(),
            "Hello".into(),
            now.clone(),
        )
        .await
        .unwrap();
        s.append_to_latest_block(
            c1,
            Role::Assistant,
            "text".into(),
            " world".into(),
            now.clone(),
        )
        .await
        .unwrap();

        let blocks = s.list_blocks(c1).await.unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].block_type, "thinking");
        assert_eq!(
            blocks[0].fields["content"].as_str().unwrap(),
            "hmm let me think"
        );
        assert_eq!(blocks[1].block_type, "text");
        assert_eq!(blocks[1].fields["content"].as_str().unwrap(), "Hello world");

        s.set_latest_block_text(
            c1,
            Role::Assistant,
            "text".into(),
            "Replaced".into(),
            now,
            None,
        )
        .await
        .unwrap();
        let blocks = s.list_blocks(c1).await.unwrap();
        assert_eq!(blocks[1].fields["content"].as_str().unwrap(), "Replaced");
    }

    #[tokio::test]
    async fn interleaved_thinking_text_blocks() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;

        s.insert_thinking_block(c1, Role::Assistant).await.unwrap();
        s.insert_text_block(c1, Role::Assistant, String::new())
            .await
            .unwrap();
        s.insert_thinking_block(c1, Role::Assistant).await.unwrap();
        s.insert_text_block(c1, Role::Assistant, String::new())
            .await
            .unwrap();

        let blocks = s.list_blocks(c1).await.unwrap();
        assert_eq!(blocks.len(), 4);
        assert_eq!(blocks[0].block_type, "thinking");
        assert_eq!(blocks[1].block_type, "text");
        assert_eq!(blocks[2].block_type, "thinking");
        assert_eq!(blocks[3].block_type, "text");
    }

    /// A fork that must differ from its source in one recorded fact says so
    /// by detaching the inherited one — and the source keeps it, because a
    /// fork is not an edit of what it came from.
    #[tokio::test]
    async fn a_detached_block_leaves_the_fork_and_stays_in_the_source() {
        let store = Store::in_memory().expect("an in-memory store opens");
        let source = store
            .create_conversation(
                "instance".into(),
                "model".into(),
                "Model".into(),
                "vendor".into(),
            )
            .await
            .expect("the conversation is created");
        let prompt = store
            .insert_system_prompt(source, "the original instructions".into())
            .await
            .expect("the prompt is recorded");
        let said = store
            .insert_text_block(source, Role::User, "something said".into())
            .await
            .expect("the message is recorded");

        let fork = store
            .fork_conversation(source, said, ModelOverride::default())
            .await
            .expect("the fork inherits the history");
        assert_eq!(
            store.list_blocks(fork).await.expect("the fork reads").len(),
            2,
            "the fork inherits both blocks through the junction"
        );

        store
            .detach_block(fork, prompt)
            .await
            .expect("the inherited prompt detaches");

        let forked: Vec<i64> = store
            .list_blocks(fork)
            .await
            .expect("the fork reads")
            .into_iter()
            .map(|block| block.id)
            .collect();
        assert_eq!(
            forked,
            vec![said],
            "the fork keeps what was said and no longer holds the prompt"
        );
        let sourced: Vec<i64> = store
            .list_blocks(source)
            .await
            .expect("the source reads")
            .into_iter()
            .map(|block| block.id)
            .collect();
        assert_eq!(
            sourced,
            vec![prompt, said],
            "the source is untouched: a fork cannot edit what it came from"
        );

        store
            .detach_block(fork, prompt)
            .await
            .expect("detaching what is not held changes nothing");
    }

    /// A sweep detaches its whole set through one door: the conversation loses
    /// every named block in one step, and a sibling fork that shares the very
    /// same block rows still reads them whole — a membership went, no content
    /// did.
    #[tokio::test]
    async fn a_bulk_detach_clears_its_set_and_leaves_a_sibling_reading() {
        let s = store();
        let source = make_conv(&s, "p1", "model").await;

        let mut said = Vec::new();
        for turn in 0..6 {
            said.push(
                s.insert_text_block(source, Role::User, format!("turn {turn}"))
                    .await
                    .expect("the message is recorded"),
            );
        }
        let last = *said.last().expect("six blocks were written");

        let swept = s
            .fork_conversation(source, last, ModelOverride::default())
            .await
            .expect("the sweeping fork inherits the history");
        let sibling = s
            .fork_conversation(source, last, ModelOverride::default())
            .await
            .expect("the sibling fork inherits the same history");

        let detached: Vec<i64> = said[1..4].to_vec();
        s.detach_blocks(swept, detached.clone())
            .await
            .expect("the whole set detaches");

        let left: Vec<i64> = s
            .list_blocks(swept)
            .await
            .expect("the swept fork reads")
            .into_iter()
            .map(|block| block.id)
            .collect();
        assert_eq!(
            left,
            vec![said[0], said[4], said[5]],
            "one step took the whole set out of the projection and nothing else"
        );

        let sibling_blocks = s.list_blocks(sibling).await.expect("the sibling reads");
        assert_eq!(
            sibling_blocks
                .iter()
                .map(|block| block.id)
                .collect::<Vec<i64>>(),
            said,
            "the sibling still holds every block the sweep detached"
        );
        for (turn, block) in sibling_blocks.iter().enumerate() {
            assert_eq!(
                block.fields["content"].as_str().expect("the text reads"),
                format!("turn {turn}"),
                "the shared blocks keep their content: a membership went, not a block"
            );
        }
    }

    /// The bulk door detaches what is there and asks no existence question: an
    /// empty list changes nothing at all, and an id the conversation does not
    /// hold costs its list nothing.
    #[tokio::test]
    async fn a_bulk_detach_is_a_no_op_when_empty_and_skips_what_is_not_held() {
        let s = store();
        let conversation = make_conv(&s, "p1", "model").await;
        let kept = s
            .insert_text_block(conversation, Role::User, "kept".into())
            .await
            .expect("the message is recorded");
        let going = s
            .insert_text_block(conversation, Role::User, "going".into())
            .await
            .expect("the message is recorded");

        let elsewhere = make_conv(&s, "p1", "model").await;
        let stranger = s
            .insert_text_block(elsewhere, Role::User, "another conversation's".into())
            .await
            .expect("the message is recorded");

        s.detach_blocks(conversation, Vec::new())
            .await
            .expect("an empty list is a no-op");
        assert_eq!(
            s.list_blocks(conversation)
                .await
                .expect("the conversation reads")
                .into_iter()
                .map(|block| block.id)
                .collect::<Vec<i64>>(),
            vec![kept, going],
            "the empty list detached nothing"
        );

        s.detach_blocks(conversation, vec![stranger, going, i64::MAX])
            .await
            .expect("unheld ids do not fail the call");

        assert_eq!(
            s.list_blocks(conversation)
                .await
                .expect("the conversation reads")
                .into_iter()
                .map(|block| block.id)
                .collect::<Vec<i64>>(),
            vec![kept],
            "the held id in the list landed while the unheld ones detached nothing"
        );
        assert_eq!(
            s.list_blocks(elsewhere)
                .await
                .expect("the other conversation reads")
                .into_iter()
                .map(|block| block.id)
                .collect::<Vec<i64>>(),
            vec![stranger],
            "naming another conversation's block detached it from nowhere"
        );
    }

    #[tokio::test]
    async fn fork_conversation_shares_blocks() {
        let s = store();
        let c1 = make_conv(&s, "p1", "gpt-4").await;

        let b1 = s
            .insert_text_block(c1, Role::User, "hello".into())
            .await
            .unwrap();
        let b2 = s
            .insert_text_block(c1, Role::Assistant, "hi".into())
            .await
            .unwrap();
        s.insert_text_block(c1, Role::User, "how are you".into())
            .await
            .unwrap();

        let c2 = s
            .fork_conversation(c1, b2, ModelOverride::default())
            .await
            .unwrap();
        let fork = s.find_conversation(c2).await.unwrap().unwrap();
        assert_eq!(fork.parent_id, Some(c1));

        let fork_blocks = s.list_blocks(c2).await.unwrap();
        assert_eq!(fork_blocks.len(), 2);
        assert_eq!(fork_blocks[0].id, b1);
        assert_eq!(fork_blocks[1].id, b2);

        assert_eq!(s.list_blocks(c1).await.unwrap().len(), 3);
    }

    /// A fork whose source was deleted under it is REFUSED, never an abort
    /// (2026-09-01). The compaction races exactly this: the cut is taken, the
    /// source is deleted, and the fork arrives after it. The source id is the
    /// caller's, so its absence is an answer the caller reads — the integrity
    /// check must never meet it as a missing guaranteed row.
    #[tokio::test]
    async fn forking_a_conversation_that_is_gone_is_refused() {
        let s = store();
        let c1 = make_conv(&s, "p1", "gpt-4").await;
        // A conversation written AFTER the source and outliving it, so the id
        // SQLite hands the fork is a fresh one. Were the source's row the
        // highest, deleting it would hand its own id straight back to the
        // fork, whose `parent_id` would then point at itself and satisfy the
        // foreign key this check stands in front of.
        let sibling = make_conv(&s, "p0", "gpt-4").await;
        assert!(c1 < sibling, "the source must not hold the highest id");
        let b1 = s
            .insert_text_block(c1, Role::User, "hello".into())
            .await
            .unwrap();
        s.delete_conversation(c1).await.unwrap();

        // The inheriting shape: nothing overridden, so the model and the
        // reasoning are read off the source's own row.
        let inherited = s
            .fork_conversation(c1, b1, ModelOverride::default())
            .await
            .expect_err("a fork of a deleted conversation must answer an error");
        assert!(
            matches!(&inherited, StoreError::Other(reason) if reason.contains("does not exist")),
            "{inherited:?}"
        );

        // The overriding shape: every setting comes from the caller, so
        // nothing reads the source row and the door's own check is the only
        // thing between this call and a foreign key on the fork's parent.
        let overridden = s
            .fork_conversation(
                c1,
                b1,
                ModelOverride {
                    provider_id: Some("p1".into()),
                    external_id: Some("gpt-4".into()),
                    display_name: Some("gpt-4".into()),
                    vendor: Some("openai".into()),
                    reasoning: Some("high".into()),
                },
            )
            .await
            .expect_err("a fork of a deleted conversation must answer an error");
        assert!(
            matches!(&overridden, StoreError::Other(reason) if reason.contains("does not exist")),
            "{overridden:?}"
        );

        // And the other caller mistake the same door answers: a live source,
        // a block that belongs to someone else. The junction cutoff finds
        // nothing, which is an argument, not a corrupted database.
        let other = make_conv(&s, "p2", "gpt-4").await;
        let stranger = s
            .insert_text_block(other, Role::User, "elsewhere".into())
            .await
            .unwrap();
        let host = make_conv(&s, "p3", "gpt-4").await;
        let foreign_block = s
            .fork_conversation(host, stranger, ModelOverride::default())
            .await
            .expect_err("a block from another conversation must answer an error");
        assert!(
            matches!(&foreign_block, StoreError::Other(reason) if reason.contains("is not in conversation")),
            "{foreign_block:?}"
        );
    }

    #[tokio::test]
    async fn fork_continuation_rerun_shares_group_via_junction() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;

        let b1 = s
            .insert_text_block(c1, Role::User, "hi".into())
            .await
            .unwrap();
        let b2 = s
            .insert_text_block(c1, Role::Assistant, "hello".into())
            .await
            .unwrap();
        let b3 = s
            .insert_user_blocks(c1, vec![text("a"), text("b")])
            .await
            .unwrap();
        let (u_a, u_b) = (b3[0], b3[1]);

        let c2 = s
            .fork_continuation(c1, u_a, Continuation::Rerun, ModelOverride::default())
            .await
            .unwrap();

        // insert_user_blocks prepends the day's date marker — it is shared
        // through the junction like every other inherited block.
        let marker = s
            .list_blocks(c1)
            .await
            .unwrap()
            .iter()
            .find(|b| b.block_type == "date_marker")
            .unwrap()
            .id;
        let fork_blocks = s.list_blocks(c2).await.unwrap();
        let fork_ids: Vec<i64> = fork_blocks.iter().map(|b| b.id).collect();
        assert_eq!(
            fork_ids,
            vec![b1, b2, marker, u_a, u_b],
            "rerun shares the entire group via the junction"
        );
    }

    #[tokio::test]
    async fn fork_continuation_edit_replaces_group() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;

        let b1 = s
            .insert_text_block(c1, Role::User, "hi".into())
            .await
            .unwrap();
        let b2 = s
            .insert_text_block(c1, Role::Assistant, "hello".into())
            .await
            .unwrap();
        let b3 = s
            .insert_user_blocks(c1, vec![text("original")])
            .await
            .unwrap();

        let c2 = s
            .fork_continuation(
                c1,
                b3[0],
                Continuation::Edit(vec![text("edited")]),
                ModelOverride::default(),
            )
            .await
            .unwrap();

        let fork_blocks = s.list_blocks(c2).await.unwrap();
        assert_eq!(
            fork_blocks.len(),
            4,
            "b1, b2, the shared date marker, the edited block"
        );
        assert_eq!(fork_blocks[0].id, b1);
        assert_eq!(fork_blocks[1].id, b2);
        assert_eq!(fork_blocks[2].block_type, "date_marker");
        assert_ne!(fork_blocks[3].id, b3[0], "an edit inserts a fresh block");
    }

    /// An edit resubmitted on a LATER day than the source was last written gets
    /// a fresh date marker — the inherited, stale marker is not carried forward
    /// as the current date. Seeds the source with a past-dated marker, then
    /// edits today.
    #[tokio::test]
    async fn edit_fork_on_a_new_day_inserts_a_fresh_marker() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;

        let seeded = s
            .insert_user_blocks_dated(
                c1,
                vec![text("original")],
                super::date_markers::DateStamp::date_only("2020-01-01"),
            )
            .await
            .unwrap();

        let c2 = s
            .fork_continuation(
                c1,
                seeded[0],
                Continuation::Edit(vec![text("edited")]),
                ModelOverride::default(),
            )
            .await
            .unwrap();

        let markers: Vec<String> = s
            .list_blocks(c2)
            .await
            .unwrap()
            .iter()
            .filter(|b| b.block_type == "date_marker")
            .map(|b| b.fields["date"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            markers.len(),
            2,
            "the inherited past marker plus a fresh one for today"
        );
        assert_eq!(
            markers[0], "2020-01-01",
            "the inherited marker rides the junction unchanged"
        );
        assert_eq!(
            markers[1],
            super::date_markers::DateStamp::now_local().date,
            "the edit's own day is recorded"
        );
    }

    #[tokio::test]
    async fn fork_continuation_new_thread_deep_copies_group_and_detaches_quote_targets() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;

        s.insert_text_block(c1, Role::User, "hi".into())
            .await
            .unwrap();
        let a1 = s
            .insert_text_block(c1, Role::Assistant, "The quick brown fox".into())
            .await
            .unwrap();
        let user_blocks = s
            .insert_user_blocks(
                c1,
                vec![
                    text("quoting:"),
                    InputBlock::Quote {
                        start_block_id: a1,
                        start_pos: 0,
                        end_block_id: a1,
                        end_pos: 9,
                    },
                ],
            )
            .await
            .unwrap();

        let c2 = s
            .fork_continuation(
                c1,
                user_blocks[0],
                Continuation::NewThread {
                    system_prompt: Some("a prompt the consumer wrote".into()),
                },
                ModelOverride::default(),
            )
            .await
            .unwrap();

        let fork = s.find_conversation(c2).await.unwrap().unwrap();
        assert_eq!(fork.parent_id, None, "a new thread has no parent");

        let fork_blocks = s.list_blocks(c2).await.unwrap();
        // The prompt, a fresh date marker, then the cloned group (text and
        // quote) — the new thread's first turn owes the model today's date.
        assert_eq!(fork_blocks.len(), 4);
        let types: Vec<&str> = fork_blocks.iter().map(|b| b.block_type.as_str()).collect();
        assert_eq!(types, vec!["system_prompt", "date_marker", "text", "quote"]);
        for fb in &fork_blocks {
            assert!(
                !user_blocks.contains(&fb.id),
                "blocks must be freshly cloned, not shared"
            );
        }

        // Deleting the source must not break the fork's quote resolution — the
        // target text block was deep-copied as a detached row.
        s.delete_conversation(c1).await.unwrap();
        let post = s.list_blocks(c2).await.unwrap();
        let quote = post
            .iter()
            .find(|b| b.block_type == "quote")
            .expect("the quote survives");
        let text = quote
            .fields
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        assert_eq!(text, "The quick");
    }

    #[tokio::test]
    async fn branch_points() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;

        s.insert_text_block(c1, Role::User, "hello".into())
            .await
            .unwrap();
        let b2 = s
            .insert_text_block(c1, Role::Assistant, "hi".into())
            .await
            .unwrap();
        let b3 = s
            .insert_text_block(c1, Role::User, "how are you".into())
            .await
            .unwrap();

        s.fork_conversation(c1, b2, ModelOverride::default())
            .await
            .unwrap();
        s.fork_conversation(c1, b2, ModelOverride::default())
            .await
            .unwrap();
        s.fork_conversation(c1, b3, ModelOverride::default())
            .await
            .unwrap();

        let mut bps = s.branch_points(c1).await.unwrap();
        assert_eq!(bps.len(), 2);
        bps.sort_by_key(|bp| bp.block_id);
        assert_eq!(bps[0].block_id, b2);
        assert_eq!(bps[0].branch_count, 2);
        assert_eq!(bps[1].block_id, b3);
        assert_eq!(bps[1].branch_count, 1);

        assert_eq!(s.list_branches(c1).await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn gc_orphan_blocks() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;
        s.insert_text_block(c1, Role::User, "hello".into())
            .await
            .unwrap();
        s.insert_text_block(c1, Role::Assistant, "hi".into())
            .await
            .unwrap();

        s.delete_conversation(c1).await.unwrap();
        let cleaned = s.gc_orphan_blocks().await.unwrap();
        assert_eq!(cleaned, 2);
    }

    /// AC8-6, the fork half: the deep-copy cloner — the one mechanism every
    /// fork path clones through — treats the dispatch anchor as a real
    /// reference. Cloned beside its target, the copy's anchor is REMAPPED to
    /// the target's clone; cloned without it, the anchor is KEPT by
    /// reference.
    #[tokio::test]
    async fn a_deep_copy_remaps_the_anchor_where_its_target_was_cloned() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;
        let msg = s
            .insert_text_block(c1, Role::User, "summon".into())
            .await
            .unwrap();
        let answer = s
            .insert_final_text_block(
                BlockDestination::anchored(c1, Some(msg)),
                Role::Assistant,
                "answered".into(),
                None,
            )
            .await
            .unwrap();

        // Both cloned, target first: the answer's anchor follows the clone.
        let c2 = make_conv(&s, "p1", "model").await;
        let descriptors = s.descriptors;
        let gate = s.gate.clone();
        let (msg_clone, answer_clone) = s
            .run(move |conn| {
                let mut cloner = block_cloner::BlockCloner::new(conn, descriptors, &gate);
                let m = cloner.clone_linked(msg, c2)?;
                let a = cloner.clone_linked(answer, c2)?;
                Ok((m, a))
            })
            .await
            .unwrap();
        let remapped = s.find_block(answer_clone).await.unwrap().unwrap();
        assert_eq!(
            remapped.dispatch_anchor,
            Some(msg_clone),
            "the anchor is remapped to the target's clone"
        );

        // The answer alone: the anchor is kept, naming the source's block.
        let c3 = make_conv(&s, "p1", "model").await;
        let gate = s.gate.clone();
        let kept_clone = s
            .run(move |conn| {
                let mut cloner = block_cloner::BlockCloner::new(conn, descriptors, &gate);
                cloner.clone_linked(answer, c3)
            })
            .await
            .unwrap();
        let kept = s.find_block(kept_clone).await.unwrap().unwrap();
        assert_eq!(
            kept.dispatch_anchor,
            Some(msg),
            "an uncloned target's anchor is kept by reference"
        );
    }

    /// AC8-6, the collection half, under the erasure rule (2026-08-22): an
    /// anchored-at block is a REFERENCED block while its conversation lives —
    /// the shared rerun fork proves the kept anchor keeps resolving. Deleting
    /// a conversation NULLS the anchors pointing into it in the same
    /// transaction, so fork-then-delete leaves no dangling anchor AND leaves
    /// nothing uncollectable: the fork's clone reads the documented null, and
    /// the deleted conversation's blocks collect instead of being pinned
    /// forever by a cross-conversation reference erasure can never release.
    #[tokio::test]
    async fn deleting_a_conversation_nulls_anchors_into_it_and_collection_proceeds() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;
        let msg = s
            .insert_text_block(c1, Role::User, "summon".into())
            .await
            .unwrap();
        let answer = s
            .insert_final_text_block(
                BlockDestination::anchored(c1, Some(msg)),
                Role::Assistant,
                "answered".into(),
                None,
            )
            .await
            .unwrap();

        // A rerun fork SHARES the history by junction: deleting the source
        // must not null the fork's anchors, because their target stays the
        // fork's own readable history.
        let shared_fork = s
            .fork_conversation(c1, answer, ModelOverride::default())
            .await
            .unwrap();

        // A deep-copied clone in another conversation KEEPS the anchor by
        // reference — the cross-conversation shape deletion has to null.
        let c2 = make_conv(&s, "p1", "model").await;
        let descriptors = s.descriptors;
        let gate = s.gate.clone();
        let fork_answer = s
            .run(move |conn| {
                let mut cloner = block_cloner::BlockCloner::new(conn, descriptors, &gate);
                cloner.clone_linked(answer, c2)
            })
            .await
            .unwrap();

        // Deleting the shared fork first: the source still junctions the
        // target, so every anchor at it stays intact.
        s.delete_conversation(shared_fork).await.unwrap();
        assert_eq!(
            s.find_block(answer).await.unwrap().unwrap().dispatch_anchor,
            Some(msg),
            "a target the source still junctions keeps its anchors"
        );

        // Deleting the source nulls the clone's cross-conversation anchor in
        // the same transaction, and collection then takes the whole deleted
        // history — nothing survives uncollectable behind a kept reference.
        s.delete_conversation(c1).await.unwrap();
        let cloned = s.find_block(fork_answer).await.unwrap().unwrap();
        assert_eq!(
            cloned.dispatch_anchor, None,
            "the clone reads the documented null, not a dangling id"
        );
        s.gc_orphan_blocks().await.unwrap();
        assert!(
            s.find_block(msg).await.unwrap().is_none(),
            "the deleted conversation's summoner collects"
        );
        assert!(
            s.find_block(answer).await.unwrap().is_none(),
            "…and its answer collects with it"
        );
        assert_eq!(
            s.find_block(fork_answer)
                .await
                .unwrap()
                .expect("the fork's own block is untouched")
                .fields["content"],
            serde_json::json!("answered")
        );
    }

    /// A conversation whose quote target is a detached block, plus a pile of
    /// ordinary junction-less blocks beside it.
    async fn store_with_a_quoted_orphan() -> (Store, i64) {
        let s = store();
        let source = make_conv(&s, "p1", "model").await;
        let quoted = s
            .insert_text_block(source, Role::Assistant, "The quick brown fox".into())
            .await
            .unwrap();
        let user_blocks = s
            .insert_user_blocks(
                source,
                vec![
                    text("quoting:"),
                    InputBlock::Quote {
                        start_block_id: quoted,
                        start_pos: 0,
                        end_block_id: quoted,
                        end_pos: 9,
                    },
                ],
            )
            .await
            .unwrap();
        let thread = s
            .fork_continuation(
                source,
                user_blocks[0],
                Continuation::NewThread {
                    system_prompt: None,
                },
                ModelOverride::default(),
            )
            .await
            .unwrap();

        // Ordinary orphans for the collector to earn its keep on: a second
        // conversation, deleted, its two blocks linked to nothing.
        let disposable = make_conv(&s, "p1", "model").await;
        s.insert_text_block(disposable, Role::User, "gone".into())
            .await
            .unwrap();
        s.insert_text_block(disposable, Role::Assistant, "also gone".into())
            .await
            .unwrap();
        s.delete_conversation(disposable).await.unwrap();

        (s, thread)
    }

    /// Collection runs to completion in a database that holds a quoted orphan,
    /// and takes only the orphans nothing points at. The quoted one used to
    /// abort the whole statement on a foreign key — after which nothing in the
    /// database was ever collected again.
    #[tokio::test]
    async fn gc_collects_the_unquoted_orphans_with_a_quoted_one_present() {
        let (s, _thread) = store_with_a_quoted_orphan().await;

        let collected = s.gc_orphan_blocks().await.unwrap();
        assert_eq!(collected, 2, "exactly the two blocks nothing points at");

        // And it stays runnable: the second pass finds nothing left to take.
        assert_eq!(s.gc_orphan_blocks().await.unwrap(), 0);
    }

    /// The other half of the same rule: the deep copy's detached quote target
    /// has no junction row ON PURPOSE, and survives collection because a quote
    /// points at it. Its text is still there to read afterwards.
    #[tokio::test]
    async fn a_detached_quote_target_survives_gc_by_design() {
        let (s, thread) = store_with_a_quoted_orphan().await;

        s.gc_orphan_blocks().await.unwrap();

        let quote = s
            .list_blocks(thread)
            .await
            .unwrap()
            .into_iter()
            .find(|b| b.block_type == "quote")
            .expect("the fork's quote is still there");
        assert_eq!(
            quote.fields["text"].as_str().unwrap(),
            "The quick",
            "the detached target was not collected out from under it"
        );
    }

    /// A new thread deep-copies a user turn holding the approval chain's two
    /// kinds. The approval blocks carry role user, which is exactly why the
    /// group walk hands them to the copy — and the copy had no way to read
    /// them, so this fork failed outright.
    ///
    /// The covered tool call is the assistant's, so it sits beside the group
    /// rather than in it; the copied request still names it.
    #[tokio::test]
    async fn new_thread_deep_copies_a_group_holding_the_approval_chain() {
        use crate::types::ApprovalChoice;

        let s = store();
        let source = make_conv(&s, "p1", "model").await;

        s.insert_user_blocks(source, vec![text("do the thing")])
            .await
            .unwrap();
        let call = s
            .insert_tool_call_block(
                source,
                Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call_1".into(),
                    name: "danger".into(),
                    input: "{}".into(),
                    interactive: true,
                },
                None,
            )
            .await
            .unwrap();
        let request = s
            .insert_approval_request_block(source, call)
            .await
            .unwrap()
            .expect("the first request writes");
        s.insert_approval_decision_block(
            source,
            request,
            ApprovalChoice::Approved,
            None,
            Some("go ahead".into()),
        )
        .await
        .unwrap();
        s.insert_text_block(source, Role::User, "carry on".into())
            .await
            .unwrap();

        let thread = s
            .fork_continuation(
                source,
                request,
                Continuation::NewThread {
                    system_prompt: Some("a prompt the consumer wrote".into()),
                },
                ModelOverride::default(),
            )
            .await
            .unwrap();

        let blocks = s.list_blocks(thread).await.unwrap();
        let types: Vec<&str> = blocks.iter().map(|b| b.block_type.as_str()).collect();
        assert_eq!(
            types,
            vec![
                "system_prompt",
                "date_marker",
                "approval_request",
                "approval_decision",
                "text"
            ],
            "the whole user turn came across, approval blocks included"
        );

        let copied_request = &blocks[2];
        let copied_decision = &blocks[3];
        assert_eq!(copied_request.role, Some(Role::User));
        assert_eq!(
            copied_request.fields["for_block_id"].as_i64().unwrap(),
            call,
            "the copy still names the call it covers"
        );
        assert_eq!(
            copied_decision.fields["for_block_id"].as_i64().unwrap(),
            copied_request.id,
            "the decision follows its request into the copy"
        );
        assert_eq!(copied_decision.fields["decision"], "approved");
        assert_eq!(copied_decision.fields["user_reason"], "go ahead");
        assert_eq!(blocks[4].fields["content"], "carry on");

        // Fresh rows, not the source's.
        for copied in &blocks {
            assert!(s.find_block(copied.id).await.unwrap().is_some());
        }
        assert_ne!(copied_request.id, request);
    }

    /// Collection survives the OTHER reference a fork can leave behind. A
    /// copied approval block names the call it covers, and that call belongs to
    /// the source conversation — so deleting the source orphans a block a
    /// living block points at, which is the quoted-orphan defect wearing a
    /// different column name. The rule is stated over every reference for
    /// exactly this reason.
    #[tokio::test]
    async fn gc_survives_an_approval_reference_left_by_a_fork() {
        use crate::types::ApprovalChoice;

        let s = store();
        let source = make_conv(&s, "p1", "model").await;
        s.insert_user_blocks(source, vec![text("do the thing")])
            .await
            .unwrap();
        let call = s
            .insert_tool_call_block(
                source,
                Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call_1".into(),
                    name: "danger".into(),
                    input: "{}".into(),
                    interactive: true,
                },
                None,
            )
            .await
            .unwrap();
        let request = s
            .insert_approval_request_block(source, call)
            .await
            .unwrap()
            .expect("the first request writes");
        s.insert_approval_decision_block(source, request, ApprovalChoice::Approved, None, None)
            .await
            .unwrap();
        s.insert_text_block(source, Role::User, "carry on".into())
            .await
            .unwrap();

        let thread = s
            .fork_continuation(
                source,
                request,
                Continuation::NewThread {
                    system_prompt: None,
                },
                ModelOverride::default(),
            )
            .await
            .unwrap();

        s.delete_conversation(source).await.unwrap();
        s.gc_orphan_blocks()
            .await
            .expect("collection runs to completion with the reference in place");

        assert_eq!(
            s.list_blocks(thread).await.unwrap().len(),
            4,
            "the new thread is intact afterwards"
        );
        assert!(
            s.find_block(call).await.unwrap().is_some(),
            "the covered call is spared while the copied request names it"
        );
    }

    /// A kind the cloner has no content mapping for says so, naming the kind.
    /// It used to report `InvalidQuery`, which describes broken SQL and sends
    /// the reader hunting through statements that are fine.
    ///
    /// The exemplar is a status block. It was the date marker until
    /// 2026-08-27, when the marker gained a clone mapping of its own —
    /// role-less as it is, a group walk reaches one on ordinary data, and an
    /// error there refused a fork nobody had done anything wrong in. A status
    /// block is role-less too but is a display record no fork has ever asked
    /// to carry, so the error shape keeps a witness.
    #[tokio::test]
    async fn an_unclonable_kind_is_named_in_the_error() {
        let s = store();
        let source = make_conv(&s, "p1", "model").await;
        let status = s
            .insert_status_block(source, "stopped".into(), None)
            .await
            .unwrap();

        let failed = s
            .fork_continuation(
                source,
                status,
                Continuation::NewThread {
                    system_prompt: None,
                },
                ModelOverride::default(),
            )
            .await;

        match failed {
            Err(StoreError::UnsupportedBlockKind {
                block_type,
                operation,
            }) => {
                assert_eq!(block_type, "status");
                assert_eq!(operation, "BlockContent::read");
            }
            other => panic!("expected an honest unsupported-kind error, got {other:?}"),
        }
    }

    /// A quote spanning several blocks reads its own conversation's ledger and
    /// nothing else. Block ids are global, so another conversation writing
    /// between the endpoints used to land inside the quoted range — text the
    /// quoting conversation cannot even see.
    #[tokio::test]
    async fn a_multi_block_quote_reads_only_its_own_conversation() {
        let s = store();
        let quoting = make_conv(&s, "p1", "model").await;
        let other = make_conv(&s, "p1", "model").await;

        let first = s
            .insert_text_block(quoting, Role::Assistant, "the beginning".into())
            .await
            .unwrap();
        s.insert_text_block(other, Role::Assistant, "INTRUDER".into())
            .await
            .unwrap();
        let middle = s
            .insert_text_block(quoting, Role::Assistant, " and the middle".into())
            .await
            .unwrap();
        s.insert_text_block(other, Role::User, "ALSO INTRUDING".into())
            .await
            .unwrap();
        let last = s
            .insert_text_block(quoting, Role::Assistant, " and the end".into())
            .await
            .unwrap();
        let _ = middle;

        s.insert_user_blocks(
            quoting,
            vec![InputBlock::Quote {
                start_block_id: first,
                start_pos: 4,
                end_block_id: last,
                end_pos: 8,
            }],
        )
        .await
        .unwrap();

        let quote = s
            .list_blocks(quoting)
            .await
            .unwrap()
            .into_iter()
            .find(|b| b.block_type == "quote")
            .unwrap();
        assert_eq!(
            quote.fields["text"].as_str().unwrap(),
            "beginning and the middle and the"
        );

        // Same answer through the single-block read path, which has no
        // conversation handed to it and has to find one.
        let alone = s.find_block(quote.id).await.unwrap().unwrap();
        assert_eq!(
            alone.fields["text"], quote.fields["text"],
            "one block or the whole list, the quote resolves the same way"
        );
    }

    /// The other shape a quote range comes in: the deep copy's targets, which
    /// hang in no conversation at all. Those are walked over detached blocks,
    /// which is why a new thread's multi-block quote still reads correctly
    /// after the source conversation is deleted and collected.
    #[tokio::test]
    async fn a_new_threads_multi_block_quote_reads_after_the_source_is_gone() {
        let s = store();
        let source = make_conv(&s, "p1", "model").await;
        let first = s
            .insert_text_block(source, Role::Assistant, "first part".into())
            .await
            .unwrap();
        let last = s
            .insert_text_block(source, Role::Assistant, " second part".into())
            .await
            .unwrap();
        let user_blocks = s
            .insert_user_blocks(
                source,
                vec![
                    text("quoting:"),
                    InputBlock::Quote {
                        start_block_id: first,
                        start_pos: 6,
                        end_block_id: last,
                        end_pos: 7,
                    },
                ],
            )
            .await
            .unwrap();

        let thread = s
            .fork_continuation(
                source,
                user_blocks[0],
                Continuation::NewThread {
                    system_prompt: None,
                },
                ModelOverride::default(),
            )
            .await
            .unwrap();

        s.delete_conversation(source).await.unwrap();
        s.gc_orphan_blocks().await.unwrap();

        let quote = s
            .list_blocks(thread)
            .await
            .unwrap()
            .into_iter()
            .find(|b| b.block_type == "quote")
            .unwrap();
        assert_eq!(quote.fields["text"].as_str().unwrap(), "part second");
    }

    /// A forked quote whose range straddles the copied group reads back whole.
    /// The deep copy splits a quote's blocks in two — the ones inside the group
    /// are cloned WITH a junction row, the ones outside it are cloned detached —
    /// so a range that starts outside the group and ends inside it covers both
    /// kinds at once. Reading only one kind returns half the quoted text.
    #[tokio::test]
    async fn a_forked_quote_spanning_group_and_detached_blocks_reads_whole() {
        let s = store();
        let source = make_conv(&s, "p1", "model").await;
        let outside = s
            .insert_text_block(source, Role::Assistant, "outer text ".into())
            .await
            .unwrap();
        // Two appends with no assistant block between them are one user group,
        // so the quote and the text block it ends on are copied together.
        let inside = s
            .insert_user_blocks(source, vec![text("inner text")])
            .await
            .unwrap()[0];
        s.insert_user_blocks(
            source,
            vec![InputBlock::Quote {
                start_block_id: outside,
                start_pos: 0,
                end_block_id: inside,
                end_pos: 10,
            }],
        )
        .await
        .unwrap();

        let quote_text = |conversation: i64| {
            let s = &s;
            async move {
                s.list_blocks(conversation)
                    .await
                    .unwrap()
                    .into_iter()
                    .find(|b| b.block_type == "quote")
                    .unwrap()
                    .fields["text"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            }
        };
        assert_eq!(quote_text(source).await, "outer text inner text");

        let thread = s
            .fork_continuation(
                source,
                inside,
                Continuation::NewThread {
                    system_prompt: None,
                },
                ModelOverride::default(),
            )
            .await
            .unwrap();

        assert_eq!(
            quote_text(thread).await,
            "outer text inner text",
            "the fork's quote covers a junctioned clone and a detached one"
        );
    }

    /// Two overlapping quotes in one group both survive the deep copy. The
    /// copy rewrites every quote's endpoints to the clones' ids, so the clones
    /// have to be written in the source's own order: cloned in the order the
    /// quotes happen to name them, the second quote's range comes out inverted
    /// and reads back empty.
    #[tokio::test]
    async fn overlapping_quotes_keep_their_ranges_through_a_fork() {
        let s = store();
        let source = make_conv(&s, "p1", "model").await;
        let mut said = Vec::new();
        for part in ["one ", "two ", "three ", "four"] {
            said.push(
                s.insert_text_block(source, Role::Assistant, part.into())
                    .await
                    .unwrap(),
            );
        }

        // The later quote reaches back before the earlier one, so first-seen
        // order and ledger order disagree.
        let group = s
            .insert_user_blocks(
                source,
                vec![
                    InputBlock::Quote {
                        start_block_id: said[2],
                        start_pos: 0,
                        end_block_id: said[3],
                        end_pos: 4,
                    },
                    InputBlock::Quote {
                        start_block_id: said[0],
                        start_pos: 0,
                        end_block_id: said[2],
                        end_pos: 5,
                    },
                ],
            )
            .await
            .unwrap();

        let quote_texts = |conversation: i64| {
            let s = &s;
            async move {
                s.list_blocks(conversation)
                    .await
                    .unwrap()
                    .into_iter()
                    .filter(|b| b.block_type == "quote")
                    .map(|b| b.fields["text"].as_str().unwrap().to_owned())
                    .collect::<Vec<_>>()
            }
        };
        assert_eq!(quote_texts(source).await, ["three four", "one two three"]);

        let thread = s
            .fork_continuation(
                source,
                group[0],
                Continuation::NewThread {
                    system_prompt: None,
                },
                ModelOverride::default(),
            )
            .await
            .unwrap();

        assert_eq!(
            quote_texts(thread).await,
            ["three four", "one two three"],
            "both remapped ranges still run forwards"
        );
    }

    /// A block is three rows, so a refused write leaves none of them. The
    /// single-system-prompt trigger fires at the junction insert — after the
    /// header row — and used to leave that header behind for good: a block
    /// linked to no conversation, carrying no content, that no query names.
    #[tokio::test]
    async fn a_rejected_system_prompt_leaves_no_residue_rows() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;
        s.insert_system_prompt(c1, "the first one".into())
            .await
            .unwrap();

        let before = row_counts(&s).await;
        assert!(
            s.insert_system_prompt(c1, "the second one".into())
                .await
                .is_err(),
            "a conversation takes one system prompt"
        );
        assert_eq!(
            row_counts(&s).await,
            before,
            "the refused write left nothing behind: no header, no junction row, no content"
        );
        assert_eq!(s.list_blocks(c1).await.unwrap().len(), 1);
    }

    /// The three row counts a block write touches.
    async fn row_counts(s: &Store) -> (i64, i64, i64) {
        s.run(|conn| {
            let count = |sql: &str| -> Result<i64, StoreError> {
                Ok(conn.query_row(sql, [], |row| row.get(0))?)
            };
            Ok((
                count("SELECT COUNT(*) FROM blocks")?,
                count("SELECT COUNT(*) FROM conversation_blocks")?,
                count("SELECT COUNT(*) FROM block_text")?,
            ))
        })
        .await
        .unwrap()
    }

    /// A header whose content row is missing is an error naming the block and
    /// its kind — for a text block, which used to come back with invented empty
    /// content, and for a quote, which used to be dropped from the history
    /// behind a log line.
    #[tokio::test]
    async fn a_missing_content_row_is_an_error_naming_the_block() {
        for kind in ["text", "quote"] {
            let s = store();
            let c1 = make_conv(&s, "p1", "model").await;
            s.insert_text_block(c1, Role::User, "a real block".into())
                .await
                .unwrap();

            let headerless = s
                .run(move |conn| {
                    conn.execute("INSERT INTO blocks (block_type) VALUES (?1)", [kind])?;
                    let id = conn.last_insert_rowid();
                    conn.execute(
                        "INSERT INTO conversation_blocks (conversation_id, block_id)
                         VALUES (?1, ?2)",
                        rusqlite::params![c1, id],
                    )?;
                    Ok(id)
                })
                .await
                .unwrap();

            match s.list_blocks(c1).await {
                Err(StoreError::MissingBlockContent {
                    block_id,
                    block_type,
                }) => {
                    assert_eq!(block_id, headerless);
                    assert_eq!(block_type, kind);
                }
                other => panic!("expected a missing-content error for {kind}, got {other:?}"),
            }
        }
    }

    /// The rewrite and the stamp are about the same block. The bound that
    /// picked the older block for the rewrite was dropped from the stamp, so
    /// one block's text changed while another block was marked as changed.
    #[tokio::test]
    async fn set_latest_block_text_stamps_the_block_it_rewrote() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;
        let older = s
            .insert_text_block(c1, Role::Assistant, "the older one".into())
            .await
            .unwrap();
        let newer = s
            .insert_text_block(c1, Role::Assistant, "the newer one".into())
            .await
            .unwrap();

        // Two blocks appended in the same millisecond share a `created_at`,
        // and then no bound can tell them apart. The days are set explicitly so
        // the test is about the bound and not about how fast the machine is.
        s.run(move |conn| {
            let stamp = |id: i64, day: &str| -> Result<(), StoreError> {
                conn.execute(
                    "UPDATE blocks SET created_at = ?1 WHERE id = ?2",
                    rusqlite::params![day, id],
                )?;
                Ok(())
            };
            stamp(older, "2026-01-01T00:00:00.000+00:00")?;
            stamp(newer, "2026-01-02T00:00:00.000+00:00")
        })
        .await
        .unwrap();
        let bound = "2026-01-01T12:00:00.000+00:00".to_string();

        s.set_latest_block_text(
            c1,
            Role::Assistant,
            "text".into(),
            "rewritten".into(),
            "the-stamp".into(),
            Some(bound),
        )
        .await
        .unwrap();

        let stamps = s
            .run(move |conn| {
                let stamp = |id: i64| -> Result<Option<String>, StoreError> {
                    Ok(conn.query_row(
                        "SELECT updated_at FROM blocks WHERE id = ?1",
                        [id],
                        |r| r.get(0),
                    )?)
                };
                Ok((stamp(older)?, stamp(newer)?))
            })
            .await
            .unwrap();

        let blocks = s.list_blocks(c1).await.unwrap();
        assert_eq!(blocks[0].fields["content"], "rewritten");
        assert_eq!(blocks[1].fields["content"], "the newer one");
        assert_eq!(
            stamps.0.as_deref(),
            Some("the-stamp"),
            "the block that was rewritten carries the stamp"
        );
        assert_eq!(
            stamps.1, None,
            "and the block that was not rewritten was not touched"
        );
    }

    #[tokio::test]
    async fn drafts_save_load_delete() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;

        assert!(s.load_draft(c1).await.unwrap().is_empty());

        s.save_draft(c1, vec![text("hello world")]).await.unwrap();

        let blocks = s.load_draft(c1).await.unwrap();
        assert_eq!(blocks.len(), 1);
        match &blocks[0] {
            DraftBlock::Text { content } => assert_eq!(content, "hello world"),
            DraftBlock::Quote { .. } => panic!("expected a text block"),
        }

        s.save_draft(c1, vec![text("updated"), text("second block")])
            .await
            .unwrap();

        let blocks = s.load_draft(c1).await.unwrap();
        assert_eq!(blocks.len(), 2);

        s.delete_draft(c1).await.unwrap();
        assert!(s.load_draft(c1).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn draft_quote_resolves_text() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;
        let b1 = s
            .insert_text_block(
                c1,
                Role::Assistant,
                "The quick brown fox jumps over the lazy dog".into(),
            )
            .await
            .unwrap();

        s.save_draft(
            c1,
            vec![
                InputBlock::Quote {
                    start_block_id: b1,
                    start_pos: 4,
                    end_block_id: b1,
                    end_pos: 19,
                },
                text("I agree with this"),
            ],
        )
        .await
        .unwrap();

        let blocks = s.load_draft(c1).await.unwrap();
        assert_eq!(blocks.len(), 2);
        match &blocks[0] {
            DraftBlock::Quote {
                text,
                start_block_id,
                ..
            } => {
                assert_eq!(*start_block_id, b1);
                assert_eq!(text, "quick brown fox");
            }
            DraftBlock::Text { .. } => panic!("expected a quote block"),
        }
    }

    #[tokio::test]
    async fn promote_draft_creates_blocks() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;
        let b1 = s
            .insert_text_block(c1, Role::Assistant, "some text to quote".into())
            .await
            .unwrap();

        s.save_draft(
            c1,
            vec![
                text("promoted text"),
                InputBlock::Quote {
                    start_block_id: b1,
                    start_pos: 0,
                    end_block_id: b1,
                    end_pos: 9,
                },
            ],
        )
        .await
        .unwrap();

        let new_ids = s.promote_draft(c1).await.unwrap();
        assert_eq!(new_ids.len(), 2);

        // The promote transaction prepends the day's date marker before the
        // promoted user blocks.
        let blocks = s.list_blocks(c1).await.unwrap();
        assert_eq!(blocks.len(), 4);
        assert_eq!(blocks[0].role, Some(Role::Assistant));
        assert_eq!(blocks[1].block_type, "date_marker");
        assert_eq!(blocks[2].role, Some(Role::User));
        assert_eq!(blocks[2].block_type, "text");
        assert_eq!(
            blocks[2].fields["content"].as_str().unwrap(),
            "promoted text"
        );
        assert_eq!(blocks[3].role, Some(Role::User));
        assert_eq!(blocks[3].block_type, "quote");
        assert_eq!(blocks[3].fields["text"].as_str().unwrap(), "some text");

        assert!(s.load_draft(c1).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn promote_draft_fails_without_draft() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;
        assert!(s.promote_draft(c1).await.is_err());
    }

    #[tokio::test]
    async fn draft_deleted_on_conversation_delete() {
        let s = store();
        let c1 = make_conv(&s, "p1", "model").await;

        s.save_draft(c1, vec![text("draft content")]).await.unwrap();
        assert_eq!(s.load_draft(c1).await.unwrap().len(), 1);

        s.delete_conversation(c1).await.unwrap();
        assert!(s.load_draft(c1).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn resolve_model_idempotent() {
        let s = store();
        let id1 = s
            .resolve_model("p1".into(), "gpt-4".into(), "GPT-4".into(), "OpenAI".into())
            .await
            .unwrap();
        let id2 = s
            .resolve_model("p1".into(), "gpt-4".into(), "GPT-4".into(), "OpenAI".into())
            .await
            .unwrap();
        assert_eq!(id1, id2);

        let id3 = s
            .resolve_model("p2".into(), "gpt-4".into(), "GPT-4".into(), "OpenAI".into())
            .await
            .unwrap();
        assert_ne!(id1, id3);
    }

    // ─── What this slice owes ────────────────────────────────────────────

    /// The ledger's whole reason to exist, end to end: append blocks to a
    /// conversation, read them back in order, advance the cursor, read it back.
    #[tokio::test]
    async fn the_ledger_round_trips_from_append_to_cursor() {
        let s = store();
        let conv = make_conv(&s, "p1", "model").await;

        let first = s
            .insert_user_blocks(conv, vec![text("what is it?")])
            .await
            .unwrap()[0];
        let second = s
            .insert_text_block(conv, Role::Assistant, "this".into())
            .await
            .unwrap();
        let third = s
            .insert_user_blocks(conv, vec![text("and then?")])
            .await
            .unwrap()[0];

        let blocks = s.list_blocks(conv).await.unwrap();
        let ids: Vec<i64> = blocks.iter().map(|b| b.id).collect();
        assert!(
            ids.windows(2).all(|w| w[0] < w[1]),
            "ledger order is the order they were appended in"
        );
        assert_eq!(
            ids.iter()
                .filter(|id| **id == first || **id == second || **id == third)
                .count(),
            3,
            "every appended block comes back"
        );
        assert_eq!(
            blocks.iter().find(|b| b.id == second).unwrap().fields["content"]
                .as_str()
                .unwrap(),
            "this"
        );

        assert_eq!(
            s.cursor(conv).await.unwrap(),
            Some(0),
            "nothing is confirmed yet"
        );
        s.update_cursor(conv, second).await.unwrap();
        assert_eq!(s.cursor(conv).await.unwrap(), Some(second));

        // The cursor survives a re-read of the conversation, because it is a
        // stored fact and not an in-memory one.
        s.update_cursor(conv, third).await.unwrap();
        assert_eq!(s.cursor(conv).await.unwrap(), Some(third));
    }

    /// The store and the scheduler's heartbeat are connected: a write to a
    /// library table reaches the change log, and a reactive loop consuming it
    /// wakes up with the change in hand.
    #[tokio::test]
    async fn a_write_wakes_a_reactive_loop_through_the_change_log() {
        let s = store();
        let conv = make_conv(&s, "p1", "model").await;

        let consumer = s.changes.consumer();
        let block_changes = Arc::new(AtomicUsize::new(0));

        let seen = Arc::clone(&block_changes);
        let loop_handle = tokio::spawn(async move {
            reactive!(consumer, change, {
                if change.table == "blocks" {
                    seen.fetch_add(1, Ordering::SeqCst);
                }
            });
        });

        // Let the loop subscribe before anything fires.
        sleep(Duration::from_millis(20)).await;
        assert_eq!(block_changes.load(Ordering::SeqCst), 0);

        s.insert_text_block(conv, Role::User, "wake up".into())
            .await
            .unwrap();

        sleep(Duration::from_millis(50)).await;
        assert!(
            block_changes.load(Ordering::SeqCst) >= 1,
            "the block insert woke the loop"
        );

        loop_handle.abort();
    }

    /// A table the hook does not name fires nothing. This is the current limit
    /// stated as a test: it is what Stage 3's descriptors exist to remove, and
    /// if that changes, this test is the one that says so.
    ///
    /// Attachments are the example because they are off the list on purpose —
    /// their rows are not ledger content. A LIBRARY TABLE that carries ledger
    /// content being off the list is the defect this pins the other side of:
    /// see the test that walks both ledgers' content tables.
    #[tokio::test]
    async fn a_change_to_an_unlisted_table_reaches_no_consumer() {
        let s = store();
        let consumer = s.changes.consumer();

        s.create_attachment(
            "a1".into(),
            None,
            "f.bin".into(),
            "application/octet-stream".into(),
            10,
            Vec::new(),
        )
        .await
        .unwrap();

        assert!(
            consumer.drain().is_empty(),
            "attachments are not on the hook's list, so nothing was announced"
        );
    }

    /// The constructor takes a database location and nothing else: no
    /// configuration directory, and no product file anywhere near it.
    #[tokio::test]
    async fn the_constructor_takes_only_a_database_location() {
        let dir = temp_dir("constructor");
        let db = dir.join("ledger.sqlite3");
        assert!(!db.exists());

        let s = Store::open(&db).unwrap();
        let conv = make_conv(&s, "p1", "model").await;
        s.insert_text_block(conv, Role::User, "on disk".into())
            .await
            .unwrap();
        assert_eq!(s.list_blocks(conv).await.unwrap().len(), 1);

        assert!(db.exists(), "the location is where the database landed");
        let strays: Vec<PathBuf> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_none_or(|n| !n.starts_with("ledger.sqlite3"))
            })
            .collect();
        assert!(
            strays.is_empty(),
            "opening a store creates nothing but its database: {strays:?}"
        );

        drop(s);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Two stores in one process do not collide: separate connections,
    /// separate actors, separate change logs.
    #[tokio::test]
    async fn two_stores_in_one_process_stay_independent() {
        let a = store();
        let b = store();

        let a_conv = make_conv(&a, "p1", "model").await;
        let b_conv = make_conv(&b, "p2", "other").await;
        assert_eq!(a_conv, b_conv, "both start their own numbering");

        let a_changes = a.changes.consumer();
        let b_changes = b.changes.consumer();
        a.insert_text_block(a_conv, Role::User, "mine".into())
            .await
            .unwrap();

        assert_eq!(a.list_blocks(a_conv).await.unwrap().len(), 1);
        assert!(
            b.list_blocks(b_conv).await.unwrap().is_empty(),
            "one store's write is invisible to the other"
        );
        assert!(
            !a_changes.drain().is_empty(),
            "the writing store's change log sees its own write"
        );
        assert!(
            b_changes.drain().is_empty(),
            "and the other store's change log stays silent — both directions"
        );
        assert_eq!(
            b.find_conversation(b_conv)
                .await
                .unwrap()
                .unwrap()
                .model
                .provider_id,
            "p2"
        );
    }

    /// A consumer's own tables share the single writer through their own
    /// domain: queries submitted before its migrations WAIT, and run once they
    /// land.
    #[tokio::test]
    async fn a_domains_queries_wait_behind_its_own_migrations() {
        let s = store();
        let tx = s.tx();

        let query_tx = s.tx();
        let mut pending = tokio::spawn(async move {
            domain_run(&query_tx, "widgets", |conn| {
                let count: i64 =
                    conn.query_row("SELECT COUNT(*) FROM widgets", [], |row| row.get(0))?;
                Ok(count)
            })
            .await
        });

        // The query is in the actor's queue and cannot run: its domain has no
        // tables yet. If it ran anyway it would fail, not park.
        assert!(
            timeout(Duration::from_millis(100), &mut pending)
                .await
                .is_err(),
            "the query waits for its domain's migrations"
        );

        domain_migrate(
            &tx,
            "widgets",
            vec!["CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL);"],
        )
        .await
        .unwrap();

        assert_eq!(
            pending.await.unwrap().unwrap(),
            0,
            "the deferred query ran once the domain was migrated"
        );

        // Re-migrating is a no-op: the domain's own version counter remembers.
        domain_migrate(
            &tx,
            "widgets",
            vec!["CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL);"],
        )
        .await
        .unwrap();

        domain_run(&tx, "widgets", |conn| {
            conn.execute("INSERT INTO widgets (name) VALUES ('one')", [])?;
            Ok(())
        })
        .await
        .unwrap();

        // The library's own schema is untouched by any of it.
        let conv = make_conv(&s, "p1", "model").await;
        assert!(s.list_blocks(conv).await.unwrap().is_empty());
    }

    /// A step that fails is rolled back whole. The batch's first statement
    /// used to survive its own batch's failure with the version left where it
    /// was — and the retry then met a table the failed attempt had created, so
    /// the domain could never migrate again.
    #[tokio::test]
    async fn a_failed_migration_leaves_no_partial_schema() {
        let s = store();

        let failed = domain_migrate(
            &s.tx(),
            "widgets",
            vec!["CREATE TABLE widgets (id INTEGER PRIMARY KEY); CREATE TABLE broken (;"],
        )
        .await;

        match failed {
            Err(StoreError::MigrationFailed {
                domain, version, ..
            }) => {
                assert_eq!(domain, "widgets");
                assert_eq!(version, 1, "the error names the step that failed");
            }
            other => panic!("expected a named migration failure, got {other:?}"),
        }

        let tables: i64 = s
            .run(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE name IN ('widgets', 'broken')",
                    [],
                    |row| row.get(0),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(tables, 0, "neither statement of the failed step survives");
    }

    /// And because nothing survived, the fix goes straight in.
    #[tokio::test]
    async fn a_corrected_migration_after_a_failure_succeeds() {
        let s = store();
        let tx = s.tx();

        assert!(
            domain_migrate(
                &tx,
                "widgets",
                vec!["CREATE TABLE widgets (id INTEGER PRIMARY KEY); CREATE TABLE broken (;"],
            )
            .await
            .is_err()
        );

        domain_migrate(
            &tx,
            "widgets",
            vec!["CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL);"],
        )
        .await
        .unwrap();

        domain_run(&tx, "widgets", |conn| {
            conn.execute("INSERT INTO widgets (name) VALUES ('one')", [])?;
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(
            domain_run(&tx, "widgets", |conn| Ok(conn.query_row(
                "SELECT COUNT(*) FROM widgets",
                [],
                |row| row.get::<_, i64>(0)
            )?))
            .await
            .unwrap(),
            1,
            "the domain works again the moment its migrations do"
        );
    }

    /// A query already waiting when the migration fails is ANSWERED with the
    /// failure. It used to be run anyway, against whatever schema the failed
    /// batch happened to leave.
    #[tokio::test]
    async fn a_query_queued_behind_a_failed_migration_gets_the_failure() {
        let s = store();

        let query_tx = s.tx();
        let mut pending = tokio::spawn(async move {
            domain_run(&query_tx, "widgets", |conn| {
                conn.execute("INSERT INTO widgets (id) VALUES (1)", [])?;
                Ok(())
            })
            .await
        });
        assert!(
            timeout(Duration::from_millis(100), &mut pending)
                .await
                .is_err(),
            "the query is parked behind the migration"
        );

        assert!(
            domain_migrate(
                &s.tx(),
                "widgets",
                vec!["CREATE TABLE widgets (id INTEGER PRIMARY KEY); CREATE TABLE broken (;"],
            )
            .await
            .is_err()
        );

        match timeout(Duration::from_secs(5), pending).await {
            Ok(Ok(Err(StoreError::MigrationFailed { domain, .. }))) => {
                assert_eq!(domain, "widgets");
            }
            other => panic!("expected the queued query to be answered, got {other:?}"),
        }
    }

    /// And a query arriving after the failure is answered too, rather than
    /// parked behind migrations that already ran and will not run again.
    #[tokio::test]
    async fn a_query_after_a_failed_migration_gets_the_failure_not_a_hang() {
        let s = store();
        assert!(
            domain_migrate(
                &s.tx(),
                "widgets",
                vec!["CREATE TABLE widgets (id INTEGER PRIMARY KEY); CREATE TABLE broken (;"],
            )
            .await
            .is_err()
        );

        let answered = timeout(
            Duration::from_secs(5),
            domain_run(&s.tx(), "widgets", |conn| {
                Ok(conn.query_row("SELECT COUNT(*) FROM widgets", [], |row| {
                    row.get::<_, i64>(0)
                })?)
            }),
        )
        .await
        .expect("the query is answered, not parked forever");

        match answered {
            Err(StoreError::MigrationFailed {
                domain, version, ..
            }) => {
                assert_eq!(domain, "widgets");
                assert_eq!(version, 1);
            }
            other => panic!("expected the migration failure, got {other:?}"),
        }
    }

    /// Every library table that carries ledger content wakes a consumer. The
    /// metadata ledger is the one this pins hardest: it is a whole second
    /// ledger, and while it was off the list nothing written to it ever woke
    /// anybody.
    #[tokio::test]
    async fn the_content_tables_of_both_ledgers_wake_a_consumer() {
        use crate::types::ApprovalChoice;

        let s = store();
        let conv = make_conv(&s, "p1", "model").await;
        let call = s
            .insert_tool_call_block(
                conv,
                Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call_1".into(),
                    name: "danger".into(),
                    input: "{}".into(),
                    interactive: true,
                },
                None,
            )
            .await
            .unwrap();

        let consumer = s.changes.consumer();
        let _ = consumer.drain();

        let request = s
            .insert_approval_request_block(conv, call)
            .await
            .unwrap()
            .expect("the first request writes");
        s.insert_approval_decision_block(conv, request, ApprovalChoice::Approved, None, None)
            .await
            .unwrap();
        s.insert_user_blocks(
            conv,
            vec![InputBlock::Quote {
                start_block_id: call,
                start_pos: 0,
                end_block_id: call,
                end_pos: 1,
            }],
        )
        .await
        .unwrap();
        s.insert_metadata(conv, "title_response", None, Some("A title"))
            .await
            .unwrap();
        // A streaming tool call writes its arguments to a content table of its
        // own, one delta at a time. Off the list, a consumer following the call
        // as it arrives would hear nothing about the arguments.
        let streaming = s
            .insert_streaming_tool_call_block(conv, Role::Assistant, "call_2".into(), "slow".into())
            .await
            .unwrap();
        s.append_to_streaming_tool_call(streaming, "{\"a\":".into(), now_iso8601())
            .await
            .unwrap();

        let announced: std::collections::HashSet<String> =
            consumer.drain().into_iter().map(|c| c.table).collect();
        for table in [
            "metadata",
            "block_quote",
            "block_date_marker",
            "block_approval_request",
            "block_approval_decision",
            "block_streaming_tool_call",
        ] {
            assert!(
                announced.contains(table),
                "a write to {table} announced nothing; announced: {announced:?}"
            );
        }
    }

    /// A rolled-back write still announces every row it touched, and this test
    /// exists to say that is the documented behaviour rather than an oversight.
    ///
    /// The hook fires per row change, not per commit. The architecture's answer
    /// is that an event is a wakeup and truth is the durable state a consumer
    /// re-reads — so the wakeup for a write that never landed costs one re-read
    /// that finds nothing, and the alternative (buffering until commit) costs
    /// wakeups that correctness depends on. Changing this is a decision, and
    /// this test is where it gets made.
    #[tokio::test]
    async fn a_rolled_back_write_still_announces_its_rows_as_documented() {
        let s = store();
        let conv = make_conv(&s, "p1", "model").await;
        let consumer = s.changes.consumer();
        let _ = consumer.drain();

        let failed = s
            .run(move |conn| {
                transact(conn, |tx| {
                    tx.execute(
                        "INSERT INTO blocks (block_type, created_at) VALUES ('text', 'never')",
                        [],
                    )?;
                    let block_id = tx.last_insert_rowid();
                    tx.execute(
                        "INSERT INTO conversation_blocks (conversation_id, block_id)
                         VALUES (?1, ?2)",
                        rusqlite::params![conv, block_id],
                    )?;
                    Err::<(), _>(StoreError::Other("rolled back".into()))
                })
            })
            .await;
        assert!(failed.is_err());

        let announced: Vec<String> = consumer.drain().into_iter().map(|c| c.table).collect();
        assert!(
            announced.contains(&"blocks".to_string()),
            "the announcement went out before the rollback, and is a prompt to \
             re-read rather than evidence of a write: {announced:?}"
        );
        assert!(
            s.list_blocks(conv).await.unwrap().is_empty(),
            "and re-reading is what finds the truth: nothing landed"
        );
    }

    /// `transact` commits on success and rolls back on failure — the whole
    /// point of handing a closure a transaction instead of a connection.
    #[tokio::test]
    async fn transact_commits_on_ok_and_rolls_back_on_err() {
        let s = store();
        let conv = make_conv(&s, "p1", "model").await;

        s.run(move |conn| {
            transact(conn, |tx| {
                tx.execute(
                    "INSERT INTO metadata (conversation_id, meta_type, content)
                     VALUES (?1, 'title_response', 'kept')",
                    rusqlite::params![conv],
                )?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let failed = s
            .run(move |conn| {
                transact(conn, |tx| {
                    tx.execute(
                        "INSERT INTO metadata (conversation_id, meta_type, content)
                         VALUES (?1, 'title_response', 'discarded')",
                        rusqlite::params![conv],
                    )?;
                    Err::<(), _>(StoreError::Other("no".into()))
                })
            })
            .await;
        assert!(failed.is_err());

        let rows = s.list_metadata_blocks(conv).await.unwrap();
        assert_eq!(rows.len(), 1, "the failed transaction left nothing behind");
        assert_eq!(rows[0].fields["content"], "kept");
    }
}
