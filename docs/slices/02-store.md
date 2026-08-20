# Slice 2 — the store

Date: 2026-08-21. Part of Stage 2 in the extraction spec, which is the authority for anything
not stated here. Slice 1 is committed; this builds on it.

## Scope

Persistence for the ledger: the connection and its single-writer discipline, block rows and
their content tables, conversation rows and the cursor, the approval chain's tables, call
records, the fork cloner, the date marker, the metadata tables, drafts, attachments, provider
and model configuration, and the migration mechanism.

Source: the read-only clone at
`the ronna-lightspeed source (github.com/xdevs23/ronna-lightspeed)`,
crate `crates/ronna-server`, directory `src/store`. **Read it; never write to it.**

| File | Lines | Tests |
|---|---|---|
| `mod.rs` | 837 | 22 |
| `messages.rs` — persists **blocks**, not messages | 861 | 4 |
| `migrations.rs` | 902 | 13 |
| `conversations.rs` | 674 | 0 |
| `blocks.rs` | 405 | 0 |
| `approvals.rs` | 322 | 5 |
| `attachments.rs` | 247 | 0 |
| `drafts.rs` | 219 | 0 |
| `block_content.rs` | 194 | 0 |
| `date_markers.rs` | 148 | 4 |
| `metadata.rs` | 141 | 0 |
| `models.rs` | 107 | 0 |
| `tool_calls.rs` | 79 | 0 |
| `block_cloner.rs` | 78 | 0 |
| `providers.rs` | 60 | 0 |

**48 tests.** Every one comes across. A test not ported is a defect, not a judgment call; if
one genuinely cannot come, name it and its reason in the report.

## The repairs this slice owes

The source's store is closed to any consumer, in four specific ways. Each is repaired **to the
extent that removes what stays behind** — and no further. Making persistence extensible is
Stage 3 and is deliberately not attempted here.

### 1. The block query names tables that stay behind

`blocks.rs` builds one hardcoded statement joining every content table by name, including
three that belong to the source application's own features and do not exist here.

Repair: drop the stay-behind joins. The statement stays hardcoded over the library's own
content tables. Leave a comment stating plainly that this list is the reason a consumer cannot
yet add a kind, and that Stage 3 replaces it with descriptors. **Do not build the descriptor
mechanism.**

### 2. The change hook names tables that stay behind

`mod.rs` feeds the reactive change log from a hardcoded table allowlist, which likewise names
stay-behind tables — and one that exists nowhere in the source at all, an entry already dead
before the move.

Repair: reduce it to the library's own tables. Drop the dead entry. Same comment about Stage 3.

### 3. The migrations mix two projects in single indivisible steps

`migrations.rs` is one ordered array under one schema-version counter. Individual steps create
library tables and product tables in the same statement, so the array cannot be split by
partitioning it.

Repair: rewrite the sequence as the library's own, containing only the library's tables. This
library has no installed base, so there is **no upgrade path to preserve** and none is to be
invented — say so in the module, because the next reader will wonder.

The source's *second* mechanism, a per-domain version table beside the core counter, is the
seam a consumer will use for its own tables. Port it and document it as that.

### 4. The constructor runs a product import

The source's open path takes a configuration directory and imports a product-specific file
into a product table.

Repair: the constructor takes a database location and nothing else, per the extraction spec's
AC10. The import goes with the product.

Also sever: two imports from modules that stay behind (session types and tracked projects).

## Rules for this slice

- **Ported comments come across**, especially those naming a failure that produced a rule.
  Rewrite a comment naming the source product; never delete it.
- **The store opens in memory for tests**, and the suite reaches nothing over the network and
  starts no service. This is already true in the source and must stay true.
- Public items get doc comments. Anything a later slice does not need stays private.
- **Do not port** the tables for projects, sessions, activities or code executions.

## Acceptance criteria

- **AC2-1** `nix develop -c cargo build --workspace` succeeds; no manifest names a path outside
  this repository.
- **AC2-2** `nix develop -c cargo test --workspace` passes and runs **at least 69** tests — the
  21 already here plus the 48 ported.
- **AC2-3** `nix develop -c cargo clippy --workspace --all-targets -- -D warnings` exits zero.
- **AC2-4** `nix develop -c cargo fmt --check` exits zero.
- **AC2-5** No file under `crates/` matches `docs/forbidden-vocabulary.txt`, whole word, case
  insensitive. The stay-behind table names are on that list.
- **AC2-6** `nix develop -c cargo doc` emits no warnings, and every public item has a doc
  comment.
- **AC2-7** A test opens a store, appends blocks to a conversation, reads them back in order,
  advances the cursor and reads it back — the ledger's whole reason to exist, proven end to
  end in one test.
- **AC2-8** A test proves the change hook fires for a library table and drives the reactive
  change log from slice 1, so the store and the scheduler's heartbeat are connected.
- **AC2-9** The constructor takes a database location and nothing else. A test constructs a
  store with no configuration directory and no product file present.
- **AC2-10** The suite passes under the default parallel runner and again single-threaded, with
  the same result. Two stores in one process must not collide.
- **AC2-11** Every new dependency, if any, is recorded in `docs/dependency-review.md` with its
  resolved version, the current version at the time of checking, and an advisory-database
  result. A dependency added without that record fails this criterion.
