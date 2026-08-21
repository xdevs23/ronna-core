# Slice 3 — the behavior layer

Date: 2026-08-21. Part of Stage 2 in the extraction spec, which is the authority for anything
not stated here. Slices 1 and 2 are committed; this builds on both.

## Scope

The layer that makes a block an actor: the behavior trait with its inert defaults, the typed
kind enum and its dispatch, one module per kind, the ratchet (the cursor drive and the
frontier decision), the redispatch walk, and the projection seam.

Source: the read-only clone at
`the ronna-lightspeed source (github.com/xdevs23/ronna-lightspeed)`,
crate `crates/ronna-server`, directory `src/agency`. **Read it; never write to it.** 2,500
lines, **47 tests** across `tests.rs`, `ratchet_tests.rs` and per-kind modules.

## What does not come

The three kinds belonging to the source application's own features (tool activity, search
result, search complete) stay behind, per the extraction spec. Their enum variants, modules
and dispatch arms are dropped. Tests asserting on their type literals are **adapted, not
dropped**: where such a test pins generic machinery (an inert record's agency, the frontier
reading an inert tail), the assertion moves onto a library kind that has the same shape, and
the adaptation is named in the report. A test whose whole subject is a stay-behind kind is
dropped by name with that reason.

The `Unknown` fallback stays: a stored kind the enum does not know parses to it and is inert
in every direction. Its doc states plainly that this is where an unregistered consumer kind
lands today, and that Stage 3's extension mechanism is what replaces silence with
registration.

## What must hold

- **The machinery never branches on block kind.** The ratchet and the walk drive hooks
  blindly; per-kind behavior lives in per-kind modules; the dispatch site is generated, with
  zero logic in it. This is the library's first invariant and this slice is where it either
  holds or does not.
- **The frontier decision stays a forward read at the terminus.** No backward scan. The
  parallel-call stall that forced this rule is pinned by ported tests; they must come across.
- **The cursor's crash contract survives the port**: effects commit before the cursor
  persists, re-drive is inclusive, and every `run()` is idempotent against durable state.
- The metadata ledger drives through the same machinery, proving the ratchet ledger-blind.

## Wiring corrections from slice 2

Slice 2 severed `crate::agency::denial_error_text` from the approvals tests; this slice
restores the real function and rewires whatever the severance stubbed. The report names every
such reconnection.

## Acceptance criteria

- **AC3-1** `nix develop -c cargo build --workspace` succeeds; no external path dependency.
- **AC3-2** `nix develop -c cargo test --workspace` passes with **at least 134** tests: the 92
  present plus at least 42 from this slice (47 minus, at most, the named stay-behind-subject
  drops).
- **AC3-3** clippy `--all-targets -- -D warnings`, `fmt --check` and `doc` all exit clean.
- **AC3-4** The vocabulary scan over `crates/` matches nothing.
- **AC3-5** The parallel-call stall regression test is present and passes: three calls,
  results arriving staggered later-first, no turn until the last result, then exactly one.
- **AC3-6** A test drives the ratchet over a ledger in the store from slice 2 — not a
  hand-built vector — proving the two slices actually compose: append through the store,
  drive, park on an unresolved call, resolve it, drive to a model-owed frontier.
- **AC3-7** The metadata ledger's ratchet test runs against the same generic drive as the
  conversation ledger's, with no metadata-specific branch in the machinery.
- **AC3-8** `grep -rn 'block_type ==' crates/agent-ledger/src/agency` and equivalent literal
  kind comparisons find nothing outside the parse site and the dispatch macro.
- **AC3-9** The suite passes identically under the default parallel runner and
  `--test-threads=1`.
- **AC3-10** Any new dependency is recorded in `docs/dependency-review.md` per the standing
  rule. The expectation is none.
