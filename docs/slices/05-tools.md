# Slice 5 — the tool layer

Date: 2026-08-21. Part of Stage 2 in the extraction spec. Slices 1–4 are committed.

## Scope

The tool registry and its handler trait, the runner that owns admission, and the approval
flow's execution side: the wakeup consumer, the recorded-facts advance, the gate evaluation,
and the submit path.

Source: the read-only clone at
`the ronna-lightspeed source (github.com/xdevs23/ronna-lightspeed)`,
crate `crates/ronna-server`, directory `src/tools`. **Read it; never write to it.**

## What does not come

The two concrete product tools — the sandboxed code executor and the manual reader — and
their 13 tests. They depend on the sandbox runtime, the proxy and the language-server client,
all stay-behind, and they are the source application's own capabilities, not the runtime's.
The registry they register into comes; they do not.

**16 tests move** (the admission suite and the registry's own).

## What must hold

- **The runner is the single admission owner.** It re-reads the ledger and advances a call
  from recorded facts only; the phantom-execution shape — a decision reconstructed from the
  absence of a block while the real answer travels in memory — must stay impossible, and the
  ported tests that pin it must come across.
- **A refused call can never execute; an approved deferred call executes exactly once per
  resolution.** The ported gating tests are the proof.
- **Insert-first, then act**: a block is in the ledger before any hook runs on it. The
  interactive stamp is read at insert, from the registry, at the one seam with registry
  access.
- The runner composes with slices 2–3: calls recorded through the store, driven by the
  ratchet, admitted by the runner, resolved by result blocks the frontier then reads.

## Acceptance criteria

- **AC5-1** `cargo build --workspace` and `--all-features` succeed; no external path
  dependency.
- **AC5-2** `cargo test --workspace --all-features` passes with **at least 308** tests (293
  present plus at least 15 of the 16, any shortfall named with its reason).
- **AC5-3** Default-feature tests still pass; the tool layer does not depend on any vendor.
- **AC5-4** clippy (all-features and mistral-only), `fmt --check` and `doc --all-features`
  exit clean.
- **AC5-5** The vocabulary scan over `crates/` matches nothing.
- **AC5-6** An end-to-end test composes slices 2–5: a tool call block inserted through the
  store, the ratchet parking on it, the runner admitting and executing a test tool, the
  result block landing, and the frontier coming back model-owed.
- **AC5-7** A gating test proves the refused path end to end: gate refuses, the error is
  recorded atomically with the response, the body never runs, the cursor advances.
- **AC5-8** The suite passes identically under `--test-threads=1`.
- **AC5-9** Any new dependency follows the standing recording rule. The expectation is none.
