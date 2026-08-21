# Slice 6 — ingestion and the session actor

Date: 2026-08-21. The last slice of Stage 2 in the extraction spec. Slices 1–5 are
committed.

## Scope

The runtime's top: stream ingestion (provider events to blocks, per-channel tracks,
streaming tails finalized into immutable twins), the session actor (the scheduler loop that
ticks the ratchet, the latch, provider binding, the turn lifecycle, the state broadcaster),
and the metadata worker that drives the second ledger.

Source: the read-only clone at
`the ronna-lightspeed source (github.com/xdevs23/ronna-lightspeed)`,
crate `crates/ronna-server` — `ingestion.rs` (1,231 lines, 19 tests), the actor and
scheduler parts of `turns.rs` (7 tests; the bus, the submit path and the insert seam are
already here from earlier slices), and `metadata.rs` (12 tests, of which 3 already ran ahead
in slice 3 and must not be double-counted). **Read it; never write to it.**

## The context split

The source threads a service locator through the actor carrying both library handles and
stay-behind ones. This slice performs the split the extraction spec names: the library
defines a context of exactly what it owns — store, bus, provider registry, tool registry —
and a consumer passes it in. No search registry, no session manager, no locator that knows
what a product is.

## What must hold

- **One scheduler drives a conversation's ratchet.** The state broadcaster consumes the
  outcome the scheduler publishes and never drives. The double-turn defect the source
  records — two ticks interleaving into two identical requests — stays impossible, and the
  ported test proving it comes across.
- **The latch is the engagement switch**: boot-latched, cleared by explicit intent, never
  re-latched by a normal stream end; a stream error latches. The latch short-circuit
  semantics and their tests come across.
- **Ingestion is insert-first and finalizes atomically**: a streaming tail is replaced by
  its committed twin in one transaction, the interrupt is re-checked per chunk, and an
  abandoned stream does not hold the provider transport open.
- **The cursor-confirm feedback edge decision falls due.** Slice 2 recorded that
  `update_cursor` announces a change on a table the scheduler wakes on, deferring the
  ruling to this slice. Decide it here: either the scheduler tolerates the extra no-op tick
  and a test pins it as bounded (one wake per confirm converging to rest), or the wake is
  filtered at a stated place. Record the choice and its reason where the edge lives.

## Acceptance criteria

- **AC6-1** `cargo build --workspace` and `--all-features` succeed; no external path
  dependency.
- **AC6-2** `cargo test --workspace --all-features` passes with **at least 358** tests (326
  present plus at least 32 of the 35 remaining, shortfalls named per test).
- **AC6-3** Default-feature tests still pass.
- **AC6-4** clippy (all-features and mistral-only), `fmt --check`, `doc --all-features`
  exit clean.
- **AC6-5** The vocabulary scan matches nothing; the actor's context names no stay-behind
  collaborator.
- **AC6-6** The whole runtime composes in one test over a scripted provider: a user message
  appended through the store wakes the scheduler, a turn fires, the scripted stream is
  ingested into blocks with a tool call, the runner admits and executes, the result wakes
  the next tick, and a second turn fires — asserted on the resulting ledger, block by
  block.
- **AC6-7** The double-turn regression test passes and is exercised with real interleaving,
  not sequential calls.
- **AC6-8** A latched conversation appends nothing on a tick; unlatch plus one tick
  resumes; a stream error latches; a stream end does not.
- **AC6-9** The suite passes identically under `--test-threads=1`.
- **AC6-10** Any new dependency follows the standing recording rule. The expectation is
  none.
