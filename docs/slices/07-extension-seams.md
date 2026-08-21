# Stage 3 — the extension seams

Date: 2026-08-21. The extraction spec's Stage 3, now due: Stage 2 closed with commit
`a5431c5`. Status: draft, awaiting cold review.

## What this stage is for

The library's reason to exist is that a consumer can add a block kind. Today it cannot: the
typed kind layer is a closed enum, the store's block query and change-hook list name the
library's content tables literally, and every ledger write is async on the store actor. This
stage opens those seams — with **one mechanism used identically by the library and every
consumer**, per the settled extension design in the extraction spec.

The proof the whole stage answers to is **AC13**: a test registers a kind defined outside the
library's own enum and proves it parses, loads, wakes a tick and takes a turn.

## The design, restated from the extraction spec

- **The runtime becomes generic over the behavior trait.** `BlockKind` already implements
  `Agency` by delegating through a generated dispatch; the ratchet, the walk, the frontier,
  the projection fold, the runner and the actor take `K: Agency + …` instead of naming
  `BlockKind`. The library's enum stops being privileged: it is one implementor.
- **A consumer composes**: an enum with a `Core(BlockKind)` variant plus its own kinds, and a
  **derive** generates the delegation the library maintains by hand today.
- **Parsing is an associated function** on the implementor: a composing enum tries its own
  kinds and delegates the rest inward, so only a genuinely unknown type reaches the inert
  fallback.
- **Content-table descriptors** are an associated constant contributed by the same type: the
  store builds its block query's joins and the change-hook allowlist from the descriptor
  list, so a consumer's table is loaded and wakes ticks by declaration, not by editing the
  library.
- **Rejected, still**: a trait-object registry. Two classes of kind is the shape this
  architecture exists to avoid, and object safety would cost native `async fn`.

## Slices of this stage

This is one unit of design with three mechanical waves; each wave compiles and passes the
whole suite before the next.

1. **Genericize the machinery.** Thread `K` through `AgencyCtx`, the ratchet, redispatch,
   projection, the runner, ingestion, the actor and the metadata worker. `BlockKind` is the
   default everywhere existing tests touch, so the suite is byte-for-byte the same behavior.
2. **The descriptor seam.** A `ContentDescriptor` list on the trait; the store takes the
   list at open, builds the query and the hook allowlist from it. The library's own
   descriptors produce the identical SQL the hardcoded strings produce today, proven by a
   test comparing the built statement against the previous literal.
3. **The derive and AC13.** `#[derive(Agency)]` (a new proc-macro crate in the workspace,
   `agent-ledger-derive`) generating the delegation, the parse chain and the descriptor
   concatenation for a composing enum. Then the AC13 test: a `ChatMessage` kind defined in
   the test — its own content table, its own awaiting, driven end to end through the
   composed runtime over a scripted provider.

## What does not change

- No behavior change for the library's own kinds: the full 392-test suite must pass
  unchanged at every wave.
- The sync-append seam from the extraction spec's earlier revision is NOT built: the
  closure API question dissolved when the consumer stopped being an in-process co-tenant of
  the store (the assistant owns its whole database through this library). Recorded here so
  it is a decision, not an omission.
- No runtime improvements from the deferred batch ride along.

## Acceptance criteria

- **AC7-1** Wave 1 ends with the full suite passing unchanged (392 all-features / 289
  default at current counts), clippy/fmt/doc clean, no test edited except type-parameter
  plumbing.
- **AC7-2** Wave 2's store accepts a descriptor list; the library's own list generates SQL
  identical to the previous literals, proven by test; a table added by a test descriptor is
  loaded and wakes the change log.
- **AC7-3** The derive compiles a composing enum with zero hand-written dispatch, and a
  compile-fail test pins the error for a non-exhaustive composition.
- **AC7-4 (= extraction AC13)** A test defines a `ChatMessage` kind OUTSIDE the library —
  own content table via descriptor, `awaiting` of Model for user-authored rows — registers
  it through the composing enum, and proves end to end: it parses from its stored row, loads
  through the store's descriptor-built query, its insert wakes a tick, the frontier owes a
  turn, and a scripted provider answers it.
- **AC7-5** The dependency rule covers the new proc-macro crate's dependencies (syn, quote,
  proc-macro2) before any manifest names them.
- **AC7-6** The suite stays parallel-safe and network-free; vocabulary scan clean.
- **AC7-7** The extraction spec's Stage 3 sketch names `agent_ledger::CoreKind`; decide the
  name (`BlockKind` stays or renames) once, in this stage, and record it.
