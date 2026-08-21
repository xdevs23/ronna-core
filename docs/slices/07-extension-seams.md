# Stage 3 — the extension seams

Date: 2026-08-21. Revision 2, rewritten after an unbriefed cold review prototyped revision 1
against the tree and returned two blockers and thirteen gaps. Status: settled for
implementation.

## Disposition 2026-08-21: what the review proved, and what changed

1. **Revision 1's generic plan does not compile on stable.** Both seats prototyped it: the
   drive is awaited inside spawned tasks, an AFIT future's Send-ness is unnameable from a
   bound on `K`, and return-type notation is both unstable and excluded for hooks generic
   over `E`. The satisfiable form — prototyped compiling by both seats — is hooks declared
   `fn …<E: RuntimeEvent>(…) -> impl Future<Output = …> + Send`, with implementors keeping
   native `async fn`. That is a signature change on `Agency`'s hooks, `LedgerSource`'s four,
   and a new Send obligation on every implementor, stated below as the work it is.
2. **The provider layer was omitted, and its failure mode is the silent one.** Raw blocks
   cross the dyn provider boundary and every vendor runs the fold itself, so a consumer kind
   reaching a stock vendor parses to the inert fallback and vanishes from the model request
   with no error. Ruled below: blocks stop crossing the provider boundary at all.
3. **The descriptor was a name without a shape.** The store's per-kind decode, the write
   path, forking, garbage-collection references, ephemeral teardown, the actor's second
   table allowlist, and the descriptor/migration lifecycle all needed contracts. Shaped
   below.
4. Thirteen further gaps (AC phrasing, naming, scan coverage, the vocabulary tripwire, the
   AC10 collision) are each resolved inline.

## The design, ruled

### R1 — hook signatures

`Agency` and `LedgerSource` hooks become `fn name<E: RuntimeEvent>(…) -> impl Future<Output
= …> + Send` with default bodies where they exist today. Implementors keep writing native
`async fn` in impl blocks. Every implementor's futures must be Send — inherent to a
multi-threaded runtime, now stated in the trait docs as the consumer's obligation.

### R2 — blocks never cross the provider boundary

`ProviderRequest::Stream` carries the **neutral messages**, not raw blocks. The projection
fold runs on the caller's side — the actor, generic over `K` — and the vendors consume what
the fold produced. This matches the recorded architecture ("one projection, per-provider
encoders after it"), removes the concrete-kind parse from all five vendors, keeps
`Box<dyn ProviderModule>` object-safe with no `K` anywhere in the provider layer, and makes
a consumer kind's visibility to the model a property of its own `Projection` impl. The kimi
request builder consumes neutral messages like every other vendor; its reasoning-fold
difference lives in its encoder, where vendor difference belongs.

### R3 — the descriptor, shaped

```rust
pub struct ContentDescriptor {
    /// The content table this descriptor owns.
    pub table: &'static str,
    /// The stored type strings whose content rows live in that table.
    pub kinds: &'static [&'static str],
    /// The content columns, read by name into the block's fields.
    pub columns: &'static [&'static str],
    /// Columns in OTHER tables that reference blocks(id) through this kind —
    /// the garbage collector's reference predicate extends over these.
    pub reference_columns: &'static [ColumnRef],
    /// Ephemeral kinds: deleted by the finalization that replaces them, never
    /// a cursor anchor. Joins the streaming teardown sweep.
    pub ephemeral: bool,
}
```

**The core kinds stay on their literal path, untouched.** The store's existing query,
decode, cloner, gc predicate and teardown remain byte-identical for the library's own
kinds. Consumer kinds load through a second step: header and junction rows from the
existing query, then per-descriptor batch reads decoding declared columns by name into
`fields`. One internal asymmetry, recorded as a dated decision: fidelity for the ported
path, a declared seam for the new one, with migrating the core kinds onto descriptors left
open for a later stage. Forking deep-copies a consumer row generically from its declared
columns; gc's reference predicate is the literal union plus declared references; the
ephemeral sweep is the literal union plus declared ephemerals.

### R4 — the write path

`Store::append_consumer_block(conversation_id, role, kind, fields)` — one public,
conditional, transactional three-row write driven by the descriptor (header, junction,
content row from declared columns). Core kinds keep their typed inserts. This is the seam
AC13 writes through.

### R5 — lifecycle: descriptors and migrations arrive together

`Store::open_with(path, StoreConfig { descriptors, domain_migrations })` runs the library's
migrations and the consumer's before any query is served, then **validates every
descriptor** — each named table exists, each declared column exists, no table or kind
collides with another descriptor or the core set — and fails open loudly otherwise. That
validation is the conformance kit's check three, mechanical at open. `Store::open(path)`
and `in_memory()` keep their arity as the core-only form; the extraction spec's AC10 gains
a dated amendment naming `open_with` as the configured form.

### R6 — one table list, one owner

The store exposes its effective content-table list (core plus descriptors); the change-hook
allowlist and the actor's block-watcher list both read it. The actor's hardcoded copy goes.

### R7 — the parse contract

Each leaf kind carries its stored type strings as an associated const; a composing enum's
parse tries its own kinds and delegates inward. `BlockKind::from_block` stays the core
enum's one literal site, now reading the consts. The "one place a stored string is
compared" doc becomes "one place per implementor".
Amended 2026-08-21, wave 3: `FromBlock` additionally carries a required `CLAIMED_KINDS`
const — the implementor's full claim, its own strings and its delegate's union. Broader
than this ruling's original wording, and kept deliberately: the union claim is what lets
the derive refuse a stored-string collision at compile time at every nesting depth, and it
has no default because an empty default would make every disjointness check vacuous.

### R8 — the derive

`#[derive(Agency)]` on a composing enum generates: the `Agency` delegation, the
`Projection` delegation, the parse chain, and the descriptor concatenation (a const
concatenation — prototyped feasible by the review). One derive, both traits, documented.
Error behavior is pinned by `compile_fail` doctests — on the error code where rustc
assigns one, and by a plain `compile_fail` doctest whose emitted message names the fix
where the rejection is the derive's own `compile_error!`, which rustc gives no code.
(Amended 2026-08-21: the original wording demanded codes for rejections that cannot carry
one.) Amended likewise: the ephemerality attribute is DROPPED — a kind's `durable()` is
the single source of the fact, the derive delegates it like every other hook, and
coherence with the descriptor's ephemeral flag is the conformance check's job at test
time. An attribute would have been a second declaration whose silent precedence discarded
a leaf's own answer, the exact two-sources shape this library forbids. The derive crate is `crates/agent-ledger-derive`; syn, quote and
proc-macro2 pass the dependency rule before the manifest names them; the vocabulary and
client-constructor scans are re-rooted to walk every workspace crate, so the new crate
cannot escape them.

### R9 — names and the tripwire

`BlockKind` keeps its name; the extraction spec's `CoreKind` sketch gains a dated note.
AC13's kind is `ChatMessage` with content table `block_chat_message` — singular, because
the vocabulary list bans the plural.

## Waves

1. **Signatures and genericizing.** R1, then thread `K` through `AgencyCtx`, ratchet,
   redispatch, projection, runner, ingestion, actor, metadata — and R2's provider-boundary
   move, which is what keeps the provider layer out of the generic surface. Test edits are
   expected and named (type annotations at construction sites; no generic-parameter
   defaults exist on functions in stable Rust): behavior identical, the full suite green.
2. **The store seam.** R3 through R6. The library's own kinds' SQL is untouched by
   construction; a test descriptor's table loads, wakes the change log, forks, collects
   and tears down.
3. **The derive and AC13.** R7, R8, then the proof in `crates/agent-ledger/tests/` — an
   integration test, outside the crate's `src`, seeing only the public API: a `ChatMessage`
   kind with its own table, composed through the derive, parsing from its stored row,
   loading through the descriptor path, waking a tick, owing a turn, and answered by a
   scripted provider end to end.

## Acceptance criteria

- **AC7-1** Wave 1 ends green at current counts with behavior unchanged; edits to tests
  are mechanical, enumerated in the report.
  Amended 2026-08-21 after the wave: two clauses were over-strict for a wave that moves a
  boundary, and the review correctly failed them as written. (1) "Behavior unchanged" is
  scoped: moving the last vendor onto the shared projection changes that vendor's replay
  wire where its private builder disagreed with the recorded fold policy. Three kimi wire
  changes are accepted and pinned by test: a text-only turn replays an empty reasoning
  field (the shared policy drops completed turns' reasoning, as every other vendor already
  did); streaming tails are not replayed (coherent with the interrupt teardown, which
  deletes those rows — the old fallback replayed rows that no longer survive an interrupt);
  and multi-line quotes take the shared per-line prefix. (2) "Type-annotation plumbing"
  becomes: mechanical edits enumerated, and any re-pin that records a boundary consequence
  names the traded behavior in its own doc — a trade must read as a trade, never as a
  rename.
- **AC7-2** Wave 2: the core kinds' statements are byte-identical to before (asserted by
  test against the literals); a test descriptor's table is created by its domain migration,
  validated at open, loaded, woken on, forked, collected and torn down — each pinned.
  `open_with` on a descriptor naming a missing table or colliding column fails loudly, also
  pinned.
- **AC7-3** The derive compiles a composing enum with zero hand-written dispatch, and
  `compile_fail` doctests pin the error codes for its rejection cases.
- **AC7-4 (= extraction AC13)** The integration test proves the out-of-crate kind end to
  end: parse, load, wake, owed turn, answered turn — through the public API only.
- **AC7-5** The derive crate's dependencies are recorded per the standing rule before any
  manifest names them.
- **AC7-6** Both source scans walk every workspace crate; vocabulary clean; suite
  parallel-safe and network-free.
- **AC7-7** No stock vendor parses blocks: a grep for the concrete kind parse under
  `providers/` finds only the neutral-message fold's caller-side site, and the provider
  request type carries no `Block`.
