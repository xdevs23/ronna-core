# Slice 1 — foundations: types, events, reactivity

Date: 2026-08-20. Part of Stage 2 in the extraction spec, which is the authority for
everything not stated here.

## Scope

The bottom of the dependency graph: the types every later slice names, the event vocabulary,
and the reactive primitives the scheduler ticks on. Nothing in this slice depends on the store,
the providers, or the behavior layer.

Source: a read-only clone at
`the ronna-lightspeed source (github.com/xdevs23/ronna-lightspeed)`,
crate `crates/ronna-server`, sibling crate `crates/ronna-contracts`. **Read it; never write to
it.**

## What is ported

### 1. The crate skeleton

A workspace at the repository root with one member, `crates/agent-ledger`. Edition 2024,
`resolver = "3"`, version and edition inherited from `[workspace.package]`, workspace lints
with `unsafe_code = "forbid"` and clippy `all` and `pedantic` at warn. The manifest declares
`license = "GPL-3.0-or-later"`.

### 2. Core value types

From the sibling wire crate's `types.rs` (94 lines): the composer input block, the approval
verdict, the ask vocabulary, and the stop reason.

**Strip the TypeScript binding derive.** Generating frontend bindings is a consumer concern; a
library that pulls a binding generator into every dependent's tree to serve one consumer's
frontend is the exact coupling this extraction exists to remove. Keep serde.

From `chat.rs` (166 lines): the block row, the role, and the block-content types.

**Do not port** the system prompt constant. It is a product's own words and belongs in a file
that consumer owns. **Do not port** the search-provider field on the conversation type. The
conversation type itself belongs to the store slice; take from `chat.rs` only what the ledger
needs.

### 3. The event vocabulary and the bus

From the sibling crate's `push.rs`: the event variants the runtime itself emits — blocks
changed, stream status, stream done, stream error, conversation state, and their kin.

**Do not port** the variants that announce a product's own concerns (search providers changing,
a provider dialog opening, a provider list changing). Those are a consumer's events.

From `turns.rs`, the bus itself.

**The bus becomes generic over the event type**, exactly as the block layer becomes generic over
the kind. This is the same idea applied twice and it must not become two ideas:

```rust
pub struct EventBus<E> { /* … */ }
```

The runtime emits its own `CoreEvent` values; the bus is parameterised by a consumer's event
type with `E: From<CoreEvent> + Clone + Send + 'static`, so a consumer composes:

```rust
enum AppEvent { Core(CoreEvent), SearchProvidersChanged, /* … */ }
```

There is no second class of event, no boxing, and no enum in this library that a consumer must
edit.

### 4. The reactive primitives

`reactivity.rs` (559 lines, **7 tests**) ports whole. It names no domain concept.

## Rules for this slice

- **Every ported test comes across.** Seven exist in the reactive module; all seven must run
  and pass. A test not ported is a defect, not a judgment call.
- **Ported code keeps its comments**, including the ones naming a failure that produced a rule.
  Those comments are the most valuable thing in the source, and a move that strips them is a
  loss the compiler cannot see.
- **A comment naming the source product is rewritten**, not deleted: state the same constraint
  without the product's name.
- Public items get doc comments. Anything a later slice does not need stays private.

## Acceptance criteria

- **AC1-1** `nix develop -c cargo build --workspace` succeeds, and no manifest names a path
  outside this repository.
- **AC1-2** `nix develop -c cargo test --workspace` passes and runs **at least 7** tests: the
  seven ported reactive tests, plus any written for the generic bus.
- **AC1-3** `nix develop -c cargo clippy --workspace --all-targets -- -D warnings` exits zero.
- **AC1-4** `nix develop -c cargo fmt --check` exits zero.
- **AC1-5** No file under `crates/` matches `docs/forbidden-vocabulary.txt`, whole word, case
  insensitive.
- **AC1-6** No dependency on a TypeScript binding generator appears in any manifest.
- **AC1-7** A test constructs an `EventBus` parameterised by an event type **defined in the
  test**, sends a core event through it, and receives it as the consumer's own type. This is
  the slice's proof that the generic seam works, and it is the smaller sibling of the
  extraction spec's AC13.
- **AC1-8** The workspace manifest declares `license = "GPL-3.0-or-later"`.
- **AC1-9** Every public item carries a doc comment; `nix develop -c cargo doc` emits no
  warnings.
