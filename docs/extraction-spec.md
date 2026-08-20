# Spec: extracting the ledger runtime into this library

Date: 2026-08-20. Revision 3. Revision 1 was rewritten after an unbriefed review probed it
against the source tree and returned three blockers; revision 3 corrects the sequencing, which
had assumed the source application would be modified. Status: draft.

## Context

A working application ("the source application") contains a complete event-sourced agent
runtime: a block ledger on SQLite, a cursor-and-frontier orchestrator, a provider layer, tool
admission with an approval chain, and 368 tests. A second project needs the same runtime.
Copying it forks it; depending on the whole application drags in source-control integration, a
language-server client, a sandbox, a proxy and a web surface that nothing here wants.

**Revision 1 called this a move. It is not.** The runtime as written is closed to out-of-crate
extension in three places, and each has to be opened before anything can be extracted. That is
the real work; the file moves are the easy part afterwards.

## Disposition 2026-08-20: what the review found, and what changed

Recorded here instead of in a changelog because these are the facts the plan now rests on.

1. **A consumer cannot add a block kind.** The typed layer is a closed enum documented
   "in-crate consumers only"; an unregistered type parses to an inert `Unknown` variant,
   silently. Revision 1's Stage 3 and its conformance kit both assumed a registry that does
   not exist.
2. **Persistence is not kind-blind.** The block query is one hardcoded statement joining every
   content table by name, and the change hook that wakes the scheduler is a hardcoded table
   allowlist. A consumer's block can neither be loaded nor wake a tick.
3. **The closure API cannot do what revision 1 claimed.** It is synchronous inside the
   closure while every ledger write is an async method dispatched to the same actor thread, so
   a consumer cannot append a block inside its own closure. Revision 1's shared-transaction
   decision named a capability that does not exist — and justified rejecting an alternative by
   an atomicity property the fork and approval paths do not currently implement either.
4. **Licensing.** The source is GPL-3.0-or-later. Revision 1 declared MPL-2.0 without a
   relicensing step. **Decided: the library stays GPL-3.0-or-later.** The maintainer's stated
   need is their own projects, so the adoption argument for weaker copyleft does not apply, and
   keeping the original terms removes the relicensing step and its consent record entirely.
5. **Four modules were unclassified** and one whole sibling crate was unnamed. Nine
   dependencies point from the "moves" list into the "stays" list. Two entries were
   misclassified outright.
6. **The conformance kit's checks had no referent.** Three of the four named checks come from
   the *other* implementation's vocabulary and do not exist in this source at all.

## The extension design

One mechanism, used identically by the library and by every consumer. There is no privileged
set of kinds.

The typed layer already implements the behavior trait **by delegating to its variants** through
a hand-written dispatch macro. So it is not "the enum" — it is *a type that implements the
trait by delegating*. The runtime becomes generic over that trait, and the library's own enum
stops being special: it is one implementor among others.

A consumer composes:

```rust
enum AssistantKind {
    Core(agent_ledger::CoreKind),
    ChatMessage(ChatMessage),
}
```

and derives the trait. The derive generates exactly the delegation the source maintains by
hand today.

**Rejected — a trait-object registry with an extension variant.** It creates two classes of
kind: the library's, statically dispatched and compiler-checked, and the consumer's, boxed and
unchecked. Two kinds of the same thing is the shape this architecture exists to avoid. It also
costs native `async fn` in the trait — a trait with async methods is not object-safe, so every
hook would return a boxed future, which the source deliberately avoided.

**What the generic form keeps:** native `async fn` with no boxing; compiler-enforced
completeness, now for a consumer's kinds as well as the library's; and static dispatch.

**What it costs, stated plainly:** a type parameter travels through the runtime API, and the
source application's own code is updated to name it. Both are mechanical.

Two associated items carry the parts that are not behavior:

- **Parsing** — a stored row becomes a typed kind. A composing enum tries its own variants and
  delegates anything unrecognised down to the inner implementor, so the library's kinds resolve
  through the library and only a genuinely unknown type reaches the inert fallback.
- **Content-table descriptors** — a static list contributed by the same type, from which the
  store builds its load query and its change-hook allowlist. This is what makes persistence
  kind-blind, and it closes finding 2 with the same static mechanism, not a second
  dynamic one.

## Sequencing: extract first, then open the seams

Revision 2 put the seam work in the source repository, to keep its passing suite as the safety
net. **Superseded 2026-08-20:** the source application is a separate project and is not
modified. It is read, never written.

The reason revision 2 gave for going there first does not survive the correction. The tests
live in the same files as the code they cover, so all 368 travel with the extraction. The
safety net comes along, and the seam work can happen here with the moved tests holding it
honest.

Two consequences, both accepted:

- This library becomes a **divergent copy** of the source's runtime. Improvements made here do
  not reach the source application unless it later adopts the library.
- "The source's suite passes unchanged" is no longer available as a check. **The moved tests
  passing is the check**, which is why no slice may drop one silently: a moved test that is
  deleted rather than ported is a defect, and the count is an acceptance criterion.

| Stage | What happens | Needs |
|---|---|---|
| **1** | The equivalence baseline is captured from a read-only clone of the source. | A clone |
| **2** | The modules move here, in dependency order, one reviewed slice at a time. The library builds standing alone and the moved tests pass. | Stage 1 |
| **3** | The seams open here: generic over the behavior trait, the derive, descriptor-driven persistence, a synchronous append path usable inside the closure API. AC13 proves an out-of-library kind works. | Stage 2 |
| **4** | The second project builds on the library: an authored chat-message kind, its adapters, the agreed access model. | Stage 3 |

**Nothing is blocked.** A read-only clone is all Stage 1 needs.

**Slices for Stage 2**, bottom of the dependency graph upward. Each compiles and passes its
moved tests before the next begins:

1. Crate skeleton, block and role types, the event vocabulary, the reactive primitives.
2. The store subset.
3. The behavior layer and its ratchet, minus the three consumer kinds.
4. The provider layer, with its cross-boundary imports severed.
5. Tool registry, runner and admission, minus the two product tools.
6. Stream ingestion, the session actor, the metadata ledger.

The identified runtime improvements land after Stage 3, so the equivalence baseline still means
something while the move is being proven.

## The test inventory, measured

Run against a clean clone: **359 tests, 354 passing.** The five failures are all in the
sandbox runtime — a stay-behind module whose subprocess is absent here — so the moving set is
entirely green at the baseline.

Per module, with what each contributes to the move:

| Module | Tests | Moves | Note |
|---|---|---|---|
| `llm` | 121 | 118 | Minus the subprocess provider's 3 |
| `store` | 49 | 49 | |
| `agency` | 47 | 47 | Some assert on consumer kinds and are ported, not dropped |
| `tools` | 29 | 16 | The admission suite; the two product tools take 13 with them |
| `ingestion` | 19 | 19 | |
| `metadata` | 12 | 12 | |
| `turns` | 7 | 7 | |
| `reactivity` | 7 | 7 | |
| **Total moving** | | **275** | |
| Stays | 84 | — | server, sandbox runtime, actions, project, session, skills, ambient context, platform, the subsystem bus, source control, the agent layer, and the two product tools |

Of the moving 275, **86 sit in vendor provider modules** that ship feature-gated, so a
default-feature run exercises 189 of them. That is why AC2 names `--all-features`.

`file_store.rs` carries no tests of its own. Anything it needs proven gets a test written
during its slice, and that is stated rather than left as an accidental gap.

## Inventory

Line counts verified against the source tree.

### Moves, cleanly

| Module | Lines | Note |
|---|---|---|
| `agency/` | 2500 | Minus three domain kinds — see splits |
| `turns.rs` | 1208 | Minus hardwired search construction |
| `ingestion.rs` | 1231 | |
| `reactivity.rs` | 559 | |
| `metadata.rs`, `metadata/` | 1008 | The second ledger, proving the machinery is ledger-blind |
| `file_store.rs` | 509 | Carries `store/attachments.rs` (247) with it |
| store subset | 4833 | Named below, including the migration mechanism |

Store files that move: the connection and its closure API; block persistence (note
`store/messages.rs` persists **blocks**, not messages — the architecture has no message row);
block content; conversation rows and the cursor; approvals; call records; the fork cloner; the
date marker; metadata tables; drafts; attachments; provider and model config; and the migration
mechanism.

`store/agent_kv.rs` (87) has no caller outside the store and is **dropped, not moved**, unless a
consumer claims it.

### Stays with the source application

`api.rs`, `http.rs`, `server/`, `bin/`, `bootstrap.rs`; `runtime/` (language server, tool
protocol, proxy, sparse files, web); source-control integration, project handling, skills, the
sandbox, search, actions, session handling, ambient context, the agent layer, its internal
subsystem bus, platform glue; and the store tables for projects, sessions, activities and code
executions.

### Splits and repairs — each is work, not a move

| Item | What is wrong | Resolution |
|---|---|---|
| `chat.rs` (166) | Contains the product's system prompt string, a `Conversation` with a search-provider field, and a re-export from the frontend wire crate | Types move; the prompt moves to **its own file in the consuming project** (prompts are data, editable without a recompile); the search field and the re-export are severed |
| `tools/` (1769) | 715 lines are two concrete product tools depending on the sandbox, proxy and language-server client — the exact bloat this extraction exists to avoid — and 13 of its 29 tests are theirs | Registry, runner and admission move; the two tools and their tests stay |
| `agency/` | Carries three search-specific kinds, and its tests assert on their type literals | Those kinds and their assertions move to the source application as consumer kinds — the first proof the extension mechanism works |
| `app_context.rs` (126) | A service locator threaded through every moved module, bundling library handles with stay-behind ones | Split: the library defines a context of only what it owns; each consumer keeps its own and passes the library's part down. A second seam as large as the store's |
| The sibling wire crate | The moved core re-exports its event and input types, and the ask-state enum lives there | Those types move here. The frontend-binding generation stays with the source application, re-exporting from here |
| `llm/` (11497) | Five provider modules import the application's HTTP helpers; the provider trait returns a search-provider handle in its public signature; the subprocess provider needs a tool-protocol server | HTTP helpers move; the search handle becomes a consumer-supplied capability the trait does not name; the subprocess provider **stays with the source application** as a consumer-registered provider |
| `store/migrations.rs` (902) | One flat array under one pragma counter, whose individual steps create framework and product tables in single indivisible statements | Cannot be split by partitioning. Rewrite as a library-owned sequence plus a consumer-owned one, with a stated upgrade path for an existing installed database |
| `store/mod.rs` | The constructor runs a product-specific import on open | The library constructor takes a database path and nothing else |

### Not classified, deliberately

`deferred.rs` (74) and `lattice/` (262) were unclassified in revision 1. They are read and
dispositioned before Stage 2, not assumed.

## Proving the move changed nothing

An **equivalence baseline** captured at Stage 1: representative ledgers run through the
source's projection path, recorded as canonical bytes, and reproduced byte-for-byte by the
library afterwards. Canonical bytes are chosen so any value or type difference fails.

Scope correction from revision 1: the baseline covers the **projection**, which moves. It does
not cover the serving path, which stays — a library with no serving surface cannot reproduce
serving bytes.

The corpus covers: an empty session; a streaming tail with a parked cursor; a fork and a fork
of a fork; parallel calls whose results arrive out of order; an approval parked then approved;
an approval denied; a tail at every ask state; and a session carrying every registered kind.
Fixtures are generated by a committed script, are **scrubbed of credentials** — the same
database holds provider API keys — and are regenerated by rerunning that script.

## The conformance kit

Tests a consumer runs against **its own** kinds and tools. Revision 1 listed four checks; three
were vocabulary from a different implementation and are struck. What can be checked against
this source:

1. Every registered kind parses from its stored form, and no kind silently resolves to the
   inert fallback.
2. Every kind that reports not-done has an out-of-band completion path — asserted as the
   documented starvation contract, so it is explicit and not a surprise.
3. Every content-table descriptor names a table the migrations actually create.
4. Every tool carrying result-side behavior stays registered under its recorded name.

Checks that would require a runtime improvement not yet built are **not** in the kit. They join
it with the improvement.

## Acceptance criteria

Stage 2 unless stated.

- **AC1** The library builds with no dependency on the source application and no path reference
  outside this repository.
- **AC2** `cargo test --workspace --all-features` passes and runs at least **275** test
  functions — the measured moving set. A default-feature run exercises 189 of them, so it is
  not the check.
- **AC3** `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo fmt --check` exit zero.
- **AC4** No file under `src/` matches the vocabulary list in `docs/forbidden-vocabulary.txt`,
  which is committed with this spec. AC4 is unfalsifiable without it.
- **AC5** The equivalence fixtures are committed, contain no credential, and a test asserts the
  library reproduces every recorded projection byte-for-byte.
- **AC6** The conformance kit is a public test entry point, and a test runs it against the
  library's own kinds with every check passing.
- **AC7** The suite starts no external service and opens no socket: the store opens in memory,
  and a test asserts an outbound connection attempt fails.
- **AC8** The suite passes under the default parallel runner and again single-threaded, with
  the same result.
- **AC9** `cargo doc --all-features` produces no warnings, and every public item has a doc
  comment.
- **AC10** The library constructor takes a database path and nothing else. It selects no
  provider, no model and no directory.
- **AC11** The manifest declares GPL-3.0-or-later, the license file is present, and no moved
  file carries a conflicting header.
- **AC12** *(Stage 2)* No moved test is dropped. Each slice's commit states the test count it
  brought and the running total against the 275 measured, and any test deliberately not ported
  is named with its reason.
- **AC13** *(Stage 3)* A test registers a kind defined **outside** the library's own enum and
  proves it parses, loads, wakes a tick and takes a turn. This is the check the whole extension
  design exists for; without it, nothing else here is proven.

## Open questions

1. **Which providers ship enabled by default.** Recommendation: none — every consumer names
   what it uses.
2. **The existing-database upgrade path** across the migration split. Since this library
   starts with no installed base, the question is whether it needs one at all before its first
   consumer ships.
