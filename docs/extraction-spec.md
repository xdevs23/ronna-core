# Spec: extracting the ledger runtime into this library

Date: 2026-08-20. Status: draft, awaiting review.

## Context

A working application ("the source application") contains a complete event-sourced agent
runtime: a block ledger on SQLite, a cursor-and-frontier orchestrator, a provider layer, tool
admission with an approval chain, and roughly 354 tests. A second project now needs the same
runtime. Copying it would fork it; depending on the whole application would drag in
source-control integration, a language-server client, a sandbox, a proxy and a web surface
that the second project has no use for.

This library is the extraction. The source application becomes its first consumer; the second
project becomes its second.

**Why extract before either consumer needs a change**: two independent implementations of this
architecture already exist, and their intersection *is* the boundary. That is better evidence
than one consumer could give, and it arrived before any new code was written. Five improvements
to the runtime are already identified; making them once in a shared library beats making them
twice.

## The boundary rule

A module belongs here if it would be identical in an agent that knew nothing about the source
application's domain. A module stays behind if it names, or exists because of, something the
product does.

Applied to the source tree, that gives three lists. Sizes are the source's line counts, for
scale only.

### Moves here

| Module | Lines | What it is |
|---|---|---|
| `agency/` | 2500 | The block behavior model: the trait, the cursor walk, the frontier decision, the redispatch walk, per-kind behavior, the projection seam, and their tests |
| `chat.rs` | 166 | The block and role types |
| `turns.rs` | 1208 | The session actor, the scheduler tick, the latch, provider binding |
| `ingestion.rs` | 1231 | Stream events to blocks: per-channel tracks, streaming tails, finalization into immutable twins |
| `reactivity.rs` | 559 | The signal primitives the scheduler ticks on |
| `tools/` | 1769 | The tool registry, the runner, admission and the approval flow, and their tests |
| `llm/` | 11497 | The provider layer and the concrete provider modules |
| `metadata.rs`, `metadata/` | 1008 | The second ledger, proving the machinery is ledger-blind |
| `file_store.rs` | 509 | Attachment and artifact storage |
| store subset | ~3900 | See below |

From the store: the connection, its write-ahead mode and the closure API; block persistence;
conversation rows and the cursor; the approval tables; call records; the fork cloner; the date
marker; the metadata tables; drafts; the migration *mechanism*; and provider and model config.

One name misleads and must not be trusted: `store/messages.rs` persists **blocks**, not
messages. The architecture has no message row. It moves.

### Stays with the source application

`api.rs`, `http.rs`, `server/`, `bin/`, `bootstrap.rs`; `runtime/` (language server, tool
protocol, proxy, web); source-control integration, project handling, skills, the sandbox,
search, actions, session handling, ambient context, the agent layer, the internal message
bus for its own subsystems, platform glue; and the store tables for projects, activities and
code executions.

### Splits, and each split is a design question

| Thing | The question |
|---|---|
| The `Store` struct | It is one struct with methods for every table. The library needs its own; the consumer needs to add tables. See "the store seam" below. |
| Migrations | The mechanism moves; the source application's own migrations stay. Two version counters, by scope. |
| The composition root | The library must not own it. It exposes constructors; each consumer wires them. |
| Provider selection | Reading which model a conversation uses is the library's; deciding *which* providers are configured is the consumer's. |

## The store seam

The single hardest question in the extraction, and the one most likely to be got wrong.

The library owns the connection, the write-ahead mode, the single-writer discipline and its
own schema. A consumer needs tables of its own **in the same database and the same
transaction**, or a write spanning both is not atomic.

**Decision:** the library exposes a closure-scoped connection API. A consumer runs its own
statements inside the same scope, so a consumer write and a ledger append share one
transaction. The library never learns what those tables are.

**Schema versions are two counters by scope**: a linear one the library owns and steps
forward, and a per-consumer one each consumer owns. A single counter would force a library
version bump every time a consumer changed its own storage, coupling projects that must stay
independent.

**Rejected — a repository trait the consumer implements.** It inverts the dependency for no
gain: the library would then have to define an interface wide enough for every access pattern
it has, which is the whole store. The consumer would implement it once, identically, forever.

**Rejected — separate databases per consumer.** It gives up cross-table atomicity, which the
approval chain and the fork path both need.

## What changes during the move

Nothing about behavior. The changes are the ones a move forces:

1. **Domain imports are severed.** Three shared test fixtures import the source application's
   own tools and workflow types. Each becomes a **test double the consuming project
   registers**, never a library import.
2. **Concrete providers are feature-gated**, defaulting off. A consumer enables the vendors it
   uses. The provider trait, the neutral message vocabulary and the streaming contract are
   always present.
3. **The composition root is inverted.** Where the source application constructs the world in
   one place, the library exposes the pieces and each consumer assembles them.
4. **Public interface is deliberate.** Anything not needed by a consumer stays private. It is
   cheaper to publish a type later than to withdraw one.

## Proving the move did not change behavior

**An equivalence baseline, captured before anything moves.** Representative ledgers are run
through the source application's own projection and serving paths, and the outputs are recorded
as canonical bytes. After extraction, the library must reproduce them **byte-identically**.
Equality on canonical bytes is chosen so that any value or type difference fails, including a
number silently changing type.

The baseline covers, at minimum: an empty session; a streaming tail with a parked cursor; a
fork and a fork of a fork; a dangling call with results arriving out of order; an approval
parked and then approved; an approval denied; a tail at **every** ask state; and a session
carrying every registered block kind.

This is the only test shape that survives a rewrite of the thing it tests, and an extraction is
exactly that. It means neither consumer has to review the move by eye.

## The conformance kit

The library ships tests a consumer runs **against its own block kinds and tools**. The moment a
consumer registers its own kinds, the library's preconditions stop being checkable by the
library, and every one of them is a prose rule a permissive test double can violate with
nothing going red.

Four checks, run over the consumer's registry:

1. A kind declaring frontier transparency is inert in every direction — no ask, no work, no
   routing.
2. A tier override appears on a leaf type only, never on a shared base.
3. Every registered kind parses from its stored form.
4. Every tool carrying result-side behavior stays registered under its recorded name.

## Sequencing

**Stage 1 — this unit.** The library exists and stands alone: the moved modules compile, their
moved tests pass, the equivalence baseline is captured and met, and the conformance kit exists
with the library's own kinds passing it. Neither consumer is migrated.

**Stage 2.** The source application depends on the library and deletes its copy. Its full suite
passes unchanged. This stage needs write access to that repository, which the maintainer has to
arrange; it is not blocked on anything here.

**Stage 3.** The second project builds on the library: an authored chat-message kind, one
adapter, and the access model already agreed.

**Not in any stage yet**: the five identified runtime improvements. They land after Stage 2, so
the equivalence baseline still means something while the move is being proven.

## Acceptance criteria

- **AC1** The library builds with no dependency on the source application, and no path
  reference to it: `cargo build --workspace` succeeds, and its manifests name no local path
  outside this repository.
- **AC2** `cargo test --workspace` passes, running the moved tests. The count of test functions
  is at least the count moved.
- **AC3** `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --check` both
  exit zero.
- **AC4** No file under `src/` names a domain concept from the source application. A grep for
  the agreed vocabulary list finds nothing outside `docs/`.
- **AC5** The equivalence baseline is committed as fixtures, and a test asserts the library
  reproduces every recorded output byte-for-byte.
- **AC6** The conformance kit exists as a public test entry point, and a test runs it against
  the library's own registered kinds with every check passing.
- **AC7** The suite runs with no network access and starts no external service; the store opens
  in memory. A test asserting an outbound connection fails is present.
- **AC8** The suite is parallel-safe: it passes with the default parallel test runner and again
  under a single thread, with the same result.
- **AC9** Every public item carries a doc comment, and `cargo doc` produces no warnings.
- **AC10** The library exposes no constructor that silently picks a provider, a model or a
  database path: each is supplied by the caller.
- **AC11** Licensing is consistent: the manifest declares MPL-2.0, the license file is present,
  and no moved file carries a conflicting header.
- **AC12** The reference documents describing this architecture live in this repository, and the
  consuming project's copies are replaced by pointers.

## Open questions for the maintainer

1. **Crate naming.** `agent-ledger` is the working name and the crate name. Renaming is free
   until first publication.
2. **Write access to the source repository** is needed for Stage 2 and not before.
3. **Which providers ship enabled by default.** The recommendation is none: every consumer
   names what it uses.
