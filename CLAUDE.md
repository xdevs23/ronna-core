# agent-ledger

A Rust library for building LLM agents on an **append-only ledger of blocks**. It is the
runtime, not an application: storage, orchestration, model providers, tool admission and the
permission chain, with no domain knowledge of any product built on it.

## What the library is

An agent session is an ordered list of **blocks**. A block is not a record the machinery
interprets by type — the block answers a small uniform interface and the machinery runs those
hooks without ever branching on block kind. A persisted cursor advances through the ledger; a
model turn fires only when the cursor reaches the last block and that block awaits the model.
Everything else — conversation state, tool admission, permission state, spend — is derived by
folding the ledger.

The design and its reasoning are recorded in the reference documents under `docs/reference`,
each rule stated with the failure that produced it.

## The invariants

These hold at every commit. A change that seems to need one relaxed is a design problem, not
an exception:

- **The machinery never branches on block kind and never names a domain concept** — not in
  code, not in comments. Behavior lives on the kind.
- **Blocks are append-only.** Only a streaming tail mutates in place; finalizing appends a
  fresh committed block in the same transaction.
- **Derived, never stored.** If a fact can be folded from the ledger, it is not a column.
- **One decision, one place, recorded once.** A decision that travels both as a durable record
  and as an in-memory value is two decisions waiting to disagree.
- **Reach and consent are separate rails.** Authority is resolved live and cannot be
  pre-delegated; consent is durable ledger state and never consults live data. Neither has a
  code path into the other.
- **A consumer's own types are its own to prove.** The library ships a conformance kit; a
  consumer runs it against its own block kinds and tools.

## What belongs here, and what does not

Here: the block model and registry, the cursor and the frontier decision, the session actor,
the event bus, the store, the projection fold, the provider layer, the tool registry and
admission, the permission chain and standing state.

Not here: routes and transport, the meaning of any access scope, the tools themselves,
workflows, and anything that knows what the product does.

## Engineering standards

- Modular structure, robust failure handling, clear separation of concerns, well-chosen
  abstractions. A feature that needs bolted-on conditionals signals a refactor, not an if.
- Every unit of work runs in a git worktree through the implement-review-verify workflow,
  merges back on completion, and the worktree is deleted.
- Tests run in parallel and fast: the store opens in memory, nothing starts a server, nothing
  reaches the network. A suite slow enough to skip is a suite that stops being run.
- Documented decisions carry their date and the rejected alternatives.
- Commit messages follow the repository style: lowercase scope prefix plus plain imperative,
  a body written from zero, and a `Test:` footer stating a past fact.

## License

GNU General Public License v3.0 or later, carried over from the code this library is
extracted from. A weaker copyleft was considered so that unrelated projects could adopt the
library; it was rejected because the maintainer's stated need is their own projects, and
keeping the original terms removes a relicensing step and its consent record entirely. Both
current consumers are GPL-3.0 already.
