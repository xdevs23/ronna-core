# agent-ledger

A Rust library for building LLM agents on an append-only ledger of blocks.

A session is an ordered list of blocks. Each block answers a small uniform interface — who
owes the next move, do my own work, am I done, how do I read to the model — and the runtime
runs those hooks without branching on block kind. A persisted cursor advances through the
ledger, and a model turn fires only when the cursor reaches the last block and that block
awaits the model. Conversation state, tool admission, permission state and spend are folded
from the ledger, never stored beside it.

**Status: pre-alpha.** Under extraction from a working application; the public interface will
change without notice until the first release.

## What it gives you

- **A block ledger** on SQLite: append-only, with a junction so forking copies references
  instead of content.
- **An orchestration runtime**: a cursor that advances and parks, a single forward decision
  about when the model may speak, a per-session actor, and an event bus whose contract makes
  an unhandled event a detectable condition.
- **A provider layer**: one neutral message vocabulary, one streaming contract, and per-vendor
  modules behind it, so nothing downstream learns a wire format.
- **Tool admission**: a registry, a runner that decides once and records it, a durable approval
  chain, and standing permission state that can pre-answer it.
- **A conformance kit**: tests you run against *your own* block kinds and tools, proving the
  library's preconditions hold for them.

## Development

Cargo runs inside the Nix development shell, which provides the toolchain:

    nix develop -c cargo test --workspace
    nix develop -c cargo clippy --workspace --all-targets -- -D warnings
    nix develop -c cargo fmt --check

Tests run in parallel with no external services: the store opens in memory and the suite
reaches nothing over the network.

## Documentation

- `docs/extraction-spec.md` — what is being moved here, from where, and in what order. The
  only document that exists yet.

The invariants that bind every change are in `CLAUDE.md`. Architecture references and
decision records arrive with the code they describe.

## License

Copyright (C) 2026 Simão Gomes Viana

This program is free software: you can redistribute it and/or modify it under
the terms of version 3 of the GNU General Public License as published by the
Free Software Foundation. See [LICENSE](LICENSE).

The store and orchestration core is ported from
[ronna-lightspeed](https://github.com/xdevs23/ronna-lightspeed) by the same
author, under the same license.
