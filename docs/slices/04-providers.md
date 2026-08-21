# Slice 4 — the provider layer

Date: 2026-08-21. Part of Stage 2 in the extraction spec, which is the authority for anything
not stated here. Slices 1–3 are committed.

## Scope

The model boundary: the provider trait and its uniform streaming contract, the neutral
message and stream-event vocabulary, the render pass from blocks to neutral messages, the
per-vendor modules, provider and model registries, and the HTTP helpers the vendor modules
share.

Source: the read-only clone at
`the ronna-lightspeed source (github.com/xdevs23/ronna-lightspeed)`,
crate `crates/ronna-server`, directory `src/llm` (11,497 lines) plus `src/http.rs`.
**Read it; never write to it.**

## What does not come

- **The subprocess provider** (`claude_code/`, and its 3 tests): it needs the source
  application's tool-protocol server, which stays. It becomes a consumer-registered provider
  there. Its stream-event vocabulary contributions stay only insofar as the shared types
  already carry them.
- **The search-provider surface.** The provider trait's public signature returns a
  search-provider handle in the source; that method is severed. Where a vendor module
  consults it, the capability arrives as a consumer-supplied hook, and the trait does not
  name search.
- The provider-picker UI strings and anything naming the source product.

## What must hold

- **The core stays API-agnostic by construction**: nothing outside a vendor module builds a
  wire shape. The loop bug the source records — the core emitting one vendor's shape so
  another vendor's path silently dropped tool calls — must stay pinned by the ported tests.
- **Vendors are feature-gated, none on by default.** The trait, the neutral vocabulary, the
  stream contract, the render pass and the registries are always present. `--all-features`
  is the check.
- **Render re-exports from the behavior layer.** Slice 3 hoisted `ContentPart` and the
  render vocabulary into `agency/projection.rs`; this slice re-exports rather than
  redefining, adding only what the wire genuinely requires (serialization derives land here,
  not in slice 3's types, if a vendor needs them).
- **Streaming resilience carries over**: the retry-with-backoff behavior, the stream-error
  translation that preserves status and provider-health class, and the explicit stream
  close. Their tests come across.
- **No network in tests.** The source's vendor tests run against recorded fixtures and local
  mocks; every ported test must too, and the suite must pass with the network-blocking
  discipline described in the testing reference under docs/reference.

## Test accounting

118 to port (121 minus the subprocess provider's 3): openai 31, openai_responses 27,
render 22, mistral 12, anthropic 9, provider_module 7, kimi 7, types 2, config 1 — plus
`store/models.rs`'s model-entry types reconnecting to their consumers.

## Dependencies

This slice adds real ones (HTTP client, SSE parsing, futures). Every addition follows the
standing rule: current version from the registry, advisory check, recorded in
`docs/dependency-review.md` before the manifest names it. Feature-gate what only a vendor
needs behind that vendor's feature.

## Acceptance criteria

- **AC4-1** `cargo build --workspace` and `cargo build --workspace --all-features` both
  succeed; no external path dependency.
- **AC4-2** `cargo test --workspace --all-features` passes with **at least 262** tests (149
  present plus at least 113 of the 118, any shortfall named per test with its reason).
- **AC4-3** `cargo test --workspace` (default features) still passes — the always-present
  layer does not depend on any vendor.
- **AC4-4** clippy `--all-targets --all-features -- -D warnings`, `fmt --check` and
  `doc --all-features` all exit clean.
- **AC4-5** The vocabulary scan over `crates/` matches nothing.
- **AC4-6** No file outside vendor modules constructs a vendor wire shape: the ported
  loop-bug test passes, and a grep for the vendors' wire-format markers outside their
  modules finds nothing.
- **AC4-7** A test proves a full block-to-neutral render over a store-built ledger from
  slices 2–3 composes: blocks in, one neutral message list out, byte-stable across two runs.
- **AC4-8** No test opens a network connection: the suite passes with the socket guard the
  testing reference describes, or with an equivalent assertion that fails on any outbound
  attempt.
- **AC4-9** The suite passes identically under `--test-threads=1`.
- **AC4-10** Every new dependency is recorded in `docs/dependency-review.md` with resolved
  version, registry-current version and advisory result, before the manifest names it.
