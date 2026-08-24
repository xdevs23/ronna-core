# Unit 19 — the model wire carries a user-authored media part (framework)

Date: 2026-08-24. Revision 2, rewritten after a cold probe found the original
one-repo spec unimplementable: the model wire cannot carry a member's image or
voice today, and the code path that actually builds the request would mislabel
it. Media understanding is therefore split. THIS unit is the framework half, in
agent-ledger: make the model wire able to carry a user-authored media part,
correctly attributed. The consumer half — the adapter downloading the bytes, the
erasable storage, the projection, the size bound, the off switch and the privacy
documents — is unit 20, built on this once it lands.

## Grounding (what the probe found)

- `ContentPart`'s own `Serialize` impl (`providers/types.rs`, the `WireContentPart`
  enum) is NOT the wire. It is used only in a debug-log line and crate-internal
  tests. The real request body for the OpenAI-compatible / Requesty path is
  hand-built by `ChatProvider::convert_messages` / `convert_parts` /
  `ConvertedParts::append_to` in `providers/chat/base.rs`, and the Anthropic path
  by its own `WireContentBlock` in `providers/anthropic/mod.rs`.
- **The latent bug this unit must fix first.** `convert_messages` (`base.rs`) does
  not pass `msg.role` into `convert_parts`; `append_to` hardcodes
  `role: "assistant"` for every non-tool-result part. This is harmless only
  because the sole producers of `MessageContent::Parts` today are `ToolCall`
  (assistant, `ToolUse` only) and `ToolResult` (routed to a `role: "tool"`
  branch). No `Role::User` kind has ever set `forces_parts()`, so a user-role
  Parts message has never reached this code. A member's image REQUIRES a
  user-role Parts message, and the moment one exists the member's photo is sent
  to the model attributed to the assistant — silent turn-attribution corruption.
- The only non-tool carrier `convert_parts` has (`AssistantTextPart`, `base.rs`)
  holds text/reasoning only — no image bytes — and its doc names the assumption
  "one ordered text-bearing part of an assistant group". The declared escape
  hatch for structured content is `WireMessageContent::Chunks(Vec<Value>)`
  (`chat/wire.rs`), reserved for a vendor encoder.
- The grouping stage below the wire (`providers/render.rs` `blocks_to_messages` /
  `group_content`) is already role-agnostic and can render a `Role::User` group
  as `MessageContent::Parts`; `Text::llm_parts()` already returns `Some`
  unconditionally. So mixing a caption and a media part in one user group is fine
  at the grouping stage — the break is only at wire serialization.
- The audio wire shape Requesty's gateway to `vertex/gemini-3.7-flash@eu` accepts
  is not recorded anywhere in the tree; even the image `image_url` data-URI shape
  is convention, unverified against the real gateway in-repo.

## Decisions taken with this unit

- **The wire carries the message's real role through the parts branch, 2026-08-24.**
  `convert_messages` passes `msg.role` into `convert_parts`, and `append_to`
  emits the wire message under that role instead of the hardcoded `assistant`.
  Tool results keep their `role: "tool"` routing; a user-authored group is
  emitted as a `user` message, an assistant-authored group as `assistant`. This
  fixes the latent misattribution for its own sake — a user-role Parts message is
  now correct on the wire — and is the precondition for any media, which is
  always user-authored inbound. Rejected: inferring the role from content shape
  (the current bug — content kind does not determine authorship; a user and an
  assistant can both carry text).
- **`ContentPart` gains an image variant, and an audio variant only if the
  gateway accepts it, 2026-08-24.** `ContentPart` (`agency/projection.rs`) gains
  `Image { mime, data }` (`data` the raw bytes, base64-encoded at the wire) and,
  gated on verification, `Audio { format, data }`. The OpenAI-compatible wire
  emits the image as an `image_url` data URI inside `WireMessageContent::Chunks`,
  and the audio as the verified inline shape; the carrier `convert_parts` uses
  gains a media case beside its text/reasoning cases, routed into `Chunks` for a
  message that carries any non-text part. The image data-URI AND the audio shape
  are BOTH verified with a real request against the gateway before their pins are
  called done — neither is assumed from convention. If inline audio is not
  accepted, this unit ships `Image` only and records `Audio` as a named
  follow-up. Rejected: stringifying media into `Text` (the model must see the
  real media, not an invented description); a URL part (the platform file URL is
  short-lived, authenticated, and leaks the platform reference — the bytes
  travel, encoded, not a link).
- **The Anthropic path is kept compiling and out of scope, verified, 2026-08-24.**
  Adding a `ContentPart` variant forces an exhaustive-match arm in the
  Anthropic vendor (`providers/anthropic/mod.rs`), which is feature-gated off by
  default and so is NOT caught by the workspace gate. This unit either implements
  the Anthropic image block too or explicitly rejects the media part there with a
  clear error, and PROVES the choice with `cargo check --features anthropic` in
  the gate — so "no other framework path changes" is true under every feature
  set, not only the default. Rejected: leaving the exhaustive match unhandled
  (it compiles clean on the default gate and breaks a future consumer or a
  default-feature change silently).

## The unit's contract

In agent-ledger: `convert_messages` threads `msg.role` into `convert_parts`, and
`append_to` emits each converted message under its real role (tool results still
`tool`). `ContentPart` gains `Image { mime, data }` (and `Audio { format, data }`
if the gateway accepts inline audio); the OpenAI-compatible wire routes a
media-bearing message into `WireMessageContent::Chunks` with the image as an
`image_url` data URI and the audio as its verified shape; the `convert_parts`
carrier gains a media case. The Anthropic path is handled (implemented or
cleanly rejected) and proven under `--features anthropic`. A base64 helper, if
not already a direct dependency, is added at its latest version after a
supply-chain check. No consumer change here; unit 20 is the consumer.

## Acceptance criteria

- **AC1** agent-ledger workspace green; clippy `--all-targets`, fmt, doc denied
  warnings; AND `cargo check --features anthropic` clean (the off-default path);
  any added dependency is the latest version and checked for a known compromise.
- **AC2** Role attribution fixed: a `Role::User` group that renders as
  `MessageContent::Parts` serializes to a wire message with `role: "user"` (not
  `assistant`); an assistant group stays `assistant`; a tool result stays
  `tool` — pinned on the real `convert_messages`/`append_to` path (asserting the
  emitted wire message's ROLE, not only its content), the exact bug the probe
  found.
- **AC3** The image part on the real wire: a user message carrying a caption and
  an `Image { mime, data }` serializes, through `convert_messages`, to a `user`
  message whose content is `Chunks` with the caption text part and an
  `image_url` data-URI part in order — pinned against the exact wire JSON, and
  the data URI's MIME and base64 verified to round-trip the bytes.
- **AC4** Audio: if the gateway accepts inline audio, an `Audio { format, data }`
  serializes to its verified inline shape on the wire — pinned; OR audio is
  recorded as a named follow-up and only `Image` ships, stated and pinned as the
  variant set.
- **AC5** The gateway shapes are verified, not assumed: the image data-URI shape
  (and the audio shape if it ships) are confirmed accepted by a real request to
  Requesty's `vertex/gemini-3.7-flash@eu` before AC3/AC4 are called done; the
  verification method and result are recorded in the unit's decision or a
  follow-up.
- **AC6** No regression on the existing wire: every current
  `convert_messages`/`convert_parts`/append_to pin passes unchanged (assistant
  text, reasoning continuity, tool use, tool results), the role rework proven
  not to disturb them — and mutation-tested where the role now varies.

## Notes for launch

- Works in an ISOLATED agent-ledger worktree (not the shared ~/projects/agent-ledger
  checkout the consumer path-depends on — editing that in place would break every
  concurrent build of the consumer and its worktrees, probe finding §7). The
  framework change is committed to agent-ledger on completion; unit 20 (consumer)
  then builds against it.
- VERIFY the gateway shapes with a real request (the config has the Requesty
  endpoint and key) before pinning AC3/AC4 — do not send a shape the gateway
  silently drops.
- The consumer half (unit 20) will need: the adapter's SECOND endpoint root for
  file bytes (`api.telegram.org/file/bot<token>/<file_path>`, distinct from the
  JSON API root); an erasable SIDE TABLE for media bytes (NOT the block content
  row — that bloats every `list_blocks` scan); erasure extended at BOTH
  `erase_principal_content` and `erase_message_named`; the size bound; the
  unconditional default-on off-switch (there is no per-model capability surface
  to gate it on); the projection; and the privacy documents.
