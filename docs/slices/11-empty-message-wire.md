# Unit 24 — the wires carry an empty assistant message their own way

Date: 2026-08-24. Unit 23 made an empty completed turn a real empty assistant text
block on the ledger, and the model-facing projection replays it. Two shipped wires
reject that message outright, so a deployment on either would break on the first
replay of a silent turn. The ledger's record is not the thing to change: the empty
block stays exactly as it is, and each wire renders it per its own rules — omitting it
where the endpoint refuses it. Grounded by provider documentation and by reading each
wire; receipts below.

## Grounding (what the research and the code found)

**The ledger hands every wire the same thing.** `text_content` joins a silent turn's
contribution to `MessageContent::Text("")` and `blocks_to_messages` pushes a message per
role group unconditionally (`providers/render.rs:52,96-103`), so
`Message { role: Assistant, content: Text("") }` reaches each wire's convert step
untouched. The constraint therefore belongs at the wire, not at the render.

**Per wire, as it stands today:**

| wire | convert site | renders the empty message as | endpoint's rule |
|---|---|---|---|
| anthropic | `anthropic/mod.rs:243,271-273,285` | `[{"type":"text","text":""}]` | **rejects** |
| chat base (mistral, openrouter) | `chat/base.rs:228,239-246` | `{"role":"assistant","content":""}` | **mistral rejects**; OpenAI-compatible accepts |
| openai (Responses API) | `openai/mod.rs:211,241-244` | `InputItem::Message{content:""}` | `null` rejected; `""` undocumented |
| kimi | `kimi/mod.rs:318,324-328,365` | nothing — already omitted | n/a |

- **Anthropic rejects both empty shapes.** An empty text block is refused with "text
  content blocks must be non-empty", and an empty content array with "all messages must
  have non-empty content except for the optional final assistant message". The final
  assistant message is the only exception, and a replayed silent turn is not final — so
  **no accepted empty shape exists** there.
- **Mistral rejects `content:""`** and equally an assistant message with neither content
  nor tool calls. The shared chat base already knows this shape is refused: its
  `ConvertedParts::append_to` drops a message carrying neither (`chat/base.rs:525-548`,
  "rejected outright by these endpoints"), and `media_chunks` already drops an empty text
  chunk (`chat/base.rs:497`) — but the plain `Text` branch reaches neither guard.
- **OpenAI-compatible chat accepts `content:""`**, confirmed both by the specification
  and by a real request to the deployed router on 2026-08-24. Nothing to fix there, and
  something to keep: an accepted empty message is the model reading its own silence back.
- **Kimi already omits**, so the behaviour this slice generalizes is already shipping on
  one wire.

**Why omission is the only option where it is needed.** Dropping just the empty text
part leaves a message with no content at all, which both refusing endpoints also reject;
there is no smaller edit that survives. A placeholder string is rejected on different
grounds — it fabricates an assistant line the model never produced, which is exactly the
dishonesty unit 23 removed from the ledger.

**Omission's own hazard: adjacent same-role messages.** A silent turn most often sits
between two member messages, so omitting it leaves two user messages in a row. Anthropic
combines consecutive same-role turns on its own API but some endpoints refuse the pair,
and `blocks_to_messages` grouped by role *before* the omission, so the wire cannot assume
the neutral layer already merged them.

## Decisions taken with this unit

- **The ledger and the projection are untouched, 2026-08-24.** The empty block stays on
  the ledger and stays in the model-facing projection; only the wire's rendering changes.
  This is the operator's ruling of 2026-08-24: the internal representation stays empty,
  and a provider module that cannot carry it strips or converts it on its own side.
  *Rejected:* omitting the empty message in `blocks_to_messages` — one wire's limitation
  would silently become every wire's history.
- **An empty assistant message is omitted per wire, by a policy the wire states,
  2026-08-24.** The rendering is not uniform because the endpoints are not: anthropic and
  mistral omit; an OpenAI-compatible chat endpoint keeps the empty message, because it is
  accepted there and it is the model's own silence. The implementer settles the seam —
  the shape that reads best is a policy the wire declares and the shared chat base
  consults, so mistral omits without openrouter's generic path changing and without the
  decision being copied into two `if`s. *Rejected:* a uniform omit on every wire (defeats
  unit 23's replay intent wherever the endpoint would have accepted it); *rejected:* a
  runtime probe of the endpoint (a request must not depend on discovering the rule).
- **Omission merges what it makes adjacent, 2026-08-24.** A wire that omits also merges
  the same-role messages the omission leaves adjacent, so no endpoint sees a pair it may
  refuse and no content is lost. *Rejected:* relying on the endpoint's own combining (one
  vendor does it, others refuse; the wire cannot tell which is behind a gateway).
- **The OpenAI Responses wire omits until its acceptance is verified, 2026-08-24.** Its
  documentation refuses `null` and says nothing about `""`. An unverified acceptance is
  not a basis for shipping a request shape, so it takes the safe side; a real request
  showing the empty string accepted flips it to keep, with the evidence recorded here.
  *Rejected:* assuming acceptance because the sibling chat API accepts it — a different
  API with its own validation.

## The unit's contract

Each wire renders an empty assistant message per its own endpoint's rule: anthropic,
mistral and the OpenAI Responses wire omit it and merge whatever same-role messages the
omission leaves adjacent; an OpenAI-compatible chat endpoint keeps it as empty content;
kimi's existing omission is unchanged in behaviour. The ledger, the projection and every
non-empty message are untouched, and no request carries an empty content part to an
endpoint that refuses one. No new dependency.

## Acceptance criteria

- **AC1** The workspace suite is green, clippy, fmt and doc pass with denied warnings,
  the forbidden-vocabulary scan is clean, and no dependency is added.
- **AC2** Anthropic omits: converting a history containing an empty assistant message
  emits no message for it and no empty content block anywhere in the request — pinned at
  the wire's own convert step, not at the neutral render.
- **AC3** Mistral omits and the generic OpenAI-compatible chat path does NOT: the same
  history converted under each policy differs exactly in the empty assistant message —
  pinned for both, so the shared base is proven to consult the policy rather than pick one
  behaviour for everyone.
- **AC4** Omission merges: a history of member message, silent turn, member message
  converts on an omitting wire to messages with no two adjacent same-role entries and with
  both members' text preserved in order — pinned.
- **AC5** Nothing else moves: a non-empty assistant message, a tool-call turn and a
  whitespace-only-but-not-empty message convert exactly as they do today on every wire —
  pinned, and the existing wire pins pass unchanged.
- **AC6** The OpenAI Responses wire omits, with the undocumented-acceptance reason
  recorded at the policy — pinned.

## Notes for launch

- Branches from `master` (worktree `~/projects/agent-ledger-wire`, branch
  `unit/empty-message-wire`). Sites: `providers/anthropic/mod.rs` (~243, 271, 285),
  `providers/chat/base.rs` (~228, 239, 497, 525-548), `providers/openai/mod.rs` (~211,
  241), `providers/kimi/mod.rs` (~318, 365) and `providers/render.rs` (~52, 96) for the
  input shape.
- The provider rules above are grounded in vendor documentation and a real request; do not
  re-derive them, and do not send a live request to a paid endpoint to check one.
- Kimi already implements the behaviour this slice generalizes — read it before designing
  the seam, and prefer folding it into the same policy over leaving a fourth hand-rolled
  guard beside it.
