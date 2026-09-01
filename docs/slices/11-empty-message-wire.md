# Unit 24 — the empty assistant message is echoed, and converted only where refused

Date: 2026-08-24. Revision 3. Unit 23 made a turn that says nothing a real empty
assistant block on the ledger, and the projection replays it. **That message is meant to
reach the model**: reading its own silence back is the point of recording it. Some
endpoints refuse the shape, and those convert it away on their own side. This slice is
that split, and nothing else.

Revision 2 got this backwards and is superseded — see the rejected alternatives.

## Grounding (documented behaviour and the tree, both checked)

**The echo is legal where we send it.** The chat-completions schema types assistant
`content` as string, array or null, and `""` is a string; the field is required only when
neither `tool_calls` nor `function_call` is present. So `{"role":"assistant","content":""}`
is a valid request message, not a gateway's tolerance.

**Two endpoints genuinely refuse it.** Anthropic rejects an empty text block ("text
content blocks must be non-empty") and an empty content array (only a *final* assistant
message may be empty, which a replayed silent turn is not). Mistral rejects empty
assistant content outright — it wants content or tool calls.

**The Responses API's documented refusal is of `null`, not of `""`** ("expected a string,
but got null instead"). Nothing establishes that it refuses an empty string, so by the
rule below it echoes.

**Its reasoning rule is separate and real.** That API requires a replayed reasoning item
to be followed by the item it produced, and rejects the request otherwise: "Item 'rs_...'
of type 'reasoning' was provided without its required following item." A group whose
reasoning ends up trailing therefore leaves without it. Note what the echo does here: a
turn that thought and then said nothing now *keeps* its reasoning item, because the
echoed empty message is the following item the API asks for.

**The shapes reach the wires.** `text_content` joins a silent turn to `Text("")` and
`blocks_to_messages` pushes a message per role group unconditionally
(`providers/render.rs:52-55,96-104`). Empty also arrives as a whitespace-only string, an
empty text part, a degraded empty `Reasoning` (`agency/thinking.rs`, the summary-only
vendor case), and an empty parts list.

**Merging is not available.** Tool results collapse to the user role
(`render.rs:64-69`), so adjacent same-role messages exist with no conversion involved,
and the chat base emits a group's message before its tool results
(`chat/base.rs:533-548,550-558`). Joining them moves a tool result away from the message
carrying its call, which these endpoints reject.

## Decisions taken with this unit

- **The empty assistant message is echoed by default, 2026-08-24.** This is the
  operator's ruling: the internal representation stays empty, and a module that cannot
  carry it converts on its own side — nothing else. A wire whose endpoint accepts the
  message sends it unchanged.
  *Rejected: dropping it on every wire (revision 2).* It was reached by generalising one
  vendor's refusal into a rule for all of them, and it threw away the replay the record
  exists for. The argument for it — that a gateway may front a refusing vendor, so no
  per-vendor answer is safe — loses to the fact that the shape is valid where we send it;
  a gateway normalising for its own upstream is the gateway's job, not a reason for every
  other endpoint to lose the echo.
- **An endpoint that refuses declares it, and converts on its own side, 2026-08-24.**
  The chat base carries a `refuses_empty_assistant` flag, default false, set by the
  vendor build that needs it — the sixth of the base's existing per-vendor seams, not a
  new mechanism. Mistral sets it; the generic OpenAI-compatible build does not, so
  openrouter and any Requesty-style endpoint echo. Anthropic converts in its own module.
  Kimi keeps the guard it already had. *Rejected:* a config key (the endpoint's shape is a
  fact about the endpoint, not an operator preference); *rejected:* probing the endpoint
  at runtime.
- **Empty means nothing survives once whitespace is trimmed, 2026-08-24.** For a wire
  that converts: the message, a text part, or a parts list of nothing but those. A message
  keeping a non-text part keeps it and loses only the empty text. Whitespace counts,
  because the refusing vendors count it.
- **A conversion that would leave no message refuses by name, 2026-08-24.**
  `LlmError::NoMessage`, rather than sending a shape the endpoint rejects for a
  vaguer-sounding reason. On an echoing wire this is now reachable only through the
  Responses trailing-reasoning removal.
- **Every removal is recorded, 2026-08-24.** In the tree's existing voice for discarded
  content, because the debug dump holds the messages before conversion and the logged
  request would otherwise differ from the sent one.
- **A trailing replayed reasoning item leaves with its group, unconditionally,
  2026-08-24.** Verified against the vendor's documented error, above. The rule is about
  the item being trailing, not about any removal: a group whose real text flushes into its
  message before the replay is pushed also strands one, and that shape was rejected before
  this unit too. `carried_payloads` is reported from what survived, so the one-shot payload
  retry is not spent on a payload the request never carried.
- **Nothing merges, on any wire, 2026-08-24.** See the grounding.

## The unit's contract

A wire whose endpoint accepts an empty assistant message sends it unchanged, whitespace
included. A wire whose endpoint refuses one converts it away on its own side — the
message, an empty text part, or a parts list of nothing but those — keeping any non-text
part and recording the removal. A conversion that would leave a request with no message at
all refuses by name instead of sending it. Adjacent same-role messages are untouched and
nothing merges. On the Responses wire a trailing replayed reasoning item leaves with its
group. The ledger, the projection and the neutral render are unchanged; no new dependency
and no new configuration.

## Acceptance criteria

- **AC1** The workspace suite is green with and without the provider features, clippy,
  fmt and doc pass with denied warnings, the forbidden-vocabulary scan is clean, and no
  dependency is added.
- **AC2** The echo holds where the endpoint accepts it: on the default chat build and on
  the Responses wire, an empty assistant message, a whitespace-only one, an empty text
  part beside a tool call, a degraded empty `Reasoning` beside a tool call, and a parts
  message of nothing but empty text all reach the request unchanged — pinned per wire
  against the real converted output.
- **AC3** The conversion holds where the endpoint refuses: the same five shapes on a build
  with `refuses_empty_assistant` (mistral's shape) and on anthropic are converted away,
  with any non-text part kept — pinned per wire, and pinned as a PAIR with AC2's echo so
  the per-endpoint split itself is proven rather than one behaviour being assumed for all.
- **AC4** A conversion that empties a request refuses by name — pinned.
- **AC5** Nothing else moves: a non-empty message, `" hi "`, a tool-call turn with real
  text, and a media-bearing parts message convert exactly as they do today; every existing
  wire pin passes unchanged.
- **AC6** Adjacency is untouched: the tool-result-then-user shape keeps the tool message
  following its `tool_calls` message, and two adjacent user messages stay two — pinned, so
  a merge cannot be added later without failing this.
- **AC7** The Responses reasoning rule: a genuinely trailing replayed reasoning item is
  removed and reports no carried payload, a trailing one whose group's text is real is
  removed too, and one followed by the echoed empty message RIDES OUT with its payload
  carried — all three pinned, the last being the behaviour the echo restores.
- **AC8** Each removal is recorded in the log, in the tree's existing voice.

## Notes for launch

- Branches from `main` (worktree `~/projects/agent-ledger-echo`, branch
  `unit/echo-empty-message`). Sites: `providers/empty.rs` (what counts as empty, and the
  record), `providers/chat/base.rs` (the `refuses_empty_assistant` seam and its gated call
  sites), `providers/mistral.rs` (sets it), `providers/anthropic/mod.rs` (converts),
  `providers/openai/mod.rs` (echoes; keeps the reasoning rule), `providers/kimi/mod.rs`
  (its pre-existing guard).
- The vendor rules above are grounded in vendor documentation; do not re-derive them and
  do not send a live request to a paid endpoint to check one.
