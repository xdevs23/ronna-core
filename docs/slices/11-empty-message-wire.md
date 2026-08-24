# Unit 24 — no wire sends empty assistant content

Date: 2026-08-24. Revision 2, rewritten after two cold seats probed revision 1 by
building the tree and converting real histories. Unit 23 made an empty completed turn a
real empty assistant block on the ledger, and the projection replays it. Several
endpoints reject an empty assistant message, so the first replay of a silent turn would
fail the request. The ledger's record does not change — the operator's ruling of
2026-08-24 is that the internal representation stays empty and a provider that cannot
carry it strips it on its own side. This slice makes every wire drop empty assistant
content on its way out, and nothing else.

## Grounding (what the probe found, by running the code)

**The empty message reaches every wire untouched.** `text_content` filters empty
contributions and joins what is left, so a silent turn becomes `Text("")`, and
`blocks_to_messages` pushes a message per role group unconditionally
(`providers/render.rs:52-55,96-104`). Every wire's plain-`Text` branch pushes it with no
guard: `anthropic/mod.rs:271-274`, `chat/base.rs:239-247`, `openai/mod.rs:241-244`.

**Empty is not one shape.** The tree produces at least five, and only the first was
addressed by revision 1:

| shape | produced at |
|---|---|
| `Text("")` | `render.rs:96-103` |
| `Text("   \n ")` — whitespace only | the same join, when a contribution is whitespace |
| `Parts([Text{""}])` | `agency/text.rs:41-45`, unconditional |
| `Parts([Reasoning{text:""}])` | `agency/thinking.rs:58-63` — the documented summary-only-vendor case |
| `Parts([])` | a consumer kind forcing parts with none |

The parts shapes are not exotic and they reach the wires today. Converted for real, a
tool-call turn whose text was empty emits `[{"type":"text","text":""}, …]` on anthropic
(`anthropic/mod.rs:275-282`; an unreplayable `Reasoning` also degrades to `Text{text}` at
`:311,:320`), `{"role":"assistant","content":"", "tool_calls":[…]}` on the chat base
(`chat/base.rs:566-579`: a one-element vec of `""` is not `parts.is_empty()`), and an
empty message item on the Responses wire (`openai/mod.rs:267-275`, `flush` fires on
`pending_text == [""]`).

**Kimi guards half of itself, not all of it.** Its `Text` branch filters on
`!text.is_empty()` (`kimi/mod.rs:325,365`) but its `Parts` branch pushes the text part
unfiltered (`kimi/mod.rs:332`), so kimi emits `{"role":"assistant","content":"",
"tool_calls":[…]}` today. It does, however, guard all three roles rather than only the
assistant (`kimi/mod.rs:259-269` system, `:306-315` user, `:365` assistant).

**Adjacent same-role messages already exist, and merging them is harmful.** The
tool-result group is `Role::Tool` collapsed to `MessageRole::User`
(`render.rs:64-69`), so the neutral layer hands the wire two adjacent `user` messages
with no omission involved. Merging them, as revision 1 required, moves a tool result
away from the `assistant` message carrying its `tool_calls` — the chat base emits the
group's own message before its tool results (`chat/base.rs:533-548` then `:550-558`) —
and every OpenAI-compatible endpoint rejects a tool message that does not follow its
call. Revision 1's merge requirement is therefore removed, not deferred: it solved a
hazard the omission does not create, and it broke a shape that works today.

**A per-vendor keep-or-omit key is unsound.** The endpoint is operator-configurable
(`chat/base.rs:642-653`), and openrouter is a multi-vendor gateway whose upstream may be
Mistral or Anthropic — exactly the endpoints that refuse empty content. A policy keyed
on the vendor module would keep the empty message on the one wire most likely to be in
front of a refusing endpoint.

**Dropping content is loud in this tree.** A wire that discards something warns, so a
later reader cannot mistake silence for understanding (`kimi/mod.rs:263-267,291-294`,
`openai/mod.rs:338-340`, and the reasoning at `chat/base.rs:526-528`). The debug log
dumps the pre-conversion messages (`chat/base.rs:452`, `anthropic/mod.rs:193`), so
without a record the logged request and the sent request differ on exactly the path an
operator would be debugging.

## Decisions taken with this unit

- **Every wire drops empty assistant content, uniformly, 2026-08-24.** No endpoint
  requires an empty assistant message, and the risk is one-sided: keeping it can fail a
  request, dropping it cannot. So the rendering is the same everywhere and there is no
  policy to configure, no per-vendor key, and no seam to thread. *Rejected:* keeping the
  empty message where the endpoint accepts it (revision 1) — it buys a replayed silence
  the model cannot perceive anyway, and it prices that in a per-vendor policy that is
  unsound in front of a gateway; *rejected:* a runtime probe of the endpoint.
- **Empty means: nothing survives once whitespace is trimmed, 2026-08-24.** The drop
  covers `Text("")` and a whitespace-only string, an empty or whitespace-only text part,
  a degraded empty `Reasoning`, and a parts list that is empty or contains nothing but
  those. A message that keeps a non-text part — media, a tool call, a tool result —
  keeps the message; only the empty text is dropped from it. *Rejected:* exact-empty
  only — anthropic refuses whitespace in assistant content too, so an exact-empty test
  would ship a request that still fails.
- **Dropping never empties the request, 2026-08-24.** If dropping would leave a wire with
  no message at all, the request is refused before it is sent, with an error naming the
  cause, rather than a malformed request the endpoint rejects for a different-sounding
  reason. The shape is degenerate — a conversation whose only assistant block is empty
  rests its own frontier and dispatches nothing — but the wire is defensive about it
  because the failure is otherwise diagnosed as a vendor error.
- **A dropped message is recorded, 2026-08-24.** Each drop warns, in the tree's existing
  voice for discarded content, so the pre-conversion debug dump and the sent request can
  be reconciled.
- **Adjacency is left exactly as it is, 2026-08-24.** No merging, on any wire. See the
  grounding: adjacent same-role messages predate this unit, and merging them breaks the
  tool-call ordering that works today.
- **The scope is the assistant role, 2026-08-24.** An empty user or system group is a
  different question with a different cause, and kimi's existing guards for those stay as
  they are. The contract below is worded to that scope rather than universally.

## The unit's contract

No wire sends assistant content that is empty once whitespace is trimmed: not as a
message, not as a text part, and not as the whole of a parts list. A message that still
carries a non-text part keeps that part and loses only the empty text. Dropping never
leaves a wire with no message to send — that request is refused with a named error — and
every drop is recorded in the log. Adjacent same-role messages are untouched, no merging
happens anywhere, and every non-empty message converts exactly as it does today. The
ledger, the projection and the neutral render are unchanged. No new dependency, and no
new configuration.

## Acceptance criteria

- **AC1** The workspace suite is green, clippy, fmt and doc pass with denied warnings,
  the forbidden-vocabulary scan is clean, and no dependency is added.
- **AC2** Message-level: on EACH of the four wires (anthropic, chat base, openai
  Responses, kimi), a history containing an empty assistant message converts to a request
  carrying no message for it and no empty content anywhere — pinned per wire against the
  real converted output, not against the neutral render.
- **AC3** Part-level, the hole revision 1 missed: on each wire, an assistant message whose
  parts are an empty text part (and, separately, a degraded empty `Reasoning`) beside a
  tool call converts to a request carrying the tool call and NO empty text block —
  pinned per wire; the pin fails on the pre-change code.
- **AC4** Whitespace counts as empty: an assistant message of `"   \n "` is dropped
  exactly as `""` is — pinned.
- **AC5** Nothing else moves: a non-empty assistant message, a whitespace-bearing but
  non-empty message (`" hi "`), a tool-call turn with real text, and a media-bearing parts
  message convert exactly as they do today — pinned, and every existing wire pin passes
  unchanged.
- **AC6** Adjacency is untouched: the tool-result-then-user shape converts with the tool
  message still following its `tool_calls` message, and two adjacent user messages stay
  two — pinned, so a future merge cannot be added without failing this.
- **AC7** The degenerate case: a history that would leave a wire with no message after the
  drop yields a named error rather than a request — pinned.
- **AC8** Each drop is recorded in the log, in the tree's existing voice for discarded
  content — pinned or demonstrated.

## Notes for launch

- Branches from `master` (worktree `~/projects/agent-ledger-wire`, branch
  `unit/empty-message-wire`). Sites, from the probe: `anthropic/mod.rs` (~271 `Text`, ~275
  `Parts`, ~311/~320 the `Reasoning` degrade, ~253-266 the system hoist),
  `chat/base.rs` (~239 `Text`, ~490-506 chunks, ~526-548 the existing empty guard, ~566-579
  `fold_assistant_content`), `openai/mod.rs` (~241, ~265-275 `flush`), `kimi/mod.rs` (~325
  guarded, ~332 unguarded), `mistral.rs` (~127-131 the replay array), `render.rs` (~52,
  ~64-69, ~96-104) for the input shapes.
- The provider rules are grounded in vendor documentation and the probe's real conversions;
  do not re-derive them and do not send a live request to a paid endpoint.
- Four wires need the same decision, so make it once and let each wire apply it — a fifth
  hand-rolled guard beside kimi's is the thing to avoid. Kimi's `Text` branch is the
  behaviour being generalized; its `Parts` branch is one of the holes being closed.
- Watch the one-shot payload fallback (`bind.rs:432-452`): it spends its single retry on an
  API error when the turn carried reasoning payloads, and an empty-content rejection would
  burn it on a false diagnosis. Closing the hole is what prevents that; do not add a second
  retry.
