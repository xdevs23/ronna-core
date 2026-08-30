# Slice 17 — a window per tool, beside the conversation's

Date: 2026-08-31. Slice 16's window caught the burst it was designed for and missed
the very next incident: a turn ground one failing lookup tool at well under sixty
calls a minute, hour after hour, and the per-conversation window never tripped —
it bounds rate, and a slow grind is in-rate. The operator's design closes the gap
from the other side: a rate limit specific to a tool, with the failing lookup bound
at six calls per minute. A model leaning that hard on one tool is looping, whatever
its overall pace.

## Grounding

Everything this slice touches shipped with slice 16 and is verified there
(`docs/slices/16-the-tool-call-window.md`): the single admission door
`execute_ready_call` with the window refusal FIRST; `resolve_with_error` writing a
durable `tool_error`; the machine prefix `ToolError::RATE_LIMIT_PREFIX`
(`tool-call rate limit:`) that the five-consecutive-refusals fold matches with a
starts-with test; refusals counting as recorded calls; the fresh-admissions-only
boundary (claimed, resolved, human-approved calls proceed); the forced turn end in
`end_turn_if_tool_calls_exhausted`; and the fold discipline — offset-aware stamps,
the writer's own clock, reverse walk with the window-edge early exit, never memory.
The `ToolCall` kind records the tool's `name` on every call block
(`agency/tool_call.rs:32`), so a per-tool count is the same fold filtered by name.
The window config is a plain field on the runner, written only while the context
builder holds the runner's sole reference (slice 16's carrier decision);
`ToolCallWindow` holds `calls`, `seconds`, `consecutive_limit`
(`tools/runner.rs:70-93`).

## Decisions taken with this slice

- **Per-tool windows are the consumer's numbers, on a public builder,
  2026-08-31.** The runner's window config gains a map from tool name to a
  per-tool bound `{calls, seconds}`; the default map is EMPTY — the framework
  ships no tool names, because which tools exist and how hard they may be leaned
  on is knowledge only the embedder has, and a general mechanism must not know a
  concrete tool (the no-smearing rule). `RuntimeContext` gains the PUBLIC builder
  method `with_tool_window(name, calls, seconds)` in the `without_title_derivation`
  shape, writing through the same sole-reference construction-time path as the
  global window; slice 16's rejection of a consumer surface covered the GLOBAL
  numbers, which are the operator's — a per-tool bound is inherently the
  consumer's. *Rejected:* framework-shipped defaults naming tools — smearing;
  *rejected:* a config registry — one builder method is the whole surface.
- **The per-tool check sits beside the global one, at the same door, refusing
  with the same prefix, 2026-08-31.** In `execute_ready_call`, after the global
  window check and before everything else, a fresh admission whose tool name
  carries a bound is counted — the same reverse fold over the conversation's call
  blocks, filtered to that name, offset-aware, the call under admission included —
  and refused when the count EXCEEDS the bound. The refusal rides
  `resolve_with_error` with the SAME machine prefix and a per-tool detail
  template interpolating the tool's name and ITS configured numbers; at the
  design bound the text reads, pinned byte for byte with `lookup_release` at
  six per sixty as the representative:
  `tool-call rate limit: this conversation has spent its 6 lookup_release calls for the last 60 seconds, and this call was not run. Answer with what you already have, or use a different tool, or wait before calling this one again.`
  Because the prefix is shared, per-tool refusals feed the SAME five-consecutive
  fold and the same forced turn end — a model looping on one bounded tool ends
  its turn the same way a burst does. Refused calls count against both windows
  (they are recorded calls). Interactive, claimed, resolved and human-approved
  calls pass the per-tool check exactly as they pass the global one — fresh
  admissions alone. *Rejected:* a distinct prefix per tool — the five-rule would
  need a prefix list, two decisions where one stands; *rejected:* checking
  per-tool BEFORE the global window — order is observable only in which text
  lands, and the global bound is the outer protection: it speaks first.
- **The failing lookup is bound by its embedder, 2026-08-31.** The assistant
  application (this framework's sibling consumer) sets `lookup_release` to six
  calls per sixty seconds at its runtime construction — one builder line in the
  app, landing with the app pin that consumes this slice. Recorded here so the
  slice's motivating number has a named home; the framework tree itself carries
  no such line outside tests.

## Acceptance criteria

- **AC1 — the per-tool window refuses.** With a tool bound at a small window and
  that window spent by recorded calls of that name, the next fresh admission of
  that tool resolves with the pinned per-tool refusal text and its handler never
  runs; a call to a DIFFERENT tool at the same moment runs untouched; the same
  tool in a second conversation runs untouched (both pinned).
- **AC2 — both windows compose.** With the global window cold and a per-tool
  bound spent, the per-tool refusal lands; with the global window spent, the
  global refusal lands whatever the per-tool state (order pinned); refusals
  count against both folds (pin).
- **AC3 — the five-rule sees per-tool refusals.** Five consecutive per-tool
  refusals on one open turn force the turn end exactly as global refusals do
  (pin through the Script harness).
- **AC4 — unbound tools are unbounded here.** A tool with no entry in the map
  never meets the per-tool check, under any global window state (pin).
- **AC5 — restart safety.** The per-tool fold derives from the ledger: reopened
  path-backed mid-window, the bound tool keeps refusing until its window
  genuinely recovers (pin, the slice-16 harness shape).
- **AC6 — the builder is the one surface.** `with_tool_window` is public, writes
  at construction time through the sole-reference path, and no other writer
  exists outside tests (pin: the map is immutable after construction).
- **AC7 — the checks.** fmt, clippy with warnings denied, the full suite, the
  doc build, exit codes read bare.

## Notes for launch

- Framework worktree branch `slice/per-tool-window` from `master`. Build first
  step: `git rebase master`.
- The app's one builder line (`lookup_release`, six per sixty) is the consuming
  application's own commit, landing with the pin move that ships this slice —
  named here, built there.
- The stale-claim sweep: slice 16's refusal-text pin comments describe ONE
  refusal text; wherever they say so exclusively, they widen to name the
  per-tool template beside the default one.
