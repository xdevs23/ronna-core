# Slice 16 — the tool-call window, and the forced end of a spending turn

Date: 2026-08-30. In a production deployment, one turn looped a failing lookup tool
for hundreds of rounds — the model stopped with ToolUse every round, the tool ran,
the continuation stream request went out, the model called the same tool again —
until the provider refused payment. One paid request per round, and nothing anywhere
bounds rounds. The operator of that deployment decided the shape of the fix, and
this slice builds exactly it: a sliding-window rate limit of 60 tool calls per
minute, counted per conversation; an over-limit call is not executed and fails
immediately with a helpful error the model reads; the model may keep calling and
each call keeps failing until the window recovers; and five consecutive rate-limit
errors force the turn to end, because a history that deep in refusals has gone bad
and the embedder will want to compact it.

## Grounding

**The loop and its one dispatcher.** The reactive scheduler wakes on store changes
and owes a turn from the frontier (`actor.rs:1259-1288`, `ratchet.rs:191-193`);
`ConversationActor::handle_blocks_ready` (`actor.rs:983`) is the turn loop's only dispatcher (the title-derivation actor sends one tool-less stream and cannot loop) —
it stands stale signals down, resolves the anchor (a held `open_turn` else a fresh
one, `actor.rs:1074-1076`) and sends the paid provider request at `actor.rs:1121`.
A tool round closes through `close_dispatch` and `settle_turn_identity`
(`actor.rs:773-799, 917-971`): while a continuation is due the turn identity is
held and the next `handle_blocks_ready` is the continuation. Nothing bounds rounds:
the drain deadline bounds a stream's tail, the provider retry budget bounds
rate-limit waiting per stream cycle (`bind.rs:44-52, 384-385`; its own doc says "the whole turn", one stream cycle per turn there), and no
document records the absence as a decision — it is silence.

**The single admission door, and what a failed tool looks like.** Every call enters
through `ToolRunner::insert_call` (`runner.rs:192-226`, insert-first) and every
body executes through `execute_ready_call` (`runner.rs:260-318`), "the single place
admission is enforced" by the runner's own header. A refusal is
`resolve_with_error` (`runner.rs:484-517`): the call block exists, the body never
runs, and a durable `tool_error` block carries the text
(`store/tool_calls.rs:100-123`), anchored on the call's dispatch anchor and
replayed to the model as an ordinary tool-result part (`agency/tool_error.rs:47-53`,
golden at `providers/render/tests.rs:255-325`). The unknown-tool refusal and the
gate's `Refuse{reason}` are the teaching-text precedents (`runner.rs:290-346`).
Interactive calls never reach the runner (`tools/mod.rs:133-147`).

**Counting is a fold, by doctrine, over the stamps as they really are.** Every
production insert names `created_at` itself — `insert_block` stamps
`now_iso8601` (`store/messages.rs:174-177`, `store/mod.rs:758-762`): RFC3339 at
millisecond resolution in LOCAL time with a fixed numeric offset; the fork
cloner copies the source block's stamp (history keeps its time) and drafts
stamp their own the same way — so the schema's UTC `datetime('now')` default
(`migrations.rs:105`) fires only in fixtures, and every tool call rides
`insert_block`. The window
fold therefore parses offset-aware instants and never compares lexically (two
offsets straddling a DST boundary do not sort as strings), and its "now" comes
from the same clock that writes the stamps (stated on its own terms; slice 15's one-reading rule for date markers is the neighboring precedent, not a general law).
Every call joins its conversation (`migrations.rs:110-115`) and every outcome
copies its call's dispatch anchor (`tool_calls.rs:75-76, 112-113`). "Derived,
never stored — if a fact can be folded from the ledger, it is not a column" is
the repository's own law, and the tools layer's war story forbids admission
decisions that travel in memory while the ledger says otherwise
(`tools/mod.rs:21-36`).

**Ending a turn early has two precedents and both are wrong here.** The abnormal
stop (`ingestion.rs:1093-1104`) and the interrupt (`actor.rs:613-674`) both end
turns from inside a stream and both LATCH the conversation; the forced end this
slice needs fires BETWEEN rounds, when no stream is open, and must not latch — the
conversation lives on. Only `settle_turn_identity` releases a held `open_turn`
today; leaving it held inherits the dead turn's anchor onto the next summons
(`actor.rs:743-760`). A status block projects no model content (`records.rs:70-74`); the turn-end
status family is walk-TRANSPARENT (`records.rs:45-50`), and the loop rests
through the summons bound (`ratchet.rs:234-263`), not through an opaque cap.

## Decisions taken with this slice

- **The window is a ledger fold, never memory, 2026-08-30.** "Tool calls in this
  conversation in the trailing sixty seconds" is a count over the conversation's
  tool-call blocks by their own `created_at` stamps; "five consecutive rate-limit
  errors" is a fold over the open turn's tool OUTCOMES — result and error
  blocks, ordered by block id, scoped to the open turn by dispatch anchor
  (the outcomes-anchored idiom, `actor.rs:849`) — asking whether the trailing
  five all carry the refusal prefix. The outcome subsequence, never raw block
  adjacency: every refusal round appends the NEXT round's call block after the
  previous error, so consecutive tool-error block ids never occur in the
  ledger and a literal block-run reading would never fire. Both are
  reads at the existing seams, both survive a restart, and no in-memory copy is
  authoritative. The window compare parses the stamps' real form (offset-aware
  RFC3339 instants, millisecond resolution) and takes "now" from the stamp
  writer's own clock — never a lexical compare, never a second clock. The
  predicate, exactly: count every recorded call in the trailing window, the one
  under admission included, and refuse when that count EXCEEDS the window — the
  sixty allowed calls all run, the sixty-first inside any trailing minute is the
  first refused. Out-of-band calls record NULL anchors and sit outside the open
  turn's consecutive run; they can only shorten it, never fake it — safe by
  direction, and the window still counts them. *Rejected:* a window in the runner's mutex or the actor's state —
  a decision in memory while the ledger says otherwise, the exact shape the tools
  layer's recorded war story forbids, and it forgets on restart while the burn it
  bounds does not.
- **The refusal composes first at the admission door, with this exact copy,
  2026-08-30.** In `execute_ready_call`, BEFORE every other check, the
  unknown-tool refusal included: when the conversation's window is spent, the
  call resolves with the refusal and nothing else runs — otherwise a hot window
  with an unknown tool name would loop on the teaching text, a second unbounded
  shape. The refusal is two constants, one decision each: the stable machine
  prefix the consecutive fold matches with a starts-with test,
  `tool-call rate limit:`
  and the rendered detail, whose one template interpolates the CONFIGURED
  values as plain numbers of calls and seconds (no unit-word branches) so an
  overridden deployment never lies about its own numbers; at the defaults the
  full text reads, pinned byte for byte:
  `tool-call rate limit: this conversation has spent its 60 tool calls for the last 60 seconds, and this call was not run. Answer with what you already have, or wait before calling tools again.`
  Interactive calls are counted by the window (they are recorded calls) and
  never refused by it: interactive admission belongs to the human, stated as the
  recorded boundary. The refusal also never touches a call that is already
  claimed, already resolved, or carrying a granted human approval — a re-driven
  pending call whose body ran, or a deferred call the human admitted, proceeds:
  refusing it would falsify "this call was not run" in the ledger and orphan
  the real result at its conditional write. The window governs FRESH admissions
  alone. The claim record is the runner's own in-flight set (`runner.rs:67`),
  peeked at the refusal site — and it is per-process, an accepted residual
  stated openly: a re-driven pending call whose body ran BEFORE a restart
  carries no claim mark afterwards, so a hot window refuses it, the pinned
  "not run" text lands false for that call and the pre-restart result loses
  its conditional write — rare, restart-shaped, and no worse than the
  check-to-append race the forced-end decision records for itself. With the
  window cold, the unknown-tool and gate refusals
  behave exactly as today, and their errors reset the consecutive run as
  ordinary failures — they are not the model looping on the window.
  *Rejected:* a machine-key column for tool errors — the error string's own
  fixed prefix is the native form, the same way status blocks carry their
  documented keys; a handler whose own error opened with the exact prefix would
  feed the five-rule, accepted knowingly: it is vanishingly unlikely, and five
  consecutive claimed rate limits ending the turn is defensible behavior even
  then; *rejected:* baking the numbers into one un-interpolated
  string — a test or deployment overriding the values would ship a message that
  lies.
- **Refused calls count, 2026-08-30.** The operator's design has every over-limit
  call keep failing until the window recovers; a window that ignored refusals
  would drain while the model spams and hand the whole protection to the
  five-rule. Every recorded call block counts, executed or refused.
- **Five consecutive refusals end the turn between rounds, without latching and
  without burying anyone, 2026-08-30.** The check runs in `handle_blocks_ready`,
  before the dispatch spends: when the open turn's trailing five tool outcomes
  are all rate-limit refusals (the outcome fold of the first decision), the
  dispatch stands down instead of sending. The turn the check scopes to is
  the RESOLVED anchor — the held `open_turn` else the fresh anchor the
  ledger owes (`actor.rs:1074-1076`) — never the held field alone, so the
  rule survives a restart that clears the held identity while the ledger
  still owes the dead turn's continuation. The forced end,
  in order: an anchored status block with the machine key `tool_calls_exhausted`
  joins the ledger, and the key JOINS `Status::records_turn_end`
  (`records.rs:42-50`) so the walk reads through it — the tree's own recorded
  burial defect is exactly what an opaque non-latching end marker causes
  (`actor.rs:880-892`), and a member's message landing in the check-to-append
  gap must not wait for a second message; the loop still rests in the quiet case
  because the summons bound disowns the ended turn (`ratchet.rs:234-263`). Only
  after the append succeeds is the held `open_turn` released — a new, named
  release edge beside `settle_turn_identity`'s — so the next summons opens a
  fresh turn with a fresh anchor; if the append fails, nothing releases and the
  next drive re-enters the check off the durable fold and retries — no latch,
  no retry queue; in a quiet system that next drive is the next store change or
  member message, and the wait is the stated residual (no dispatch is spent
  meanwhile). One residual is inherited from the marker semantics, the same one
  `settle_turn_identity` records for itself: an outcome that commits between
  the five-check's snapshot and the status append reads as answered by the
  marker and never gets its continuation — the sibling edge is no cleaner than
  its predecessor, said openly. The conversation is NOT
  latched, and the count is per open turn, never per conversation lifetime. The
  embedder observes the status block (BlocksChanged and the durable fold);
  keying a compaction on it is the embedder's own policy. *Rejected:* an opaque
  status key — the recorded burial defect; *rejected:* riding the StreamError
  edge — the error edge latches, and this end is a decision, not an error;
  *rejected:* a new bus event vocabulary — the keyed status block IS the native
  signal; *rejected:* a non-rate-limit error breaking the turn — only the
  refusal run means the model is looping on the window.
- **The values live on the runner, in one lock-guarded field, 2026-08-30.**
  `WINDOW_CALLS = 60`, `WINDOW_SECS = 60`, `CONSECUTIVE_LIMIT = 5`, the
  operator's numbers, are the defaults of a config held BY THE RUNNER — a
  lock-guarded field beside its existing in-flight set, since the runner is the
  admission owner and the check is its own (`execute_ready_call` reads
  `self`; the deliberately blind `AgencyCtx` is untouched — "the restriction is
  the point" stands). The actor's five-check reads the SAME field through the
  context's public runner accessor (`RuntimeContext::runner()`), so one home
  serves both readers. Writing it is crate-private: a `RuntimeContext` builder
  method in the `without_title_derivation` shape sets the runner's field
  through a crate-visible setter, tests build a context with a small window,
  and no consumer-facing surface appears — the numbers are the operator's
  decision. Verified against the tree before deciding: the runner is built
  eagerly inside `RuntimeContext::new` (`actor.rs:123`) behind a private field
  with a public getter (`actor.rs:164-165`), `AgencyCtx` carries only the
  conversation id, store and bus (`agency/mod.rs:105-112`), and the runner
  already holds a `Mutex` for in-flight state — the config field is the same
  idiom. *Rejected:* a consumer-facing config surface — no such registry exists
  and the numbers are decided; *rejected:* a `RuntimeContext` field read at the
  check — the admission site never holds the context, only its blind
  derivation, the trap two review rounds circled; *rejected:* widening
  `AgencyCtx` — the struct's own doc records the blindness as the point;
  *rejected:* threading values through the public call signatures — a
  delivery-time snapshot with a public-surface ripple.
- **The cost arithmetic is stated honestly, 2026-08-30.** The window bounds TOOL
  CALLS, not paid requests: a refused call still buys its continuation round, so
  a runaway burns up to the window plus five refusal rounds before the forced
  end, and each later summons costs five paid requests (the dispatch buying
  each refusal; the stand-down after the fifth spends nothing) while the window
  stays hot. Bounded and small, and the operator
  chose the numbers; recorded so nobody reads the window as a request ceiling.

## Acceptance criteria

- **AC1 — the window refuses.** With the window spent in one conversation, the
  next call resolves with the pinned refusal text and its handler never runs;
  a second conversation's calls run untouched at the same moment (per-conversation
  scope, both pinned).
- **AC2 — refusals keep the window spent.** A run of refused calls holds the
  window at its limit (pin: refusals count as calls).
- **AC3 — the forced end, two observations.** Five consecutive rate-limit
  refusals on the open turn: the would-be continuation dispatch after the fifth
  stands down — no provider request, the `tool_calls_exhausted` status lands
  anchored on the ended turn and walk-transparent, `open_turn` is released, the
  conversation is not latched. Then a fresh summons opens a fresh turn with a
  fresh anchor — which, on a still-hot window, dispatches and refuses again by
  design (both pinned).
- **AC4 — an ordinary error resets the run.** Four refusals, one non-rate-limit
  tool error resolved out of band on a pending call (`fail_tool_call_block`, the
  public store surface), another refusal: the turn does not end — the run is
  id-ordered, so it reads the same however the window moved meanwhile (pin).
- **AC5 — restart safety.** The fold derives from the ledger: reopened
  mid-window, the conversation keeps refusing until the window genuinely
  recovers — pinned through a path-backed store reopen (named harness work: the
  in-memory idiom cannot reopen; the first handle drops to release the lock and
  the rebooted runtime is driven by an append). The five-rule survives the same
  reopen: refusals landed before the stop still force the end after it,
  because the check reads the resolved anchor, not the held field (pin).
- **AC6 — interactive calls are counted, never runner-refused.** Pinned at the
  counting site; the refusal-site interactive skip is defensive only — no
  interactive call reaches `execute_ready_call` today (`tool_call.rs:228-241`)
  — and is stated as such, not tested through an unreachable path.
- **AC7 — the multi-round pins stand, on an extended harness.** The existing
  Script-driven multi-round and close-edge tests pass unchanged; the Script
  vocabulary gains a parameterized many-round variant to drive AC3/AC4 (named
  build work, not a pass-only criterion); the replay goldens cover the
  result-plus-error shape the forced end lands.
- **AC8 — the checks.** fmt, clippy with warnings denied, the full suite, the doc
  build, exit codes read bare.

## Notes for launch

- Worktree `~/projects/agent-ledger-toolcap`, branch `slice/tool-round-ceiling`,
  from `master` (`f2bf250`). Build first step: `git rebase master`.
- The key vocabulary's recorded claims that go stale with the widening are
  touched in the same change: `Status::records_turn_end`'s own doc ("the exact
  two machine keys the close writes", `records.rs:42-44`), the
  `frontier_transparent` doc's "Exactly one shape answers true"
  (`agency/mod.rs:156`), and the coordination follow-up pinning transparency to
  "the two turn-closure keys" (`docs/coordination/
  08-dispatch-identity-follow-ups.md`, item 7), and the `Status` struct-level
  doc's close-appended framing ("appended by a close that ends a turn over an
  unanswered outcome", `records.rs:16-25`), which recurs inside the cited
  `agency/mod.rs` paragraph (`:151-164`). The runtime reference's
  cancellation bullet concerns the interrupted key alone and stays as it is.
- The consumer's auto-compaction on the status key is the consumer's own unit
  (with `/compact`), deliberately out of this slice.
