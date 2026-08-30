# Slice 18 — a tool that ends the turn

Date: 2026-08-30. The direction this slice answers: models bred for long-horizon
autonomous work struggle to end a turn on their own — given any constraint they
keep finding SOMETHING to do, usually one more tool call. The remedy is a
do-nothing, no-reply-needed tool the model can call instead of looping or writing
filler text, and that call simply parks the session.

## The problem

A model bred for long-horizon autonomous work does not rest. When a turn has
nothing left to do, it finds SOMETHING — and under this framework's constraints
that something is a tool call, which summons a continuation round, which finds
something again. The tool-call window (slice 16) caps the spend and the forced
end stops the runaway, but both are backstops: the model never had a way to SAY
"nothing to do here" that the machinery could hear. Text is the only turn-ender
it owns, and a model that would rather act than write keeps acting.

This slice gives the machinery an ear for it: a tool whose successful resolution
ENDS the turn. No continuation is dispatched, nothing is owed to the
conversation, no latch is raised, no forced-end status fires. The consumer
registers concrete tools on the capability (a do-nothing, a no-reply-needed —
its choice); the framework knows only the property.

## The capability — a stamp, not a name

Nothing in the machinery may branch on a tool's name. The capability follows the
interactive stamp, the settled precedent for per-tool behavior variance:

- `ToolHandler` gains a defaulted method, `ends_turn() -> bool`, default
  `false`, beside `gated()` and `interactive()`.
- The property is read from the handler at the ONE seam that holds the handler
  at resolution time — the runner's `run_body` — and stamped onto the
  RESOLUTION write: `complete_tool_call_block` carries it, the
  `block_tool_result` row stores it, and `ToolResult` parses it back. The
  resolution block answers who owes its next move from its own stored data
  forever, the exact shape `ToolCall.interactive` set.
- `ToolResult::awaiting()` answers `None` for an ends-turn-stamped result and
  `Some(Awaiting::Model)` otherwise, unchanged. The frontier doctrine — a model
  turn is owed iff the cursor rests on a block whose ask is model — then rests
  the frontier with no new machinery at the TWO READS: `frontier_owes_turn`
  and the actor's delivery-time re-check both consult `awaiting()` already.
  The held-identity revalidation is new machinery and is owned below, in
  the release rule.

This is a resolution-time stamp in the `dispatch_anchor` precedent class: a
decision recorded once, at write time, on the row that must answer for it.

## The outcome sites — one stamp, read consistently

"Outcome" is defined at several kind-keyed sites, and each must give the
stamped result the right reading. The rulings, one by one:

- `resolved_in` and the store's `call_resolution_exists`: UNCHANGED. A park
  resolution is a real `ToolResult`; the call is resolved. Both match on kind
  and keep matching.
- `outcomes_anchored_in`: the fold EXCLUDES ends-turn-stamped results — it
  counts the outcomes that ask for a continuation, which is what both of its
  consumers mean by it. This is the one edit that keeps the release rule and
  the dispatch mark agreeing by construction: the actor's counting arm
  (`turn_continuation_due`) and the `answered_outcomes` mark the dispatch
  records read the SAME fold, so no filtered/unfiltered pair can drift after
  a park-plus-sibling round. Filtering inside `turn_continuation_due` alone
  is the wrong shape and is named as such: the comparison's two sides would
  count differently, and a sibling's later outcome would be silently
  dropped in the second round.
- `trailing_refusal_run`: UNCHANGED — a park SUCCESS is a result and breaks a
  refusal run, exactly as any success does.
- `unanswered_outcome_anchor`: the stamped result is NOT an unanswered
  outcome. It asks nothing of the model, so no later summons may have its turn
  attached to the dead park turn.
- `system_owed_call_anchored_in`: UNCHANGED, because it reads resolution,
  not the ask — the fold excludes any call `resolved_in` the ledger, and a
  park call is held only while unresolved, which is right.
- `turn_continuation_due` (the actor's release rule): a park-stamped outcome
  does NOT hold the turn open — which now follows from the shared
  `outcomes_anchored_in` fold above, with no second filter of its own.
- The held identity is REVALIDATED AT REUSE — the slice's named new work,
  corrected from this spec's own second draft, which put a release at the
  delivery-time stand-down. That site is unreachable for a park outcome:
  the scheduler signals the actor only when the drive's outcome owes a
  turn, a park tail owes none, so no signal arrives and a "release on
  delivery" is a dead edge. No new signal is added for it — a signal whose
  only job is to refresh an in-memory cache is machinery the stored fact
  makes unnecessary. Instead, the one site that resolves a dispatch
  anchor from the held identity — the summons-time reuse, which today wins
  "regardless of the tail" — first
  re-asks `turn_continuation_due` against the current ledger: the same
  shared fold, the one decision, now consulted where it matters. A hold the
  rule no longer supports is dropped and the summons takes a fresh anchor;
  a hold it still supports (an unresolved call, an owed continuation)
  survives exactly as today. This is restart-equivalent by construction:
  the in-memory hold is a cache over the stored rule, and both the running
  actor and a restarted one answer from the same rows. The known residual:
  a summons arriving while the park call is still UNRESOLVED attaches to
  the held identity, as any turn with an outstanding system-owed call does
  today — the resolution lands, the rule answers false from then on, and
  the next summons is fresh.

## The turn's end is a stored fact

Slice 08's doctrine stands: turn closure is stored, never inferred. The
park-stamped resolution block IS the stored closure — the stamp is on the row,
the walk reads it, a restart derives the same answer from the same block. No
separate `Status` marker is written for the ordinary park end: the resolution
already records everything a marker would, and a second record of one decision
is the defect rule 9 names. The `records_turn_end` key list and every doc
claiming exactly three machine keys or exactly two early-end precedents is
swept in this slice (see the sweep below).

The failure edge, stated: the resolution write is the commit point. If the
append fails, the call stays unresolved and the ordinary unresolved-call
machinery applies — nothing was released, nothing half-ended, and the retry
path is the one that already exists. No new failure machinery.

## The stamp's storage — a migration step and a spoken column

The column on `block_tool_result` lands as the NEXT NUMBERED MIGRATION STEP —
an `ALTER TABLE ... ADD COLUMN`, never an edit to the shipped `CREATE TABLE`.
The library has an installed base, and editing the shipped statement is the
trap the migrations doc names: every fresh-database test passes and every
existing store is stranded. Rows written before the column exists read as
UNSTAMPED — the parse takes the interactive column's shape, an optional read
defaulting to false — so an old ledger restarting under new code behaves
exactly as it did before the slice.

The write seam is reconciled with the public door, correcting this spec's
own earlier contradiction: `Store::complete_tool_call_block` is PUBLIC and is
both the runner's Done-path write and the out-of-band door, so it cannot
gain the stamp and stay untouched. The write lives once, in a crate-private
stamped variant; the public method keeps its signature and delegates to it
UNSTAMPED, and the runner's Done path calls the stamped variant with the
handler's answer. One write implementation, two doors, the stamp decided at
exactly one of them.

The read path is named whole, because a column written and never selected is a
column the projection can never speak: the blocks query's SELECT gains the
column — and its byte pin (`PINNED_BLOCKS_QUERY`) moves in lockstep, as the
lockstep test demands — and the tool payload's field fold carries it into the
parsed block. Write seam, select and its pin, fold, parse — all of them, or
the stamp is stored and mute.

An upgrade-in-place pin proves it, in the migrations suite's own precedent
shape: a ledger created at the previous version opens under the new one, its
pre-column resolutions read unstamped, and a new park resolution reads
stamped.

## What a park call does NOT do

- **A refused park call cannot end the turn.** The window check runs before the
  registry lookup, so at a hot window the park call is refused like any call —
  a Model-owing error that summons one more round, in which the model ends the
  ordinary way. This is stated, not exempted: a name-keyed bypass of the window
  would be smearing, and refused calls count as recorded calls by doctrine.
- **A park stamp silences only its own outcome.** When the model calls park
  beside a real tool in one round, the sibling's outcome keeps its debt and
  summons its continuation. This takes the fold edit already ruled above:
  `unanswered_outcome_anchor` today matches outcome KINDS with no ask
  consulted, so unedited it would anchor on the park result itself. The
  edited fold SKIPS ends-turn-stamped results and looks PAST the tail — a
  sibling's unanswered outcome behind a park tail still anchors the next
  summons. The widened residual is accepted and recorded: a park result no
  longer shields an OLDER stranded outcome (the documented store-failure
  residual) the way today's newest-outcome cap would, so a later summons can
  attach to an older dead turn than it could before — rarer than the defect
  the skip prevents, and the alternative (stop at the first stamped result)
  would drop a sibling's inheritance across a restart. The consumer's
  teaching should tell the model to call park alone, but the mechanism is
  safe when it does not.
- **A park tool cannot defer.** `ends_turn` is read where the handler is in
  hand — the runner — and the out-of-band door (`Store::complete_tool_call_block`,
  the one public door a deferred result takes) holds no handler and carries no
  stamp: a `Pending` outcome from an ends-turn handler would resolve
  UNSTAMPED and silently summon the model after a park. The ruling closes the
  door instead of widening it: the runner resolves a `Pending` outcome from an
  ends-turn handler as a loud `ToolError` — a tool that defers its own
  "nothing to do" is not doing nothing. The error text is pinned byte for
  byte, the repo's law for refusal copy:
  `an ends-turn tool must resolve at once: deferring the end of a turn is a contract defect, and this call is refused.`
  The public door's signature stays untouched (see the storage section's
  write-seam ruling). Errors never carry the stamp, so no error path can end
  a turn.
- **An interactive park tool cannot exist.** An interactive call resolves
  through the public door with a human's answer in hand and no handler in
  reach — the stamp could never be written, and the capability would
  silently never fire. The registration refuses the pairing the way
  gated-plus-interactive is refused today: a debug assertion at register
  time, so the contradiction dies at the consumer's first test run instead
  of shipping mute.
- **No latch, no forced-end status, no `tool_calls_exhausted`.** Park is the
  ordinary, wanted end of a turn — beside the ordinary text end, the two
  latching stops (the abnormal stop and the interrupt) and slice 16's forced
  end, and the calmest of them all.

## The spending lands already

Verified against the tree, correcting this spec's own first draft: usage does
not ride the empty commit — every request's final usage rides the stream-done
event, emitted at message end for EVERY stop reason, the tool-use stop
included, and the framework persists no usage anywhere. A park-ended round's
spending therefore lands today exactly as any tool round's does, the existing
ingestion pins already cover the tool-use stop, and this slice adds NOTHING
here. The section stands so no future reader re-opens the question from the
same wrong premise.

## The stale-claim sweep

Docs this slice falsifies, each corrected at its home:

- `records.rs` — the "exact three machine keys" claim on turn-end statuses (the
  park end is stored on a resolution, not a status; say so where the count is
  claimed).
- `agency/mod.rs` — "exactly one shape answers true" on `frontier_transparent`,
  where the wording assumes every rest is text or marker.
- Slice 16's "ending a turn early has two precedents" — the corrected line
  names the full post-park taxonomy, decided here so the correction is not
  the implementer's invention: a turn ends early four ways — the abnormal
  stop and the interrupt (both latching), the forced end and the park end
  (both between-rounds and non-latching) — and the ordinary end is text.
- The release-rule docs' "every outcome summons exactly one continuation"
  (`actor.rs`, stated at more than one home) — false for a park-stamped
  outcome; every home is found and corrected, not just the known ones.
- `agency/tool_result.rs` — the kind doc's "information the model has not
  seen yet, so it asks for a model turn", the most on-the-nose claim the
  stamp breaks.
- `agency/tool_call.rs` — `outcomes_anchored_in`'s doc, "every outcome a
  turn's calls produce summons one continuation", which the fold edit
  falsifies in both directions.
- `types.rs` — `Awaiting::Model`'s doc, "A model turn is warranted (user
  text, tool results, harness messages)": a stamped tool result now
  warrants none.
- `actor.rs` — the `answered_outcomes` field doc's "exactly the outcomes
  that request carries": a stamped park result still rides the request
  (the pairing demands it) while the edited fold excludes it from the
  mark.
- `records.rs` and `agency/mod.rs` — beyond the key counts, the CLAUSE "a
  marker is written wherever the runtime ends a turn as a stored fact" is
  the sentence park falsifies: the ordinary park end is a stored fact on
  the resolution row with no marker. The correction lands on that clause
  at both homes, not only on the counts.
- `ingestion.rs` — the message-end doc's claim that the owed empty block
  "carries the turn's usage": no usage is persisted anywhere; usage rides
  the stream-done event. Stale independently of park, corrected with the
  sweep because the spending section leans on the truth.
- `actor.rs` — the held identity winning "regardless of the tail" at the
  summons-time reuse: the revalidation ruling makes the hold conditional on
  the shared rule, and the doc moves with it.

## The consumer note

The consumer's commit — its own, in its own repository — registers the concrete
tools and the teaching: what the tools are called, whether do-nothing and
no-reply-needed are two registrations or one, and how the teaching invites them
in place of looping or filler text. Two consumer traps this slice must name in
its own docs:

- A consumer wrapper that forwards `ToolHandler` methods BY HAND silently
  answers `false` for a new defaulted method until it gains the forwarding
  line. The capability compiles and never fires. The framework cannot fix a
  consumer's wrapper, but the method's doc says this plainly.
- The park resolution's result text is the model-facing close of the turn; the
  framework leaves its content entirely to the handler.

## Acceptance criteria

- AC1: a registered tool with `ends_turn() = true`, called and resolved, ends
  the turn — no continuation dispatch is spent, and the NEXT SUMMONS takes a
  fresh anchor because the reuse re-asks the shared rule against the ledger.
  Pinned through the script harness, and the pin is NAMED BUILD WORK, not a
  pass-only criterion: the harness today registers no ends-turn tool and has
  no script ending on a tool round with nothing after, so the slice adds the
  test tool, the script shape, the request-counter assertion that the
  dispatch count stays where it was, AND the second summons whose recorded
  anchor differs from the park turn's — including the interleaving where the
  close ran before the park result committed, the exact shape the
  revalidation exists for.
- AC2: the stamp survives the walk and a restart — the fact derives from the
  stored resolution block alone, never actor memory. Pinned.
- AC3: a park call refused by the window does NOT end the turn: the refusal
  summons one continuation and the five-rule counts it like any refusal.
  Pinned.
- AC4: a park call beside a sibling call silences only itself. The pin's
  script CONTROLS THE ORDERING: the sibling's outcome lands last, the tail
  owes, and the continuation is summoned — that is the shape the pin proves.
  The other ordering, the park result landing last, rests the frontier and
  defers the sibling's continuation to the summons-time rule, which the AC1
  pin already exercises; the deferral is stated once here, not pinned twice.
- AC5: an existing ledger upgrades in place — a store created at the previous
  schema version opens under the new one, its pre-column resolutions read
  unstamped, and a new park resolution reads stamped. Pinned in the
  migrations suite's precedent shape.
- AC6: no machinery branches on a tool NAME anywhere in the change —
  the property rides the handler and the stamped row only.
- AC7: the stale-claim sweep is complete: the named homes read true, and a grep
  for the falsified claims finds no surviving copy.
- AC8 (added 2026-08-31, from review): an addressed member message absorbed
  while an ends-turn round's window was open is never buried. After the round
  closes with the message sitting behind the stamped result, the ledger owes a
  model turn and the next summons happens without any further inbound — through
  the machinery the ledger already has for a message stranded behind a
  turn-ending tail (the turn-end marker or the frontier walk), never a new
  signal. Pinned by a test that absorbs the message during the round and
  asserts the summons; the ordering the pin claims is observed, not slept for.
  The rejected alternative: relying on the next unrelated inbound to surface
  the buried message — a member's question would wait on someone else writing.

## Bounds

- No new dependency. The one schema change is the resolution column, landed
  as a numbered migration step with its select, fold and parse — nothing
  else in the schema moves.
- The consumer's tools, teaching, and wrapper forwarding are the consumer's
  commit, not this one.
- Nothing about the window, the five-rule, or the forced end changes; park
  lives beside them.
