# Slice 8 — the consumer follow-ups this slice unblocks

Date: 2026-08-22. Slice 8 (dispatch identity) ships the dispatch anchor and the
reworked turn-close edge. The sibling consumer — developed against this library
by sibling checkout, per its own decision record on the dependency — carries
these follow-ups, which were waiting on exactly these facts. They are one
coordinated unit on the consumer's side, not drive-by edits, because the
second and third only hold once the first is in place. Item 6 lists the
reserved-name breaks the slice introduces beside the field itself.

1. **The trailing done on its scripted providers.** The turn now closes on the
   stream's closed signal, which the reader emits at the provider's `Done` —
   real wires already send it after every completed turn, and this repository's
   scripted providers gained it as enumerated edits in this slice. The
   consumer's scripted providers need the same trailing `Done` after each
   completed turn's events, or its suites stall on a dispatch state that never
   closes.
2. **The duplicate-turn canary un-ignores.** The consumer's tools suite carries
   an ignored canary test for the duplicate-turn redispatch, written to flip
   when the framework fix ships. It has shipped: a message appended in the held
   post-message-end window no longer dispatches a second turn, and the close's
   re-check dispatches the absorbed message's turn deterministically — pinned
   here with real interleaving.
3. **The echo tolerance comes out.** The consumer's tool-scripted provider
   answers a repeated call round as an empty turn and counts it, so the defect
   could not skew its suites. With the defect closed, the tolerance is dead
   code that would mask a regression; it and its `echoes` counter come out.
4. **The block-struct field.** `Block` gained `dispatch_anchor: Option<i64>` —
   the compile break this slice's design accepted and recorded. Every consumer
   `Block { .. }` literal gains the field; loads through the store carry it
   automatically.
5. **The registration floor lifts.** The consumer floored tool registration at
   its lowest authority because nothing recorded which message summoned a
   turn, and reconstructing it from stored shape was adversarially refuted
   (its decision record closing that question is the citation for the
   "derived, never stored" exception in this repository's invariants). The
   fact now exists: from a tool-call block, one `find_block` on the call's
   anchor loads the summoning frontier, in round one and in a continuation
   round alike. For a turn summoned by a message, that frontier IS the
   message; a null anchor is the documented out-of-band answer to fold to the
   floor; and a turn whose dispatching frontier is itself a null-anchored
   turn product — the out-of-band path's tool result — anchors on that result
   block, not on a message, so the gate's fold decides that non-message shape
   beside the null one.

   **The recommended gate form, for every cross-author shape:** read the
   anchor as the LOWER BOUND of the turn's authors, never as the single
   author. The dispatched request is built from the ledger up to the call, so
   the sound admission is the minimum-authority fold over the interval from
   the summoner to the call, `[summoner..call]`: fold in every author whose
   blocks lie inside it, admit at the minimum. With the anchor as the lower
   bound the fold is well-defined and it covers the inherited shapes by
   construction. The instructive one: a message absorbed while a turn is
   open is answered by the turn's continuation, and the continuation's calls
   anchor on the PREVIOUS summoning message even though the request carries
   the absorbed text — for EVERY placement of the absorbed message. Amended
   2026-08-22, after the consumer's adversarial verification proved the
   tail-derived inheritance insufficient (a message absorbed after a round's
   result became the continuation's dispatching tail and re-anchored the
   turn onto itself — the proven escalation): the turn's identity is actor
   state, held from the first dispatch until a close that ends the turn, so
   a tool-use close leaves it open and every continuation reuses it
   whatever the tail is. Refined 2026-08-22 after the fifth break proved
   the unconditional hold leaks onto the next unrelated summons: a tool-use
   close keeps the identity only while a continuation is genuinely due in
   its snapshot, and a lost tool round (a truncated lifecycle that recorded
   no call) ends the turn like every other close, so the fold never sees
   one turn's summoner stamped on another turn's products. Amended
   2026-08-23 after the sixth break proved the genuinely-due test's own two
   arms leak: the release rule now keeps the identity iff a tool outcome
   anchored on the turn has not yet had its continuation dispatched — the
   actor marks the answered count from each dispatch's own snapshot — or an
   unresolved, non-interactive call with a non-empty id, anchored on the
   turn, exists at the close. A frontier owing the model no longer keeps
   the identity (a model-owed tail is someone's summons — a message
   absorbed into a lost round's window summons a turn of its own), and a
   parked interactive call ends it (the approval's later outcome carries
   the turn's anchor, and the tail inheritance re-attaches at that
   dispatch). Amended 2026-08-23 after the seventh break — a regression
   of the sixth fix's release, plus the pre-existing restart hole —
   proved the fresh resolution read only the tail, so a released turn
   lost its identity whenever a message was absorbed behind its outcome
   and the continuation anchored on the absorbed line, the original
   escalation again. A fresh dispatch is now LEDGER-FIRST: a
   null-anchored model-owed tail inherits the newest UNANSWERED
   outcome's anchor — no assistant text and no status block with that
   anchor after it in the snapshot — so a parked approval's resolution
   and a restart-recovered round keep the original summons past any
   absorbed message, and the actor's held identity is a consistency
   cache over that ledger-derivable fact. Settled 2026-08-23 after the
   eighth break proved four more side-effect-free ends (a parked
   interactive call in a later round, a lost later round, the abandoned
   close after a result, the error edge after a result) each stranded a
   turn's last outcome as unanswered forever and captured the next
   unrelated summons: turn closure is now a STORED fact — a close that
   ends the held identity while its snapshot holds an unanswered outcome
   for the turn appends a status block anchored on the turn, machine-keyed
   `turn_ended:closed` or `turn_ended:errored` by the closing edge (the
   interrupt teardown keeps its own `interrupted` status as that end's
   record). The walk needed no change — it already honors status
   markers — and this is the durable failed-turn record the consumer's
   improvements list has wanted since its live-model unit. One residual
   for the gate, accepted and documented, narrowed by the marker: only a
   turn ended while the store itself cannot write — the unstamped
   store-failure shape — leaves no status marker, so its outcome can
   over-attach one later summons — the direction is over-decline, and it
   heals at that turn's close. The anchor names the turn's dispatch identity,
   never the newest author in the request; the fold still decides the shape
   correctly, because the absorbed message lies inside `[summoner..call]`
   and its author is folded in. With
   that reading the floor lifts into the intended minimum-authority
   provenance gate: null folds to the floor, a non-message frontier is one
   more author the interval covers. Amended 2026-08-30 by the ends-turn
   stamp: a turn a tool ended is stored on its own resolution row and asks
   for nothing, so no close and no delivery ever sees it — the summons-time
   reuse therefore re-asks the release rule against its own snapshot before
   resolving an anchor from the hold, and a hold the rule no longer supports
   is dropped for a fresh one. The outcome folds read the same stamp: an
   ends-turn resolution is not an outcome that asks for a continuation, in
   the counting arm and in the unanswered-outcome walk alike, so a sibling's
   own outcome behind an ends-turn tail still inherits its turn.
6. **The reserved-name breaks, by name.** Beside the struct field, the slice
   reserves the name `dispatch_anchor` on both consumer-facing surfaces:
   - `RESERVED_FIELD_NAMES` (the block serializer's header-owned keys) now
     contains `dispatch_anchor`. A consumer block kind whose payload carries a
     field of that name has it DROPPED from the serialized form with a logged
     warning — the row header wins, so a payload cannot forge a turn identity
     the header never recorded. Rename the payload field.
   - `RESERVED_COLUMNS` (the content-table descriptor validator's list) now
     contains `dispatch_anchor`. A consumer descriptor declaring a column of
     that name is REFUSED AT STORE OPEN, at runtime: the store's open fails
     with the descriptor-validation error before any query is served, so the
     consumer's process does not start until the column is renamed. This is
     the sharp edge of the two — it is an open-refusal, not a silent drop.
7. **The status kind's machine key, and the frontier transparency.** The
   `Status` kind now carries its stored machine key as a public field
   (parsed from the row like every other kind field), so a consumer
   constructing a `Status { .. }` literal gains the field; parses need no
   change. The key feeds the agency trait's new `frontier_transparent`
   hook (2026-08-23, the verified burial defect): the owed-turn frontier
   reads THROUGH a dead turn's trailing closure run — the trailing
   turn-closure markers plus the trailing blocks anchored on the turns
   they disown — so an addressed message absorbed anywhere in a dead
   turn's window, the stretch between a call and its result included,
   still summons its turn at the close's re-check — anchored on itself —
   instead of resting behind the marker forever. A composed kind enum picks the hook up by regenerating
   the derive; a hand-written `Agency` impl that delegates per hook adds
   the one-line delegation. Consumer statuses and the interrupt's
   `interrupted` stay opaque — transparency is scoped to the rows that STORE
   a turn's end. On the status kind those are the machine keys published as
   constants: the two the close writes, and (2026-08-30, the tool-call
   window) `tool_calls_exhausted`, which the forced end writes between rounds
   when a run of the window's refusals ends a turn. Beside them, and read
   through the same hook, is the resolution an ends-turn tool stamped
   (2026-08-30, the ends-turn stamp): that row is its turn's stored end, so a
   message absorbed into the round's window is surfaced from behind it the
   same way.

## Residuals recorded at the slice's close, 2026-08-23

Two verified residual shapes stay open past the final verification, both
outside the closed defect and neither a regression:

- **A post-marker block re-buries the window (error edge only).** When a
  response already in flight commits a block anchored on the dead turn AFTER
  its closure marker, the trailing-run walk stops on that block — anchored
  blocks past the marker stay opaque so a late approval outcome can still
  resume — and a message absorbed in the dead turn's window rests until the
  next append. The pre-fix walk rested at the same shape, so the widened read
  loses nothing; the gap is that an inert late block (a text, a status) could
  be read through where an outcome must not be. Closing it means telling
  those two apart at the walk, judged not worth the sharpened bound for an
  in-flight race on the error edge alone.
- **An anchor-less closure marker disowns nothing.** The walk needs the
  marker's anchor to name the dead turn; a marker written without one is
  skipped and the dead turn's own outcome would redispatch it. The library's
  one close site always writes the anchor, so this is reachable only by a
  consumer writing the machine key through the bare write surface — recorded
  as a contract on that surface, not defended in the walk.
