# Unit 23 — the empty completed turn is a real text block on the ledger (framework)

Date: 2026-08-24. A consumer is removing an outbound sentinel convention (a magic answer
string that meant "no response") in favour of the model simply ending its turn with no
user-visible text — the native form of "nothing to say". For that to be sound, the
framework must treat an empty completed turn as a first-class event: **an empty assistant
text block, committed to the ledger, empty included.** It currently discards it, which is a
design error carried in from a prior implementation; this slice removes the discard and
records the empty turn faithfully. A second, separable capability rides along: a signal that
distinguishes user-visible text from thinking, so the consumer can raise a compose cue only
for real replies. Both are grounded in a cold mechanism probe and a soundness review of this
repo; the file:line receipts below are theirs.

## Grounding (what the probe and the review found)

**Discarding the empty turn is the bug, not a safety measure.** Today the stream reader
throws the empty turn away: `finalize_streamed_text_tail` returns `NothingToPersist` when no
streaming block was opened, and *discards* a streaming block whose content `is_empty()`
rather than committing it (`ingestion.rs:803-850`; the `ContentFinal`/`TextFinal` empty arms
route to the same discard at 705-712 and 856-861; the test at 2328-2356 pins the discard).
The stated justification — "a committed empty text block renders as an empty content part,
which some vendors reject on every later turn" (`ingestion.rs:798-802`) — does not hold: a
real request to the deployed provider on 2026-08-24 replayed an assistant message with empty
content (both `""` and `null`) in history and it was **accepted, no error**. Empty is a
value; the discard is a prior misimplementation to be removed, not worked around.

**What the discard costs: the frontier wedges.** The owed decision is a fact about the
frontier *block*: `ratchet::frontier_owes_turn` is `K::from_block(tail).awaiting() ==
Some(Model)` (`ratchet.rs:191-193`), and an addressed user message awaits the model by a
write-time stamp a running turn never clears (that stamp is the consumer's own derivation and
lives outside this repo's fence; the in-repo fact the pins exercise is `Text::awaiting()`,
unconditional by role — `agency/text.rs:30-33`). What moves
the frontier off it is the turn leaving a newer, non-owing tail block — an assistant `text`
block awaits nobody (`agency/text.rs:30-33`). A non-tool turn that produces no text and
discards leaves **no such block**: the owing message stays the tail, the turn is over, and —
as the soundness review established against the tree — the actor does not autonomously
re-drive (its close nudges the scheduler only when `owed_turn_deferred` is set, i.e. only for
a message absorbed *during* streaming — `actor.rs:796-798,1003`; the reactive loop's only
wakes are the latch, that nudge, and store changes — `actor.rs:1318-1338`). So the turn wrote
no row, nothing wakes the actor, and `send_admissible` refuses a new user message while the
model owns the frontier: the session is **wedged — unreachable from every direction**. On a
busy multi-conversation deployment the unfiltered store-wide change watcher
(`actor.rs:1318,1333`) does wake the actor on other conversations' rows, and each such wake
re-dispatches the same empty turn (no per-anchor dedup — `actor.rs:1045-1076`). Either way
the debt never closes. (The docstrings at `actor.rs:268-272,726-728` describe this exact
"wrote nothing forever" hazard as a thing the current rest-behaviour avoids — the fix is to
stop writing nothing, not to add a re-drive.)

**Committing the empty block fixes it by construction, and carries two more things a
discard throws away.** An empty completed response is a real event: the model chose to say
nothing and *spent tokens doing so* (a real request on 2026-08-24 showed the provider
returning empty text after ~150 reasoning tokens). The empty text block is (a) the model's
move, which settles the frontier the same way any assistant text does; and (b) a faithful
history entry, so on replay the model gets its own empty message back rather than a hole.
Those spent tokens are accounted for where usage actually travels — the stream-done event the
message end emits for every stop reason; no block carries usage and none ever did (verified
2026-08-30). Recording an
*error* instead would forge an event that never happened; a genuinely failed response never
reaches finalize (the stream raises and the death is recorded elsewhere).

**The placeholder asymmetry that keeps tool turns clean.** The streaming placeholder is
created **lazily** on the text channel's first delta / `text_block_start`
(`ingestion.rs:521-562`, insert at 550). A turn that ends in tool use never reaches the
non-tool text finalize, so it leaves no orphaned empty block. Finalize is unconditional only
once a non-tool turn reaches it. Emptiness is orphan-shaped only when nothing decided to end
there — so the fix commits empties for non-tool turns without littering tool turns.

**No clean text-vs-thinking signal exists.** `StreamStatus{label:""}` fires at both
`text_block_start` (`ingestion.rs:531-535`) and `thinking_start` (`ingestion.rs:585-589`), and
the `text_delta` lazy-create emits nothing (`ingestion.rs:550`). A consumer that wants to raise
a cue only while user-visible text streams has nothing reliable to key on.

## Decisions taken with this unit

- **Remove the empty-turn discard; a non-tool turn that COMPLETES commits its text channel's
  final state, empty included, 2026-08-24.** `finalize_streamed_text_tail` commits an empty
  streaming block instead of discarding it, and a non-tool turn that reaches finalize with no
  streaming block still lands an empty final text block (the unconditional finalize); the
  `ContentFinal`/`TextFinal` empty arms commit rather than discard. The lazy placeholder is
  unchanged, so tool turns leave no orphan. The existing tests that pin the discard
  (`ingestion.rs:2328-2356` and the poisoning-guard doc/tests) encoded the misimplementation
  and are rewritten to pin the commit — a spec-compliance reviewer should read those test
  changes as the intended inversion of a bug, not a regression.
- **Only NORMAL completion commits the empty block — the error and teardown edges do NOT,
  2026-08-24.** The empty commit rides the successful `message_end`/`EndTurn` finalize path.
  A turn that ERRORS (the stream raises before or during content) or is TORN DOWN must NOT
  land an empty block: the error edge exists so the turn latches and, on unlatch, resumes as a
  new decision resolved from the tail (`actor.rs:754-759`; the scheduler re-derives `owes_turn`
  fresh on the latch change, `actor.rs:1309-1338`). Committing an empty block on an error would
  bury the still-owing message and drop it forever instead of retrying — strictly worse than
  the wedge this unit fixes, and reachable on an ordinary transient provider error. A failed
  response never reaches the finalize that commits; the death is recorded by the existing error
  path. The implementer scopes the commit to the completion edge and leaves the error/teardown
  edges' latch-and-retry untouched.
  *Rejected:* a transparent turn-closure marker (an earlier draft of this slice) — it closes
  the debt but discards the usage and leaves a hole in the model's replayed history; the empty
  block is the honest record and needs no new mechanism. *Rejected:* recording an error for an
  empty turn — it forges an event that never happened. *Rejected:* keeping the discard and
  hiding the wedge behind a re-drive — it treats the symptom and the token usage is still lost.
- **The empty block settles the frontier as any assistant text does, 2026-08-24.** No marker,
  no special case: an assistant `text` block awaits nobody, so `frontier_owes_turn` is false on
  it and the debt closes exactly once. The block carries the turn's `dispatch_anchor` like every
  block a turn writes.
- **Where the empty turn's usage attaches, 2026-08-24, settled 2026-08-30.** The turn's spent
  tokens (reasoning included) ride the stream-done event the message end emits for every stop
  reason, exactly as a non-empty turn's do; nothing in the framework persists usage on a block
  or in a column, so an empty turn's usage is not lost and no block is its carrier.
- **The empty block replays to the model per each wire's rules, 2026-08-24.** It appears in the
  model-facing projection as the assistant's (empty) message; it is not omitted, not made
  transparent. Each provider wire renders it per its rules; the deployed provider accepts empty
  assistant content (grounded by real request, above). *Rejected:* projecting it as transparent
  / omitting it on the wire — that recreates the "hole in history" the empty block exists to
  avoid.
- **A user-visible-text signal, distinct from thinking, fired on real content, 2026-08-24.**
  The framework emits a signal the moment user-visible text actually starts flowing — on the
  first text *delta* (the `text_delta` path, `ingestion.rs:543-562`, which also lazy-creates the
  block) — and NOT at `text_block_start` and NOT at `thinking_start`. Keying on the delta, not
  the block open, is load-bearing: a text block can open at `text_block_start` and then finalize
  with zero content — that IS the empty turn this unit records — so a signal fired at block-open
  would announce "composing a reply" for a turn that says nothing, the exact false positive the
  consumer's compose cue must avoid. The delta only fires when content exists. The implementer
  settles the smallest shape that lets a subscriber tell "text is flowing" from "the model is
  thinking" (a distinct `StreamStatus` discriminator vs a new `CoreEvent`).
  *Rejected:* firing at `text_block_start` (it precedes an empty finalize — the false positive
  above); overloading `StreamStatus{""}` (it already fires for thinking — the conflation is the
  defect); a text-delta payload event carrying the bytes (a consumer needs the fact, not the
  content).

## The unit's contract

A non-tool turn that COMPLETES normally (the `message_end`/`EndTurn` finalize) commits its
text channel's final state to the ledger as an assistant text block, **empty included** —
never discarded. That block settles the frontier the same way any assistant text does (so the
debt closes exactly once and the session never wedges), carries its
`dispatch_anchor`, and replays to the model as the assistant's (empty) message. It carries no
usage: nothing in the framework persists usage anywhere, and every request's final numbers ride
the stream-done event, which fires at message end for every stop reason (verified against the
tree, 2026-08-30). A turn that
ends in tool use leaves no orphaned empty block (the lazy placeholder is unchanged); a turn
that ERRORS or is TORN DOWN commits no empty block and keeps its existing latch-and-retry. A
non-tool turn that produced only thinking still lands the empty text block — the thinking block
incidentally already rests the frontier, but the empty text block is the canonical assistant
move, so it is committed regardless. Separately, the framework emits a
signal when user-visible text actually starts flowing (the first text delta), distinct from and
never raised by thinking or by a text block that opens and finalizes empty. No new dependency;
no other change to the dispatch-identity rules or the provider wire beyond rendering the empty
message.

## Acceptance criteria

- **AC1** The workspace suite is green, clippy, fmt and doc pass with denied warnings, the
  forbidden-vocabulary scan is clean, and no dependency is added.
- **AC2** A non-tool turn that produces no user-visible text commits a real EMPTY assistant
  text block to the ledger — it is NOT discarded — pinned; the pin fails on the pre-change
  code (the discard is the mutation), and the old discard tests are inverted to the commit.
- **AC3** The empty block settles the frontier and closes the debt: after it,
  `frontier_owes_turn` is false and the frontier no longer awaits the model, the session does
  not wedge, and a subsequent scheduler drive does NOT re-dispatch a second turn for the same
  message — pinned, mutation-proven (this is the wedge the discard caused).
- **AC4** The placeholder asymmetry holds: a turn that ends in tool use leaves NO orphaned
  empty text block — pinned; and a non-tool turn that produced only thinking and no text still
  lands an empty final block (unconditional finalize; the empty block is the canonical move
  even though the thinking block already rests the frontier) — pinned.
- **AC5** The empty turn's usage is shown recorded where it actually attaches, and the pin
  names that place: the stream-done event the message end emits for every stop reason. No
  block carries usage — the framework persists none (settled 2026-08-30, against the tree).
- **AC6** The empty block replays into the model-facing projection as the assistant's empty
  message — present, not omitted, not transparent — pinned against the projection.
- **AC7** The user-visible-text signal fires on the first text DELTA (real content), and is
  NOT raised by `text_block_start` alone, NOT by `thinking_start`, and NOT by a text block that
  opens and finalizes empty — pinned for the text case (fires), the thinking case (silent), and
  the empty-turn case (a block opens with no delta → silent, no false compose cue).
- **AC8** The error and teardown edges do NOT commit an empty block: a turn whose stream errors
  before or during content latches and keeps the message owing for retry (no empty block buries
  it) — pinned; teardown likewise writes no empty block. The commit rides only the normal
  completion edge.

## Notes for launch

- Branches from `main` (worktree `~/projects/agent-ledger-noresponse`, branch
  `unit/empty-turn-closes-debt`). The discard is `ingestion.rs::finalize_streamed_text_tail`
  (~803) and the empty `ContentFinal`/`TextFinal` arms (~705, ~856); the owed decision is
  `ratchet.rs` (~191); the projection is `agency/projection.rs` (~101); the stream signals are
  `ingestion.rs` (text_block_start ~531, thinking_start ~585, text_delta lazy-create ~550) and
  `event.rs::StreamStatus` (~80).
- The mechanism is already grounded by the probe and the soundness review (receipts above) —
  do NOT re-cold-probe it; verify the receipts against the tree and build. Two open resolutions
  the implementer settles: where the turn's usage attaches so an empty block carries it (AC5),
  and the concrete shape of the text signal (AC7).
- This touches an event-sourced ledger's owed-state machine and stream finalize path, whose
  docstrings record hard-won duplicate-turn fixes. Every existing dispatch-identity pin is a
  constraint; the only tests that intentionally CHANGE are the ones that pinned the discard
  (AC2), which are inverted to pin the commit.
