# Unit 23 — a no-text turn closes its own debt, and text streaming is observable (framework)

Date: 2026-08-24. A consumer is removing an outbound sentinel convention (a magic
answer string that meant "no response") in favour of the model simply ending its turn
with no user-visible text — the native form of "nothing to say". Two framework
capabilities must exist for that to be sound. Both are grounded in a cold mechanism
probe of this repo; the file:line receipts below are the probe's, carried here so the
implementer builds to the true state rather than re-deriving it.

## Grounding (what the probe found)

**An empty turn already persists no block.** When a completion carries no text (no
deltas, or an empty final) the stream reader inserts nothing: `finalize_streamed_text_tail`
returns `NothingToPersist` with no open streaming block, and discards a streaming block
whose content `is_empty()` rather than committing it — a committed empty text block
renders as an empty content part that some vendors reject on every later turn
(`ingestion.rs:803-850`, doc at 798-802; the `ContentFinal`/`TextFinal` empty arms route
to the same discard at 705-712 and 856-861). `message_end` still emits
`StreamDone{stop_reason: EndTurn}` (`ingestion.rs:880-924`). This behaviour is correct and
stays.

**But a no-text turn does not close its debt — it re-dispatches forever (the defect this
unit fixes).** The owed decision is a fact about the frontier *block*, never about whether
a turn ran: `ratchet::frontier_owes_turn` is `K::from_block(tail).awaiting() == Some(Model)`
(`ratchet.rs:191-193`), and an addressed user message awaits the model by a **write-time
stamp** that a running turn never clears — `ChatMessage::awaiting()` derives from
`answer_due`, stamped at ingestion and cancelled only by erasing the message
(consumer `kind.rs:488-497,641-648`). What moves the frontier off that message today is the
turn leaving behind a newer, non-owing tail block: either a real assistant `text` block
(an assistant text awaits nobody — `agency/text.rs:30-33`), or, for a turn that produced a
tool round but no resting text, the transparent turn-closure marker. That marker is written
by `settle_turn_identity` **only when the turn left an unanswered tool outcome**
(`actor.rs:950-952`: it returns early unless `ToolCall::unanswered_outcome_anchor(&blocks)
== Some(anchor)`). A no-tool, no-text turn leaves neither a resting block nor an unanswered
outcome, so nothing is written, the tail reverts to the still-owing message, `owes_turn`
stays true, and the scheduler re-drives an identical empty turn — with no
already-dispatched-this-anchor dedup (`actor.rs:1045-1076`; the hazard is even documented
at `actor.rs:268-272,726-728`: "a turn that wrote nothing left the frontier unchanged", "a
blind re-check would redispatch a turn that wrote nothing forever"). Today this never fires
because the consumer's sentinel is always a real text block; removing it re-arms the loop.

**There is no clean signal for "user-visible text is streaming".** `StreamStatus{label:""}`
fires at both `text_block_start` (`ingestion.rs:531-535`) and `thinking_start`
(`ingestion.rs:585-589`), so it conflates reasoning with user-visible text; and
`text_delta`'s lazy-create of the streaming block (`ingestion.rs:543-571`, esp. 550) emits
nothing at all. A consumer that wants to raise a "composing a reply" cue only while real
text is being produced has nothing reliable to key on.

## Decisions taken with this unit

- **A closing turn that leaves no frontier-resting block writes the transparent
  turn-closure marker, 2026-08-24.** `settle_turn_identity`'s marker precondition is
  generalized from "the turn left an unanswered tool outcome" to "the turn is closing and
  left no block that rests the frontier" — which subsumes both the existing tool-outcome
  case and the new no-text case. The marker is the framework's existing record of "this
  turn completed, the frontier rests, and the model never sees it"
  (`frontier_transparent()` at `agency/records.rs:64-67`, honoured by the ratchet at
  `ratchet.rs:234-263`); this unit widens its coverage, it does not invent a mechanism. The
  implementer resolves the exact predicate against the code — the precise notion of "left a
  resting block this turn" (a real answer already rests the frontier and must NOT also get a
  redundant marker; a tool-outcome turn keeps today's marker; a no-text turn newly gets
  one) — and confirms the marker written on the no-text edge carries the turn's
  `dispatch_anchor` so the turn's persisted identity survives on it.
  *Rejected:* clearing the message's write-time `answer_due` stamp on turn completion — it
  mutates a historical ingestion fact, breaks the append-only ledger, and conflates "was
  addressed" (permanent) with "has been answered" (a turn outcome). *Rejected:* a
  per-anchor "already dispatched" dedup in the scheduler — it suppresses the symptom without
  durably recording that the turn completed, so a later frontier re-read still finds an
  owing message with no completion record; wrong layer.
- **A user-visible-text signal, distinct from thinking, 2026-08-24.** The framework emits a
  signal the moment user-visible text begins streaming and never for reasoning: at
  `text_block_start` and at the `text_delta` lazy-create of the streaming block (the first
  text delta when no explicit start arrived), and NOT at `thinking_start`. The implementer
  settles the shape — a distinct `StreamStatus` discriminator (a non-empty label, or a kind
  field) versus a new `CoreEvent` variant — choosing the smallest change that lets a
  subscriber tell "text is flowing" from "the model is thinking". *Rejected:* overloading
  the existing `StreamStatus{""}` (it already fires for thinking — the conflation is the
  defect); a text-delta payload event carrying the streamed bytes (a consumer needs only the
  fact that text started, not its content — bytes on the bus for nothing).

## The unit's contract

A turn that ends (EndTurn or any close) having produced no block that rests the frontier —
the no-tool, no-text turn above included — writes the existing transparent turn-closure
marker, so its debt closes exactly once, the frontier stops owing the model, the scheduler
does not re-dispatch, and the marker stays invisible to the model's history and carries the
turn's `dispatch_anchor`. A turn that already rests the frontier with a real answer block is
unchanged and gets no redundant marker; a tool-outcome turn keeps today's marker. Separately,
the framework emits a signal when user-visible text starts streaming (text-block start and
first-text-delta), distinct from and never raised by thinking. No new dependency; no change
to the empty-block discard, to the dispatch-identity rules beyond the widened marker
precondition, or to any provider wire.

## Acceptance criteria

- **AC1** The workspace suite is green (the framework's full battery), clippy, fmt and doc
  pass with denied warnings, the forbidden-vocabulary scan is clean, and no dependency is
  added.
- **AC2** A no-text EndTurn turn on an addressed (model-owed) frontier closes the debt:
  after the turn, `owes_turn` is false and the frontier no longer awaits the model, and a
  subsequent scheduler drive does NOT dispatch a second turn for the same message —
  pinned deterministically, and the pin fails on the pre-change code (mutation-proven: the
  loop is the defect).
- **AC3** The closure marker written on the no-text edge is frontier-transparent: it rests
  the frontier yet does not appear in the model-facing projection/history, and it carries the
  turn's `dispatch_anchor` — pinned.
- **AC4** No regression to the existing closures: a real-answer turn still closes via its
  text block and gets NO redundant marker; a tool-outcome turn still gets exactly today's
  marker; every existing dispatch-identity and turn-closure pin passes unchanged.
- **AC5** The user-visible-text signal fires when text starts (at text-block start and at
  the first-text-delta lazy-create) and is NOT raised by `thinking_start` — pinned for both
  the text case (fires) and the thinking case (does not).
- **AC6** The empty-block discard is untouched: an empty streaming block is still discarded,
  never committed (the vendor-poisoning guard holds) — existing pin passes.

## Notes for launch

- Branches from `master` (worktree `~/projects/agent-ledger-noresponse`, branch
  `unit/empty-turn-closes-debt`). The owed decision is `ratchet.rs`; the marker and its
  transparency are `actor.rs::settle_turn_identity` (~950), `agency/records.rs` (~64) and
  `ratchet.rs` (~234); the stream signals are `ingestion.rs` (text_block_start ~531,
  thinking_start ~585, text_delta lazy-create ~550) and `event.rs::StreamStatus` (~80).
- The mechanism is already grounded by the probe (receipts above) — do NOT re-cold-probe it;
  verify the receipts against the tree and build. The one open resolution is the exact
  "left a resting block this turn" predicate in `settle_turn_identity` — settle it against
  the block/awaiting types so a real answer never double-marks and a no-text turn always
  marks.
- This is a delicate change to the owed-state machine (its docstrings record hard-won
  duplicate-turn fixes). Treat every existing dispatch-identity pin as a constraint, not a
  suggestion; AC4 is the guard that the widening did not disturb them.
