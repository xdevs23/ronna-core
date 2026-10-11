# Slice 22 — no turn is dispatched around a dangling call

Date: 2026-09-26. Corrects a defect confirmed against the tree on that date.

## What is wrong today

A model turn must never be dispatched while a non-interactive tool call the scheduler has
not yet walked has no outcome: the request would carry a call without its result. Two
sites decide whether a turn is owed, and neither asks that question of the ledger it
decides on.

- **The drive.** `drive_ledger` walks from the cursor over the snapshot it opened with and
  computes `parked` there; `ToolCall::run` re-reads the store at each call it visits. It
  then reads a fresh one-row tail to decide whether the frontier awaits the model. When a
  sibling call and an earlier call's result land after the opening list, the walk reaches
  the earlier call, finds it resolved in its fresh read, and parks on nothing, while the
  fresh tail (the result) awaits the model. The drive signals an owed turn with the sibling
  unanswered.
- **The delivery.** `handle_blocks_ready` re-checks only the tail before dispatching, on the
  stated assumption that every signal came from a drive that would have parked. That
  assumption fails in the window above, and also when a scheduler signal from before a round
  reaches a ledger that now holds a dangling sibling, since the actor's select does not order
  the two.

The end-to-end test `a_two_call_round_never_dispatches_around_a_dangling_sibling` has been
seen failing with a second request that carried both calls and only the first result.

## The change

- One crate-private fold answers whether the part of a snapshot past the conversation's
  cursor holds a non-interactive tool call with no outcome. The outcome is looked for
  anywhere in the snapshot, as the resolution predicate `ToolCall::run` parks on does;
  only the candidate calls are limited to those past the cursor. The fold makes one pass
  that collects the answered call ids and one pass over the candidates, so its cost is
  linear in the snapshot, not one ledger scan per call. It and
  `ToolCall::system_owed_call_anchored_in` share one test of what makes a call owed by the
  system, and differ only in which calls they take as candidates.
- The fold is limited to calls past the cursor on purpose. A call behind the cursor was
  walked and found resolved or interactive when the cursor passed it, or was inherited
  that way by a fork. A whole-ledger fold would stall for good a fork that inherited a
  call without its result behind its cursor; that separate existing defect is recorded as
  its own item and is not changed here.
- The frontier rule both sites share takes the snapshot and the cursor, reads the frontier
  block from that snapshot through `frontier_block`, and owes a turn only when the frontier
  awaits the model and the fold finds no dangling call. A caller cannot hand it a frontier
  from another snapshot.
- The drive, when its fresh tail awaits the model or is transparent, decides on a full fresh
  listing, as its transparent branch already does. The one-row tail read stays for every tail
  that owes nothing, and the rule only ever sees a full listing. A drive that stands down because the fold found a dangling call reports
  the conversation as having work due, as a parked drive does, so the published conversation
  state does not claim idleness while a call is outstanding.
- The delivery passes its snapshot and cursor to the same rule, so a stale signal stands
  down over a dangling call. It is driven again by an explicit later drive: the store write
  that resolves the call wakes the scheduler, and that drive signals once, off the last
  result.
- The fold is a library kind read inside the ratchet, the same kind of read
  `fresh_turn_anchor` and `ledger_turn_durably_over` already make there through `ToolCall`
  folds. No kind hook is added. A `ready_for_model` hook on the runtime kind was rejected,
  because it widens the kind surface of every consumer for a question only tool calls
  answer.
- These documents state the rule as it now is: the `Outcome.owes_turn` and
  `frontier_owes_turn` docs in `agency/ratchet.rs`, the tail-decision paragraph before the
  one-row read in `drive_ledger`, the `ToolCall` type doc and the
  `system_owed_call_anchored_in` doc in `agency/tool_call.rs`, the `owed_turn_deferred` and
  close docs in `actor.rs`, the comment before the ledger read in `handle_blocks_ready`, the
  `TwoCallsInOneTurn` test script's doc, and the `InterruptingLedger` test source's doc,
  whose count of reads changes.

## What does not change

`parked` and its meaning; the cursor's crash-ordering contract; fresh-tail cap suppression;
frontier transparency; anchor resolution; the close-edge nudge; interactive calls do not hold
a turn back. No new kind hook, trait method, retry, timer, table or public API.

## Acceptance criteria

1. A ratchet test with a scripted ledger source whose first listing is stale: it returns
   `[user, call-a]` while the store already holds `call-b` and `result-a` after them. The
   walk finds `call-a` resolved in its fresh read, and the fresh tail is `result-a`. The
   drive owes no turn and reports work due; the next drive parks on `call-b`; after
   `result-b`, one drive owes exactly one turn. On the tree before the change the first
   drive owes a turn.
2. An actor test using the bare actor over a ledger of a message, two calls anchored on it
   and only the first result: an explicit delivery sends no request, counted on the scripted
   provider's probe. After the second result and a second explicit delivery, exactly one
   request goes out, carrying both results. On the tree before the change the first
   delivery sends a request.
3. A fork taken after a healed interrupt, which inherits a call without its result behind its
   cursor, behaves as it did before the change. This test documents the separately recorded
   defect without fixing it, and it fails if the fold ever stalls such a fork.
4. `a_two_call_round_never_dispatches_around_a_dangling_sibling` passes with its assertions
   unchanged. Its failure is timing-dependent, so repeated passes are a smoke check only;
   the first two criteria are the proof.
5. The ratchet, tool, actor and ingestion tests that cover cap suppression, burial,
   interactive approval, empty turns and cursor convergence keep passing.
6. `cargo test --all-features`, `cargo clippy --all-features --all-targets` and
   `cargo fmt --all --check` pass.
