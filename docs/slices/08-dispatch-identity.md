# Slice 8 — dispatch identity

Date: 2026-08-22. Revision 1, written for the unbriefed spec review. The first
consumer's fifth unit proved two defects in the dispatch path with running exploits;
this slice closes both. Both live where the actor decides and records a turn, which
is why they are one slice.

## The two proven defects

1. **No dispatch identity.** A consumer enforcing authority-scoped tool admission
   needs to know which message summoned the turn a tool call belongs to. Nothing
   records it: tool-call blocks carry fixed columns, blocks carry no turn identity,
   and the consumer's attempt to reconstruct the summoner from stored ledger shape
   was adversarially refuted three times — stored shape cannot tell a turn's own
   narration from a previous turn's answer. The consumer now floors tool
   registration at its lowest authority until this slice ships the fact.
2. **The duplicate-turn redispatch.** A message appended in the window after a
   stream's message-end and before its tool calls are recorded dispatches a second
   concurrent model turn: the actor's streaming flag clears at message-end, the
   append's change event fires a tick, the frontier shows a model-owed tail, and a
   second binding dispatches while the first turn's tool round is still in flight.
   Proven live by the consumer's suite — echoes in five of six full runs, a
   deterministic reproduction with a 300-millisecond hold, and a second assistant
   answer in the ledger when the echo is answered as a real provider would.

## Decisions

- **The summoning frontier is recorded onto the turn's blocks, 2026-08-22.** When
  the actor dispatches a turn, it resolves the frontier block id that owed the turn
  — the summoning message — and that id is threaded through the stream binding into
  every block the turn writes: the streaming tail, the finalized texts, the tool
  calls, the tool results, the status blocks. One new nullable column on the block
  header, `dispatch_anchor`, written at insert, null for every block that is not a
  turn's product (user messages, consumer appends). Consumers read the anchor off a
  tool-call block and load the summoning message directly — no reconstruction, no
  shape reading. Rejected: a consumer fact on tool-call inserts only (the anchor is
  a property of the whole turn, and texts need it for the same questions later); a
  separate turn table (a second record of facts the blocks can carry; the anchor IS
  the turn identity for every purpose named so far).
- **The dispatch window closes with the anchor, not a lock, 2026-08-22.** The
  actor treats a turn as open from dispatch until the turn's terminal event has
  been processed AND its tool round, if any, has resolved — the in-flight state the
  actor already tracks per binding is extended over the message-end-to-tool-record
  window, so a tick in that window sees an open turn and does not dispatch. A
  message appended there is absorbed, exactly as the consumer's recorded absorption
  semantics already state. Rejected: a store-level lock (the actor is the one
  dispatcher; the fix belongs in its state, not in the store); ignoring the tick
  (the tick must still run for other conversations).

## What must hold

- The anchor is written by the actor's paths only; the public consumer write path
  never sets it. Load surfaces expose it on the block.
- The existing suites pass unchanged except where they assert block shapes that now
  carry the anchor column (mechanical, enumerated).
- The duplicate-turn reproduction from the consumer's canary — a message absorbed in
  the post-message-end window with a held tool round — dispatches no second turn,
  pinned in this repository with the same shape.
- Anchor correctness under the consumer's proven shapes: narrated turns, multi-round
  tool turns, failed rounds (tool errors), interrupts and stream errors — the
  anchor on every turn-product block equals the dispatching frontier's id, pinned
  per shape.
- Migration: the header column arrives by the framework's own migration discipline;
  pre-existing rows read null; the durability check and descriptor validation are
  untouched.

## Acceptance criteria

- **AC8-1** `cargo build --workspace` and `--all-features` succeed; no new external
  dependency.
- **AC8-2** The full suite passes in parallel and single-threaded identically;
  edits to existing tests are mechanical and enumerated in the report.
- **AC8-3** The anchor: for each proven shape (plain turn, narrated turn,
  multi-round tool turn, failed round, interrupted turn), every turn-product block
  carries the dispatching frontier's block id, and non-turn blocks carry null —
  pinned per shape.
- **AC8-4** The window: the consumer's deterministic reproduction (append during a
  held post-message-end window) dispatches no second turn, and the absorbed
  message is answered by the following turn — pinned with real interleaving.
- **AC8-5** A consumer-facing read exists: given a tool-call block, the summoning
  message block is loadable through the public API in one step.
- **AC8-6** clippy (all-features and mistral-only), `fmt --check`, doc under denied
  warnings, and the vocabulary scan are clean.
- **AC8-7** The extraction spec's one-literal-site and derived-never-stored
  dispositions gain dated notes where the anchor touches them (the anchor is an
  insert-time recorded decision, in the same tradition as the consumer's stamps).
