# Slice 8 — dispatch identity

Date: 2026-08-22. Revision 2, rewritten after the unbriefed two-reviewer probe returned
eighteen findings against revision 1 — three blockers among them: the turn-close edge
was undefined and every existing candidate either does not exist or deadlocks a pinned
shape; the anchor's meaning contradicted itself for continuation rounds; and the
binding cannot carry what revision 1 asked it to. Every mechanism below is named
against the actor's real state machine. Status: settled for implementation.

The first consumer's fifth unit proved two defects in the dispatch path with running
exploits; this slice closes both. Both live where the actor decides and records a
turn, which is why they are one slice.

## The two proven defects

1. **No dispatch identity.** A consumer enforcing authority-scoped tool admission
   needs to know which message summoned the turn a tool call belongs to. Nothing
   records it, and reconstruction from stored shape was adversarially refuted three
   times. The consumer floors tool registration at its lowest authority until this
   slice ships the fact.
2. **The duplicate-turn redispatch.** A message appended after a stream's
   message-end and before its tool calls are recorded dispatches a second concurrent
   model turn, because the dispatch state settles at message-end. Proven live:
   echoes in five of six consumer suite runs, deterministic under a held window.

## Decisions

- **The anchor is resolved at dispatch, from the dispatch's own snapshot,
  2026-08-22.** Inside the actor's turn delivery, the frontier is re-read from the
  same snapshot the model request is built from; the tail block that owes the turn
  is the dispatch's anchor source, and a tail that no longer owes stands the
  dispatch down — which retires the recorded stale-signal window in the same
  stroke (dated note on that deferral). Rejected: resolving from the wakeup signal
  (it carries nothing and races the very appends this slice is about).
- **Continuation rounds inherit the anchor, 2026-08-22.** The anchor of a turn
  whose dispatching frontier is itself a turn product — a tool result, a status
  block — is that block's own anchor, inherited; only a frontier with a null
  anchor (a message, a consumer append) starts a new identity. So every round of
  one tool conversation carries the original summoning message's id, and the
  consumer's one-step read holds for round one and round ten alike. Rejected: a
  per-dispatch anchor with chase-the-chain reads (re-imports the shape walking
  this slice exists to end).
- **One nullable header column, written per writer class, 2026-08-22.**
  `dispatch_anchor` on the block header, written at insert through the shared
  header helper, null for everything that is not a turn's product. Per class:
  the streaming reader carries the current turn's anchor through a new
  actor-owned per-turn seam on the binding (set at dispatch, cleared at close —
  the reader's channel stays neutral); tool results, tool errors and approval
  blocks copy the anchor from their source call block at the resolution write,
  which also makes restart-recovered rounds correct for free; the interrupt's
  status block takes the actor's current anchor; the out-of-band tool path
  writes null, and the consumer's stated fold for a null anchor is its floor.
  Rejected: threading ids through the provider channel (it deliberately carries
  neutral values, never ledger identity); a separate turn table (a second record
  of facts the blocks can carry).
- **The anchor is a real reference: collected, remapped, exposed, 2026-08-22.**
  The column joins the orphan predicate's reference union (the self-referential
  arm) and the deep-copy cloner's remap set, with both pinned literals updated
  in lockstep and dated notes on their contracts; the header select gains the
  column with the byte-identical-pin contract amended the same way. The load
  surface is an `Option` field on the block struct — a cross-repo compile break
  for the sibling consumer, recorded as a coordinated follow-up under its
  sibling-checkout decision. Rejected: a weak reference that may dangle after
  fork-and-delete (an identity that can silently point at nothing is the
  refuted walk wearing a column).
- **The turn closes on a named edge, and the close re-checks, 2026-08-22.** The
  dispatch state opens at delivery and closes on exactly one of: the stream's
  closed signal, the stream's error signal, or the interrupt teardown (whose
  torn-down reader's terminal is discarded by generation — the teardown itself
  is the close). Message-end no longer settles the dispatch state; "the tool
  round is recorded" is covered because the closed signal is emitted after the
  reader drains the tool lifecycles. Every scripted provider — the framework's
  and, as a named coordinated edit, the consumer's — gains the trailing done
  event real wires already send; those edits are enumerated mechanical changes,
  not silent test weakening. Closing re-runs the owed-turn check (the actor
  self-signals), so a message absorbed during the window is dispatched by the
  close deterministically, never by luck. Rejected: closing at message-end (the
  proven defect); a store-level lock (the actor is the one dispatcher);
  first-tool-lifecycle-end as the edge (several ends per turn are legal).

## What must hold

- The anchor is written by the framework's own paths only; the public consumer
  write path never sets it. The metadata worker's title turns are turn products
  of their own ledger and carry their anchors under the same rules.
- The existing suites pass with edits that are mechanical and enumerated: block
  shapes gaining the column, scripted providers gaining the trailing done, and
  the two pinned literals updated with their dated contract notes.
- The duplicate-turn reproduction — append during a held post-message-end
  window — dispatches no second turn, and the close dispatches the absorbed
  message's turn, pinned with real interleaving.
- Anchor correctness under the consumer's proven shapes: plain, narrated,
  multi-round, failed-round, interrupted — every turn-product block carries the
  original summoning frontier's id, pinned per shape, forks included (the
  cloner remaps; fork-then-delete leaves no dangling anchor, pinned).

## Acceptance criteria

- **AC8-1** `cargo build --workspace` and `--all-features` succeed; no new
  external dependency.
- **AC8-2** The full suite passes in parallel and single-threaded identically;
  edits to existing tests are mechanical and enumerated in the report.
- **AC8-3** The anchor per proven shape (plain, narrated, multi-round with
  inheritance, failed round, interrupted): every turn-product block carries the
  original summoning frontier's id; non-turn blocks carry null — pinned per
  shape.
- **AC8-4** The window: an append during a held post-message-end window
  dispatches no second turn; the close's re-check dispatches exactly one
  following turn whose request contains the absorbed message; exactly one
  further answer block lands — pinned with real interleaving.
- **AC8-5** Given a tool-call block, the summoning message block loads through
  the public API in one step, in round one and in a continuation round alike;
  a null anchor is the documented out-of-band answer.
- **AC8-6** Fork and collection: a deep copy remaps the anchor; deleting the
  source conversation after a fork leaves no dangling anchor and the collector
  treats anchored blocks as referenced — pinned.
- **AC8-7** clippy (all-features and mistral-only), `fmt --check`, doc under
  denied warnings, and the vocabulary scan are clean.
- **AC8-8** The dated notes: the repository invariant "derived, never stored"
  gains the note that the anchor is an insert-time recorded decision because
  the derivation was adversarially refuted (citing the consumer's closure); the
  descriptor module's byte-identical-pin contract and the orphan-predicate and
  cloner literals carry their lockstep notes; the stale-signal deferral is
  marked retired by the stand-down rule.
- **AC8-9** A coordination note names the consumer follow-ups this slice
  unblocks: the trailing-done edits to its scripted providers, the canary
  un-ignore, the echo-tolerance removal, the block-struct field, and the
  registration floor's lift.
