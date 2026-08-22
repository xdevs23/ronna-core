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
  Amended 2026-08-22, after the consumer's adversarial verification proved the
  tail-derived inheritance insufficient: a message absorbed between a round's
  result and the continuation dispatch becomes the tail, the continuation
  re-anchors on it, and the original summoner falls out of the turn's identity —
  a proven escalation in the consumer's gate. The inheritance is therefore
  ACTOR STATE, as this decision's own wording anticipated: the actor holds the
  open turn's anchor from its first dispatch until a close that ends the turn
  (an end-turn stop, the error edge, the interrupt teardown — a tool-use stop
  closes the stream but leaves the turn open), and every continuation dispatch
  reuses the held anchor regardless of what the tail is. A fresh turn resolves
  from the tail as before.
  Refined 2026-08-22, after the consumer's fifth adversarial break proved the
  unconditional hold leaks: the close is the identity's only release site, so
  a tool-use close whose continuation never comes — a truncated tool
  lifecycle the reader discards records nothing owing — held the identity
  forever, and the next UNRELATED summons inherited it (over-declined tool
  admission, and an idle interrupt stamping a dead turn's summoner). A
  tool-use close therefore keeps the identity only while a continuation is
  genuinely due on the close's own snapshot: an unresolved tool call bearing
  this turn's anchor exists — a recorded call whose result is pending resumes
  the turn, so a message absorbed before the result cannot re-anchor it — or
  the frontier owes the model; otherwise the close ends the turn like every
  other edge, and a truncated lifecycle that recorded no call ends it.
  Rejected: reusing the held anchor only for turn-product frontiers — the
  absorbed message is exactly a non-turn-product frontier, so that re-opens
  the proven escalation the hold exists to close.
  Amended 2026-08-23, after the consumer's sixth adversarial break proved the
  refinement's two arms leak in both directions. The frontier arm held a dead
  identity for someone ELSE'S fresh summons: a truncated round with a message
  absorbed in its window makes the absorbed message the close's tail, the
  tail owes the model, and the arm kept the identity on exactly that — so the
  fresh summons' answer anchored on the dead turn. And the unresolved-call
  arm counted a parked INTERACTIVE call — and a call with an empty id, which
  no outcome can ever match — as an owed continuation forever, pinning the
  identity indefinitely. The release rule replacing both arms: a tool-use
  close keeps the identity iff the count of tool results and tool errors
  anchored on the turn exceeds the count its dispatches have already
  answered — the actor records that mark beside the held identity, from each
  dispatch's own snapshot — or an unresolved, non-interactive tool call with
  a non-empty call id, anchored on the turn, exists in the close's snapshot.
  The frontier arm is deleted: a model-owed tail is someone's summons, not
  evidence of our continuation. A parked interactive call ends the identity;
  when its approval later resolves, the outcome carries the turn's anchor and
  the tail inheritance re-attaches. The answered mark is kept in outcome
  units, not as a dispatch tally (decided at implementation, 2026-08-23): one
  dispatch can answer several outcomes at once, and a turn resumed off its
  outcome tail — an approval resolution, a restart recovery — has that
  outcome answered by the resuming dispatch itself; a per-dispatch increment
  undercounts the first and overcounts the second, and each error re-opens a
  proven leak shape. Rejected: keeping the frontier arm for turn-product
  tails only (the parked shapes still pin the identity forever); ending on
  every tool-use close (re-opens the escalation the hold exists to close).
  Amended 2026-08-23, after the consumer's seventh verified break — a
  regression the sixth fix's release opened, plus a pre-existing restart
  hole — proved the FRESH-turn resolution reads too little. It read only
  the tail, so a released turn — a parked interactive call resumed by its
  approval, a restart recovering a round — lost its identity whenever a
  message was absorbed behind its outcome: the continuation anchored on
  the absorbed line, and in the consumer that is the original escalation
  again. The fresh resolution is now LEDGER-FIRST: at a dispatch with no
  held identity, a model-owed tail carrying a null anchor walks the
  dispatch snapshot backward for the newest tool outcome (a result or an
  error), and if that outcome's turn is UNANSWERED — no assistant text
  block and no status block with the same anchor after it in the
  snapshot — the dispatch inherits the outcome's anchor; otherwise the
  tail starts a fresh identity as before. A non-null-anchored tail
  inherits as before. This DEMOTES the held identity to a consistency
  cache over a ledger-derivable fact: a fresh actor over the same
  ledger — the restart shape — resolves the same turn a live actor was
  holding, and the cache's job is consistency within a process, not
  truth. Accepted residual, documented at the rule: a turn ended by an
  unstamped store-failure error leaves no status marker, so its outcome
  may over-attach one later summons — over-decline direction in the
  consumer's authority fold, self-healing at that turn's close, whose own
  text or status stamps the anchor. Rejected: re-holding the identity
  across the parked close (the sixth break proved exactly that hold pins
  it forever); a persisted turn-state row (a second record of a fact the
  blocks already decide — the restart shape is the proof the blocks
  suffice).
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

- **Turn closure is a stored fact, 2026-08-23 — the design review's verdict.**
  Eight adversarial rounds broke every attempt to DERIVE "this turn is over"
  from side effects: tail shapes, machinery classes, held actor state, outcome
  counters, and marker-inference each failed on the edge that left no side
  effect behind (the eighth break's four shapes: a parked call in a later
  round, a lost later round, the abandoned close, the error edge — every one
  ends a turn without writing a text or status, stranding its last outcome as
  "unanswered forever" so it captures the next unrelated summons). The review
  stops the derivation cycle: when a close ENDS a held identity while the
  close's snapshot holds an unanswered outcome for that turn, the close
  appends a status block anchored to the turn — the durable record of the
  turn's end, the same primitive the first consumer's improvements list has
  wanted since its live-model unit as the durable failed-turn record. The
  unanswered-outcome walk already honors status markers, so no resolution
  rule changes: the one place that knows the truth now writes it down. The
  approval-resume stays correct by ordering — the marker answers only the
  outcomes before it, and a later approval's outcome lands after it and
  inherits as before. A restart mid-round writes no marker because the
  tool-use close keeps the identity, so recovery inheritance is untouched.
  Rejected: continuing to infer closure (eight refutations are the record);
  a separate turn table (the status block IS the ledger's own shape for a
  machine fact about a turn).

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
