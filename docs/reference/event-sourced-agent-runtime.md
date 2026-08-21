# An event-sourced agent runtime — architecture reference

This is a **reference document, not a description of this project.** It generalizes the
architecture of a production agent system built independently of this one, so the patterns
can be judged and adopted on their merits. Nothing here is a commitment; what this project
has actually decided lives in the decision records.

Every rule below was paid for. Where a rule exists because something broke, the failure is
stated — a rule without its failure gets refactored away by the next person who finds it
inconvenient.

---

## 1. The shape in one paragraph

An agent session is an **append-only ledger of blocks**. A block is not a record that a
central loop interprets by type — **the block is the actor**: it answers a small uniform
interface (who owes the next move, do my own work, am I done, how do I read to the model),
and the orchestration layer runs those hooks without ever branching on block kind or naming
a domain concept. A per-session in-memory actor advances a persisted **cursor** through the
ledger, calling each block's work hook and reading its boolean doneness. When the cursor
reaches the last block **and** that block awaits the model, exactly one model turn fires.
Everything else — tool admission, human approval, workflow state, permission state, spend,
capability — is **derived by folding the ledger**, never stored as mutable state.

The payoff is structural. New intent slots into the shape instead of being welded on, and
cases nobody explicitly wrote still work because they are implied by the shape. The cost is
equally real and stated in §16.

---

## 2. The architectural law

Twelve principles, described in the source system as *descriptive of decisions already made
and defended, not aspirational*. Violations are treated as bugs with names and dates.

| # | Principle | What it forbids |
|---|---|---|
| 1 | **The block is the actor.** Every block answers one uniform interface, inert by default. Generic machinery never branches on block kind and never names a domain concept — not even in comments. | `if block.kind == …` in shared code; a side table of behavior keyed by kind. |
| 2 | **Insert first, then act.** A block is appended before any hook runs on it. | Computing then inserting; predicting a future block's identity. |
| 3 | **Truth is durable state; bus events are wakeups.** An event is a prompt to re-derive from durable state. Duplicates must be no-ops; losses must replay. | Treating an event payload as truth. |
| 4 | **Collaborators are handed in, never reached for.** A block's work hook receives the store and the bus, never a domain registry. | Service-locator lookups inside a unit of work. |
| 5 | **Self-registration over central enums.** The discriminator must be an identity the durable record already carries — a block's kind, a tool's registered name — never a key scavenged from inside a payload. | A central switch every domain must edit; dispatch keyed on "which interior key is present". |
| 6 | **Idempotency at a deliberate granularity, keyed off durable state.** Here the granularity is the block, meaning a ledger position. Crossing into an external system uses a deterministic id derived from ledger positions. | In-memory "already did this" sets. |
| 7 | **Cross-system coupling lives in ONE named bridge.** Neither side learns about the other. | Teaching two buses about each other. |
| 8 | **The bus contract: declare, route, fail loud.** Subscribers declare accepted types; the sender answers "what if nobody is listening?" in exactly one of three modes. | Silencing the strict mode on an event that genuinely needs a listener. |
| 9 | **When the architecture is not ready, STOP and say so.** A called-out misfit is cheap; a buried one is found later under worse conditions. | Forcing a feature in and calling it done. |
| 10 | **Document the decision AND the wrong turns.** | Recording only the final shape. |
| 11 | **Make the architecture carry the intent.** If you are about to write a function whose name restates the intent (`ensureFirstX`, `appendInitialY`), that is the smell. | Hand-coding an intention the data flow could imply. |
| 12 | **One decision, one place, recorded once.** Every admission or routing choice is made once and recorded as durable state; everything else reads the record. | A second answer channel for a decision that already has one. |

**Principle 11, worked.** A feature injected a parameter block at session creation. That
block became a lone model-awaiting tail on an empty session and spuriously fired a turn.
The fix was not a condition: the session was created with the parameter *unset*, so the
first real user message trips the same change-detection path for free.

**Principle 12, worked — the phantom execution.** A tool runner re-derived "may this
admission-controlled body run" from the *absence* of an approval-request block, while the
real answer travelled in an in-memory future. A refused call executed its body. The fix was
not to fence the duplicate. It was to delete it.

---

## 3. The ledger

### 3.1 Identity is position

A block's id is its **1-based index, derived at load time and never stored**. The ledger is
append-only and only the streaming tail mutates in place, so a block's index is permanent
and identifies it. Deriving the id removes the only thing two concurrent appends could
collide on. Every cross-block reference is therefore an integer position.

### 3.2 The uniform block interface

All hooks are inert by default; a kind overrides only what it needs.

**Agency hooks**

| Hook | Meaning | Default |
|---|---|---|
| `awaiting()` → `user` / `system` / `model` / `none` / nothing | who owes the outstanding ask | nothing — no ask at all, invisible to the frontier read |
| `run(...)` → bool | do this block's own work; `true` = done, cursor may advance; `false` = still owed, re-run next tick and **must** be safe to re-run | `true` |
| `post_gate_id(session)` → position or nothing | pure routing for deferred work; nothing stops the walk, which *is* the idempotency check | nothing |
| `run_post_gate(...)` | the deferred work at a chain terminus | no-op |

The four-valued ask vocabulary matters: `none` parks the loop exactly like `user`; the only
difference is presentational (a client disables its composer). It is distinct from *no ask
at all*, which means the block is not a participant in the frontier question. Collapsing
those two is a bug that hides a whole class of parked state.

There is deliberately **no admission hook on the block**. Admission for a controlled call is
the tool-running subsystem's single recorded decision; a block-level hook would reopen the
second answer channel principle 12 exists to forbid.

**Projection hooks**

| Hook | Meaning |
|---|---|
| `llm_projection(ctx)` | how the block reads to a typed model turn; `ctx` is the ledger prefix up to and including it. Nothing = invisible. |
| `speech_projection(ctx, ledger)` | how it reads into a re-seeded **spoken** session. **Defaults to the typed projection, never to invisible** — an invisible-by-default seam silently drops instructions and context from a re-seeded call, and nothing fails. |
| `turn_text()` per modality | the words a turn-member block contributes when an assistant turn is reassembled |
| `projection_position()` | an **annotation** projects where its subject is, not where it was appended — a late transcript must not read as if the user spoke after the reply |

**Declarative traits**

| Trait | Meaning |
|---|---|
| `frontier_transparent()` | this kind is a pure record the model-turn frontier reads **through**. Permitted only for blocks inert in every direction. Without it, an out-of-band record appended behind a frontier that owes a turn masks that turn **forever**. |
| `client_visible()` | whether the block appears in session-view payloads at all — independent of model visibility |
| `sensitive_fields()` | fields rendered into the **model-facing** projection but stripped from every client payload |
| `admits_user_message(ledger)` | whether a typed user message may be appended while this block stands |
| `derived_state(ctx)` | a read-only display lens computed at read time |

`derived_state` carries a three-part contract: never persisted; **never read back to
influence any block decision**; and exempt from replay determinism *precisely because* it is
non-durable. It must still be a pure in-process read of the prefix.

### 3.3 The block catalogue, by role

Registration is the only dispatch: a kind registers itself under a literal, and parsing is a
registry lookup with an unregistered kind as a hard error. The catalogue groups as:
conversation (user and assistant text, reasoning, the mutable streaming tails, status, turn
error); tools (call, result, error, admission response, approval request, approval
decision); session parameters and access (system prompt, parameters, scope, guardrail, scope
context, permission mode, standing per-tool grant); observations (model capability, tool
palette); out-of-band messages (a structured harness envelope, an operator speaking through
the agent); files (user upload, tool-emitted media); client context; queued sends; speech;
and workflow-registered kinds, which register from their own packages exactly as the
framework's own do.

### 3.4 Folds — everything derived, nothing stored

There is **no stored agent type, scope, permission mode, language, goal, document or task
list.** Each is a latest-wins or accumulating fold over the ledger: the effective type, the
effective scope (**failing closed** on an absent or malformed block), the effective
guardrail, palette and capability, the permission mode, per-tool standing grants, the
current language (latest parameters block *that carries one* — deliberately asymmetric with
the type), an open speech call (a forward fold over the open/close pair), pending queued
messages, a document's head text folded over the immutable edit calls that produced it, and
each workflow's own state projection.

Exactly two mutable single-field writes exist on the session document, and both are called
out as such: the **cursor** and the **title**.

### 3.5 Storage

The source system stores a session in **three collections** because its database has a
per-document size ceiling: session metadata plus the cursor and a tail counter; one document
per block; and a **junction row per (session, sequence)** with a unique index on the pair. A
read is two indexed round trips. This shape is what makes the next property possible.

**Forking is by reference.** A fork inserts a new session whose junction rows point at *the
very same block documents* up to the cut, and stamps the inherited count. Nothing is copied.
Two consequences fall out and both are load-carrying: pending queued messages must exclude
the inherited prefix, or a record waiting at the cut is delivered in both branches; and a
root id exists so a whole fork tree is one indexed query.

**Appends run in a multi-document transaction**: reserve the next sequence atomically
against the observed tail, insert the blocks, insert the junction rows. A lost race surfaces
as a transaction conflict or a unique-index rejection, and the seam distinguishes "the
engine told us this did not commit" from genuine ambiguity before retrying.

**Session versioning is a hard cut, not a migration.** Reads filter out anything below the
current version. Stated reason: the feature is beta and old sessions are disposable. The
trade is explicit — the escape from a mistaken migration is a forward migration and nothing
else.

---

## 4. The runtime

Four layers: a reactor that routes, an actor per session that ticks, an orchestrator that
owns one turn, and a core that performs it.

### 4.1 The reactor

Owns no per-session logic. It lazily creates one actor per live session and routes two
inputs to it: **database change events** (three concurrent streams — junction inserts,
block updates for streaming flushes, session-document writes) and **latch events** from the
bus. Any stream failing unwinds the group into a short retry that resets all actors, which
come back suspended. The reactor also owns the lifetimes of the bus consumers.

A deploy-handover affordance exists: *stop feeding the actors* while the HTTP surface keeps
serving its tail. An actor only ever acts on a wakeup from one of the two routed inputs.

### 4.2 The actor: two questions, kept apart

> The **ledger** answers "is there work owed?"
> The **latch** answers "may I engage this session at all?"

The **latch** is in-memory and defaults to *suspended* at boot. Two reasons, both crucial: a
restart never stampedes every in-flight session into running, and a freshly booted process
in a deploy overlap has no way to act on sessions it holds no user intent for. It is cleared
only by explicit user intent (creation, a user message, a fork, opening a call) and
re-engaged only at boot and on an unrecoverable client-side model error. **A normal turn end
never re-suspends.**

The latch governs the *engagement* family — running a turn, the settled-frontier observers,
the frontier dispatch. It deliberately does **not** govern system work or the redispatch
walk: *a suspended session finishes what it owes; it never starts anything new.*

**The tick is serialized by a lock.** The failure that forced it: unserialized, two ticks
interleave across their awaits, each holding a pre-turn snapshot, and both pass the turn
check — one task, two identical model requests a second apart, observed in production.

Tick order: redispatch walk → advance system work → return early if a turn is running →
re-read the session (a work hook may have appended) → **latch check** → frontier observers,
in order, only when the cursor is at the last block → frontier dispatch, once → the
model-turn check.

### 4.3 The cursor

The cursor walks forward from its persisted position:

- blocks before it are **confirmed and never touched again**;
- every block from the cursor onward runs and is read for doneness; inert and
  model-or-user-owed blocks return the trivially-done default and the cursor sails past;
- only a `false` parks the cursor, to be re-run next tick;
- **the cursor's own block re-runs inclusively on resume** — that is the crash-recovery
  path: a lost in-memory registration is re-made, a pending event re-emitted. Every work
  hook must therefore be idempotent, keyed on durable state ("is my next block already
  present?");
- an exception logs and breaks without advancing.

What this replaced is instructive: a separate "is this fulfilled?" predicate on sixteen
block classes, plus an in-memory driven-set. That predicate was *a second prediction of what
the work hook accomplishes, kept in sync by hand — and it had drifted.* Making the doing and
the done-signal the same code is the whole point.

**Starvation is an explicit contract**: a never-done block parks the cursor forever and
starves everything behind it. That is acceptable only because every real not-done block has
an out-of-band completion path. It is documented and tested, not a surprise.

### 4.4 The frontier — the single arbiter of a model turn

> A model turn is owed **iff** the cursor reached the **last** block (nothing parked it —
> every tool call resolved) **and** that block's ask is `model`.

Two axes kept separate: the cursor axis answers "is this block's own system work done?"; the
turn axis answers "may the model speak now?", read from the cursor's terminus and **never
from a backward scan**.

The retired implementation was a backward "latest ask" scan, and reintroducing it is
forbidden by name. The bug: with three parallel tool calls whose results arrive staggered,
the backward scan sees a later sibling's result (awaiting the model) while an earlier call
still dangles, green-lights a turn, and produces an ill-formed request — a call with no
output — which reads as a completed turn and stalls.

The frontier read passes **through** transparent kinds. This is the second half of the fix:
an inert record appended by an out-of-band writer into the window where the tail owes a
not-yet-started turn would otherwise become the tail and make that turn unreachable forever.
The cursor term is untouched — everything must still be processed.

After a turn completes, one more tick is scheduled **explicitly forbidden from starting a
turn**: a turn that wrote nothing leaves the frontier exactly as it found it, still owed to
the model, and a tick that could dispatch would fire the identical turn again forever.

### 4.5 The redispatch walk

Anchored on the **latest** block each tick. If its routing hook points somewhere, follow the
chain to its end and unwind, running the deferred work **terminus first**. It names no kind
and no domain concept; idempotency lives entirely in the routing hook returning nothing once
the work is done.

The security-critical instance: an approval request routes **only** when a matching approved
decision exists *and* the call is still unresolved. The recorded reason — the walk anchors on
the latest block, and the request *is* the latest block the moment a deferral is recorded, so
an unconditional route runs the controlled body **before any approval**. Observed live: a
destructive call executed without waiting.

### 4.6 The orchestrator: one turn, and the nudge

Per turn it stands down if a live speech connection owns the turn (one place takes that decision),
resolves the agent from the event-sourced type, builds a core, snapshots the response ids
seen *before* the run, and runs exactly one turn.

Failure handling distinguishes what each failure means:

- **Cancellation** → append an `interrupted` status owned by the *user*; by definition
  nothing but a user message may follow it.
- **A client-side model error we caused** — a request built wrong fails identically on every
  retry, so suspend the session and log loudly. A **provider-class** failure (exhausted
  account, refused key) explicitly does *not* suspend: that would wedge every session of an
  outage behind a state with no affordance to clear it. The class is read from one shared
  set so the breaker and this decision cannot disagree. A throttle response is *not*
  provider-class — it is the one refusal that proves the account works.
- **Anything else** → the turn died mid-flight.

Every death path goes through one recorder which: **resolves every open tool call with an
error block first** — otherwise the cursor parks on a dangling call and the send-admission
check refuses the very retry the error message promises; then appends the turn-error block
as the true, user-owed tail; and **never raises**, because escaping would restore the exact
silent death the recorder exists to end.

That the turn-error block awaits **user** is doubly important: it makes a crash-retry loop
structurally impossible, and it makes retry work with no retry-button code at all. The
composer *is* the retry.

**The nudge** (an automated harness message when the model ended a turn with its goal
unsatisfied) is paused when any tool call is unresolved, when a real queued message is
waiting (a real message always outranks a synthetic one), when the tail awaits system, user
or none, when an escalation is open, or when an interrupt is unresumed. Its prose is
authored as translation keys with parameters, and it is explicit on three axes: attribution
(this is the control loop, not the user), imperative (call a tool now, do not narrate a
plan), and escape (from the second attempt, point at the escalation tool).

### 4.7 The core: exactly one turn

The core runs **one** turn and returns. The removed inner loop is named as a bug — it ran the
model on its own text output, because the second pass bypassed the frontier.

Per turn: load the session → build neutral messages → check for interrupt → resolve media
references through an injected transport pass → resolve tool schemas → stream.

Streaming is routed to **per-channel tracks** (text, reasoning, tool calls), each owning one
channel's lifecycle: a lazy placeholder on the first delta, so a tool-only or empty turn
leaves no orphan block; delta accumulation; a periodic flush; and finalization into an
**immutable twin appended alongside** the mutable tail. The stream is closed explicitly — an
abandoned generator holds the provider transport open past the turn. The interrupt is
re-checked per chunk against a fresh read.

**Usage rides the last assistant block the core finalized** — the text block, or the
reasoning block for a reasoning-only request, or the last tool call for a calls-only
request. A dedicated usage-carrier block was rejected in review: *a new block kind is for a
new kind of ledger event, not a carrier for metadata another block generated.* Only reported
facts are stored; a missing count stays absent, never a computed stand-in.

**An empty completed response records an empty text block**, deliberately with no special
case. Recording nothing left the model-owed tail wedged forever — the frontier stuck on a
reaction owed, every send refused, no change event to re-run it — and recording an error
would forge an event that never happened.

---

## 5. The event bus

A tiny in-process publish/subscribe. Subscribers declare accepted **types** and routing is
by declared type, which makes "an event nobody handles" a *detectable* condition.

**Three mutually exclusive no-subscriber modes** (passing more than one raises):

| Mode | Behavior | When |
|---|---|---|
| strict (default) | raise at the emit site | the event must be handled; nobody listening is a bug. Catches the hang where an awaited effect goes nowhere. |
| wait-for-subscriber | **buffer**, flush on subscribe | boot-time intents whose consumer will exist but may not yet |
| allow-unclaimed | **drop** | broadcast fan-out where zero listeners is normal. **Never** for an event carrying an awaited effect. |

Subscription is **eager** — the queue registers synchronously before the caller iterates,
closing the race where a strict send between task creation and first iteration wrongly sees
no subscriber. A **one-shot registration** (filter, action and done-signal in one) composes
with buffering: a matching buffered event is drained into it at registration.

Two events deliberately carry payload as truth, against principle 3, and both are documented
exceptions: they carry **in-process facts no durable store or change stream can observe** —
whether a turn is live, and that an in-execution parked set moved. The second names its
production symptom: a parked write sitting invisible until the operator reloaded the page,
which is the one moment a person most needs to be told.

The **named bridge** of principle 7 holds both bus handles and translates one system's
completion events into the agent bus, in one direction only. Neither bus learns about the
other.

---

## 6. Building the model request

### 6.1 The neutral projection

A **module-level pure function** builds neutral messages from a session. It names no block
kind and no domain concept: it asks each block for its own projection and collects
generically. An **assistant turn** — reasoning, text and tool calls sharing one response id —
is reassembled into one neutral assistant message through each member's turn-text seam. A
system-prefix contribution joins the **cached prefix**, but only as a maximal consecutive run
from the top; the first ordinary message **seals** it. Invisible blocks neither emit nor
seal.

The seal rule carries an incident: a late contribution used to *raise*, and stalled every
workflow session whose guardrail sealed the prefix before the seed arrived. Post-seal
contributions are now a legitimate shape, rendered as an ordinary in-sequence system
message — which is also the faithful rendering of a mid-session workflow switch.

An **empty prefix raises loudly** instead of falling back to re-deriving the prompt from the
agent object: that fallback was never a faithful duplicate and would silently ship a degraded
system prompt to a real user.

Ordering is a stable sort on each block's own position unless it declares an annotation
position. A declared position that is not a real *earlier* position is ignored — projecting a
block ahead of the block it claims to annotate would reorder the conversation on a lie.

**The same fold serves every consumer**: the typed turn, the spoken re-seed (with the
projection and turn-text hooks handed in as a pair) and the title transcript. One fold, with
per-provider encoders after it — never a second fold to drift.

### 6.2 The neutral vocabulary

Text, tool use, tool result (paired by id), a **media reference** (a pointer to bytes in
durable storage), **inline media** (bytes resolved for this request only), and reasoning —
carried as verbatim text **and** the opaque provider item, *beside* content, never inside
it. The core is API-agnostic by construction: it cannot leak a provider's wire shape because
it never builds one.

### 6.3 The client layer

A protocol over a uniform streaming contract, with the client chosen by **published model
capability** — the richer item-based API wherever supported, the older message-based one
otherwise. The wire-format derivation is **one function**, shared with the capability
recorder, so a recorded capability can never drift from the client actually built. The
item-based client runs **non-stateful**: the full history is rebuilt from the ledger each
turn, so there is no server-side conversation state to reconcile.

Stream errors are translated at the boundary so that SDK failures — at request time *or*
mid-stream — surface as one neutral error preserving the status and a provider-health class,
and so every turn's outcome reaches the circuit breaker.

### 6.4 Tool schemas for a turn

1. **The static effective set** = core ∪ added − disabled − withheld, resolved through one
   definition shared by the schema builder and the palette observer, so they cannot diverge.
   A subtraction wins, even over a core tool.
2. **Intersected with the session's stamped palette block**, read off the ledger and never
   recomputed — so the schema the model receives and the palette that will judge its calls
   this turn come from the *same* block.
3. Each tool is asked for its **served schema** with a live read of the platform kill
   switch, so withdrawing a capability stops advertising it from the next turn, with no
   restart.

A ledger with no palette block means **no filtering** — deliberately fail-open, because the
palette is an *exposure* mechanism and enforcement in depth lives in the tools' own checks.

---

## 7. Tools

### 7.1 Declaration and namespacing

A tool is a decorated **factory** taking a frozen, fully typed dependency bundle. Importing
the module registers it; one explicit trigger walks each declared package. Why a factory and
not the object: declaration is import-time and dependency-free, construction is startup-time
and dependency-bound.

**Namespacing is applied by the framework** from the package a factory lives in — the
composition root declares (package, namespace) pairs, so an author never writes a prefix.
One place prefixes the registry key **and** the model-facing schema name atomically, so the
two can never drift; a mismatch would let the model emit a name the registry cannot resolve.

### 7.2 The tool is the actor too

Capabilities, all looked up by name, all inert by default: run; **admission check**
(side-effect-free, returning proceed / refuse / defer); **submit** for a human's out-of-band
answer; **visibility** (pure and synchronous); served schema; declared media destinations; a
display lens; and a full set of **result-side** hooks.

Metadata read as data points, never name matches: interactive, admission-controlled, core,
category plus deferrable, admin-only, pooled, and **protected** — a class attribute
defaulting to *true*.

**Result-side behavior** is the second half of one dispatch idiom: a result carries the call
id, the call carries the recorded final name, and the session load resolves the definition
and **attaches it to the result block** so its hooks delegate with no registry lookup at hook
time. This replaced four registries that dispatched on "which interior key is present" —
presence-testing an interior key collides across tools by construction and divorces behavior
from its owner.

The **keep-registered rule**: a tool whose historical results still live in current ledgers
must stay registered under its recorded name, or those results silently degrade to inert
defaults.

### 7.3 The runner: the single admission owner

One serial consumer re-reads the ledger and advances a call **from recorded facts alone**:

| Recorded state | Action |
|---|---|
| result or error exists | stale wakeup, skip |
| uncontrolled tool | run directly; no admission evaluation, no response block |
| response says proceed | run |
| response says refuse | skip — the error was recorded atomically with the response |
| response says defer | run only after an approved decision, consulting standing permission state first |
| no response yet | evaluate the tool's side-effect-free check, record the answer, and act on **that same answer** in this pass |

**Every pre-body write rides a compare-and-swap against the very read it decided from** —
the observed tail and cursor. A concurrent evaluator or a mid-pass human submission moves
the tail, this pass's write loses, and it re-reads and re-decides. This is what keeps one
call from ever carrying both a success and a refusal, and one controlled call from ever
gaining two responses.

Body execution is **at-least-once** and deliberately unfenced by claims or locks: a resume
re-runs an unresolved body and a second result is the recovery.

**Two dispatch paths, chosen by a data point on the tool**: inline on the serial consumer by
default — the serialism absorbs duplicate wakeups structurally — or on a **bounded pool of
slots**. The pool exists because a model-authored deadlock inside a sandboxed execution once
hung until its wall clock and head-of-line-blocked *every other session's* tool calls. The
pool dedups against its own live slots and re-checks resolution freshly on the slot, because
freeing the consumer immediately opens a window where a stale read and a cleared mark both
miss. Both paths run the **same** body phase, so the invariants cannot fork.

### 7.4 Admission, approval and permission

The durable chain, with only read-only folds afterwards:

```
tool call
  └─ admission response (proceed | refuse | defer)   ← the ONE admission answer
       └─ approval request (asks nobody: parks)      ← recorded atomically with a defer
            └─ approval decision (approved | denied)
```

The tool's check **writes nothing**; the runner records the response *and* its companion in
one atomic batch. A response without its companion would leave the call unresolved forever,
or leave no clearance a human could decide.

An optional **request payload** is a read-only preview the tool computes while it still holds
its services, frozen onto the request, so the human approves the *concrete effect* — the
record's name, the path and its before/after, what a deletion orphans. The display lens
cannot do this: that hook is a synchronous ledger-only fold, while the admission check is
async and holds the services.

**Denial reasons are two mutually exclusive fields, not one string plus a source flag** —
"who denied" is structural, not inferred.

**Permission modes sit on top of, never inside, the admission law.** A mode block (manual /
accept-edits / autopilot) and a per-tool standing grant block, both latest-wins. Both are
frontier-transparent inert records with **no projection at all** — *the model never learns
that permission vocabulary exists; it must not reason about what it is allowed to do.*

Auto-approval is a pure predicate: an explicit **grant** covers a call in every mode and
beats a mode (it is the more specific standing choice and stays truthful after the mode
flips back); autopilot covers everything; accept-edits covers exactly the unprotected tier.
**Protected is the default**, and a call whose tool no longer resolves is treated as
protected. The tier is deliberately **not stamped** on the call block: a stamp would freeze
an already-parked call of a mis-tiered tool on the wrong tier, failing in exactly the
direction a re-tiering exists to fix.

There are **two transports for an auto-approval and no drain machinery**: *born covered* —
the decision joins the deferral's own atomic batch, pointing at a sibling of that batch,
which the compare-and-swap pins — so the request is never open at all; and *parked before
coverage existed* — resolved by the admission-read consult, because a parked call re-emits
its wakeup on every tick and **any append is that wakeup**. No sweep, no second write path.

### 7.5 Interactive tools

An interactive tool stops the turn and its result arrives out-of-band from a human: the call
awaits **user**, its work hook reports done (the user owes the result), and the tool
implements submit to validate and append the resolving block. The HTTP route is a dumb pipe
to it. A controlled interactive tool still emits its wakeup so admission is recorded before
the handoff.

The framework's own structured-question tool validates at the check against the schema and
at submit against the **original question read fresh from the ledger**, and renders answers
as a transcript in its result projection — the only place the structured truth becomes prose.

The approval request is itself interactive but is not a registry tool, so the framework owns
its submit, including a conflict response for the loser of a two-tab race and a teaching
error that names **which requests are actually open** when a stale reference arrives.

### 7.6 Visibility, the palette and deferred tools

Three-valued visibility, mirroring the data-disclosure enum:

- **visible** — schema shipped, calls run;
- **unavailable** — no schema, calls refused, but the model is **told by name** that the tool
  exists and is out of reach, so it neither improvises a missing capability nor spends a call
  discovering the refusal;
- **hidden** — no schema, calls refused, and the name appears in **no projection anywhere**.
  *Hiddenness is the absence of evidence.*

The **palette observer** runs each settled tick, resolves the session's principal **live**
(which is exactly why a demoted operator loses a controlled tool on the very next turn), asks
every tool in the effective set for its own visibility against one frozen snapshot, applies
deferral, and appends a new palette block **iff the name sets differ**.

**Deferred tools** are a context-economy layer strictly *after* visibility: a visible tool
moves to deferred iff it declares itself deferrable, is not workflow-added (a workflow's own
tools are its purpose, never behind a round trip), and its category is not loaded. "Loaded"
is a **pure ledger fold** — a load call plus its successful result *is* the durable fact,
with no marker block. The rendering degrades under a line cap in a documented order: drop
per-tool description tails first, then collapse whole categories largest-first.

The refusal policy lives in one place and its arms are chosen by **what was disclosed**: an
unavailable tool is named back with why; a deferred tool is taught the exact load call; and a
**hidden** tool — or any name outside the palette — gets the **byte-identical wording of the
unknown-tool error**, deliberately, because confirming "hidden, but real" would itself
disclose existence.

### 7.7 Tool-design rules that generalize

- **List-returning tools are always paginated** — a page input, a bounded size, a total and
  a has-more flag, and an affordance to request more.
- **Detect behavior from a data point, not a name match.** Names are namespaced at the
  registry, so any equality check on a name silently breaks.
- **Context is king: an error must carry what the model needs to proceed.** Every teaching
  error retrieves its fix knowledge from the central registry or manual for the failing
  thing — never a hardcoded per-case hint. The observed failure: a shape error on one field
  answered by re-editing an unrelated one.
- **Do not encode human judgment as a hardcoded list.** Keep the mechanical check strict and
  give the model an explicit **override** parameter — the model has the evidence and is the
  judge; the check only flags.
- A fetch-like tool carries three recorded incidents: **one hard total deadline** over the
  whole chain, because a per-request timeout does not bound a redirect chain; head-wise
  truncation within *both* a byte and a line budget; and an **origin-refusal registry** that
  says what a refused status *means* and what to do instead — naming the gatekeeper from the
  response's own headers, stating the observed scope as an observation and not a law
  about the host, and answering a *second* URL on an already-refusing host from the session's
  own ledger receipt **without sending anything**. The receipt that forced it: every URL on
  one host answered with a bare status line, so the model tried seven of them across two
  days.
- **A link a user pasted in this session is consent to fetch it**, even where a robots file
  disallows crawlers: that file is a site's approximation of consent written for crawlers,
  and a human handing over the link *is* the consent it approximates. Matching is normalized
  and **downward only** — equality or subdomain.

---

## 8. Sandboxed execution and a typed service surface

Model-authored code runs in a **subprocess with all permissions denied** — no network, file
system, environment or subprocess access. It is deliberately **uncontrolled by admission**:
admission is for side effects, and this can only read prior results and compute.

The host speaks line-delimited JSON-RPC over the subprocess's standard streams, serving:
reads of prior tool results from a **frozen snapshot** built once against the ledger prefix
*at the call's position*, so a concurrent append cannot change what the code sees; chunked
artifact and media write-out through one sink, distinguishing a **file for the user** from a
**file for the model's own next turn**; chunked pull-reads of the session's uploads, where
the sandbox's pull pace is the host's read pace; and a **single dispatch chokepoint** for the
service surface.

Limits are wall time, output size and heap; breaching any kills the subprocess and produces
an error **naming which one**, with the message teaching against the specific bound it
enforces. Cleanup runs under a finite budget and can never extend, replace or mask the
execution's own outcome.

The **service surface** is typed and predefined, one module per backend service, so a script
reads and writes in bulk instead of calling tools one page at a time. The sandbox side is
**transport only**; all policy lives in the dispatcher: resolve the descriptor, failing loud
with a teaching error that names the real function names; validate arguments against the
descriptor's **own parameter model** with unknown fields forbidden, refusing with the model's
own field names — never a hand-written hint that can drift; resolve the principal **once per
execution**; enforce admission with the same predicates the platform's own surfaces use, not
predicates invented at the chokepoint; ask the **human-approval question here and nowhere
else, and only of a write** — a read never consults permission state at all; then hand off,
reporting a write's exact stored output.

Two refinements generalize well. **Fold protection**: if a workflow's state is folded from
recorded tool calls, then writes of those entity kinds from inside a script are refused with
the tool name to call instead — a write that never becomes a recorded call would be invisible
to the fold, so the workflow's state would stop describing reality. And **draft builders**
give every draft-backed module the same four verbs, implemented once, knowing nothing about
any entity; a malformed target resolves to the **stricter** write class, so a call that cannot
say what it is about is never treated as the cheaper one.

**In-execution parking**: a write the standing permission state does not cover parks *inside*
the execution. The pending state is in-memory only — nothing about it is ever written, and a
restart drops it with its execution — is served to the client through the call block's
display lens, and the **first** answer wins. While parked, the execution **hands its pool
slot back**, so the wait happens off the bound, and the parked duration becomes the offset
both the wall clock and the work timer read.

---

## 9. Scope, access and disclosure

### 9.1 The scope stamp

A **scope block** is appended at creation, after the prompt and parameter blocks and always
**before any user message** — that placement is the invariant that makes forks inherit it. Its
kind is unrestricted, organization-bound, or a single end user's private session.

It is its **own block** and not a field on the parameters block because it is read by both
the tool layer (to scope data) and the API layer (to filter visibility): keeping a security
fact out of the workflow block keeps the boundary legible.

**Resolution fails closed.** An absent *or* malformed block resolves to *unreachable*, never
to unrestricted. Resolving the unknown to the most powerful scope is fail-open: a dropped or
malformed block would silently become full reach. Failing closed, the API returns not-found,
the per-turn scope array is empty and matches nothing, and the write check refuses.

### 9.2 The per-turn scope array and the disclosure rule

Scopes are derived **per turn** as typed variants, and a private session does a **live read**
of the user's role and memberships, so a demotion takes effect on the very next turn. The
principal resolver is one half of the platform's **single** write-access decision service,
shared with the HTTP checks so the two can no longer disagree about a row.

The read rule has two axes: **full** if the row matches any scope or it is published;
otherwise **name-only** — a listing may show the name, but a *detail* read of a name-only row
is **denied**. The name-only rendering deliberately drops every detail field, and matching is
dispatched on the structured scope variant, never a string split.

### 9.3 The guardrail

A permission-light private session must not turn the agent into a general-purpose personal
assistant. The guardrail is expressed by **appending**, never editing: an envelope on
entering the scope, a retraction on leaving. One predicate decides it, read at both
activation points, and its second term is a real fix — an operator's private session already
reaches everything an unrestricted session reaches, so guardrailing them restricts the one
person the product restricts nowhere else.

The prose is **graduated pushback**, not a hard refusal: a small harmless aside gets a light
reminder, and resistance rises as requests drift further off-mission, grow larger, or keep
coming.

Both the guardrail's **token and its supersede-token are sensitive fields** — rendered into
the model-facing projection inside a verbatim authoritative frame, and **stripped from every
client payload**, so a forged "guardrail lifted" message typed into the chat cannot reference
the real token. The frame states plainly that the token is anti-spoof context, not
enforcement.

The unrestricted-scope sibling is **purely informative** — it states the context and instructs
nothing, because access is enforced at the data layer — carries no token chain, and announces
itself on a *switch* only, never at creation.

### 9.4 The transition consequent

The scope block's work hook compares the **full scope value** before and after its own
position, so a move between two organizations and a move from one to several both count as
deltas that a kind-only check missed. It emits a **pure wakeup carrying only its own ledger
id**; the service re-derives destination and prior from the session *at that position* and
appends the consequent idempotently and **position-aware**, so a re-entry appends a fresh
consequent instead of finding a superseded one.

---

## 10. Files, media and recorded capability

### 10.1 The capability record

A recorder is the actor's **first** frontier observer, appending a capability block whenever
the observed provider, model, wire format and capability state differ from the last recorded
one. The motivation is stated as a law:

> The projected transcript must be a function of **the ledger alone** — so the ambient model
> configuration is recorded **onto** the ledger.

Its observation source closes over the **same** configuration facts the client factory
consumed, through the same derivation, so the record cannot drift from the client built.
Change-equality is identity plus capability fields. The provider identity and parameters are
**sensitive** — an internal endpoint is infrastructure, and sessions are visible to
non-operator callers.

A text limit carries its **unit**, because providers genuinely differ (characters versus
tokens) and enforcement must measure in the declared unit — never a conversion. An unknown
model takes a **split default**: optimistic for endpoints, conservative for media. Validation
fails loud at startup on an unknown key or a wrong type. The **governing** record for a
position is the nearest record strictly *before* it — a record governs what follows it, never
itself.

### 10.2 Uploads, artifacts and media

Two dedicated private storage prefixes, one for user uploads and one for generated artifacts.
Each prefix's key scheme lives in its own module; what is common — filename sanitization, the
signed content-disposition, the delete scope check, the orphan sweep — is shared. The **scope
check is the last line of defence on every delete**, independent of how the key was derived
upstream, so a bug anywhere in key derivation can corrupt at most *which* object is deleted,
never reach outside the prefix.

**One ingest pipeline, two provenances.** Processing runs once at the record moment — at send
for a user upload, at execution completion for tool-attached media — through a **mime-keyed
handler registry**. The result is stored on the block **once** and the projection only reads
it: nothing re-parses per turn, and replay stability comes from immutability. Whether a file
went natively is **derived** at render from the governing capability record; there is no
stored decision field.

A **request-build transport pass**, injected blind into the core, turns each neutral media
reference into inline bytes, or a **demoted presentation** (temporarily parsed text under a
preamble, or an honest withheld note), or nothing. The degraded set is **derived only — no
field, no column** — so it self-clears when capability returns. The text budget composes the
snippet law with the provider limit, and the truncation notice rides **outside** the
allotment so the continuation offset is exact.

**Tool media has one append seam**: a body returns pending emissions on its result, and the
**runner — never the body —** records one media block per file, appended *between* the call
and the result. Each reference resolves per mime class against the governing capability
record, with an honest line for every unresolvable case and for a malformed ledger. *Never a
silent drop.*

An **attach resolver** is the call-layer chokepoint: the model cites artifact ids, never raw
storage links, and the resolver — run immediately before every body, never as per-tool logic —
promotes the artifact and fills provenance from the artifact's own stamp. The model
hand-carries neither links nor provenance, and a misplaced reference in another field is a
**teaching refusal**, never silently stored garbage.

The sweep's primary term: a key **referenced by any handle in the session's ledger is never
deleted, at any age, under any process id.**

---

## 11. A model-parametric field editor

A domain declares **one surface** — its model class, its read-only map, its manual, its
teaching noun and example paths — and a bounded uncontrolled **read** plus an
admission-controlled **replace-at-path write** fall out of it.

The vocabulary and the addressable roots are **derived from the model definition**, so a new
field joins by existing instead of by being added to a second hand-kept list.

The path grammar enforces three ergonomic rules: a per-language list addresses **by
language**, never by index, merging per language; an id-addressed sub-document list addresses
**by its stamped id**, because indexes shift and the id is the identity; everything else is a
plain replace at the path, with no clever deep-merging, because predictability wins.

The **bounded-read law**: a large array or string truncates with an honest in-band notice
naming **the sub-path to read next**, spelled in whatever addressing that list actually uses.
Asking for a field's description serves the **manual entry** instead, deliberately unbounded —
the context economy applies to schemas, not to field knowledge.

**Validation is candidate-based**: apply the replace to a copy, prove it still validates,
then compute the narrowest update. **Every validation refusal appends the failing field's own
manual entry.**

Paths whose rules cannot be expressed as a replace leave the generic engine and are shared by
every surface that carries the field. One of them carries a rule with wide application: its
refusal is asked **identically at the preview and at the write**, so a human is never shown an
approval request for a call the write is going to decline.

---

## 12. Agents and workflows

> "Chat" is the **absence** of a workflow, not a separate class: a bare agent *is* the
> plain-chat agent.

Agents are **pure** — ledger in, decisions and prompts out — constructed with no arguments and
holding no collaborators. An agent declares capabilities that the framework reads as **data
points, never type-string matches**: its prompts; its added, disabled and withheld tools;
whether it has a goal and whether that goal is satisfied; whether it has **unfinished work**
(deliberately a different question from having a goal, because a workflow that switched the
nudge off in one phase is still mid-task); what scope it requires; its first chain block; and
its catalog descriptors.

A three-armed presentation union is discriminated by which keys are present: a literal the
client renders verbatim; a **pure-UI** translated string the model never saw; and a
**model-facing** translated string that carries the exact original the model read, which a
"show original" affordance reveals.

**The self-assembling prompt chain.** Session creation seeds only the irreducible minimum: a
system prompt and a parameters block. That block's work hook fires **only on a type delta to
a named type** and emits an event carrying its own ledger id. One service — the only place the
type-to-workflow lookup lives, on the *workflow* side of the bus — appends the agent's first
chain block. That block's hook appends the next, and so on. The agent names only its own
opening block and the chain owns the rest. Each link's idempotency check and its doneness are
**the same question** ("is my next step already present?"), identified by envelope kind rather
than position so a replay recognizes what already arrived.

**Workflow state is an accumulating fold that never clears anything** — every question is
derived from accumulated facts. Ids derived from position are **activation-local**, so a
second activation numbers from one again; anything displaying them must fold within the same
activation window, or the displayed ids and the created ids disagree.

Two patterns from the workflows generalize strongly:

- **A recorded-fact checkpoint registry** turns "the model asked the user" from a prompt hope
  into a recorded fact. Each checkpoint declares what it records, its phase, and **which
  creating tools it withholds while open** — so the model is never handed a capability before
  its preconditions are recorded, and the tool is simply *absent* from the palette instead of
  announced and refused, so the model cannot half-build something and then be told no. Every
  checkpoint's recording tool is generated from the registry, so the built name and the
  granted name are one string.
- **A completeness fold measured against registries parsed from the generating prompt
  itself**, so the bar and the guidance cannot drift; a missing section breaks the suite at
  import. Measurement runs against **shadow documents replayed from the ledger's own record**
  of what the create and edit calls wrote, applying edits through the same machinery the live
  edit used.

---

## 13. Realtime speech

Entirely **inert by default**: the builder returns nothing wherever no speech model is
configured, and that same switch decides whether the router is even mounted. A deployment
that has not configured speech has no route to reach and no affordance to click.

Three laws:

1. **Speech appends finalized blocks only.** Nothing mutates, ever. The live bubble is
   streamed *presentation*, not ledger rows.
2. **Nothing is fabricated.** No block for a UI state, none for a projection gap: no user turn
   for a failed transcription, no cost without a usage record, no reasoning without a
   reasoning signal.
3. **The backend is the only author.** Every row derives from the backend's own authoritative
   connection, never relayed by the client, and carries the **neutral source identity** of the
   fact it came from, so a re-delivered event is a no-op instead of a second row.

**Entering speech mode is appending a block.** An open/close pair is the parameters-block
idiom applied to a call: whether one is open, who holds it, whether the text orchestrator
stands down, whether typed input is admitted — all derived from the pair. Nothing is a flag on
the session document, which is what lets a call survive a type switch and lets a fork close an
inherited call for itself. Typed input is refused while a call is open **for holder and
non-holder alike**: they hold a microphone and a call has one voice — hiding the composer is a
courtesy to the honest client, while the block-level refusal is the real boundary, because a
direct request crosses no template.

End reasons are a **closed vocabulary**, and the table that lists them also classifies each as
an ordinary ending or a fault, so no consumer keeps a second copy of the list.

**The row allowlist inverts the usual direction.** Each row **names** the fields a client may
receive; everything else is backend-only by construction, inherited fields included. A
denylist asks each row to remember what must not travel, so a field added later travels by
default and the omission is invisible. The scoping reason generalizes: these are rows a
*provider* authors half of, and its machine codes, its prose about its own machinery and the
identity we key re-deliveries on are ours to hold and nobody's to receive.

**The user's side is two facts.** One block records that the user *spoke*, at the moment
speech ended, **with no text**. Its late-arriving annotation carries the words and projects
**at the utterance's position**, so the conversation reads in the order the world happened in
even when the transcript arrives behind a tool call made meanwhile. The transcript comes from
a different model than the conversation's, so it is advisory: the text model reads it behind a
**transcribed-audio mark taken from the block's identity, never a content sniff**, while the
spoken re-seed reads it verbatim. A text model reading a transcript as typed text cannot
account for a homophone, a mangled name or a dropped word.

The spoken assistant block **extends** the ordinary text block, so every shared fold never
learns the difference; the rejected alternative was a spoken-marker field on the generic
block, which would have put a modality-shaped field on generic infrastructure.

**Ingestion is an idempotent projection, not a stream of side effects**: every derived block
carries a source identity, and the append is already-ingested-or-append against it, answering
*whether this delivery is the one that wrote*. One object owns open, roll and close together,
because every open path can fail into a close and a roll ends one connection and begins
another. One insertion chokepoint enforces one open call per session **by the write itself** —
a read that says no call is open is only true until the write.

The driver is the actor's third frontier observer **and** the orchestrator's stand-down read,
in one object, because those are two halves of one rule and splitting them would let a process
stand the text turn down with nobody taking it. Each settled tick it makes the provider
session agree with the ledger, injects an **event-shaped note** for an unresolved approval
("approval was requested for X at T" — true forever) that stops the moment a *decision* exists,
then releases owed outputs and asks for exactly one response — **re-validating the holder's
authorization on every release**, because a call turns a one-time endpoint check into an hour
of unchecked driving.

---

## 14. Cross-cutting systems

**Spend and timing are never persisted.** The context gauge reads the **latest**
usage-carrying block; cost folds **every** carrier. A carrier is detected by **data point**
(an input-token count is present), never by block family or position. The price matrix is
**versioned**, and each request block stamps the regime in force when it ran, so a price
correction never silently reprices history. A version that no longer resolves prices as
**unknown → incomplete, never at current rates**. Per-modality counts are described parts of
the totals, carved out at their published rates; a duration-billed model is a per-minute
carrier instead. A modality the adapter cannot price marks the total **visibly incomplete**
instead of inventing a number. Durations derive from spans, with human wait and queue wait
excluded **by construction**, never by subtraction.

**Titles** are an ephemeral, self-deleting task on a small model, triggered **from the
blocks** — one helper shared by the typed and the spoken trigger, so one decision path with
two input events cannot drift. The transcript is built through the **same projection hooks the
model reads**. The block-side trigger replaced an orchestrator-side one because a session that
ran a workflow and then flipped to plain chat used to never get a title.

**Queued messages** are the reference implementation of resolution-by-reference. A message
typed while the session cannot take one is **recorded, not refused**. It is delivered when the
user message built from it **names** it, and withdrawn when a removal block names it, and
**whichever arrives first wins forever** — which makes double delivery and withdraw-after-
delivery *unrepresentable* instead of merely checked, with a validator making the reference
unconstructable on an assistant block. The record stores the send's **inputs**, not the blocks
they become, so N waiting messages fold exactly as N immediate sends would have — except
attachments, which are verified and ingested at send time so an unresolvable upload refuses
loudly *while the sender is still looking*. Delivery is the actor's **frontier dispatch**: one
blind callable invoked after every observer and before the turn check, appending through the
*same* materializer and admission the normal send path uses. Admissibility decomposes as
"drain-admissible **and** the model does not own the frontier" — one predicate decomposed, not
two predicates.

**Out-of-band operator messages** travel through **one named bridge** into a session ledger,
with every field **frozen at delivery** (including a copy of the source thread's channel and
body, so the record stays self-explaining), carrying **no operator identity**, and with two
deliberately opposite seams: the *model* sees it — that is how the words reach the user at all —
and no session **view** does. The operator's words reach the user as the agent's own reply, in
their language and in the agent's words. Delivery reconciles the thread against the ledger and
appends what is missing in one conditional batch.

**The client-context envelope** stores **data points only** — surface, device and session ids,
timezone, page, a typed trail — and **all prose is a derived projection**, versioned so old
blocks keep projecting as written and an unknown version falls back to a minimal neutral
rendering. Comparisons the wording needs are derived **at projection time against the previous
envelope in the prefix**: no stored comparison fields, no orchestrator involvement. The
backend filters the trail to what is **new for this conversation**, against a watermark held
by the ledger itself.

**The realtime push side** is optional and gated on configuration. Frames are **display
optimism reconciled by the ordinary read projection** — a dropped or duplicated frame is
harmless by design.

---

## 15. Recurring patterns, named

| Pattern | Where it recurs |
|---|---|
| **Append-on-delta observation** | capability recorder, palette observer — recompute at the settled frontier, append **iff** a genuine change |
| **Latest-wins fold** | agent type, scope, guardrail, palette, capability, permission mode, language |
| **Reference-based resolution** | queued messages, approval decisions, speech annotations — whichever names it first wins forever |
| **Position-aware idempotency** | activation events carry their own ledger id, so a *second* activation of the same kind still fires |
| **Activation-scoped state** | a re-activated workflow gets fresh state instead of inheriting a prior run's history |
| **Teaching errors that retrieve from a central registry** | field manuals on every validation refusal; origin refusals; palette refusals; the parameter listing on a bad call |
| **Strict check plus explicit model override** | a duplicate check the model can overrule with evidence |
| **Derived display lens, never read back** | the display hooks on blocks and tools |
| **Allowlist over denylist for outbound payloads** | the row field allowlist |
| **One decision, two askers** | the guardrail predicate; a path refusal asked at preview *and* at write; an arming refusal asked at check *and* at run |
| **Data point over name match** | every piece of tool metadata; "an input-token count is present"; "a created id is in the payload" |

---

## 16. Where the cost shows

Stated plainly, because a reference that only lists strengths is an advertisement.

- **The system is very large for its team.** Much of its correctness lives in prose
  invariants instead of in types: the frontier-transparent trait is a *documented
  precondition* that nothing mechanically enforces, and the keep-registered rule degrades
  silently by design.
- **Several ledger reads are linear in block count** and acknowledged as such.
- **The storage shape puts real weight on transaction behavior and a unique index.**
- **The ratio of design documentation to code is itself a maintenance surface.** The
  convention that a document never lists file paths is a partial answer to exactly that.
- **The migration posture is unforgiving.** Settled migrations do not roll back, boot
  migrations re-strip what a revert would reintroduce, and versioning is a hard cut. That is
  what stops re-derivation, and it means the escape from a *wrong* migration is a forward
  migration and nothing else.

The source system's own bar for what may be called an **accepted limitation** is one to
adopt whole: only ever an external system's limit, carrying the limit, its cost, an
official source, a check date and a named human sign-off. Work that was scoped out, a
shortcut, or a rule that was broken is a **decision** or a **defect**, never laundered into a
constraint — and an automated worker may never accept a limitation at all. It surfaces the
evidence and stops.
