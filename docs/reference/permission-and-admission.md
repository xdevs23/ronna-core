# Permission, scope and tool admission — architecture reference

A companion to the event-sourced agent runtime reference, covering one subsystem end to
end: from a credential to a running tool body. Same standing: **a reference document, not a
description of this project.** It generalizes a production subsystem so the patterns can be
judged on their merits, with every rule stated alongside the failure it prevents.

Domain vocabulary is generalized. "Organisation" means any tenant that owns records;
"region" means a shared locality dimension that records belong to but nobody owns; "shared
surface" means a public listing several tenants appear on.

---

## 0. Three questions, each answered exactly once

1. **Who is asking?** — a scope stamp on the ledger, resolved per turn into a scope array
   and a principal.
2. **May they touch this at all?** — the authority layer: disclosure on reads, one
   write-access decision on writes, standing checks, and the tool-visibility palette.
3. **Has a human agreed to this specific act?** — the admission layer: the tool's own
   admission check, the durable approval chain, and the standing permission state that can
   pre-answer it.

Questions 2 and 3 are deliberately **not** the same question, and keeping them apart is the
structural choice the whole subsystem rests on:

> Auto-approval never grants authority. Whether the session's human may run a tool at all is
> enforced before and inside the tool regardless; a mode or a grant only pre-delegates that
> human's own approval.

## 1. Two rails that never merge

```
CREDENTIAL              LEDGER STAMP                  PER-TURN RESOLUTION
(auth only:      ──▶    scope block                ──▶  scope array  → list[Scope]
 signature +            kind: root | org | user         principal    → roles, scopes,
 subject)               appended at creation,                          regions
                        before any user message
                                  │
              ┌───────────────────┼────────────────────┬──────────────────┐
              ▼                   ▼                    ▼                  ▼
        AUTHORITY (2)         EXPOSURE            ADMISSION (3)     SANDBOX CHOKEPOINT
        disclosure            visibility()        ordered checks    (same mode predicate,
        write decision        → palette block     → admission answer  no grants, in-memory
        standing checks       (stamped)           → approval chain    parking)
                                                  → standing state
```

- **The authority rail answers *reach*.** Resolved live from the database every turn and
  every call. Nothing about it is stamped on a block; nothing about it can be pre-delegated
  by a human.
- **The admission rail answers *consent*.** Entirely durable ledger state. Nothing about it
  consults the database.

A call must clear **both**, and neither rail has a code path into the other. That is what
makes it true that no amount of standing permission widens what a session may touch, and no
amount of authority skips a human's agreement to a specific destructive act.

---

## 2. Identity lives on the ledger

### 2.1 The stamp

A scope block is appended at session creation, immediately after the system prompt and
parameters blocks and **always before any user message**. The placement is not cosmetic:
forks carry the ledger prefix by reference, so a stamp written before the first user message
is inherited by every fork for free.

| Kind | Payload | Meaning |
|---|---|---|
| root | — | unrestricted; the operator context |
| org | one or several owner ids | scoped to those tenants' content |
| user | the stable user id | one person's private session |

It is a **separate block kind**, not a field on the parameters block, because two layers with
different lifetimes read it — the tool layer scopes data by it, the API layer filters session
visibility by it — and folding a security fact into a workflow block blurs that boundary.

It is **inert on the model axis**: no ask, no projection. The model never learns its scope
from this block. A separate informative block (or a guardrail envelope) carries any awareness
the model should have, and neither is the access decision.

The multi-owner shape is an **atomic full replace** read through one accessor: a block
carries the complete intended set, so adding a second owner writes both, never just the new
one.
*Prevents:* a partial "add" silently dropping an existing owner, and two code paths
disagreeing about which field is authoritative.

### 2.2 Fail closed, twice

Resolution returns **unreachable, never root**, for an absent block, an owner-scoped block
with no owners, or a user-scoped block with no user id.

> Resolving the unknown to the most powerful scope is fail-open: a dropped or malformed block
> would silently become full reach.

Every consumer collapses that the same way — the HTTP check returns not-found, the scope array
resolves empty and matches nothing, the principal resolves empty so every channel refuses, and
the palette hides every standing-dependent suite.
*Prevents:* corruption, a partial write or a schema change becoming escalation to the most
powerful context in the system.

### 2.3 Route access is a different question from tool scope

One shared per-session HTTP check sits in front of every session-scoped route.

| Session scope | Who may reach it |
|---|---|
| root | operators only |
| one org | operators, or a caller with that org's access |
| several orgs | anyone who could reach any one of them |
| user U | **only U** — not peers, not other operators |

Two properties stated as rules, not left to implementation:

- **Denial is not-found, never forbidden.** An out-of-scope caller must not learn the session
  exists. Knowing an id is not access.
- **Losing access is not deletion.** A caller removed from a tenant stops satisfying the
  check, so the session becomes unreachable *to them*; the ledger is untouched and the
  session's own actor keeps running, because it runs as the scope, not as the caller.

Exactly one narrow widening exists — a support path where an operator is admitted on the deny
path when a record on file consents to that specific session. It is bounded on four axes: deny
path only, operators only, keyed to that one session, and unable to override an unreachable
scope. Write and act routes never pass it, so a consented session is **readable, not
drivable**.
*Prevents:* a support workflow becoming a general bypass, and an id-enumeration oracle.

---

## 3. From a stamp to a principal

### 3.1 The scope array

A list over a **sealed, typed union** — and the typing is a hard rule:

> A scope is a structured typed value — a sealed union of value objects, never a string that
> is built, parsed, split on a separator, prefix-matched or compared as text.

| Variant | Matches |
|---|---|
| root | everything |
| org(id) | rows owned by that tenant |
| user(id) | rows owned by that person |

Any wire form is parsed into the typed variant **once at the auth boundary** and never
reconstituted as a string for comparison.
*Prevents:* the classic prefix-match escalation, where an id that is a prefix of another
matches it, and silent breakage when a textual form changes.

Resolution branches on the stamp: root is fixed with **zero backchannel queries** (structural,
not a caching choice — the source is never touched on that path); owner-scoped is fixed from
the block, one variant per owner; user-scoped is the **live** derivation, re-read every turn.

> Memberships and the operator bit are re-derived every turn from live data, never baked onto
> the block, so a person removed from a tenant, demoted, or blocked loses that reach on the
> very next turn.

*Prevents:* a durable token outliving a revocation. The design note is explicit that the token
*was* the stale cache and that staleness *is* the defect being removed.

### 3.2 The principal

The scope array answers "which rows match". The principal additionally carries the standing
facts a synchronous decision needs: the scopes, a set of **role facts**, and the set of
**regions** the caller has standing in. Three different kinds of fact, never conflated.

> The operator channel never reads the role set, so "is this caller an operator" cannot be
> answered two ways; both resolvers derive the root scope from the same parsed roles, and
> *root scope present ⟺ operator role present* is a resolver invariant.

Both resolutions happen **once per turn**, and every check that turn folds over that one
result.
*Prevents:* a tool that asks two checks — one about a row, one about a destination region —
evaluating its two halves against two different reads of the world, admitting a combination
neither read alone would have.

### 3.3 One resolution, both surfaces

The user branch resolves through the **same function the ordinary HTTP surface uses**, so for
one database state an HTTP request and an agent turn for the same person resolve to **equal**
values.

Two predicates ride that read: a caller is valid only if their row exists and is active, and a
membership counts only in an active tenant. A blocked or deleted account holding a
cryptographically valid, unexpired token resolves to **empty standing, roles included**.

One subtlety that is easy to get wrong:

> The identity scope is dropped deliberately, never kept as a harmless tag: a user scope
> *matches* rows carrying that owner, so keeping it would leave the ownership channel able to
> admit a blocked account the moment any row type carries a user owner.

A union of token-derived and live-derived facts was considered and rejected: it is two code
paths reborn inside one function with two answers, making every staleness bug permanent until
expiry while breaking revocation in one direction.

---

## 4. The authority decision

### 4.1 Two questions, one fold

- **entity-write** — may this principal write *this row*, asked with the row in hand.
- **standing** — may this principal act *in this region*, asked for creates, imports,
  promotions and region-parameterised operations, where no row exists or the question is
  genuinely about the region.

Promotion onto a shared surface is filed as a **region** question even with a row in hand,
because it orders content on a surface whose audience belongs to the region, not to the row's
owner.
*Prevents:* ownership of one item becoming editorial control over a shared surface.

### 4.2 Channels are grants, evaluated in order

```
decide(principal, capability, target):
    1. operator                         → admit
    2. ownership (entity targets only)  → admit
    3. region standing (target has one) → admit
    3b. delegate role (no region)       → admit
    else                                → deny, carrying levers + facts
```

> Precedence is **evaluation order, never override order**: every channel is a grant, none
> subtracts, and a denial is only "every channel refused". That makes the fold a monotone OR,
> so adding a further standing source can never weaken an existing admit.

*Prevents:* a subtracting rule making a new standing source capable of *removing* access
someone already has — which is the property that makes bounded delegation safe to add later.

One exclusion is structural instead of policy:

> No channel reads content vocabulary — labels, tags, categories. Access that rides content an
> editor can edit is self-service privilege escalation, and the enumerated-channel shape
> excludes it structurally.

The target-without-a-region case is handled explicitly instead of by omission, because **a
check that cannot fire must deny everyone else, not pass.**

### 4.3 The verdict is data; the wording is the surface's

The decision returns what was decided **and the facts read while deciding it**: the admitting
channel, the levers that could have admitted for this target kind, the target's region, the
caller's own regions, and whether the caller carries any standing at all. Each surface words
its own refusal from those facts.
*Prevents:* a refusal describing a *different moment* than the decision it explains — the
classic "denied, but the reason lists things you do have access to".

### 4.4 Two coarse predicates

- **any standing at all** — used as the outer check of row-bearing routes so the fetch can
  happen and the ownership channel can be reached. A caller with none is refused **before the
  fetch**, which keeps existence undisclosed. A bare identity is refused here: knowing who you
  are is not standing.
- **region standing** — used where the coarse check *is* the whole check, with its audience
  pinned to reproduce exactly what the previous mechanism described, read live.

*Prevents:* an owner of a row outside their tenant's regions being refused before the
ownership channel could see the row; and a "cheap" coarse check silently widening or narrowing
an endpoint during a refactor.

### 4.5 Read-side disclosure

| Verdict | Meaning |
|---|---|
| full | matches any scope **or** the row is published |
| name-only | a listing may show the name and nothing else |
| denied | a **detail** read of a name-only row |

The name-only rendering drops every detail field, so it reflects only "this exists".
*Prevents:* a listing leaking an unpublished competitor's data through fields nobody thought
of as sensitive, and a detail endpoint reachable by first finding an id in a listing.

---

## 5. Exposure — what the model is told exists

Authority is enforcement. Visibility is a separate, earlier concern: what appears in the
schema list at all.

| State | Schema | Calls | Model is told |
|---|---|---|---|
| visible | shipped | run | — |
| unavailable | none | refused | **yes, by name** — exists, out of reach |
| hidden | none | refused | **nothing, anywhere** |

Unavailable exists for a behavioural reason: told a tool exists but is out of reach, the model
neither improvises the capability nor spends a call discovering the refusal, and can tell the
user cleanly. Hidden is for capabilities whose *existence* is privileged.
*Prevents (the state that motivated it):* a tool advertised in every session's schema, called,
and refused inside the body as an ordinary result — per session, per tool, forever.

**Visibility is pure and synchronous.** Each tool answers off a frozen context (the resolved
principal plus a ledger snapshot); tools never do I/O here, and the framework dispatches by
name over the registry with **no table anywhere mapping tool names to visibility**. The single
live read happens once per settled tick, in an observer.

**The palette is a stamped ledger block, not a recomputation.** The observer computes the name
sets at the settled frontier and appends a block **iff they differ**. Two consumers read that
one block: the schema builder intersects with it, and the admission chokepoint refuses
anything outside it.

> The schema the model received and the palette that judges its calls are the same durable
> truth — the same block, never a recomputation.

*Prevents:* the model receiving a schema in one moment and being refused for calling it in the
next, an unwinnable state for the model and an unexplainable one in a replayed transcript.

Two further properties. **A ledger with no palette block filters nothing** — deliberately
fail-open, because the palette is an exposure mechanism and enforcement in depth lives in the
tools' own checks. And **the hidden set is a sensitive field**, stripped from every payload;
the function that renders the palette envelope does not even take it as a parameter, so by
construction it cannot leak.

**Deferred tools** are context economy strictly after visibility: a visible tool defers iff it
declares itself deferrable, is not workflow-added, and its category is unloaded. "Loaded" is a
pure ledger fold over load calls with successful results — no marker block. One subtlety keeps
the layers from leaking: the stamped loaded set is the fold **intersected with "still has a
visible tool"**, so a category that went all-hidden mid-session drops out and a re-load errors
like an unknown category, keeping hiddenness the absence of evidence.

**Visibility and its run-level check must be one predicate over one principal**, so the palette
can never announce a suite the check behind it would bounce, nor hide one it would admit. The
run-level checks stay the enforcement; the palette never was.
*Prevents:* treating a context-economy mechanism as a security boundary, so a bug in palette
derivation becomes escalation instead of a cosmetic glitch.

---

## 6. Admission — the exact order

One subsystem owns admission. It re-reads the ledger and advances a call **from recorded facts
only**, bounded at a handful of attempts, each starting with a fresh read.

```
1. LOAD        read the session; absent → return
2. RESOLVED?   a result or error already exists for this call → return (stale wakeup)
3. FIND CALL   locate the call block; absent → return
4. RECORDED?   read the admission response for this call (also detect the legacy shape)
5. IF RECORDED:
     a. refuse → return (its error was recorded atomically)
     b. defer with no approved decision → consult the standing state
          covered → record the approval now, fall through
          not covered → return (the human still owes this)
          lost the compare-and-swap → retry
     c. resolve the tool; unknown → unknown-tool error, return
     d. break        ← the palette is NOT re-checked here
6. PALETTE     (new calls only) name in the stamped available set?
                 no → append the teaching refusal, return
7. RESOLVE TOOL  unknown → unknown-tool error, return
8. UNGATED?    → break (no admission evaluation, no response block, ever)
9. CHECK       await the tool's own admission check (side-effect-free)
                 raises → append a plain tool error, return (a crash is NOT a decision)
10. RECORD     refuse → [response + tool error]         one atomic batch
               defer  → [response + approval request
                         (+ decision if already covered)] one batch
               proceed→ [response]
then: interactive → return (the human owes the result)
      pooled      → dispatch to a bounded slot, which re-checks resolution freshly
      otherwise   → execute the body
```

The body phase is identical on both dispatch paths: resolve declared references, stamp the
processing span **immediately before** the body so waiting time is excluded by construction,
run, record any emitted media, append the result.

### Why each ordering choice is what it is

| Choice | Failure prevented |
|---|---|
| Resolved-check first | a duplicate wakeup re-running a completed body |
| A recorded response beats a later palette | a palette shift stranding work a human already approved |
| Palette before the tool's own check | a hidden tool's check running at all, disclosing existence through timing, a side effect or different wording |
| Ungated tools never gain a response block | "has a response" ceasing to mean "was gated" |
| A crashed check is not a decision, but still resolves the call | a session parked forever behind an exception nobody recorded |
| Response and companion land atomically | a half-decided call no reader can interpret |
| A freshly recorded, uncovered defer retries instead of returning | two code paths for "covered now" and "covered later", the second of which drifts |

**Every pre-body write rides a compare-and-swap** against the very read the decision came
from, pinning both the ledger tail and the cursor. If anything arrived in between — a human
submission, a second process in a deploy overlap, a cursor move — the write loses, nothing is
recorded, and the pass re-reads and re-decides. Three consequences: one call can never carry
both a submitted result and a racing refusal; one call can never gain two divergent responses;
and a losing evaluator acts on the **recorded** decision, never its own stale one.

**The body is at-least-once, deliberately.** The invariant owed is about the *decision*, not
the body's delivery count: a refused call can never execute, a deferred call can never execute
unapproved, and one call can never resolve as both success and error. A second result is the
recovery. A full claims-and-locking layer to fence this was built and dropped:

> It fenced the duplicate decision instead of removing it. Both channels survived, newly
> synchronized, and every future change had to keep the fence airtight. The mechanism became
> unnecessary the moment the decision was recorded once.

### Where authority sits relative to admission

Both placements are used, deliberately.

**Before the defer.** A check that knows a call will be refused returns a refusal **first,
inside the admission check**, instead of deferring.

> …refused here, so no approval request is ever parked for a call the body would refuse — a
> durable request a human says yes to must never describe a refused effect.

**Again, live, inside the body.** The same check re-runs post-approval against the live
per-turn scopes, never a value cached at check time, because approval can be delayed and
standing can change under a parked call.

Together: **authority is checked at both ends of the human's decision, and consent is recorded
in the middle.**
*Prevents:* a human shown an approval for something the system was always going to refuse; a
durable record of them approving an impossible effect; and the window between approval and
execution being used by a caller who has since been demoted.

---

## 7. The admission records and the standing state

### 7.1 The chain

```
tool call                    stamps its tier flags at append
  └─ admission response      proceed | refuse | defer   ← THE admission answer
       └─ approval request   (parks; asks nobody)       ← recorded WITH a defer
            └─ decision      approved | denied
```

The response is **system-only in both directions** — no model projection, absent from every
session-view payload. The two approval blocks are model-invisible but client-visible: the
human sees and answers them.

The admission check is **side-effect-free by contract**:

> A check that wrote its own ledger state put the same answer on two channels — the return
> value and a sibling block — and that duality is what the one-decision rule forbids.

**The failure this whole design exists for**, stated as a mechanism because it looked
reasonable at every step:

1. A rework made the cursor re-run every unresolved block on every tick, so a deferred call
   began re-emitting its wakeup while parked. The old invariant "a defer emits no event"
   silently stopped being true.
2. The runner grew a check: hold a controlled body unless an approval request exists and is
   approved. That check **reconstructed the decision from ledger shape** — the *absence* of a
   request meaning "it proceeded" — while the real answer travelled in memory.
3. A call was **refused** by its own check and its body ran anyway: the parked call re-emitted
   before the refusal's error landed, the check saw no approval request (a refusal creates
   none), read that absence as "proceeded", and executed. One call recorded both an error and
   a success.

> The absence of a block is not a decision, and a decision that travels both as a durable
> record and as an in-memory value is two decisions waiting to disagree.

### 7.2 The request's own routing check

The request routes the redispatch walk onward **only** while an approved decision exists *and*
the call has no result. The walk anchors on the latest block, and the request *is* the latest
block the moment a defer is recorded — so an unconditional route would run the controlled body
before any approval.

### 7.3 Denial carries structure

Two mutually exclusive fields, never one string plus a flag: a **system reason** (the world
changed under the request) and a **user reason** (a human's typed judgement). Who denied is
structural, not inferred. The model reads "the user denied this action", optionally with the
reason, or "this action was automatically rejected: …".
*Prevents:* the model believing a person judged it when the world merely changed under it. The
approver is always called "user", never by privilege, so the model is never taught vocabulary
it should not reason about.

### 7.4 The standing state: two inert blocks

A **mode block** (latest wins; manual by default) and a **per-tool grant/revoke block** (per
tool, latest wins). No ask, trivially done, no routing, **no model projection at all**,
client-visible, recording the human's own choice. Nothing is ever deleted — a revocation is an
appended fact.

Three properties with their reasons:

- **Plain unconditional appends, deliberately not the compare-and-swap.** A latest-wins fact
  has no validity predicate; a later block simply wins. That is what makes flipping the mode
  *while the model owns the turn* — the intended use — always land instead of losing
  repeatedly against a streaming ledger.
- **Frontier-transparent.** If a flip lands in the window where the tail owes a turn that has
  not started, an opaque inert block becomes the tail and makes that turn unreachable forever.
  *Prevents:* loosening permissions to unblock a session and thereby wedging it permanently.
- **Forks inherit as of the fork point.** The folds over the inherited prefix *are* the
  standing state as it stood then. No reset, no re-confirmation.

> The model never learns permission vocabulary exists — it must not reason about what it is
> allowed to do.

*Prevents:* a model that negotiates with, plans around, or attempts to influence its own
permission state.

### 7.5 The tier and the auto-approval predicate

The tier is a **class attribute on the tool, protected by default**. Protected is
auto-approvable only under the broadest mode or an explicit per-tool grant; the unprotected
edit tier is additionally covered by an edit-accepting mode.

| Rule | Failure prevented |
|---|---|
| Protected is the default | a new destructive tool inheriting the loosest treatment |
| The tier is **not stamped** on the call block | a mis-tiered tool's already-parked calls frozen on the wrong tier — failing in exactly the direction a re-tiering exists to fix |
| The override lives on the **leaf class only**, never a domain base | one convenient override silently re-tiering every future sibling |
| A call whose tool no longer resolves reads as protected | an unresolvable tool auto-approving |

The predicate is pure over the folded standing state, the declared tier and the call. **A
grant wins over a mode** when both cover it — it is the more specific standing choice and it
stays truthful after the mode flips back. **A grant keys on the call's recorded name**, not
the resolved definition's, or a grant would become *weaker* than the general mode for a call
whose tool has left the registry, inverting the rule that an explicit grant is stronger.

Interactive tools are structurally out of reach of all of it: the consult sits behind a
recorded defer, and interactive results arrive by submission, never by decision block.
*Prevents:* an auto-approving mode silently answering a question meant for a human — the mode
covers *permission*, never *input*.

### 7.6 Two transports, one recorded shape

The decision block has **one construction site** and two writes carry it.

**Born covered** — the standing state already answers the call the instant it defers, so
response, request and decision land in one conditional append. The decision's reference to its
request is computable before the write because a block id *is* its position and the
compare-and-swap pins the batch: the reference is correct exactly when the write lands, and it
dies with the batch when the swap loses.

**Parked before coverage existed** — a mode flip or grant arriving while a request waits,
resolved at the admission-read consult. There is deliberately **no auto-approver component and
no drain machinery**: a parked call re-emits its wakeup on every tick and any ledger change is
that wakeup, so appending a mode or grant block *is* what resolves already-parked requests.

An auto decision carries a **system** role, never a user one — it must not masquerade as a
human's click. And a rejected alternative, because it is the shape people reach for: a
projection flag marking a request the standing state already covers, so the client suppresses
the card. It hides the window instead of closing it, and makes the request's delivered form a
second statement of a decision the ledger already records.

### 7.7 Writing the standing state, and the human's submission

One endpoint, four cases, authorized by the **same per-session check the approval submission
rides** — whoever may decide this session's approvals may pre-delegate them, so nothing
escalates. Idempotency is a pre-append check, and **a body that changes nothing appends
nothing**. The response clears the latch, which is precisely how already-parked requests the
change now covers get resolved.

The human's decision arrives through the framework's own submission handler:

- **Targeting is explicit and optional** — a client showing a *specific* open request sends
  its id, so "approve what you see" can never resolve a different, newer one.
- **A bad target gets the live open ids**, pulled from the machinery, never a hardcoded hint.
- **A standing grant cannot accompany a denial** — a refused action does not record a standing
  permission to run it.
- **The granted tool is resolved server-side** from request to originating call, so a client
  cannot grant a tool other than the one it is approving.
- **Deny batches its tool error; a grant batches with the decision** — one atomic write each.
- **The loser of a two-tab race** gets a conflict response, re-reads, and shows the resolved
  state.

Validity is the decision's own question, not the send path's: an approval is legitimate exactly
while its request is undecided — a state where the cursor is legitimately parked behind the
tail — so the swap pins the cursor where it was *observed*, and only "no decision exists yet"
decides validity.

---

## 8. The parallel path: a sandboxed script surface

Scripts driving the same services enter one dispatcher, which asks the same questions in a
different arrangement: resolve the descriptor, validate arguments against its own parameter
model, resolve the principal **once per execution**, enforce authority through the bindings'
own standing checks, and ask the consent question **only for a write**.

**The same** — the write class reuses the tool tier vocabulary, read off the mirrored tool's
own attribute so there is one source of truth; the same mode predicate answers it, factored out
precisely because a tool call is not the only thing carrying a tier; a read never consults
permission state at all; and the class is *this call's*, so a function whose effect depends on
what it addresses is answered for what it is about to do, with a malformed target resolving to
the **stricter** class.

**Deliberately different** — per-tool grants do not reach in (they key on a recorded call name,
and a script call is not one), so a narrow grant cannot widen into a whole module. Parking is
in-memory and dies with the execution; a parked call hands its slot back so the wait happens off
the time bound. A missing consult or registry **fails loud, never open** — a wiring bug is never
a silently ungated write. And a call that cannot be identified well enough to ask about is
refused instead of parked where nobody can see it.

**Fold protection** is a refusal no mode can answer: where a workflow's state is a fold over its
own recorded tool calls, a write issued from inside a script lands in storage without passing
that ledger, so it is refused outright with a teaching error naming the tools to call instead.
*Prevents:* a workflow's completeness measure and goal state silently ceasing to describe
reality because the work went around the ledger they are folded from.

---

## 9. Refusals are the interface

Every refusal is a **structured teaching error the model re-plans against**, never a raise, and
every one carries the fix.

| Refusal | What the model reads |
|---|---|
| unknown tool | the name is unknown |
| **hidden tool** | **byte-identical to unknown** |
| unavailable tool | it exists, is not available here, do not call it again; tell the user if they need it |
| deferred tool | it exists, schema not loaded — call the load tool for this category, then retry |
| no standing | this session holds no access of that kind, and who does |
| wrong region | no access to that one, **and the caller's own regions listed** |
| entity out of reach | not in your scope **and** standing does not reach it — plus which levers *would* admit |
| detail read denied | not readable from your scope — owned elsewhere and not published |
| operator-only domain | that suite is for operators |
| human denial | the user denied this action (+ reason) |
| automatic rejection | this action was automatically rejected: … |

Two rules govern the table. **The hidden/unknown collision is deliberate** — confirming
"hidden, but real" would itself disclose existence. And **every refusal retrieves its fix from
the registry that owns the failing thing** — the caller's own regions from the verdict that
read them, valid values from the model's own type, open request ids from the machinery,
accepted parameters from the descriptor — never a hand-written hint that can drift.
*Prevents:* the observed pattern where a shape error on one field is "answered" by editing an
unrelated field, and at scale a retry loop against a wall whose shape the model was never told.

---

## 10. Invariants to pin

**Admission.** A refused call can never execute under any interleaving. A deferred call
executes only after an approved decision, exactly once. A denied call errors and routes
nowhere. One call never carries both a success and an error. One call never gains two
responses. Ungated tools produce no evaluation and no response block. The response block is
invisible to the model and absent from session-view payloads.

**Authority.** An unknown scope resolves to no scopes and no standing, never root. Root scope
present ⟺ operator role present, for both resolvers. An HTTP request and an agent turn for the
same person on one database state resolve to **equal** principals. The decision fold is a
monotone OR. Every live fact a check needs arrived at the turn's single resolution moment.

**Exposure.** The effective tool set feeding the schema builder and the palette observer is the
**same** computation. A hidden tool's name appears in no projection, envelope or payload. A
hidden tool's refusal is byte-identical to an unknown tool's.

**Standing state.** The model receives no projection of any permission block, in any modality.
A body that changes nothing appends nothing. Nothing in the ledger distinguishes which
transport wrote an auto decision.

---

## 11. Open seams, stated as they are

- **Approval precondition staleness.** A call is decided at one time and acted on later; in
  that window the target can vanish. In the source system the **client** is the first observer
  of that staleness and submits an automatic denial — which is a client exercising authority
  over durable ledger state. The record is explicit that this is structural and *not a chosen
  design*, and that any fix must move the **decision** off the client without losing the fact
  that the client is the first to **observe**. Marked unresolved.
- **The palette's fail-open window.** A ledger with no palette block filters nothing. Bounded
  to one tick at session start and to legacy sessions, mitigated by run-level checks being the
  real enforcement — documented as deliberate instead of absent.
- **At-least-once bodies.** A proceeded call's body can run more than once across a crash or a
  deploy overlap; a second result is the recovery.
- **A shaped but empty extension point.** The decision fold carries an explicit capability and
  a typed target so a delegated-grant channel could join without reshaping every call site.
  Nothing ships — the socket is the function's *shape*, not a stub pretending to exist, and the
  monotone-OR property was chosen specifically to make filling it safe later.

---

## 12. What generalizes

**Separate reach from consent, structurally.** No amount of standing permission widens what a
session may touch; no amount of authority skips a human's agreement to a specific act. Neither
rail can answer the other's question, because they read different sources — one live from the
database, one purely from the ledger — and neither has a code path into the other.

**A decision is a record, not a return value.** The historical failure was not a missing check.
It was a *correct* check whose answer travelled in memory while a second consumer inferred the
same answer from the shape of the ledger. Deleting the second channel worked; synchronizing the
two did not. When you feel the need for a check that reconstructs a decision, that need is the
signal that the decision was not recorded at its owner.

**Refusals are part of the interface, not the error path.** Roughly a third of the code in this
subsystem is refusal wording, all built the same way: retrieve the fix from the registry that
owns the failing thing, name the levers that *could* admit, and never state a fact the caller
already had. That is what turns a permission boundary from a wall the model bounces off
repeatedly into information it can act on once.
