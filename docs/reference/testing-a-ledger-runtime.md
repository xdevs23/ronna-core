# Testing a ledger runtime — architecture reference

Third companion to the event-sourced agent runtime and permission references. Same standing:
**a reference document, not a description of this project.** It generalizes how a production
ledger runtime is tested, because a framework two projects share is only shared if both can
trust one suite.

Domain vocabulary is generalized throughout.

---

## 0. Three prerequisites for a suite two projects trust

1. **The fixtures must be honest.** A test that doubles the thing under test proves nothing;
   one that doubles too little becomes an integration test nobody runs.
2. **The granularity must be legible.** A reader must be able to tell, for any invariant, at
   which level it is pinned — so a second project knows what it inherits and what it must
   re-prove locally.
3. **The regressions must be identifiable.** A shared suite's most valuable content is the set
   of tests encoding a bug someone already paid for.

In the source system the suite is **larger than the source it covers**. That ratio is not
accidental: much of it is *equivalence* and *contract* suites instead of unit tests, and both
shapes cost more lines than the code they pin.

---

## 1. The environment: real infrastructure, nothing else

### 1.1 Network is blocked at the socket layer

The socket connect call is patched: loopback allowed, everything else raises with a message
naming the address. Applied by default, opt-out only by an explicit marker. Not a doubled HTTP
client — a patch beneath it, so *every* path outward fails loudly, including transitive ones
inside third-party libraries.
*Prevents:* a test that passes because a vendor endpoint happened to answer, and a suite whose
runtime and flakiness depend on somebody else's uptime.

### 1.2 The database is real

No in-memory substitute, no fake collection layer, no repository interface to swap. A
session-scoped fixture spawns an **actual database server** from a **pinned dependency
revision** — pinned instead of tracking a channel, so it cannot drift between runs.

Three details carry weight:

- **It runs in the configuration the storage law needs.** The ledger's append path uses
  multi-document transactions, which that engine only offers in a replica-set configuration. A
  plain single server refuses to open a transaction at all, so the suite cannot test its own
  storage law without it.
- **Storage selection is defensive.** The engine refuses to create an index below a free-space
  threshold, so a small memory-backed volume would let it *start* and then fail the first
  index. The fixture checks free space first, prefers memory-backed storage, and falls through
  to real disk instead of choosing a location that fails later.
- **The degraded fallback fails loudly.** A dedicated fixture is declared by every test that
  opens a transaction, and on a fallback that cannot provide one it **fails instead of
  skipping**: a skip would silently un-test the transactional path.

*Prevents:* the most dangerous shape of green suite — one where a whole subsystem is silently
unexercised on the machine that ran it, because skips scroll past.

### 1.3 Process-wide state is reset per test, with stated reasons

Two automatic fixtures clear module-level caches, and one distinguishes *staleness* from
*collision*: its keys are scope identifiers that every test reuses over its own throwaway
database, so a leftover entry from another test would answer this test's listing with the
previous one's data.
*Prevents:* order-dependent results, and a cache-keyed-too-broadly defect that only ever shows
up as one test seeing another's rows.

### 1.4 What the environment does not do

The deploy pipeline builds and publishes on a push to the main branch. **It does not run the
suite.** Verification is developer-initiated: a workspace script runs tests plus lint plus
format-check, printing one line per passing step or a failing step's complete output, with
piping through `head` or `grep` forbidden because it hides the error.

Stated plainly: **a suite this large with no required check is one careless push away from
being decorative.** For one project that is a discipline problem. For two projects sharing a
framework it is a correctness problem — neither has a mechanical guarantee the other ran it.

---

## 2. The fixture layer

Shared fixture modules contain **no tests**, and the separation is itself a rule:

> It lives in a module of its own instead of inside one of the suites because it has two
> consumers and no tests of its own: imported out of a suite it would drag that suite's whole
> file and fixtures into every collection of the other, and it would make one set of tests the
> owner of something the other equally depends on.

**A throwaway database per test**, dropped afterwards. No session-scoped reuse, no
transactional-rollback trick — full isolation by construction, paid for in setup cost.

### 2.1 The frontier oracle — a lens, not a double

The problem: the model-turn decision lives inside the actor, mixed with the latch, the
observers and the redispatch walk. A unit test wanting to pin *just* that decision would have
to stand all of it up.

The answer is **not a double of the decision**. It is a re-implementation of the same short
walk over a bare ledger:

```
frontier_owes_turn(blocks, registry=None):
    session = stamped_session(blocks)      # positional ids, as the store derives them
    bind registry if given                 # result-side behaviour attaches
    for block in typed_blocks:
        if not await block.run(...): return False    # parked → someone else owes
    return frontier_awaiting(typed_blocks) == "model"
```

The stubs are minimal and each has a documented reason: the store answers with the session
under test; the bus resolves a submitted future **but carries no completion**, so an
asynchronously-waiting block reports not-done and the walk parks exactly as the live cursor
would.

**Why this is a defensible pattern and not a duplicated implementation:** the oracle exercises
the *real* work hooks on the *real* block classes and calls the *real* frontier read. What it
re-implements is an eight-line loop, and that loop is pinned separately against the real actor.
The oracle is a **lens**, not a second source of truth.

A companion helper brings a test session to the settled, user-owed frontier **that production
always reaches**, and explains why the shape differs from a naive one: in production the
boot-era observers append their inert first records on the first engaged settled tick, so a
fresh session's tail is always an inert observation block by the time the actor rests.
*Prevents:* a suite full of ledgers that could never occur in production, quietly proving
properties about shapes the system never sees.

### 2.2 When a dispatch change invalidates existing fixtures

After result-side behaviour moved onto the tool registry, a whole class of pre-existing
fixtures became **invalid**, and the fixture module says so: a test exercising non-inert result
behaviour now needs a **ledger-realistic call preceding the result** and a registry-bound load.
Orphaned-result fixtures — a result block with no originating call — were only ever valid under
the dead payload-key dispatch; under the ledger-join dispatch they silently attach nothing and
every assertion about behaviour becomes vacuous.

The helpers build the behaviour-carrying tools under their **final registered names**, the same
objects the composition root registers minus the runtime dependency bundle.

### 2.3 Callers seeded as rows, never as tokens

Since access resolution reads live rows, a fixture handing over a token proves nothing about
resolution. Callers are therefore **seeded as the database state their standing derives from**:
an active user row, an active tenant carrying that user and those regions. The named set covers
an operator (using the role shape production actually stores), a delegate, a tenant member, a
plain user, a **valid token with no standing at all**, and an unknown subject.

Two installers, and the distinction is the interesting part:

- one overrides **both** authentication and principal resolution together, so a suite can never
  leave them describing two different people;
- the other authenticates and leaves the **real** resolution in place, reserved for suites whose
  *subject* is that read — ownership through an actual membership, staleness, the cross-surface
  matrix — where a bridged principal would be the test describing the answer it is meant to
  check.

Any hand-built principal derives its role facts **from the scope array through the same single
function the resolvers use**, so a fixture cannot pin behaviour on a combination live
resolution could never produce.
*Prevents:* the classic authorisation-test rot where fixtures drift into describing principals
the production resolver cannot emit, and the suite starts protecting fictional cases while
missing real ones.

### 2.4 The fixture-design rule of the whole suite

The realtime harness assembles the **real** machinery — store, connection registry, supervisor,
readiness barrier, seeding pump, call supervisor — over the test database, with **only the
vendor doubled**:

> The double stands exactly where our own infrastructure ends.

That is the transferable sentence: **the double goes at the boundary of what you own, and
nowhere earlier.**

A separate pure fixture table holds one representative ledger shape **per registered block
kind**, kept apart because the table grows whenever a kind is registered while the measurement
using it does not.

---

## 3. The granularity ladder

Eight levels. The suite is legible largely because each invariant sits at exactly one, and the
higher levels do not re-prove the lower ones.

| Level | What sits here |
|---|---|
| 1 | **Pure functions, no database** — folds, grammars, the access decision truth table, lineage reconciliation, staleness detection, the document fold |
| 2 | **Block hooks on a hand-built ledger** — the ask, the projection, the display lens, the transparency trait, on typed blocks, never stored |
| 3 | **The cursor walk, via the oracle** — the frontier decision over representative ledgers, without the actor's latch and redispatch machinery |
| 4 | **The real store** — positional ids through the streaming mutation path, the append transaction, the compare-and-swap, cursor advance ordering, fork by reference, delete reaping |
| 5 | **The real actor** — latch, tick serialisation, liveness edges, park and advance, observer ordering |
| 6 | **The real runner and the full flow** — admission evaluation, recording, the approval walk, body execution |
| 7 | **Routes and transport** — payload shape, sensitive-field strips, omissions, not-found-not-forbidden, the standing-state endpoint |
| 8 | **Cross-surface and process-level** — the surface-agreement matrix and the import-order probe |

Level 1's selection criterion is stated where it is used: *the cases that make it subtle are all
graph shapes instead of database behaviour, so they are pinned here, without a database.*

Level 2 enforces domain-blindness by import discipline: **this file imports no domain.**

Level 4's ordering tests carry explicit trace commentary, because the property is about which
write lands first: the work hook's own append commits *before* the loop persists the cursor, so
the ledger grows first and the cursor advances second.

Level 5 carries the sharpest hazard note in the suite:

> The transparency trait is answered explicitly: an ordinary block is NOT a declared pure
> record, and a permissive mock's auto-attribute would answer truthy — the frontier would then
> read **through** this block and **no test could ever pin a turn.**

Generalize that: **a permissive double can silently disable the very mechanism under test, and
every assertion above it goes vacuous while staying green.**

Level 6 subclasses the bus instead of doubling it, capturing wakeups for deterministic manual
delivery, so ordering is controlled by the test instead of by the scheduler.

Level 8 holds two shapes that exist nowhere else. The **surface-agreement matrix** runs both an
HTTP request and its twin agent tool, for the same person, against an identical row — catching
the failure the whole access design exists to prevent, two surfaces answering the same question
differently. The **import-order probe** spawns a subprocess per package entry, because
in-process imports inherit whatever the run already imported; the defect it encodes is a system
that booted only by entry-order accident, which the full suite masked because an
alphabetically-earlier module happened to import the right thing first.

---

## 4. The equivalence-suite pattern

The signature move, used wherever **a rewrite's correctness is defined by producing an
identical result.**

The most rigorous instance landed **before any storage code changed** — a tests-first decision
— and pins that the old and new storage layouts yield **byte-identical** served payloads *and*
byte-identical model projections. Equality is on canonical bytes (sorted keys, tight
separators, one encoding) chosen so that any value or type difference fails, including a float
silently becoming an integer in a rewrite.

Two fixture sources exercise the same assertions. **Synthetic** covers the enumerated edge
shapes: empty sessions, streaming tails with a parked cursor, forks and forks of forks, every
observation and attachment kind, a transferred multi-scope ledger and a scope-less one, every
sensitive-field strip, and **a tail at every possible ask state**. **Golden captures** are real
transcripts, machine-local, skipped if absent, and their protocol is a calibration then a
proof: serve and project on the old path and require equality with the capture (proving the
captures represent reality), then migrate and require the same equalities again (proving the
migration).

The golden path documents two accommodations that apply to the **calibration only**, never to
old-versus-new: a capture format that prints whole floats as integers is normalised on both
sides, and the display lens is excluded because current code recomputes it on every read — *a
lens difference is a code-version fact, not a storage fact.* The old-versus-new assertions do
compare it byte for byte, since the same code runs on both sides.

> Knowing precisely which comparisons may be loosened, and why, is what separates a rigorous
> equivalence suite from one quietly weakened to go green.

Other instances pin the exact resulting block sequence for every streaming turn shape — types,
order, roles, content, response identity, the streaming placeholders **and** their finalized
twins, and the usage stamps on each shape's carrier — declared explicitly as a frozen spec that
any refactor must reproduce.

**What this buys a shared framework:** an equivalence suite is the only test shape that survives
a rewrite of the thing it tests. If two projects share a runtime, these suites are the contract
that lets either refactor the shared core without the other reviewing the diff.

---

## 5. The regression register

Every entry is a test whose docstring names the failure mechanism. This is the highest-value
content in the suite. The recurring shapes:

| Encoded defect | Mechanism |
|---|---|
| **The parallel-call stall** | Three parallel calls, staggered results; a backward "latest ask" scan read a later sibling's result while an earlier call still dangled, green-lighting a turn into an ill-formed request |
| **The phantom execution** | An admission check refused; the body ran anyway, because the parked call re-emitted before the refusal landed and a second consumer read the *absence* of a request as "it proceeded" |
| **The double turn** | Two wakeups for one send; the second tick's awaits ran slow on a freshly started process and reached the decision holding a pre-turn snapshot |
| **The silent wedge** | A boot-time observer appended an inert record *after* a user message, the frontier saw an inert tail and rested, and the session sat with the message unanswered |
| **The dormant-session mask** | A standing-state flip landing while the tail owed a turn that never started, making that turn unreachable |
| **The silent turn death** | A turn raising was logged and swallowed: no ledger entry, no client change, the session read idle |
| **The parked-decision reload** | A result appended and then freshly loaded must still resolve behaviour through its call's recorded name |
| **Match by kind, not by id** | An unrelated result landing after a decision wrongly settled it |
| **Double execution across two walks** | The cursor walk and the redispatch walk both touch the approval blocks each tick; the body must run exactly once |
| **A crash between two non-transactional writes** | A transactional append versus a plain field update within one pass |
| **The wire-shape loop** | The core emitted one provider's wire shape directly, so another provider's path silently dropped calls and results |
| **Empty completions** | A model answering a tool result with silence |
| **Two surfaces, two answers** | A route and its twin tool deliberately held to different audiences until one service answered for both |
| **The stored role shape** | A stored role is a delimited string, not a list; the fixture uses the production shape on purpose |
| **The multi-owner not-found** | A multi-owner scope leaves the singular field empty, so a raw predicate refuses the session for everyone including its members |
| **Barge-in reordering** | A cancelled response's empty completion arriving *after* a newer turn's result; reading the frontier through the empty record preserves the newer state |

Two structural properties are the ones to copy.

**The test names the mechanism, not the ticket.** Every one is readable years later with no
access to an issue tracker.

**Several tests assert they would fail against the old code.** That is the only way to know a
regression test actually protects its regression.

---

## 6. Contract-shaped suites

For features with a written design, the suite mirrors the design's acceptance criteria —
numbered in the docstring, grouped as classes, one class per claim. Two things this buys:

- **A design's claims become individually falsifiable.** One class pins "every existing ledger
  behaves byte-identically"; another pins that the model never sees permission vocabulary;
  another pins that no domain base declares a tier — the invariant that would otherwise erode
  one convenient override at a time.
- **Coverage is auditable against the design instead of against line counts.** A missing
  acceptance criterion is a missing class, which is visible.

---

## 7. Honest-edge tests

A category handled unusually well: tests that **document a limitation** instead of asserting a
desirable property.

**The starvation contract.** If a parked block can never complete, the cursor parks forever and
everything behind it starves. The test asserts *exactly that*, and explains why it is
acceptable — every real not-done block has an out-of-band completion path that eventually fires
— so the contract is **explicit, not a silent surprise**.

**A measurement marked out of the default run**, gated behind an environment variable and its
own marker, because it builds very large ledgers and is not a regression check.

**Skip-if-absent, scoped and justified.** Golden captures skip when missing, but the *synthetic*
half of the same suite always runs, so the property is never fully unprotected. Contrast the
transaction fixture, which **fails** instead of skipping. The suite distinguishes "this
specific external artefact is unavailable" from "this capability is unavailable", and treats
only the first as skippable.
*Prevents:* the erosion pattern where a limitation is discovered, discussed, then forgotten
until someone rediscovers it as a defect and re-derives the same argument.

---

## 8. What a shared framework can and cannot inherit

### Portable as-is

- **The equivalence suites** — they pin behaviour, not implementation.
- **The frontier oracle** — any project inheriting the block model inherits it free.
- **The seed-the-rows discipline for principals** — directly portable to any system whose
  authorisation reads live state.
- **The regression register** — bugs already paid for, each readable without external context.
- **The double-at-the-boundary rule.**

### Genuine gaps

- **No required check before publishing.** The highest-value, lowest-cost fix, and the one that
  turns individual discipline into a mechanical guarantee.
- **No property-based or generative testing.** Every ledger shape is hand-enumerated. The
  enumerations are careful — a tail at every ask state, one representative per registered kind —
  but they are *closed*. Given that nearly every expensive defect recorded here was an
  **ordering** defect, this is the gap most likely to be hiding another one.
- **No enforcement of the traits the design calls preconditions.** The transparency trait may
  only be declared by a block inert in every direction. That is asserted *per known block*, but
  nothing asserts it for **all** registered kinds. One test iterating the registry and asserting
  the implication would make the rule structural instead of conventional. The same applies to
  the leaf-only tier override and the keep-registered rule.
- **Fixture-shape validity is documented, not enforced.** The invalid-fixture hazard is
  explained in prose; nothing stops a new test from building one and asserting vacuously.
- **Golden captures are machine-local.** Correct for a one-off migration, wrong as a permanent
  shape: a shared framework needs a committed, sanitised corpus or the second project has
  nothing to calibrate against.
- **Suite runtime is unmeasured but structurally significant.** A real database server, a
  throwaway database per test, thousands of tests. If wall clock is what deters running the
  suite, that is the real reason the required check is missing.

### Where the boundary goes

The split follows the granularity ladder almost exactly.

**Framework-owned (levels 1–6, minus domains):** the block model and registry, the cursor walk
and frontier decision, the actor's latch and tick serialisation, the bus contract, the store's
append, compare-and-swap, fork and delete, the projection fold and turn assembly, the tool
registry and admission, the permission chain and standing state, the palette observer. Their
suites port with them, and the dependencies to sever are small and visible — each domain import
inside a shared fixture should become a **test double registered by the consuming project**,
never a framework import.

**Consumer-owned (levels 7–8, plus every domain):** routes, the access channels' domain
meaning, the tools, workflows, extra modalities, and the cross-surface matrix — which by
definition asserts about *that* project's surfaces.

---

## 9. The conformance kit — the seam to define first

The moment a second project registers its own block kinds and tools, **the framework's
preconditions stop being checkable by the framework.** Every one of them is a prose rule that a
permissive double or a well-meaning override can violate without a single test going red.

So a framework suite needs a **conformance kit**: tests a consumer runs against **its own**
kinds and tools, asserting the framework's preconditions hold for them. The candidates are
already named by the gaps above:

1. Declaring frontier transparency implies inertness in every direction.
2. A tier override appears on a leaf class only, never on a shared base.
3. Every registered block kind parses.
4. Every tool carrying result-side behaviour stays registered under its recorded name.

That kit is the difference between a framework that documents its contract and one that
enforces it.

---

## 10. Assessment

**What this suite does better than most**, each a deliberate cost: it tests against real
infrastructure and blocks everything else, so a green run means the storage law actually held
on a real transaction and nothing reached the network to make it true; it encodes **mechanisms,
not symptoms**, which makes it the most reliable documentation in the repository, because
unlike a design document it fails when it goes stale; and it is **honest about its own limits**
— a suite that documents where it is weak is far more trustworthy than one that is silently
weak.

**What would worry a second consumer.** The absence of a required check undermines everything
above: a suite this good with no mechanical enforcement depends entirely on individual
discipline, which does not survive contact with a second team. Below that, the closed
enumeration of ledger shapes is the structural risk, and the suite's own history says so —
nearly every expensive defect was an ordering defect, which is exactly the class hand
enumeration finds last and generative testing finds first.
