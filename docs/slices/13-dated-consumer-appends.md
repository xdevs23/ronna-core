# Slice 13 — the date discipline reaches consumer appends, and the marker learns its timezone

Date: 2026-08-27. Revision 2, corrected against a cold probe that ran the claims on the real
host. What changed: the abbreviation source (chrono's `%Z` prints `+02:00`, never `CEST` —
proven by running it), the site list (the read path in `store/blocks.rs` was missing, without
which the new columns are written and never read), the test seam (the rejected parameter left
AC2 and AC4 with nothing to drive), the NULL comparison rule, the migration as a numbered
step, and the projected line written out in full with every degrade form. The probe also
settled the make-or-break question in the slice's favour: a consumer-domain transaction
reaches the core tables, because a domain is a migration-ordering gate on one shared
connection, not a schema of its own — `append_consumer_block` already writes `blocks` and
`conversation_blocks` through it today (`store/mod.rs:695-730`, `store/descriptors.rs:1070`).

## Grounding

**The mechanism exists whole.** A `date_marker` block kind (`agency/date_marker.rs`)
projects as a system line, `Current date: {YYYY-MM-DD} ({Weekday})`, degrading to the bare
date when the stored value does not parse (`date_marker.rs:36-43`). It is agency-inert so
the ratchet sails past it (`:10-16`). The insert seam, `ensure_date_marker`
(`store/date_markers.rs:22-48`), is change detection: compare against the LATEST marker in
junction order (`ORDER BY cb.id DESC LIMIT 1`), insert only on difference, first message
trips it for free. Ordering holds structurally: `conversation_blocks.id` is assigned in
insert order (`store/migrations.rs:110-114`) and the projection folds `ORDER BY cb.id`
(`store/blocks.rs:95`), so a marker inserted first in the transaction precedes its message.

**Four call sites run it, all library-internal.** The group-append seam
`insert_user_blocks_dated` (`store/messages.rs:757-765`), the draft finalization
(`store/drafts.rs:225`), and both fork continuations that write fresh user content
(`store/conversations.rs:419`, `:443`). Three of the four build their date argument inline
at the call site; all four must pass the widened stamp this slice introduces.

**The consumer write path deliberately skips it, and the skip caught bystanders.**
`append_consumer_block`'s documentation (`store/descriptors.rs:1039-1043`) scopes the
discipline to the group-append seam. That was written about the composer and the approval
chain, and it silently decided for every consumer: the halogenOS assistant has exactly one
production user-voiced consumer append — its chat-message landing
(consumer `assembly.rs:743`, `Some(Role::User)`) — and its store carries the
`block_date_marker` table with zero rows. Every other production consumer append passes
`None` for the role (context notes, palette blocks, reports). One honest narrowing: that
single user-voiced site also lands command rows — privacy commands, the deletion mirror —
which are answered without the model and owe no turn. The rule this slice implements is
"user-voiced", not "turn-owing"; the same-day dedupe bounds the cost to at most one marker
on a day whose only traffic is a command, and that is accepted rather than special-cased.

**The read path is two sites in `store/blocks.rs`, and two schema pins guard it.**
`BLOCKS_QUERY` selects `bdm.date AS bdm_date` and nothing else from the marker table
(`store/blocks.rs:41`); `row_to_block`'s `"date_marker"` arm builds exactly one field
(`:378-383`). Without widening both, `DateMarker::parse` never sees the new columns. Two
pins change deliberately with this: `PINNED_BLOCKS_QUERY`, a byte-identical copy of the
statement (`store/descriptors.rs:1244-1315`), and the column-vector assertion
`the_date_marker_table_is_present` (`store/migrations.rs:538-543`). The RENDER pins
survive untouched: they hand-build blocks with a `date` field only
(`providers/render/tests.rs:521-563`, `providers/anthropic/tests.rs:609`,
`providers/kimi/tests.rs:110`), and a date-only row renders as before.

**The timezone sources, proven on this host** (chrono 0.4.45 from the lockfile,
`/etc/localtime → /etc/zoneinfo/Europe/Berlin`):

- chrono `%Z` prints `+02:00` — an offset, never an abbreviation. It is NOT a source.
- `libc::localtime_r().tm_zone` answers `CEST` (`gmtoff=7200`, `isdst=1`); `libc` is
  already in the tree.
- `iana_time_zone::get_timezone()` answers `Ok("Europe/Berlin")`, and the crate is
  **already vendored in both lockfiles as chrono's own dependency** (`Cargo.lock:148-158`)
  — taking it as a direct dependency adds no new supply-chain surface. Note for any
  fallback reading of `/etc/localtime`: on this host the target is `/etc/zoneinfo/...`,
  not `/usr/share/zoneinfo/...` — never hardcode the prefix.

**Where the marker sits on each wire.** The consumer's wire (chat base) keeps a system
message inline at its ledger position (`providers/chat/base.rs:518-523`): a new day appends
at the tail and the cached prefix is untouched — the caching claim holds there. The
anthropic and openai modules fold every system group into the one top-level system
parameter (`anthropic/mod.rs:256-270`, `openai/mod.rs:225-238`), so past days' markers pile
up in the prefix and a new day rewrites it. That behaviour predates this slice and is not
changed by it; this slice only makes each line longer. Stated so the caching claim is
honest about which shape it was checked on.

## Decisions taken with this slice

- **A user-voiced consumer append runs the same change detection, inside the same
  transaction, 2026-08-27.** `append_consumer_block` gains the discipline for exactly the
  appends whose role is `User`: the marker's change detection runs in the transaction that
  carries the block, ordered before it. The detection stays defined once in
  `date_markers.rs`. The recorded "deliberately does NOT run here" reasoning in
  `descriptors.rs` is amended in place with this date: it was written about the composer's
  finalizing inserts and the approval chain, both of which keep their skip. *Rejected:* a
  public `ensure_date_current` for consumers to call (forgettable, and the ordering promise
  cannot be kept from outside the transaction); *rejected:* a `dated` flag on the public
  method (the same forgettable decision wearing a parameter); *rejected:* an actor-side
  append at turn start (races the projection it feeds).
- **The test seam is a `pub(crate)` dated sibling, mirroring the one that exists,
  2026-08-27.** `append_consumer_block` delegates to
  `append_consumer_block_stamped(..., stamp: DateStamp)`, `pub(crate)`, exactly as
  `insert_user_blocks` delegates to `insert_user_blocks_dated` (`messages.rs:753-757`).
  Production passes the stamp built from now; tests drive midnight, zone changes and NULL
  cases deterministically. This is what AC2 and AC4 drive.
- **The stamp is one value, built in one place, passed to all five sites, 2026-08-27.** A
  `DateStamp { date, tz_abbrev: Option, tz_name: Option, written_at: Option }` built by one
  constructor beside `today_local()`. The four existing trip sites and the new consumer
  site all take it, so "what a marker records" is decided once. *Rejected:* widening each
  call site's inline argument list (five copies of the same decision).
- **The timezone sources, amended 2026-08-28 after the build:** the IANA name comes from
  `iana-time-zone` (promoted to a direct dependency at 0.1.65 after the web and registry
  check recorded in `docs/dependency-review.md`), NULL when it errors. The abbreviation
  clause of 2026-08-27 named `localtime_r`'s `tm_zone` via libc — **unbuildable on this
  tree**, found by the implementer: the workspace forbids unsafe code
  (`[workspace.lints.rust] unsafe_code = "forbid"`, not liftable by a local allow) and
  `libc` was transitive, not direct. Production therefore writes `tz_abbrev` NULL and the
  line renders the spec's own abbrev-NULL degrade form, which carries the zone identity in
  full (`timezone Europe/Berlin; marker written at HH:MM`). The recorded path to a real
  abbreviation is `chrono-tz` (derive the abbreviation from the stored IANA name at the
  stamp's moment), taken in a later slice only after its own registry check — the earlier
  rejection called it heavyweight against `tm_zone`, and with `tm_zone` off the table that
  comparison is void. A NULL is honest; a guessed value is not. *Rejected:* chrono `%Z`
  (proven to print the offset); *rejected:* lifting the unsafe forbid for one FFI call (a
  workspace-wide guarantee traded for a nicety).
- **Change detection: dates differ, or the zone knowably changed, 2026-08-27.** A fresh
  marker is inserted when the stored date differs from the stamp's, OR when the stored and
  current `tz_name` are BOTH non-NULL and differ. A NULL on either side of the zone
  comparison never counts as a difference. This closes both traps the probe named: an
  upgraded store (all NULL zones) writes no same-day marker storm — the widened columns
  simply appear at the next natural date change — and an intermittently failing zone
  lookup, flapping between NULL and a value, cannot write a marker per message.
  *Rejected:* comparing NULL as a distinct value (the flapping and the upgrade storm);
  *rejected:* including the abbreviation in the comparison (it changes twice a year with
  DST at the same wall-clock midnight the date changes at, and `tz_name` is the stable
  identity).
- **The projected line, written out in full with every form, 2026-08-27.** The parsed-date
  full form:

      Current date: 2026-08-27 (Thursday), timezone CEST (Europe/Berlin); marker written at 22:41

  Degrades by parts, each clause independent:
  - name NULL: `Current date: 2026-08-27 (Thursday), timezone CEST; marker written at 22:41`
  - abbrev NULL: `Current date: 2026-08-27 (Thursday), timezone Europe/Berlin; marker written at 22:41`
  - both zone parts NULL: `Current date: 2026-08-27 (Thursday); marker written at 22:41`
  - time also NULL (every pre-slice row): `Current date: 2026-08-27 (Thursday)` — character
    for character today's line.
  - unparseable stored date: the existing bare degrade keeps its shape and the new clauses
    still attach to it, since none of them depends on the date parsing:
    `Current date: {raw}, timezone CEST (Europe/Berlin); marker written at 22:41`.
- **The marker states the time it was appended, labelled as exactly that, 2026-08-27.** A
  line claiming "the current time" is false within a minute and false forever on replay;
  "marker written at" is ledger-true and anchors the model to within a day. One noted
  consequence, decided rather than discovered later: the marker trips on the day's first
  user-voiced append, so its minute is the minute of one person's message, projected to
  the model — where `blocks.created_at` already records full timestamps for every block on
  header rows erasure never touches. The marker adds no new stored precision, only a new
  rendering of the day's first activity minute, and that is accepted. *Rejected:* a live
  clock per turn (changes every projection, stale the moment it is read); *rejected:*
  rounding to the hour (a false label is not improved by being vaguer; the honest label is
  the fix).
- **The migration is a new numbered step, never an edit to the shipped schema,
  2026-08-27.** Three nullable columns arrive as a new entry in the `MIGRATIONS` array
  under `user_version`, following the module's own rule that "every later change to the
  shipped schema arrives the same way" (`migrations.rs:15-19`). Editing the v1
  `CREATE TABLE` (`migrations.rs:235-238`) would pass every fresh-database test and leave
  every existing store without the columns forever. The populated-upgrade precedent to
  copy is `a_populated_v1_database_upgrades_to_v2_in_place` (`migrations.rs:497-514`).
- **Named residuals, accepted with reasons, 2026-08-27.**
  1. A turn owed without a fresh user-voiced append — a redispatch crossing midnight —
     runs on yesterday's marker. Narrow, and closing it means the turn-start write
     rejected above.
  2. A failed marker insert rolls back the member's message it rides with, surfacing as
     the store error the ingest path already reports. Consistent with every existing seam;
     the alternative — the marker outside the transaction — gives up the ordering promise.
  3. A command row that owes no turn can trip the day's marker. Bounded to one per day by
     the dedupe; a "turn-owing" filter would smear knowledge of consumer command semantics
     into the store.
- **No consumer code changes, and the complaint closes at the merge fence, 2026-08-27.**
  The consumer inherits the markers through the path it already calls; its composing enum
  already delegates the framework kinds (consumer `kind.rs:1096-1121`). The consumer
  path-depends on `~/projects/agent-ledger`, so the complaint that opened this slice is
  closed when this branch merges there, not before. Its deployment's host timezone is that
  repository's concern.

## The slice's contract

A user-voiced block appended through the consumer write path is preceded, in its own
transaction, by a fresh date marker whenever the stamp's date differs from the newest
marker's, or both hold a zone name and the names differ — exactly as the library's own
group appends, drafts and fork continuations are, all now writing the same widened stamp
through one constructor. The marker records the date, the timezone abbreviation from the
platform's own `tm_zone`, the IANA name where the resolver answers, and the wall-clock
minute it was written; each part is independently nullable and the projection renders
exactly the forms written above, so every pre-slice row renders character for character as
it does today. The columns arrive by a new numbered migration that upgrades a populated
store in place. Non-user consumer appends, the approval chain and the composer's finalizing
inserts are unchanged. The change detection exists in one place. No render pin for existing
kinds changes; the two schema pins (`PINNED_BLOCKS_QUERY`, the marker column vector) change
deliberately and byte-exactly with the schema.

## Acceptance criteria

- **AC1** Workspace suite green with and without every provider feature; clippy, fmt, doc
  under denied warnings; the promoted `iana-time-zone` version checked on the web and its
  registry history checked for a compromised release, stated in the report; no other new
  dependency.
- **AC2** A user-voiced consumer append trips the marker: driven through the stamped seam
  with explicit values, a consumer-shaped kind appended with `Role::User` on a new day
  inserts a marker ordered BEFORE it in junction order — pinned, and pinned again for the
  same-day case inserting nothing.
- **AC3** Non-user consumer appends never trip it: an assistant-voiced and a role-less
  consumer append on a fresh day insert no marker — pinned.
- **AC4** The change-detection rule holds at every edge, driven with explicit stamps: same
  date + both names present and differing → inserts; same date + stored NULL vs present
  name → does NOT insert; same date + present name vs NULL → does NOT insert; date change
  alone → inserts — each pinned, since the NULL rules are where the storm and the flapping
  live.
- **AC5** The projection renders every written form: full, name-NULL, abbrev-NULL,
  zone-NULL, all-NULL, and the unparseable-date form with clauses attached — each pinned
  character for character against the lines in this spec, and the all-NULL pin asserted
  equal to the pre-slice expectation string unedited.
- **AC6** Replay is faithful end to end: a store written through the stamped seam, read
  back through `BLOCKS_QUERY` and folded through the real projection, renders the same
  lines — pinned through the fold, not the kind in isolation, since the read path in
  `store/blocks.rs` is half this slice.
- **AC7** The one-place property holds: one change-detection definition, one stamp
  constructor, five call sites passing the stamp — checked on the diff, and the
  `descriptors.rs` reasoning carries its dated amendment.
- **AC8** The migration upgrades a populated store: a store built at the previous version
  with marker rows gains the columns in place, old rows all-NULL, new appends writing the
  widened row — pinned on the `a_populated_v1_database_upgrades_to_v2_in_place` precedent.
- **AC9** The existing seams still work: group-append, draft and both fork sites pass
  their existing pins, now through the stamp — and the two schema pins are updated
  byte-exactly rather than loosened.

## Build outcome notes (2026-08-28)

- Verifier: PASS on AC1-AC9, full gate green including no-default-features.
- A role-less marker begins its own group boundary, so a fork's deep copy groups
  [marker][user text …] rather than splitting a user group — reproduced, fixed, and the
  boundary shift is recorded in `append_consumer_block`'s documentation, not only in a
  test. The deep-copy multi-block coverage lives in the neighbouring remap test.
- `tz_abbrev` ships as an always-NULL column with its writer's reason documented at
  `local_tz_abbrev` — the one statement of the fact. See the amended source decision.

## Notes for launch

- Branches from `main` (worktree `~/projects/agent-ledger-dated-appends`, branch
  `unit/dated-consumer-appends`). Sites: `store/date_markers.rs` (the widened seam, the
  stamp type and constructor, the change-detection rule and its tests),
  `store/descriptors.rs` (`append_consumer_block`, its stamped sibling, the amended
  reasoning, and `PINNED_BLOCKS_QUERY`), `store/blocks.rs` (`BLOCKS_QUERY` and
  `row_to_block`'s marker arm), `store/migrations.rs` (the new numbered step and the
  column-vector pin), `store/messages.rs`, `store/drafts.rs`, `store/conversations.rs`
  (stamp threading), `agency/date_marker.rs` (parse and line), `Cargo.toml` (the promoted
  dependency), and the marker tests beside each.
- The midnight tests (`date_markers.rs:50` onward) are the pattern: the seam takes values,
  production passes now, tests pass what they mean. Extend, do not invent a clock.
- Slice 12 (`12-projection-may-resolve.md`) lives on an unmerged branch and turns the
  projection async; the only shared surface is the render tests' call shape. Whichever
  lands second rebases mechanically.
