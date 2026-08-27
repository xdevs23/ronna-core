# Slice 13 — the date discipline reaches consumer appends, and the marker learns its timezone

Date: 2026-08-27. The library already answers "does the model know what day it is" — and a
consumer of the library gets none of it. This slice closes that gap and widens the marker by
one fact the operator asked for on this date: the timezone, stated as both the abbreviation
and the IANA name.

## Grounding

**The mechanism exists whole.** A `date_marker` block kind (`agency/date_marker.rs`)
projects as a system line, `Current date: {YYYY-MM-DD} ({Weekday})`, degrading to the bare
date when the stored value does not parse, "because a ledger row is data and a reader that
panics on data cannot replay" (`date_marker.rs:36-43`). It is agency-inert by design so the
ratchet sails past it (`:10-15`). The insert seam, `ensure_date_marker`
(`store/date_markers.rs:22-48`), is change detection: compare today against the LATEST
marker in junction order, insert only on difference, first message trips it for free,
same-day appends insert nothing.

**Four call sites run it, all library-internal.** The group-append seam
`insert_user_blocks_dated` (`store/messages.rs:757-765`, "one atomic transaction that runs
the date marker's change detection BEFORE the user blocks land"), the draft finalization
(`store/drafts.rs:225`), and both fork continuations that write fresh user content
(`store/conversations.rs:419` for an edit, `:443` for a new thread).

**The consumer write path deliberately skips it, and the reason is recorded.**
`append_consumer_block`'s documentation (`store/descriptors.rs:1039-1043`): "The composer's
date-marker discipline deliberately does NOT run here: it belongs to the group-append seam
(`Store::insert_user_blocks`), which detects the day change once per submitted group." That
reasoning is correct for the composer it was written about — and it silently decides for
every consumer too. The halogenOS assistant lands every chat message through
`append_consumer_block` and never touches `insert_user_blocks`; its store carries the
`block_date_marker` table (the migration ran) and zero rows. Verified against the running
deployment's sibling on 2026-08-27. The model behind that consumer does not know the date,
which is the complaint that opened this slice.

**The stored shape is one column.** `block_date_marker (block_id, date)`, written from
`today_local()` — `chrono::Local` formatted `%Y-%m-%d` (`date_markers.rs:14-16`). No
timezone is stored, so the line cannot honestly say one today.

## Decisions taken with this slice

- **A user-voiced consumer append runs the same change detection, inside the same
  transaction, 2026-08-27.** `append_consumer_block` gains the discipline for exactly the
  appends whose role is `User`: before the block lands, the marker's change detection runs
  in the transaction that carries it, so the marker rides the ledger immediately before the
  message that owes the turn — the same ordering promise the group seam makes, and the
  detection itself stays where it lives today, in `date_markers.rs`, called and never
  duplicated. The recorded "deliberately does NOT run here" reasoning in `descriptors.rs` is
  amended in place with this date: it was written about the composer's own finalizing
  inserts and the approval chain, both of which keep their skip; it was never a decision
  about consumers, it just caught them. *Rejected:* a public
  `Store::ensure_date_current(conversation_id)` for the consumer to call at its ingest edge
  — it moves the invocation into every consumer, each of which can forget it, and the
  marker's ordering promise ("rides the same atomic append") cannot be kept from outside
  the transaction; *rejected:* a `dated` flag on `append_consumer_block` — a boolean that
  every caller must set correctly is the same forgettable decision wearing a parameter;
  *rejected:* an actor-side append at turn start — the existing design puts the marker at
  the append that makes the turn owed, atomically, so the wire never carries a date the
  ledger cannot replay, and a turn-start write would race the projection it feeds.
- **The residual is named rather than papered over, 2026-08-27.** A turn that becomes owed
  without a fresh user-voiced append — a redispatch or an unparked cursor crossing midnight
  — still runs on the newest marker, which may be yesterday's. Accepted: the window is
  narrow, the failure is a date one day stale in a conversation nobody wrote to overnight,
  and closing it would put a write at turn start with the race just rejected. Revisit only
  if it is observed to mislead.
- **The marker stores the timezone it was written in: the abbreviation and the IANA name,
  2026-08-27.** Two new nullable columns, `tz_abbrev` (for example `CEST`) and `tz_name`
  (for example `Europe/Berlin`), written at insert. The projection line becomes
  `Current date: {YYYY-MM-DD} ({Weekday}), timezone {ABBREV} ({NAME})`, degrading by parts:
  a row with no timezone renders today's exact line unchanged, a row with only one of the
  two renders what it has — old rows keep rendering, and no expectation is rewritten
  retroactively. The abbreviation comes from `chrono::Local` (`%Z`); the IANA name from the
  platform's own answer, for which a small dedicated crate exists — **check its latest
  version on the web and its registry history for a compromised release before adding it**
  (standing rule), and if it does not earn its keep, fall back to reading the `TZ`
  environment variable and the `/etc/localtime` symlink target, in that order, storing NULL
  when neither answers. A NULL name is honest; a guessed one is not.
- **Change detection compares the date and the timezone pair, 2026-08-27.** A daylight-time
  transition changes `CEST` to `CET` on a day the date also changes at the same wall-clock
  midnight in the same zone — but a machine whose configured zone is changed mid-day, or a
  deployment moved between hosts, would otherwise keep announcing the old zone until
  midnight. The pair costs the same one-row read the date alone costs. *Rejected:* keying
  on the date alone (the stale-zone case above); *rejected:* keying on the full timestamp
  (a marker per message).
- **The marker states the time it was appended, labelled as exactly that, 2026-08-27.** The
  operator asked for time as well as date. A line claiming "the current time is HH:MM" is
  false within a minute of being written and false forever on replay; a line reading
  `; marker written at {HH:MM}` is ledger-true, replay-faithful, and still anchors the model
  to within a day's precision — which is what "roughly when is now" needs. Stored as one
  more nullable column, `written_at_local` (`HH:MM`), rendered only when present.
  *Rejected:* a live clock injected per turn — it changes every projection, defeats
  provider-side prompt caching daily gains, and is stale the moment it is read anyway;
  *rejected:* leaving time out — the operator named it, and the labelled form costs
  nothing.
- **No consumer code changes, 2026-08-27.** The consumer inherits the markers through the
  path it already calls. Its composing enum already delegates the framework's kinds, so the
  marker projects without a consumer edit. The consumer repository's own follow-ups —
  whether its deployment's host is configured to the intended `Europe/Berlin` zone — are
  deployment concerns, named for its operator, not part of this slice.

## The slice's contract

A user-voiced block appended through the consumer write path is preceded, in its own
transaction, by a fresh date marker whenever the local date or the local timezone differs
from the newest marker in that conversation, exactly as the library's own group appends
already are. The marker records the date, the timezone abbreviation, the IANA zone name
where the platform answers, and the local wall-clock time it was written. Its projection
names all of what it has and nothing it lacks, and every marker written before this slice
renders exactly as it did. Non-user consumer appends, the approval chain and the composer's
finalizing inserts are unchanged. The change detection exists in one place before and after.
No render pin for existing kinds changes.

## Acceptance criteria

- **AC1** Workspace suite green with and without every provider feature; clippy, fmt, doc
  under denied warnings; any new dependency version-checked on the web and its registry
  history checked for a compromised release, with the check stated in the report.
- **AC2** A user-voiced consumer append trips the marker: a consumer-shaped kind appended
  with `Role::User` on a new day inserts a marker ordered BEFORE it in junction order —
  pinned, and pinned again for the same-day case inserting nothing.
- **AC3** Non-user consumer appends never trip it: an assistant-voiced and a role-less
  consumer append on a fresh day insert no marker — pinned, since "exactly the user-voiced
  appends" is the decision and its edge is where it erodes.
- **AC4** The timezone pair is part of the change detection: same date, changed zone
  inserts; same date, same zone does not — pinned by driving the seam with explicit values,
  the way the midnight tests already do.
- **AC5** The projection degrades by parts: a full row renders date, weekday, abbreviation,
  name and written-at; a date-only row renders character-for-character what it renders
  before this slice; a row missing only the name renders without the parenthesised name —
  each pinned.
- **AC6** Replay is faithful: a store written under this slice, read again, renders the
  same lines — pinned through the real projection fold, not through the kind in isolation.
- **AC7** The one-place property holds: the change detection has exactly one definition and
  every trip site calls it — checked by inspection of the diff, and the `descriptors.rs`
  reasoning carries its dated amendment.
- **AC8** The existing seams still work: the group-append, draft and both fork sites pass
  their existing pins unchanged, now writing the widened row.

## Notes for launch

- Branches from `master` (worktree `~/projects/agent-ledger-dated-appends`, branch
  `unit/dated-consumer-appends`). Sites: `store/date_markers.rs` (the seam and its tests),
  `store/descriptors.rs` (`append_consumer_block` and its amended documentation),
  `store/migrations.rs` (three nullable columns on `block_date_marker`),
  `agency/date_marker.rs` (parse and line), and the render pins that cover date markers
  (`providers/render/tests.rs`, `providers/anthropic/tests.rs`, `providers/kimi/tests.rs`).
- The midnight tests drive `insert_user_blocks_dated` with explicit dates
  (`store/date_markers.rs:50` onward); extend that pattern rather than inventing a clock
  abstraction — the seam takes values, production passes now, tests pass what they mean.
- Slice 12 (`12-projection-may-resolve.md`) exists on an unmerged branch; this slice is
  numbered around it and touches none of the same sites except the render tests' call
  shape, which slice 12 changes to async. Whichever lands second rebases mechanically.
