# Slice 15 — one public clock reading, and the marker stops stating a time

Date: 2026-08-29. A consumer wants to state the current date and time — the operator
asked for it after the date markers went live and answered a time question with the
marker's written-at stamp, twenty-five minutes stale. The operator's ruling, verbatim
in effect: the time does not belong in the date block, because it loses its validity
one minute later, while the date stays right all day. The framework already owns the one
clock that answers such questions: `DateStamp::now_local`
(`store/date_markers.rs:46-56`), which reads a single instant for every part — local
date via chrono, the IANA zone name via `iana-time-zone`, the wall-clock minute — so a
reading taken in the last millisecond of a day cannot carry tomorrow's date beside
today's minute. That function and its struct are `pub(crate)`; a consumer re-deriving
"now, local, named zone" would record the clock decision a second time and could
drift from what the markers say.

## The changes

**The marker's projected line drops its time clause.** The dated system line renders
the date, the weekday and the zone, and no longer the "marker written at HH:MM"
clause — a model reads a stated time as the present, and it is stale within a minute.
The `written_at` COLUMN stays stored exactly as today: it is the ledger's own label
for when the row was written, a record fact, just never again model-facing prose. The
render golden and the kind's unit tests move with the line; the change-detection rule,
the seams and the stored shape do not change.

**The crate exports one public, read-only clock reading:** a small public struct carrying
the local date (`YYYY-MM-DD`), the weekday name, the zone abbreviation and IANA zone
name (each `Option`, the stamp's exact NULL honesty — an unanswered source is NULL,
never a guess), and the wall-clock time (`HH:MM`), plus one public constructor that
reads now. One instant answers every part, and the reading shares its source with
`DateStamp::now_local` through one private read so the clock stays one recorded
decision — the public reading and the marker's stamp can never disagree on sources.
Placement and names follow the module's own conventions; the export is ADDITIVE ONLY:
no existing API moves and `DateStamp` stays `pub(crate)`. The one behavior change in
this slice is the projected line above, nothing else.

## Acceptance criteria

- **AC1** Workspace green; clippy, fmt, doc under denied warnings; no new dependency.
- **AC2** The public reading and `DateStamp::now_local` are two views of ONE private
  read — checked structurally (one source function, cited file:line), so the clock
  decision stays recorded once.
- **AC3** One instant answers every part of the public reading — pinned the way the
  stamp's own discipline is stated, and the weekday matches the date it rides with.
- **AC4** The reading's export is additive — `git diff` shows no existing public
  item changed — and the only behavior change anywhere is the projected line losing
  its time clause, with the goldens and the kind's own tests updated to the new line
  and the stored `written_at` column untouched by the diff.

## Notes for launch

Branches from `master` (worktree `~/projects/agent-ledger-clock`, branch
`slice/clock-read`). Site: `store/date_markers.rs` (or the module its conventions
prefer for a public reading), tests beside it. The consumer half is the app
repository's unit 34, which builds only after this merges to master.
