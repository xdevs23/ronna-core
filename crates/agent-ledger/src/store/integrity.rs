//! The one place a database answer is judged against the state the design
//! guarantees (2026-09-01).
//!
//! A database error is one of two things, and the difference decides whether
//! the process may keep running:
//!
//! - **Operational.** The world is busy, out of space, or the disk hiccuped.
//!   The statement did not run; nothing the process believes is wrong. It is
//!   an ordinary error, handled by whoever asked.
//! - **Impossible state.** A constraint the schema declares was violated, the
//!   file is not the database it claims to be, the library was used in a way
//!   its contract forbids, or a row the design guarantees exists is missing.
//!   Reaching here means the process's picture of its own data is already
//!   wrong, and every write it makes from now on writes that wrongness down.
//!
//! Impossible state aborts the process. A supervisor restarts it against the
//! durable state, which is the only picture that was ever true. The decision,
//! stated by the operator: "a database error should hard crash the
//! application, not leave it running in a corrupted state."
//!
//! The judgement happens ONCE, on the store's actor thread, while the answer
//! is still a typed `rusqlite` error with its extended code intact — see
//! [`IntegrityCheck`]. No call site classifies, so no call site can decide to
//! carry on with a corrupted picture, and no call site can even observe the
//! answer first: the abort happens before it travels back.
//!
//! An abort, never a panic. A panic unwinds, and an unwinding panic is caught
//! by a task runtime, logged, and stepped over — which is exactly the
//! "keep running" this refuses.

use rusqlite::ffi;

use super::StoreError;

/// What a database answer says about the process's own state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Integrity {
    /// The statement failed for a reason outside the process. An ordinary
    /// error; the caller handles it.
    Operational,
    /// The data is not what the design says it is. The process cannot be
    /// trusted to keep writing.
    ImpossibleState,
}

/// The primary result codes that mean the process's picture is already wrong.
///
/// Read off the PRIMARY code, which the low byte of the extended code carries,
/// so every refinement of a class is covered by its class: the incident's
/// `SQLITE_CONSTRAINT_FOREIGNKEY` is `SQLITE_CONSTRAINT` in its low byte, and
/// a code the database library adds later falls in the same way without a
/// list to update.
const IMPOSSIBLE_STATE_CODES: [i32; 4] = [
    // A declared constraint was violated: the write contradicts the schema.
    ffi::SQLITE_CONSTRAINT,
    // The file is damaged.
    ffi::SQLITE_CORRUPT,
    // The library was called in a way its contract forbids.
    ffi::SQLITE_MISUSE,
    // The file is not a database at all.
    ffi::SQLITE_NOTADB,
];

/// Judge one answer.
///
/// Positive by construction: a code says impossible state only by being named
/// above. Everything else is operational, including the codes an operator
/// meets on a healthy system — busy, disk full, I/O — which must never take
/// the process down.
pub(crate) fn classify(error: &StoreError) -> Integrity {
    let StoreError::Sqlite(error) = error else {
        // Everything else in this enum is the library's own vocabulary for a
        // refusal it decided: a migration that failed, a kind with no mapping,
        // a rule that said no. Those are answers, not corruption.
        return Integrity::Operational;
    };
    match error {
        // A query the design guarantees answers found nothing. The row is
        // gone or was never written, and the code above it is reading a world
        // that does not exist. Absence that is LEGAL is written as
        // `Option` — `.optional()` at the call, never this error.
        rusqlite::Error::QueryReturnedNoRows => Integrity::ImpossibleState,
        rusqlite::Error::SqliteFailure(failure, _) => {
            if IMPOSSIBLE_STATE_CODES.contains(&(failure.extended_code & 0xff)) {
                Integrity::ImpossibleState
            } else {
                Integrity::Operational
            }
        }
        _ => Integrity::Operational,
    }
}

/// The right to judge an answer, held only on the store's actor thread.
///
/// It exists to make the chokepoint the only door: the actor constructs one
/// and hands it to the work it runs, and a query's answer passes through
/// [`judge`](IntegrityCheck::judge) on that thread before it is sent back to
/// whoever asked. Nothing outside this module can build one, so the
/// classification cannot drift out to a call site.
pub struct IntegrityCheck {
    _private: (),
}

impl IntegrityCheck {
    /// Held by the actor thread alone.
    pub(crate) fn new() -> Self {
        Self { _private: () }
    }

    /// Abort the process if this answer says the data is not what the design
    /// guarantees; do nothing at all otherwise.
    ///
    /// Called with the answer still on the actor thread, so an impossible
    /// state never reaches the caller who could act on it.
    pub fn judge<R>(&self, answer: &Result<R, StoreError>) {
        let Err(error) = answer else {
            return;
        };
        if classify(error) == Integrity::Operational {
            return;
        }
        abort_on_impossible_state(&format!("{error}"));
    }
}

/// The end of the process, with the reason written down first.
///
/// Separated so the log line and the abort stay together for every way in:
/// the classifier's verdict here, and a panic on the store thread, which is
/// the same fact arriving as an unwind.
pub(crate) fn abort_on_impossible_state(reason: &str) -> ! {
    tracing::error!(
        reason,
        "store: the database is not in the state the design guarantees — aborting instead of \
         writing on top of it"
    );
    std::process::abort()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build the error rusqlite hands up for one result code, extended code
    /// included — the same shape a failing statement produces.
    fn sqlite(code: i32) -> StoreError {
        StoreError::Sqlite(rusqlite::Error::SqliteFailure(
            ffi::Error::new(code),
            Some("scripted".to_owned()),
        ))
    }

    #[test]
    fn a_foreign_key_violation_is_impossible_state() {
        // The incident's own error: a write against a conversation that was
        // deleted under it.
        assert_eq!(
            classify(&sqlite(ffi::SQLITE_CONSTRAINT_FOREIGNKEY)),
            Integrity::ImpossibleState
        );
    }

    #[test]
    fn every_constraint_refinement_is_impossible_state() {
        for code in [
            ffi::SQLITE_CONSTRAINT,
            ffi::SQLITE_CONSTRAINT_FOREIGNKEY,
            ffi::SQLITE_CONSTRAINT_UNIQUE,
            ffi::SQLITE_CONSTRAINT_NOTNULL,
            ffi::SQLITE_CONSTRAINT_CHECK,
            ffi::SQLITE_CONSTRAINT_PRIMARYKEY,
        ] {
            assert_eq!(
                classify(&sqlite(code)),
                Integrity::ImpossibleState,
                "constraint code {code} must abort"
            );
        }
    }

    #[test]
    fn corruption_and_misuse_are_impossible_state() {
        for code in [
            ffi::SQLITE_CORRUPT,
            ffi::SQLITE_CORRUPT_VTAB,
            ffi::SQLITE_MISUSE,
            ffi::SQLITE_NOTADB,
        ] {
            assert_eq!(
                classify(&sqlite(code)),
                Integrity::ImpossibleState,
                "code {code} must abort"
            );
        }
    }

    #[test]
    fn a_missing_guaranteed_row_is_impossible_state() {
        assert_eq!(
            classify(&StoreError::Sqlite(rusqlite::Error::QueryReturnedNoRows)),
            Integrity::ImpossibleState
        );
    }

    #[test]
    fn busy_full_and_io_stay_operational() {
        for code in [
            ffi::SQLITE_BUSY,
            ffi::SQLITE_BUSY_SNAPSHOT,
            ffi::SQLITE_LOCKED,
            ffi::SQLITE_FULL,
            ffi::SQLITE_IOERR,
            ffi::SQLITE_IOERR_READ,
            ffi::SQLITE_IOERR_WRITE,
            ffi::SQLITE_CANTOPEN,
        ] {
            assert_eq!(
                classify(&sqlite(code)),
                Integrity::Operational,
                "code {code} must NOT abort"
            );
        }
    }

    #[test]
    fn the_librarys_own_refusals_stay_operational() {
        for error in [
            StoreError::ActorStopped,
            StoreError::Other("an approval was already decided".to_owned()),
            StoreError::MigrationFailed {
                domain: "core".to_owned(),
                version: 3,
                reason: "syntax error".to_owned(),
            },
            StoreError::MissingBlockContent {
                block_id: 7,
                block_type: "message".to_owned(),
            },
        ] {
            assert_eq!(classify(&error), Integrity::Operational);
        }
    }

    #[test]
    fn an_answer_that_succeeded_is_never_judged_a_failure() {
        // The check is silent on success — the shape every query takes.
        let check = IntegrityCheck::new();
        check.judge::<i64>(&Ok(7));
        check.judge::<i64>(&Err(StoreError::ActorStopped));
    }
}
