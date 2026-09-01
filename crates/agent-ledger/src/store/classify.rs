//! Sorting what the database said into the class that decides who handles it
//! (2026-09-01).
//!
//! An expectable query error and a serious database failure are different
//! things, and the store is not the place that knows which reaction either one
//! deserves. A write refused by a foreign key is an error the code that made
//! the write understands; a race with another writer is expected and can be
//! retried when a retry makes sense; a file that is not a database is beyond
//! anything the caller can do. So the store classifies and propagates, and the
//! code above chooses.
//!
//! The classification happens once, here, in the conversion from the database
//! library's error into [`StoreError`]. Every `?` in the library and in a
//! consumer's own tables runs through it, so no call site re-reads a result
//! code and no two call sites can disagree about what one means.
//!
//! Decided 2026-09-01, replacing a chokepoint on the store's actor thread that
//! classified the same answers and ended the process on the serious ones. It
//! was rejected for putting the reaction where the blast radius is unknown: the
//! store cannot tell a failure scoped to one inbound message from one that
//! makes every later write wrong, and a process ended inside a query gives the
//! code above no chance to say which it was. The words that settled it: "You
//! aren't meant to panic inside a db query but instead wrap and propagate the
//! error properly so a codepath competent to handle it can decide what to do
//! about it."

use rusqlite::ffi;

use super::StoreError;

/// The database refused the statement because it contradicts the schema.
const REJECTED: i32 = ffi::SQLITE_CONSTRAINT;

/// Another writer held what the statement needed.
const CONTENDED: [i32; 2] = [ffi::SQLITE_BUSY, ffi::SQLITE_LOCKED];

/// The connection cannot be trusted for anything further.
const UNUSABLE: [i32; 3] = [
    // The file is damaged.
    ffi::SQLITE_CORRUPT,
    // The file is not a database at all.
    ffi::SQLITE_NOTADB,
    // The library was called in a way its contract forbids, which means this
    // code is wrong about the connection it is holding.
    ffi::SQLITE_MISUSE,
];

impl From<rusqlite::Error> for StoreError {
    /// Wrap one database error in the class that says who can act on it.
    ///
    /// The result code is read off the PRIMARY code, which the low byte of the
    /// extended code carries, so a class covers every refinement of itself: a
    /// `SQLITE_CONSTRAINT_FOREIGNKEY` is `SQLITE_CONSTRAINT` in its low byte,
    /// and a refinement the database library adds later sorts the same way with
    /// no list to update.
    ///
    /// Everything the three classes do not name stays [`StoreError::Sqlite`] —
    /// out of space, an I/O error, a value too big, a statement that would not
    /// compile. Those are ordinary failures with nothing special to say about
    /// who should handle them.
    fn from(error: rusqlite::Error) -> Self {
        let rusqlite::Error::SqliteFailure(failure, _) = &error else {
            return Self::Sqlite(error);
        };
        let code = failure.extended_code & 0xff;
        if code == REJECTED {
            Self::Rejected(error)
        } else if CONTENDED.contains(&code) {
            Self::Contended(error)
        } else if UNUSABLE.contains(&code) {
            Self::Unusable(error)
        } else {
            Self::Sqlite(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The error the database library hands up for one result code, extended
    /// code intact — the shape a failing statement produces.
    fn sqlite(code: i32) -> rusqlite::Error {
        rusqlite::Error::SqliteFailure(ffi::Error::new(code), Some("scripted".to_owned()))
    }

    #[test]
    fn every_constraint_refinement_is_rejected() {
        for code in [
            ffi::SQLITE_CONSTRAINT,
            ffi::SQLITE_CONSTRAINT_FOREIGNKEY,
            ffi::SQLITE_CONSTRAINT_UNIQUE,
            ffi::SQLITE_CONSTRAINT_NOTNULL,
            ffi::SQLITE_CONSTRAINT_CHECK,
            ffi::SQLITE_CONSTRAINT_PRIMARYKEY,
            ffi::SQLITE_CONSTRAINT_TRIGGER,
        ] {
            assert!(
                matches!(StoreError::from(sqlite(code)), StoreError::Rejected(_)),
                "constraint code {code} is a refusal the caller understands"
            );
        }
    }

    #[test]
    fn a_race_with_another_writer_is_contended() {
        for code in [
            ffi::SQLITE_BUSY,
            ffi::SQLITE_BUSY_SNAPSHOT,
            ffi::SQLITE_BUSY_TIMEOUT,
            ffi::SQLITE_LOCKED,
            ffi::SQLITE_LOCKED_SHAREDCACHE,
        ] {
            assert!(
                matches!(StoreError::from(sqlite(code)), StoreError::Contended(_)),
                "code {code} is a race a retry may settle"
            );
        }
    }

    #[test]
    fn a_damaged_or_misused_database_is_unusable() {
        for code in [
            ffi::SQLITE_CORRUPT,
            ffi::SQLITE_CORRUPT_VTAB,
            ffi::SQLITE_NOTADB,
            ffi::SQLITE_MISUSE,
        ] {
            assert!(
                matches!(StoreError::from(sqlite(code)), StoreError::Unusable(_)),
                "code {code} leaves nothing the caller can do with this connection"
            );
        }
    }

    #[test]
    fn the_ordinary_failures_carry_no_class() {
        for code in [
            ffi::SQLITE_FULL,
            ffi::SQLITE_IOERR,
            ffi::SQLITE_IOERR_READ,
            ffi::SQLITE_IOERR_WRITE,
            ffi::SQLITE_CANTOPEN,
            ffi::SQLITE_TOOBIG,
            ffi::SQLITE_READONLY,
        ] {
            assert!(
                matches!(StoreError::from(sqlite(code)), StoreError::Sqlite(_)),
                "code {code} is an ordinary failure"
            );
        }
    }

    #[test]
    fn an_error_that_carries_no_result_code_is_ordinary() {
        // A row that answered nothing, a column read as the wrong type: the
        // database library's own vocabulary, with no result code to sort on.
        assert!(matches!(
            StoreError::from(rusqlite::Error::QueryReturnedNoRows),
            StoreError::Sqlite(_)
        ));
    }

    #[test]
    fn the_class_keeps_the_error_it_wrapped() {
        // Propagating means the caller still has everything the database said.
        let wrapped = StoreError::from(sqlite(ffi::SQLITE_CONSTRAINT_FOREIGNKEY));
        assert!(
            format!("{wrapped}").contains("scripted"),
            "the database's own message survives the wrapping: {wrapped}"
        );
    }
}
