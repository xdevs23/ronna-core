//! The ledger's own calendar.

use crate::block::{Block, Role};

use super::Agency;
use super::projection::{ContentPart, Projection};

/// A record of the machine's local date at the moment user blocks were
/// appended.
///
/// Agency-inert — no ask, trivially-done `run()` — because it rides BEFORE the
/// user message that owes the turn: a marker with any ask of its own would sit
/// between the cursor and the block that actually owes the turn, and the turn
/// would never fire. So the ratchet sails past it.
///
/// On the projection axis it speaks as a system line, ledger-true and
/// replay-faithful, changing at most daily.
#[derive(Debug, Clone)]
pub struct DateMarker {
    /// The recorded local date, in `YYYY-MM-DD` form.
    pub date: String,
}

impl DateMarker {
    pub(super) fn parse(block: &Block) -> Self {
        Self {
            date: super::string_field(block, "date"),
        }
    }

    /// Exactly `Current date: {YYYY-MM-DD} ({Weekday})`. A stored date that
    /// does not parse degrades to the bare date — never a panic, because a
    /// ledger row is data and a reader that panics on data cannot replay.
    fn line(&self) -> String {
        match chrono::NaiveDate::parse_from_str(&self.date, "%Y-%m-%d") {
            Ok(date) => format!("Current date: {} ({})", self.date, date.format("%A")),
            Err(_) => format!("Current date: {}", self.date),
        }
    }
}

impl Agency for DateMarker {}

impl Projection for DateMarker {
    fn group_role(&self) -> Option<Role> {
        Some(Role::System)
    }

    fn llm_parts(&self) -> Option<Vec<ContentPart>> {
        Some(vec![ContentPart::Text { text: self.line() }])
    }

    fn llm_text(&self) -> Option<String> {
        Some(self.line())
    }
}
