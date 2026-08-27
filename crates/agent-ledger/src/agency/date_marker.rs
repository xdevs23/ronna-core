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
    /// The zone abbreviation, `CEST` — absent when nothing answered for it,
    /// and absent on every marker written before the column existed.
    ///
    /// Absent on every marker this library WRITES, as it stands: the store's
    /// `local_tz_abbrev` records why the abbreviation has no reachable source
    /// here, and is the single statement of it. A row carries a value only
    /// when something else wrote it. The kind reads and renders one either
    /// way — this field is what a reader of a row must expect, not a promise
    /// about what a writer supplies.
    pub tz_abbrev: Option<String>,
    /// The IANA zone name, `Europe/Berlin` — absent under the same rule, and
    /// independently of the abbreviation.
    pub tz_name: Option<String>,
    /// The wall-clock `HH:MM` this marker was written at — the minute the
    /// day's first user-voiced append landed, never a claim about now.
    pub written_at: Option<String>,
}

impl super::LeafKind for DateMarker {
    const KINDS: &'static [&'static str] = &["date_marker"];

    fn parse(block: &Block) -> Self {
        Self {
            date: super::string_field(block, "date"),
            // The three nullable columns read through the optional reader, not
            // the empty-string one: a clause the row does not carry must drop
            // out of the line, and an empty string would print an empty clause.
            tz_abbrev: super::optional_string_field(block, "tz_abbrev"),
            tz_name: super::optional_string_field(block, "tz_name"),
            written_at: super::optional_string_field(block, "written_at"),
        }
    }
}

impl DateMarker {
    /// `Current date: {YYYY-MM-DD} ({Weekday})`, then a timezone clause and a
    /// writing-minute clause for whichever of them the row carries:
    ///
    /// ```text
    /// Current date: 2026-08-27 (Thursday), timezone CEST (Europe/Berlin); marker written at 22:41
    /// ```
    ///
    /// Each clause is independent of the others and of the date parsing, so a
    /// row missing any of them simply omits that clause — and a row carrying
    /// only a date renders exactly the line it rendered before the columns
    /// existed. A stored date that does not parse degrades to the bare date,
    /// keeping whatever clauses it has: never a panic, because a ledger row is
    /// data and a reader that panics on data cannot replay.
    fn line(&self) -> String {
        let date = match chrono::NaiveDate::parse_from_str(&self.date, "%Y-%m-%d") {
            Ok(date) => format!("Current date: {} ({})", self.date, date.format("%A")),
            Err(_) => format!("Current date: {}", self.date),
        };
        let zone = match (self.tz_abbrev.as_deref(), self.tz_name.as_deref()) {
            (Some(abbrev), Some(name)) => format!(", timezone {abbrev} ({name})"),
            (Some(zone), None) | (None, Some(zone)) => format!(", timezone {zone}"),
            (None, None) => String::new(),
        };
        let written = self
            .written_at
            .as_ref()
            .map_or_else(String::new, |at| format!("; marker written at {at}"));
        format!("{date}{zone}{written}")
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

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::DateMarker;
    use crate::agency::{LeafKind, Projection};
    use crate::block::Block;

    /// A stored marker row, exactly as the read path builds one: the date, and
    /// each optional column either a string or JSON null.
    fn marker(date: &str, abbrev: Option<&str>, name: Option<&str>, at: Option<&str>) -> Block {
        let mut fields = serde_json::Map::new();
        fields.insert("date".into(), Value::String(date.into()));
        for (key, value) in [("tz_abbrev", abbrev), ("tz_name", name), ("written_at", at)] {
            fields.insert(
                key.into(),
                value.map_or(Value::Null, |v| Value::String(v.into())),
            );
        }
        Block {
            id: 1,
            role: None,
            block_type: "date_marker".into(),
            created_at: String::new(),
            dispatch_anchor: None,
            fields,
        }
    }

    fn line(block: &Block) -> String {
        DateMarker::parse(block).llm_text().unwrap()
    }

    /// The pre-slice line, spelled out once and used as the all-NULL
    /// expectation below: every marker written before the zone columns existed
    /// must still project THIS, character for character.
    const PRE_SLICE_LINE: &str = "Current date: 2026-08-27 (Thursday)";

    /// Every written form, pinned against the line the slice specified.
    #[test]
    fn the_projected_line_degrades_clause_by_clause() {
        assert_eq!(
            line(&marker(
                "2026-08-27",
                Some("CEST"),
                Some("Europe/Berlin"),
                Some("22:41")
            )),
            "Current date: 2026-08-27 (Thursday), timezone CEST (Europe/Berlin); marker written at 22:41"
        );
        assert_eq!(
            line(&marker("2026-08-27", Some("CEST"), None, Some("22:41"))),
            "Current date: 2026-08-27 (Thursday), timezone CEST; marker written at 22:41",
            "no IANA name: the abbreviation carries the zone alone"
        );
        assert_eq!(
            line(&marker(
                "2026-08-27",
                None,
                Some("Europe/Berlin"),
                Some("22:41")
            )),
            "Current date: 2026-08-27 (Thursday), timezone Europe/Berlin; marker written at 22:41",
            "no abbreviation: the name carries the zone alone, unparenthesized"
        );
        assert_eq!(
            line(&marker("2026-08-27", None, None, Some("22:41"))),
            "Current date: 2026-08-27 (Thursday); marker written at 22:41",
            "no zone at all: the clause drops out, the minute stays"
        );
        assert_eq!(
            line(&marker("2026-08-27", None, None, None)),
            PRE_SLICE_LINE,
            "every pre-slice row renders as it always did"
        );
    }

    /// A row whose date does not parse keeps the bare-date degrade AND its
    /// other clauses: no clause depends on the date parsing.
    #[test]
    fn an_unparseable_date_keeps_the_bare_degrade_and_its_clauses() {
        assert_eq!(
            line(&marker(
                "not-a-date",
                Some("CEST"),
                Some("Europe/Berlin"),
                Some("22:41")
            )),
            "Current date: not-a-date, timezone CEST (Europe/Berlin); marker written at 22:41"
        );
        assert_eq!(
            line(&marker("not-a-date", None, None, None)),
            "Current date: not-a-date"
        );
    }

    /// A block whose payload never carried the fields at all — an older build
    /// reading a row it has no columns for, or any caller building a marker by
    /// hand — is the all-NULL case, not an empty clause.
    #[test]
    fn absent_fields_and_null_fields_are_the_same_line() {
        let mut fields = serde_json::Map::new();
        fields.insert("date".into(), Value::String("2026-08-27".into()));
        let bare = Block {
            id: 1,
            role: None,
            block_type: "date_marker".into(),
            created_at: String::new(),
            dispatch_anchor: None,
            fields,
        };
        assert_eq!(line(&bare), PRE_SLICE_LINE);
    }
}
