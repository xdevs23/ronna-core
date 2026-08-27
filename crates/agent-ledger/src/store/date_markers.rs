//! The date-marker insert seam: change detection, not a creation special case.
//!
//! The user-voiced insert paths call [`ensure_date_marker`] inside their own
//! transaction, BEFORE the user blocks — the marker rides the same atomic
//! append as the message that owes the turn, so the wire never carries a date
//! the ledger cannot replay.
//!
//! WHAT a marker records is one value, [`DateStamp`], built by one constructor
//! and passed to every seam that trips one. A widening argument list at five
//! call sites would be the same decision written five times.

use rusqlite::{Connection, OptionalExtension, params};

use super::StoreError;
use super::messages::insert_block;

/// Everything a fresh date marker records: the machine's local date, the
/// timezone as far as this platform answers for it, and the wall-clock minute
/// the marker was written.
///
/// Every part but the date is independently nullable, and each has its own
/// source: a NULL says a source answered nothing, which is honest, where a
/// guessed value would not be.
#[derive(Debug, Clone)]
pub(crate) struct DateStamp {
    /// The local date, in `YYYY-MM-DD` form.
    pub(crate) date: String,
    /// The platform's own zone abbreviation (`CEST`) — and NULL on every row
    /// production writes today. [`local_tz_abbrev`] is the one statement of
    /// why: the source is unreachable from this crate, so only a hand-built
    /// stamp ever carries a value here.
    pub(crate) tz_abbrev: Option<String>,
    /// The IANA zone name (`Europe/Berlin`), when the resolver answers.
    pub(crate) tz_name: Option<String>,
    /// The wall-clock `HH:MM` the marker was written at — the ledger-true
    /// label, never a claim about the time a reader reads it.
    pub(crate) written_at: Option<String>,
}

impl DateStamp {
    /// The stamp production writes: now, in the machine's local timezone.
    ///
    /// One instant answers every part, so a marker written in the last
    /// millisecond of a day cannot carry tomorrow's date beside today's
    /// minute.
    pub(crate) fn now_local() -> Self {
        let now = chrono::Local::now();
        Self {
            date: now.format("%Y-%m-%d").to_string(),
            tz_abbrev: local_tz_abbrev(),
            tz_name: iana_time_zone::get_timezone().ok(),
            written_at: Some(now.format("%H:%M").to_string()),
        }
    }
}

#[cfg(test)]
impl DateStamp {
    /// A date-only stamp: the shape every marker carried before the zone
    /// columns existed, and what a test driving a midnight crossing means.
    pub(crate) fn date_only(date: &str) -> Self {
        Self {
            date: date.to_owned(),
            tz_abbrev: None,
            tz_name: None,
            written_at: None,
        }
    }

    /// A stamp carrying a zone name, for driving the change detection's zone
    /// rule and its NULL cases.
    pub(crate) fn zoned(date: &str, tz_name: Option<&str>) -> Self {
        Self {
            tz_name: tz_name.map(str::to_owned),
            ..Self::date_only(date)
        }
    }
}

/// The platform's own zone abbreviation — `CEST` — or `None` when nothing
/// here can answer for it.
///
/// It answers `None` today, and that is a constraint of this crate rather than
/// a choice. The abbreviation lives in `localtime_r`'s `tm_zone`, reachable
/// only through an unsafe FFI call, and this workspace FORBIDS unsafe code
/// (`workspace.lints.rust`); no crate in the dependency tree wraps that call
/// safely. chrono is not a second source: its `%Z` prints the numeric offset
/// (`+02:00`) here, never an abbreviation. Deriving one from the IANA name
/// means carrying a whole timezone database to recompute what one C struct
/// field already holds.
///
/// So the abbreviation column is written NULL, which is exactly the "the
/// platform answers nothing usable" case the marker's NULL rule is built for:
/// the abbreviation clause drops out of the projected line and the IANA name
/// carries the zone. The day a safe reader of `tm_zone` is available, it lands
/// here and nothing else moves.
fn local_tz_abbrev() -> Option<String> {
    None
}

/// Whether the stored and current zone names KNOWABLY differ.
///
/// A NULL on either side is never a difference. Two traps live here: an
/// upgraded store, whose existing markers carry no name at all, must not write
/// a same-day marker per message until the next natural date change; and a
/// resolver flapping between a name and nothing must not write one either.
fn zone_knowably_changed(stored: Option<&str>, current: Option<&str>) -> bool {
    matches!((stored, current), (Some(stored), Some(current)) if stored != current)
}

/// Compare `stamp` against the LATEST date marker in the conversation's ledger
/// (junction order); differ — or none, which the first message trips for free —
/// insert a fresh marker and return its id. Unchanged appends insert nothing,
/// and a conversation nobody writes to never gets one.
///
/// "Differ" is the date, or a zone name that knowably changed
/// ([`zone_knowably_changed`]). The abbreviation is deliberately not compared:
/// it turns over twice a year at the same wall-clock midnight the date turns
/// over at, and the IANA name is the zone's stable identity.
pub(super) fn ensure_date_marker(
    conn: &Connection,
    conversation_id: i64,
    stamp: &DateStamp,
) -> Result<Option<i64>, StoreError> {
    let latest: Option<(String, Option<String>)> = conn
        .query_row(
            "SELECT bdm.date, bdm.tz_name FROM block_date_marker bdm
             JOIN conversation_blocks cb ON cb.block_id = bdm.block_id
             WHERE cb.conversation_id = ?1
             ORDER BY cb.id DESC LIMIT 1",
            [conversation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;

    let unchanged = latest.is_some_and(|(date, tz_name)| {
        date == stamp.date && !zone_knowably_changed(tz_name.as_deref(), stamp.tz_name.as_deref())
    });
    if unchanged {
        return Ok(None);
    }

    let block_id = insert_block(conn, conversation_id, "date_marker")?;
    conn.execute(
        "INSERT INTO block_date_marker (block_id, date, tz_abbrev, tz_name, written_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            block_id,
            stamp.date,
            stamp.tz_abbrev,
            stamp.tz_name,
            stamp.written_at
        ],
    )?;
    Ok(Some(block_id))
}

#[cfg(test)]
mod tests {
    use super::DateStamp;
    use crate::store::Store;
    use crate::types::InputBlock;

    async fn fixture() -> (Store, i64) {
        let store = Store::in_memory().unwrap();
        let conv = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();
        (store, conv)
    }

    fn text(content: &str) -> Vec<InputBlock> {
        vec![InputBlock::Text {
            content: content.into(),
        }]
    }

    async fn markers(store: &Store, conv: i64) -> Vec<(usize, String)> {
        store
            .list_blocks(conv)
            .await
            .unwrap()
            .iter()
            .enumerate()
            .filter(|(_, b)| b.block_type == "date_marker")
            .map(|(i, b)| (i, b.fields["date"].as_str().unwrap().to_string()))
            .collect()
    }

    /// The abbreviation the slice asked for is NOT written, and this pins the
    /// gap rather than leaving it to be noticed in production.
    ///
    /// `docs/slices/13-dated-consumer-appends.md` decided the abbreviation
    /// comes from `localtime_r`'s `tm_zone`, and its contract repeats it. That
    /// source is unreachable from this crate: `tm_zone` is an unsafe FFI read,
    /// the workspace FORBIDS unsafe code (a `forbid` no module can lift), and
    /// no dependency in the tree wraps the call safely. So every marker
    /// production writes carries NULL there and the projected line drops the
    /// clause — the honest degrade, not the specified behaviour. The
    /// disagreement is a spec decision, not a code one; until it is taken,
    /// THIS test is the record, and it is what fails the day a safe reader
    /// lands and the contract can be met.
    #[test]
    fn the_abbreviation_has_no_reachable_source_and_is_pinned_null() {
        assert_eq!(
            super::local_tz_abbrev(),
            None,
            "no safe reader of tm_zone exists in this crate"
        );
        assert_eq!(
            DateStamp::now_local().tz_abbrev,
            None,
            "so the stamp production writes carries no abbreviation"
        );
    }

    /// The first user message trips "no marker yet is not today" for free: one
    /// marker, positioned BEFORE the user blocks in the ledger.
    #[tokio::test]
    async fn first_message_inserts_the_marker_before_the_user_blocks() {
        let (store, conv) = fixture().await;
        store.insert_user_blocks(conv, text("hello")).await.unwrap();

        let blocks = store.list_blocks(conv).await.unwrap();
        let types: Vec<&str> = blocks.iter().map(|b| b.block_type.as_str()).collect();
        assert_eq!(
            types,
            vec!["date_marker", "text"],
            "the marker precedes the message"
        );
        assert_eq!(
            blocks[0].fields["date"].as_str().unwrap(),
            super::DateStamp::now_local().date,
            "the marker carries today's local date"
        );
    }

    /// A same-day second message appends no fresh marker — change detection,
    /// not per-message decoration.
    #[tokio::test]
    async fn same_day_second_message_inserts_no_marker() {
        let (store, conv) = fixture().await;
        store.insert_user_blocks(conv, text("one")).await.unwrap();
        store.insert_user_blocks(conv, text("two")).await.unwrap();

        assert_eq!(markers(&store, conv).await.len(), 1);
    }

    /// A conversation spanning midnight gets a fresh marker on the next user
    /// message — driven through the injectable-date seam.
    #[tokio::test]
    async fn changed_date_inserts_a_fresh_marker() {
        let (store, conv) = fixture().await;
        store
            .insert_user_blocks_dated(conv, text("yesterday"), DateStamp::date_only("2026-07-11"))
            .await
            .unwrap();
        store
            .insert_user_blocks_dated(conv, text("today"), DateStamp::date_only("2026-07-12"))
            .await
            .unwrap();

        let markers = markers(&store, conv).await;
        assert_eq!(
            markers.len(),
            2,
            "midnight crossed — a fresh marker rides the new message"
        );
        assert_eq!(markers[0].1, "2026-07-11");
        assert_eq!(markers[1].1, "2026-07-12");

        // The fresh marker sits immediately before its message.
        let blocks = store.list_blocks(conv).await.unwrap();
        assert_eq!(
            blocks[markers[1].0 + 1].fields["content"].as_str().unwrap(),
            "today"
        );
    }

    /// The promote path — the other insert seam — rides the same change
    /// detection inside the promote transaction.
    #[tokio::test]
    async fn promote_draft_inserts_the_marker_once() {
        let (store, conv) = fixture().await;
        store.save_draft(conv, text("drafted")).await.unwrap();
        store.promote_draft(conv).await.unwrap();

        let blocks = store.list_blocks(conv).await.unwrap();
        let types: Vec<&str> = blocks.iter().map(|b| b.block_type.as_str()).collect();
        assert_eq!(types, vec!["date_marker", "text"]);

        store.save_draft(conv, text("again")).await.unwrap();
        store.promote_draft(conv).await.unwrap();
        assert_eq!(
            markers(&store, conv).await.len(),
            1,
            "same-day promote adds no marker"
        );
    }

    /// The zone rule's positive case: the same day, both markers knowing their
    /// zone, and the zone changed — a machine carried across a border, or a
    /// host reconfigured — writes a fresh marker, because the line it projects
    /// is no longer true.
    #[tokio::test]
    async fn a_knowable_zone_change_on_the_same_day_inserts_a_fresh_marker() {
        let (store, conv) = fixture().await;
        store
            .insert_user_blocks_dated(
                conv,
                text("in berlin"),
                DateStamp::zoned("2026-07-11", Some("Europe/Berlin")),
            )
            .await
            .unwrap();
        store
            .insert_user_blocks_dated(
                conv,
                text("in lisbon"),
                DateStamp::zoned("2026-07-11", Some("Europe/Lisbon")),
            )
            .await
            .unwrap();

        let markers = markers(&store, conv).await;
        assert_eq!(markers.len(), 2, "the zone changed knowably");
        assert_eq!(markers[0].1, "2026-07-11");
        assert_eq!(markers[1].1, "2026-07-11", "same day, second marker");
    }

    /// The zone rule's NULL cases, both directions. A stored marker that knows
    /// no zone (every row an upgraded store carries) against a stamp that
    /// does, and a stored zone against a stamp whose resolver just failed:
    /// neither is a difference. Otherwise an upgraded store writes a marker
    /// storm on its first day, and a flapping resolver writes one per message.
    #[tokio::test]
    async fn a_null_zone_on_either_side_is_never_a_change() {
        let (store, conv) = fixture().await;
        store
            .insert_user_blocks_dated(
                conv,
                text("upgraded row"),
                DateStamp::date_only("2026-07-11"),
            )
            .await
            .unwrap();
        store
            .insert_user_blocks_dated(
                conv,
                text("zone now known"),
                DateStamp::zoned("2026-07-11", Some("Europe/Berlin")),
            )
            .await
            .unwrap();
        assert_eq!(
            markers(&store, conv).await.len(),
            1,
            "stored NULL against a present name is not a change"
        );

        let (store, conv) = fixture().await;
        store
            .insert_user_blocks_dated(
                conv,
                text("zone known"),
                DateStamp::zoned("2026-07-11", Some("Europe/Berlin")),
            )
            .await
            .unwrap();
        store
            .insert_user_blocks_dated(
                conv,
                text("resolver failed"),
                DateStamp::date_only("2026-07-11"),
            )
            .await
            .unwrap();
        assert_eq!(
            markers(&store, conv).await.len(),
            1,
            "a present name against a failed lookup is not a change"
        );
    }

    /// A date change is a change whatever the zones say — including when the
    /// zone went from known to unknown across it.
    #[tokio::test]
    async fn a_date_change_inserts_even_when_the_zone_lookup_stopped_answering() {
        let (store, conv) = fixture().await;
        store
            .insert_user_blocks_dated(
                conv,
                text("yesterday"),
                DateStamp::zoned("2026-07-11", Some("Europe/Berlin")),
            )
            .await
            .unwrap();
        store
            .insert_user_blocks_dated(conv, text("today"), DateStamp::date_only("2026-07-12"))
            .await
            .unwrap();

        assert_eq!(markers(&store, conv).await.len(), 2);
    }

    /// Replay, end to end and through the real fold: a marker written with
    /// every part, read back through the block query, parsed by the kind and
    /// grouped by the projection, speaks the full line. This is the pin that
    /// fails if the read path is widened only halfway — the columns written
    /// and never selected.
    #[tokio::test]
    async fn a_written_marker_replays_through_the_read_path_and_the_projection() {
        let (store, conv) = fixture().await;
        let stamp = DateStamp {
            date: "2026-08-27".into(),
            tz_abbrev: Some("CEST".into()),
            tz_name: Some("Europe/Berlin".into()),
            written_at: Some("22:41".into()),
        };
        store
            .insert_user_blocks_dated(conv, text("hello"), stamp)
            .await
            .unwrap();

        let blocks = store.list_blocks(conv).await.unwrap();
        let messages =
            crate::providers::render::blocks_to_messages::<crate::agency::BlockKind>(&blocks);
        let system = serde_json::to_value(&messages[0]).unwrap();
        assert_eq!(
            system["content"],
            serde_json::json!(
                "Current date: 2026-08-27 (Thursday), timezone CEST (Europe/Berlin); \
                 marker written at 22:41"
            ),
            "what was written is what the model is told"
        );
    }

    /// The pre-slice shape survives the round trip untouched: a date-only
    /// marker reads back with the three fields null and projects the line it
    /// projected before the columns existed.
    #[tokio::test]
    async fn a_date_only_marker_replays_as_it_always_did() {
        let (store, conv) = fixture().await;
        store
            .insert_user_blocks_dated(conv, text("hello"), DateStamp::date_only("2026-07-12"))
            .await
            .unwrap();

        let blocks = store.list_blocks(conv).await.unwrap();
        let marker = &blocks[0];
        assert_eq!(marker.fields["tz_abbrev"], serde_json::Value::Null);
        assert_eq!(marker.fields["tz_name"], serde_json::Value::Null);
        assert_eq!(marker.fields["written_at"], serde_json::Value::Null);

        let messages =
            crate::providers::render::blocks_to_messages::<crate::agency::BlockKind>(&blocks);
        let system = serde_json::to_value(&messages[0]).unwrap();
        assert_eq!(
            system["content"],
            serde_json::json!("Current date: 2026-07-12 (Sunday)")
        );
    }
}
