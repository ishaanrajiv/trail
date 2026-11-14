//! `seq`: every event, error and log's position in arrival order, and reading by it.

use super::*;
use crate::parquet_io::FileRows;
use std::collections::{BTreeSet, HashSet};
use trail_core::decl::Retention;

fn forever(_: &str) -> Retention {
    Retention::Forever
}

fn archive(store: &Store, now: i64) {
    let report = store.archive(Ts(now), &forever).unwrap();
    assert!(report.failures.is_empty(), "{:?}", report.failures);
}

/// One page of events after `after`, `limit` at a time.
fn page(store: &Store, after: i64, limit: usize) -> Tail<Event> {
    store
        .events_tail(&EventFilter {
            after_seq: Some(after),
            limit,
            ..EventFilter::default()
        })
        .unwrap()
}

/// A reader that remembers the last position it was given and asks again from there.
struct Reader {
    at: i64,
    seen: Vec<(String, String, i64)>,
}

impl Reader {
    fn new() -> Reader {
        Reader {
            at: 0,
            seen: Vec::new(),
        }
    }

    /// Reads until a page comes back empty; returns what was new.
    fn catch_up(&mut self, store: &Store, limit: usize) -> Vec<String> {
        let mut new = Vec::new();
        loop {
            let tail = page(store, self.at, limit);
            assert!(tail.next_seq >= self.at, "the position never goes back");
            let mut last = self.at;
            for e in &tail.rows {
                let seq = e.seq.unwrap();
                assert!(seq > last, "oldest seq first, each above the last");
                last = seq;
                self.seen.push((e.source.clone(), e.id.clone(), seq));
                new.push(e.id.clone());
            }
            let done = tail.rows.is_empty() && tail.next_seq == self.at;
            self.at = tail.next_seq;
            if done {
                return new;
            }
        }
    }

    fn assert_each_once(&self) {
        let keys: HashSet<(&str, &str)> = self
            .seen
            .iter()
            .map(|(s, i, _)| (s.as_str(), i.as_str()))
            .collect();
        assert_eq!(
            keys.len(),
            self.seen.len(),
            "a record was given twice: {:?}",
            self.seen
        );
    }
}

#[test]
fn seq_rises_in_commit_order_is_never_reused_and_survives_a_restart() {
    let dir = TempDir::new("seq");
    let config = dir.config();
    {
        let store = Store::open(&config).unwrap();
        store
            .write(&Batch {
                events: vec![
                    event("e1", "shop.order.placed", T0),
                    event("e2", "shop.order.placed", T0),
                ],
                errors: vec![error("x1", "fp", "boom", T0)],
                logs: vec![log("l1", Level::Info, "started", T0)],
                ..Batch::default()
            })
            .unwrap();
        // A duplicate takes no seq.
        write_events(
            &store,
            vec![
                event("e1", "shop.order.placed", T0),
                event("e3", "shop.order.placed", T0),
            ],
        );
        let seqs: Vec<Option<i64>> = all_events(&store).iter().rev().map(|e| e.seq).collect();
        assert_eq!(seqs, [Some(1), Some(2), Some(5)]);
        let errors = store.errors(&ErrorFilter::default()).unwrap();
        let logs = store.logs(&LogFilter::default()).unwrap();
        assert_eq!((errors[0].seq, logs[0].seq), (Some(3), Some(4)));
    }
    let store = Store::open(&config).unwrap();
    write_events(&store, vec![event("e4", "shop.order.placed", T0 + D)]);
    assert_eq!(all_events(&store)[0].seq, Some(6));

    // Rows that leave SQLite keep their seq, and the counter doesn't go back.
    archive(&store, T0 + 10 * D);
    assert_eq!(count(&store, "SELECT count(*) FROM events"), 0, "evicted");
    let seqs: BTreeSet<i64> = all_events(&store).iter().filter_map(|e| e.seq).collect();
    assert_eq!(seqs, BTreeSet::from([1, 2, 5, 6]));
    write_events(&store, vec![event("e5", "shop.order.placed", T0 + 10 * D)]);
    assert_eq!(all_events(&store)[0].seq, Some(7));
}

#[test]
fn a_reader_is_given_every_record_stored_after_its_position_once() {
    let (_dir, store) = store_with("tail", |c| c.hot_days = 1);
    // 2026-11-10 gets a file and leaves SQLite; 2026-11-17 is today.
    let today = T0 + 7 * D;
    write_events(
        &store,
        vec![
            event("old-1", "shop.order.placed", T0 + H),
            event("old-2", "shop.order.placed", T0 + 2 * H),
        ],
    );
    archive(&store, today);
    assert_eq!(count(&store, "SELECT count(*) FROM events"), 0);
    write_events(
        &store,
        (0..5)
            .map(|i| event(&format!("hot-{i}"), "shop.order.placed", today + i * M))
            .collect(),
    );
    let mut reader = Reader::new();
    let first = reader.catch_up(&store, 2);
    assert_eq!(
        first,
        ["old-1", "old-2", "hot-0", "hot-1", "hot-2", "hot-3", "hot-4"]
    );
    assert_eq!(reader.at, 7);

    // A record with a week-old ts, whose day already has a file and has left SQLite.
    write_events(&store, vec![event("late-1", "shop.order.placed", T0 + 3 * H)]);
    assert_eq!(reader.catch_up(&store, 2), ["late-1"]);

    // Another for the same day; then the day's file is rewritten with both, and they leave
    // SQLite. The reader, which has late-1 already, is given late-2 alone, from the file.
    write_events(&store, vec![event("late-2", "shop.order.placed", T0 + 4 * H)]);
    archive(&store, today);
    assert_eq!(
        count(&store, "SELECT count(*) FROM events WHERE ts < 1794355200000"),
        0
    );
    assert_eq!(reader.catch_up(&store, 2), ["late-2"]);
    assert_eq!(reader.catch_up(&store, 2), Vec::<String>::new());

    // A resend of a filed record is stored until the next archive drops it, but not given.
    let written = write_events(&store, vec![event("old-1", "shop.order.placed", T0 + H)]);
    assert_eq!(written.events, [true]);
    assert_eq!(reader.catch_up(&store, 2), Vec::<String>::new());
    assert_eq!(reader.at, 10, "the position moves past what was skipped");

    // A late record for a filed day still inside the hot window: in SQLite and, after the
    // rewrite, in the file too, under the same seq. Given once.
    let yesterday = today - D;
    write_events(&store, vec![event("y-1", "shop.order.placed", yesterday + H)]);
    archive(&store, today);
    write_events(&store, vec![event("y-2", "shop.order.placed", yesterday + 2 * H)]);
    assert_eq!(reader.catch_up(&store, 1), ["y-1", "y-2"]);
    archive(&store, today);
    assert_eq!(reader.catch_up(&store, 1), Vec::<String>::new());
    reader.assert_each_once();

    // Filters combine with it.
    let only = store
        .events_tail(&EventFilter {
            after_seq: Some(0),
            name: Some("shop.order.*".to_string()),
            start: Some(Ts(T0)),
            end: Some(Ts(T0 + D)),
            ..EventFilter::default()
        })
        .unwrap();
    let ids: Vec<&str> = only.rows.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(ids, ["old-1", "old-2", "late-1", "late-2"]);
}

#[test]
fn errors_and_logs_are_read_by_seq_too() {
    let (_dir, store) = store_with("tail-errors", |c| c.hot_days = 1);
    store
        .write(&Batch {
            errors: vec![error("x1", "fp", "boom", T0), error("x2", "fp", "boom", T0 + H)],
            logs: vec![
                log("l1", Level::Info, "a", T0),
                log("l2", Level::Warn, "b", T0 + H),
            ],
            ..Batch::default()
        })
        .unwrap();
    archive(&store, T0 + 7 * D);
    store
        .write(&Batch {
            errors: vec![error("x3", "fp", "boom", T0 + 2 * H)],
            logs: vec![log("l3", Level::Warn, "c", T0 + 2 * H)],
            ..Batch::default()
        })
        .unwrap();
    let errors = store
        .errors_tail(&ErrorFilter {
            after_seq: Some(1),
            ..ErrorFilter::default()
        })
        .unwrap();
    let ids: Vec<&str> = errors.rows.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(ids, ["x2", "x3"]);
    assert_eq!(errors.next_seq, 5);
    let logs = store
        .logs_tail(&LogFilter {
            after_seq: Some(0),
            min_level: Some(Level::Warn),
            ..LogFilter::default()
        })
        .unwrap();
    let ids: Vec<&str> = logs.rows.iter().map(|l| l.id.as_str()).collect();
    assert_eq!(ids, ["l2", "l3"]);
}

/// A small deterministic generator, so the interleaving is the same every run.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % n
    }
}

#[test]
fn writes_archives_and_reads_interleaved_give_each_record_once() {
    let (_dir, store) = store_with("tail-mixed", |c| c.hot_days = 1);
    let mut rng = Lcg(7);
    let mut reader = Reader::new();
    let mut stored: HashSet<(String, String)> = HashSet::new();
    let mut now = T0 + 3 * D;
    let mut n = 0;
    let mut resends_stored = 0;
    for _ in 0..300 {
        match rng.next(10) {
            0..=5 => {
                // A few records, at any age back to 2026-11-10, some resends, two sources.
                let mut batch = Vec::new();
                for _ in 0..=rng.next(4) {
                    let resend = n > 0 && rng.next(4) == 0;
                    let id = if resend { rng.next(n) } else { n };
                    if !resend {
                        n += 1;
                    }
                    let ts = T0 + i64::try_from(id).unwrap() * 37 * M % (now - T0);
                    let mut e = event(&format!("r{id}"), "shop.order.placed", ts);
                    e.source = if id % 3 == 0 { "till" } else { "shop" }.to_string();
                    batch.push(e);
                }
                let mut fresh = Vec::new();
                for e in &batch {
                    fresh.push(stored.insert((e.source.clone(), e.id.clone())));
                }
                let written = write_events(&store, batch);
                // A resend stored anyway: its first copy's day has left SQLite.
                resends_stored += written
                    .events
                    .iter()
                    .zip(&fresh)
                    .filter(|(w, f)| **w && !**f)
                    .count();
            }
            6 => {
                now += i64::try_from(rng.next(3)).unwrap() * D / 2;
                archive(&store, now);
            }
            _ => {
                let limit = usize::try_from(rng.next(5) + 1).unwrap();
                reader.catch_up(&store, limit);
            }
        }
    }
    archive(&store, now + 3 * D);
    reader.catch_up(&store, 3);
    reader.assert_each_once();
    assert!(
        resends_stored > 0,
        "the run never stored a resend of a filed record"
    );
    assert!(
        count(&store, "SELECT count(*) FROM archive_days") > 3,
        "the run filed few days"
    );
    let seen: HashSet<(String, String)> = reader
        .seen
        .iter()
        .map(|(s, i, _)| (s.clone(), i.clone()))
        .collect();
    assert_eq!(seen, stored, "every record stored was given");
}

#[test]
fn an_upgrade_numbers_the_rows_in_sqlite_by_arrival_and_refiles_hot_days() {
    use super::formats::{Col, write_raw};
    let dir = TempDir::new("tail-upgrade");
    let config = dir.config();
    super::migrate::v1_folder(&config);
    // 2026-11-20 (UTC) already has a file, written by version 1: no seq column.
    let day = T0 + 10 * D;
    let path = config.data_dir.join("archive/events/2026-11-20.parquet");
    write_raw(
        &path,
        super::formats::V1_EVENTS,
        vec![
            Col::Str(vec!["e-1", "e-2", "e-3"]),
            Col::Str(vec!["shop.order.placed"; 3]),
            Col::I64(vec![day, day + M, day + 2 * M]),
            Col::I64(vec![day + 5, day + M + 5, day + 2 * M + 5]),
            Col::Str(vec!["shop"; 3]),
            Col::OptStr(vec![None; 3]),
            Col::OptStr(vec![None; 3]),
            Col::OptStr(vec![None; 3]),
            Col::OptStr(vec![None; 3]),
            Col::OptStr(vec![None; 3]),
            Col::Str(vec!["{}"; 3]),
            Col::OptStr(vec![None; 3]),
        ],
    );
    {
        let conn = rusqlite::Connection::open(config.data_dir.join("trail.db")).unwrap();
        conn.execute(
            "INSERT INTO archive_days (store, class, day, rows, bytes) VALUES ('events', 'events', '2026-11-20', 3, 1)",
            [],
        )
        .unwrap();
    }
    let store = Store::open(&config).unwrap();
    // The log and the error arrived first (same time; by id), then the three events.
    let logs = store.logs(&LogFilter::default()).unwrap();
    let errors = store.errors(&ErrorFilter::default()).unwrap();
    assert_eq!((logs[0].seq, errors[0].seq), (Some(1), Some(2)));
    let seqs: Vec<Option<i64>> = all_events(&store).iter().rev().map(|e| e.seq).collect();
    assert_eq!(seqs, [Some(3), Some(4), Some(5)]);
    write_events(&store, vec![event("new", "shop.order.placed", day + H)]);
    assert_eq!(all_events(&store)[0].seq, Some(6));
    assert_eq!(
        count(
            &store,
            "SELECT count(*) FROM archive_late WHERE day = '2026-11-20'"
        ),
        1
    );

    // The next archive rewrites the file, giving its rows the seq SQLite gave them, before the
    // rows leave SQLite.
    archive(&store, day + 5 * D);
    assert_eq!(count(&store, "SELECT count(*) FROM events"), 0);
    let mut rows = FileRows::<Event>::open(&path, None).unwrap();
    let mut filed = Vec::new();
    while let Some(e) = rows.next_row().unwrap() {
        filed.push((e.id, e.seq));
    }
    assert_eq!(
        filed,
        [
            ("e-1".to_string(), Some(3)),
            ("e-2".to_string(), Some(4)),
            ("e-3".to_string(), Some(5)),
            ("new".to_string(), Some(6))
        ]
    );
    let mut reader = Reader::new();
    assert_eq!(reader.catch_up(&store, 2), ["e-1", "e-2", "e-3", "new"]);
}

#[test]
fn a_page_that_skips_resends_stops_where_sqlite_stopped() {
    use super::formats::{Col, write_raw};
    let (_dir, store) = store("tail-horizon");
    // 2026-11-10 is filed and has left SQLite. Its file holds r1 and r2 (seq 1, 2) and two
    // records that arrived later, f6 and f8. SQLite holds resends of r1 and r2 (seq 3, 4) for
    // that day, and h5 and h7 for a later day.
    let mut schema = super::formats::V1_EVENTS
        .trim_end()
        .trim_end_matches('}')
        .to_string();
    schema.push_str("optional int64 seq; }");
    let path = store.day_path("events", "2026-11-10".parse().unwrap());
    let ids = ["f6", "f8", "r1", "r2"];
    write_raw(
        &path,
        &schema,
        vec![
            Col::Str(ids.to_vec()),
            Col::Str(vec!["shop.order.placed"; 4]),
            Col::I64(vec![T0 + H; 4]),
            Col::I64(vec![T0 + H; 4]),
            Col::Str(vec!["shop"; 4]),
            Col::OptStr(vec![None; 4]),
            Col::OptStr(vec![None; 4]),
            Col::OptStr(vec![None; 4]),
            Col::OptStr(vec![None; 4]),
            Col::OptStr(vec![None; 4]),
            Col::Str(vec!["{}"; 4]),
            Col::OptStr(vec![None; 4]),
            Col::OptI64(vec![Some(6), Some(8), Some(1), Some(2)]),
        ],
    );
    super::formats::adopt(&store, "events", "events", "2026-11-10", 4, T0 + D);
    {
        let w = store.writer().unwrap();
        w.conn.execute("UPDATE archive_days SET max_seq = 8", []).unwrap();
        w.conn
            .execute(
                "INSERT INTO marks (key, value) VALUES ('seq', 8) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                [],
            )
            .unwrap();
        for (id, ts, seq) in [
            ("r1", T0 + H, 3),
            ("r2", T0 + H, 4),
            ("h5", T0 + 3 * D, 5),
            ("h7", T0 + 3 * D, 7),
        ] {
            w.conn
                .execute(
                    "INSERT INTO events (id, name, ts, received_at, source, props, seq) VALUES (?1, 'shop.order.placed', ?2, ?2, 'shop', '{}', ?3)",
                    (id, ts, seq),
                )
                .unwrap();
        }
    }
    let mut reader = Reader {
        at: 2,
        seen: Vec::new(),
    };
    // Both of SQLite's first two rows are skipped: the page must not run on into the file's
    // rows past them, or h5 and h7, which SQLite hadn't read yet, would be jumped over.
    let first = page(&store, 2, 2);
    assert!(first.rows.is_empty());
    assert_eq!(first.next_seq, 4);
    assert_eq!(reader.catch_up(&store, 2), ["h5", "f6", "h7", "f8"]);
}

#[test]
fn a_file_rewritten_after_the_read_began_gives_nothing_past_what_the_read_saw() {
    use super::formats::{Col, write_raw};
    let (_dir, store) = store("tail-ceiling");
    // What a reader finds when ingest and an archive run happen between its read of SQLite and
    // its read of a day file. Its view of SQLite ends at seq 9 and holds h5. After that view was
    // taken, a10 was stored for today (SQLite, not in the view) and b11 for 2026-11-10, whose
    // file the archive then rewrote: the file now holds f1 and b11.
    let mut schema = super::formats::V1_EVENTS
        .trim_end()
        .trim_end_matches('}')
        .to_string();
    schema.push_str("optional int64 seq; }");
    let path = store.day_path("events", "2026-11-10".parse().unwrap());
    write_raw(
        &path,
        &schema,
        vec![
            Col::Str(vec!["b11", "f1"]),
            Col::Str(vec!["shop.order.placed"; 2]),
            Col::I64(vec![T0 + H; 2]),
            Col::I64(vec![T0 + H; 2]),
            Col::Str(vec!["shop"; 2]),
            Col::OptStr(vec![None; 2]),
            Col::OptStr(vec![None; 2]),
            Col::OptStr(vec![None; 2]),
            Col::OptStr(vec![None; 2]),
            Col::OptStr(vec![None; 2]),
            Col::Str(vec!["{}"; 2]),
            Col::OptStr(vec![None; 2]),
            Col::OptI64(vec![Some(11), Some(1)]),
        ],
    );
    super::formats::adopt(&store, "events", "events", "2026-11-10", 2, T0 + D);
    {
        let w = store.writer().unwrap();
        w.conn.execute("UPDATE archive_days SET max_seq = 1", []).unwrap();
        w.conn
            .execute(
                "INSERT INTO marks (key, value) VALUES ('seq', 9) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                [],
            )
            .unwrap();
        w.conn
            .execute(
                "INSERT INTO events (id, name, ts, received_at, source, props, seq) VALUES ('h5', 'shop.order.placed', ?1, ?1, 'shop', '{}', 5)",
                [T0 + 3 * D],
            )
            .unwrap();
    }
    // b11 is past everything the read saw in SQLite. Taking it would move the position to 11
    // and a10, which this read could not see, would never be given.
    let tail = page(&store, 0, 10);
    let ids: Vec<&str> = tail.rows.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(ids, ["f1", "h5"]);
    assert!(
        tail.next_seq <= 9,
        "next_seq {} is past what the read saw",
        tail.next_seq
    );
}

#[test]
fn a_resend_of_a_record_filed_before_seq_existed_never_comes_back_as_new() {
    use super::formats::{adopt, v1_events_file};
    let (_dir, store) = store("tail-resend-v1");
    // 2026-11-10 was filed and left SQLite before seq existed: old-1 and old-2 have none.
    let path = store.day_path("events", "2026-11-10".parse().unwrap());
    v1_events_file(&path);
    adopt(&store, "events", "events", "2026-11-10", 2, T0 + D);
    let mut reader = Reader::new();
    assert!(reader.catch_up(&store, 10).is_empty());

    // Its sender sends old-1 again. SQLite no longer has it, so the resend is stored, with a seq.
    let mut again = event("old-1", "shop.order.placed", T0 + H);
    again.source = "shop".into();
    write_events(&store, vec![again]);
    // Read before the archive merges the day, and after: either way it is a resend, not news.
    assert!(reader.catch_up(&store, 10).is_empty());
    archive(&store, T0 + 5 * D);
    assert!(reader.catch_up(&store, 10).is_empty());
    let mut fresh = Reader::new();
    assert!(fresh.catch_up(&store, 10).is_empty());
    let mut rows = FileRows::<Event>::open_if_exists(&path, None).unwrap().unwrap();
    let mut filed = 0;
    while let Some(row) = rows.next_row().unwrap() {
        assert_eq!(row.seq, None, "{} was filed before seq existed", row.id);
        filed += 1;
    }
    assert_eq!(filed, 2);
}
