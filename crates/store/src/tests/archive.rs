use super::*;
use crate::parquet_io::{FileRows, SampleRow};
use std::collections::HashSet;
use trail_core::decl::Retention;
use trail_core::time::Zone;

fn forever(_: &str) -> Retention {
    Retention::Forever
}

fn archive(store: &Store, now: i64) -> ArchiveReport {
    let report = store.archive(Ts(now), &forever).unwrap();
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    report
}

fn file(store: &Store, class: &str, day: &str) -> std::path::PathBuf {
    store
        .data_dir()
        .join("archive")
        .join(class)
        .join(format!("{day}.parquet"))
}

fn read_file<T: crate::parquet_io::ArchRow>(path: &std::path::Path) -> Vec<T> {
    let mut rows = FileRows::<T>::open(path, None).unwrap();
    let mut out = Vec::new();
    while let Some(row) = rows.next_row().unwrap() {
        out.push(row);
    }
    out
}

fn sqlite_rows(store: &Store, table: &str, lo: i64, hi: i64) -> i64 {
    count(
        store,
        &format!("SELECT count(*) FROM {table} WHERE ts >= {lo} AND ts < {hi}"),
    )
}

/// Three events on each of 2026-11-10 to 13 (UTC), two names.
fn four_days(store: &Store) {
    let mut events = Vec::new();
    for day in 0..4 {
        for i in 0..3 {
            let name = if i % 2 == 0 {
                "shop.order.placed"
            } else {
                "shop.page.viewed"
            };
            events.push(event(&format!("d{}-{i}", 10 + day), name, T0 + day * D + i * H));
        }
    }
    write_events(store, events);
}

fn ids(events: &[Event]) -> Vec<String> {
    events.iter().map(|e| e.id.clone()).collect()
}

fn assert_newest_first_once(events: &[Event]) {
    let unique: HashSet<&str> = events.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(
        unique.len(),
        events.len(),
        "an event came back twice: {:?}",
        ids(events)
    );
    assert!(
        events
            .windows(2)
            .all(|w| (w[0].ts, &w[0].id) > (w[1].ts, &w[1].id)),
        "{:?}",
        ids(events)
    );
}

#[test]
fn a_finished_day_gets_a_file_and_its_rows_leave_after_the_hot_window() {
    let (_dir, store) = store("day-file");
    four_days(&store);
    // Today is the 11th: the 10th is finished and in the hot window.
    let report = archive(&store, T0 + D + 2 * H);
    assert_eq!(report.written.len(), 1);
    let f = &report.written[0];
    assert_eq!(
        (f.class.as_str(), f.day.to_string().as_str(), f.rows),
        ("events", "2026-11-10", 3)
    );
    assert!(file(&store, "events", "2026-11-10").exists());
    assert_eq!(report.evicted, 0);
    assert_eq!(sqlite_rows(&store, "events", T0, T0 + D), 3, "still hot");
    assert!(archive(&store, T0 + D + 3 * H).written.is_empty(), "written once");
    // Today is the 13th: the 11th and 12th are hot, the 10th leaves.
    let report = archive(&store, T0 + 3 * D + H);
    assert_eq!(
        report
            .written
            .iter()
            .map(|f| f.day.to_string())
            .collect::<Vec<_>>(),
        ["2026-11-11", "2026-11-12"]
    );
    assert_eq!(report.evicted, 3);
    assert_eq!(sqlite_rows(&store, "events", T0, T0 + D), 0);
    assert_eq!(sqlite_rows(&store, "events", T0 + D, T0 + 4 * D), 9);
    let stats = store.stats().unwrap();
    assert_eq!((stats.stores[0].rows, stats.stores[0].files), (9, 3));
    assert!(stats.stores[0].file_bytes > 0);
    // Today is never in a file.
    assert!(!file(&store, "events", "2026-11-13").exists());
}

#[test]
fn a_query_across_hot_and_archived_days_returns_each_event_once_in_order() {
    let (_dir, store) = store("span");
    four_days(&store);
    archive(&store, T0 + 3 * D + H);
    let all = all_events(&store);
    assert_eq!(all.len(), 12);
    assert_newest_first_once(&all);
    assert_eq!(
        all.last().unwrap().id,
        "d10-0",
        "the archived day is read from its file"
    );
    // Paging through the boundary gives the same list.
    let mut paged = Vec::new();
    let mut before = None;
    loop {
        let page = store
            .events(&EventFilter {
                limit: 5,
                before: before.clone(),
                ..Default::default()
            })
            .unwrap();
        if page.is_empty() {
            break;
        }
        before = page.last().map(|e| (e.ts, e.id.clone()));
        paged.extend(page);
    }
    assert_eq!(ids(&paged), ids(&all));
    let viewed = store
        .events(&EventFilter {
            name: Some("shop.page.viewed".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(ids(&viewed), ["d13-1", "d12-1", "d11-1", "d10-1"]);
    let window = store
        .events(&EventFilter {
            start: Some(Ts(T0 + H)),
            end: Some(Ts(T0 + D + H)),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(ids(&window), ["d11-0", "d10-2", "d10-1"]);
    // A scan visits every event of a name once.
    let mut seen = Vec::new();
    let n = store
        .scan_events("shop.order.placed", Ts(T0), Ts(T0 + 4 * D), &mut |e| {
            seen.push(e.id)
        })
        .unwrap();
    assert_eq!(n, 8);
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 8);
}

#[test]
fn a_late_event_for_an_evicted_day_is_merged_and_a_resent_id_is_dropped() {
    let (_dir, store) = store("late-event");
    four_days(&store);
    archive(&store, T0 + 3 * D + H);
    // A phone that was offline sends one new event for the 10th and resends one already filed.
    let written = write_events(
        &store,
        vec![
            event("late", "shop.order.placed", T0 + 5 * H),
            event("d10-0", "shop.order.placed", T0),
        ],
    );
    assert_eq!(
        written.events,
        [true, true],
        "the resend is only caught at the rewrite"
    );
    let all = all_events(&store);
    assert_eq!(
        all.len(),
        13,
        "the late event shows before the rewrite; the resend once"
    );
    assert_newest_first_once(&all);
    let mut seen = Vec::new();
    store
        .scan_events("shop.order.placed", Ts(T0), Ts(T0 + D), &mut |e| seen.push(e.id))
        .unwrap();
    seen.sort();
    assert_eq!(seen, ["d10-0", "d10-2", "late"]);

    let report = archive(&store, T0 + 3 * D + 2 * H);
    assert_eq!(report.written.len(), 1);
    assert_eq!(
        (report.written[0].day.to_string(), report.written[0].rows),
        ("2026-11-10".to_string(), 4)
    );
    assert_eq!(report.evicted, 2);
    assert_eq!(sqlite_rows(&store, "events", T0, T0 + D), 0);
    let filed: Vec<Event> = read_file(&file(&store, "events", "2026-11-10"));
    assert_eq!(
        ids(&filed),
        ["d10-0", "d10-2", "late", "d10-1"],
        "sorted by name, then ts"
    );
    assert_eq!(all_events(&store).len(), 13);
}

#[test]
fn a_late_event_for_a_hot_day_with_a_file_rewrites_it() {
    let (_dir, store) = store("late-hot");
    four_days(&store);
    archive(&store, T0 + D + H);
    write_events(&store, vec![event("late", "shop.order.placed", T0 + 7 * H)]);
    let report = archive(&store, T0 + D + 2 * H);
    assert_eq!(report.written.iter().map(|f| f.rows).collect::<Vec<_>>(), [4]);
    assert_eq!(sqlite_rows(&store, "events", T0, T0 + D), 4, "still hot");
}

#[test]
fn events_with_a_retention_go_to_their_own_folder_and_expire() {
    let (_dir, store) = store("retention-class");
    four_days(&store);
    let classes = |name: &str| {
        if name == "shop.page.viewed" {
            Retention::Days(30)
        } else {
            Retention::Forever
        }
    };
    let report = store.archive(Ts(T0 + 3 * D + H), &classes).unwrap();
    let ten: Vec<(String, u64)> = report
        .written
        .iter()
        .filter(|f| f.day.to_string() == "2026-11-10")
        .map(|f| (f.class.clone(), f.rows))
        .collect();
    assert_eq!(ten, [("events".to_string(), 2), ("events-30d".to_string(), 1)]);
    assert!(file(&store, "events-30d", "2026-11-10").exists());
    assert_eq!(all_events(&store).len(), 12);
    // The 10th ends at T0 + 1 day; its 30-day file goes 30 days after that.
    assert!(store.expire(Ts(T0 + 31 * D - 1)).unwrap().deleted.is_empty());
    let report = store.expire(Ts(T0 + 31 * D)).unwrap();
    let gone: Vec<String> = report
        .deleted
        .iter()
        .map(|f| format!("{}/{}", f.class, f.day))
        .collect();
    assert_eq!(gone, ["events-30d/2026-11-10"]);
    assert!(!file(&store, "events-30d", "2026-11-10").exists());
    assert!(file(&store, "events", "2026-11-10").exists());
    assert_eq!(all_events(&store).len(), 11);
}

#[test]
fn a_cap_deletes_the_oldest_days_until_it_fits() {
    let dir = TempDir::new("cap");
    let mut config = dir.config();
    let sizes = {
        let store = Store::open(&config).unwrap();
        four_days(&store);
        let report = archive(&store, T0 + 3 * D + H);
        report.written.iter().map(|f| f.bytes).collect::<Vec<_>>()
    };
    assert_eq!(sizes.len(), 3);
    config.caps.events = Some(sizes[1] + sizes[2]);
    let store = Store::open(&config).unwrap();
    let report = store.expire(Ts(T0 + 3 * D + H)).unwrap();
    assert_eq!(
        report
            .deleted
            .iter()
            .map(|f| f.day.to_string())
            .collect::<Vec<_>>(),
        ["2026-11-10"]
    );
    assert!(!file(&store, "events", "2026-11-10").exists());
    assert_eq!(all_events(&store).len(), 9);
    // A cap below one hot day also deletes that day's rows from SQLite, so it isn't written again.
    config.caps.events = Some(1);
    drop(store);
    let store = Store::open(&config).unwrap();
    let report = store.expire(Ts(T0 + 3 * D + H)).unwrap();
    assert_eq!(report.deleted.len(), 2);
    assert_eq!(report.rows, 6);
    assert!(archive(&store, T0 + 3 * D + 2 * H).written.is_empty());
    assert_eq!(ids(&all_events(&store)), ["d13-2", "d13-1", "d13-0"]);
}

#[test]
fn local_day_boundaries_follow_the_zone() {
    let (_dir, store) = store_with("zone", |c| c.zone = Zone::new("Asia/Colombo").unwrap());
    // 18:00 UTC is 23:30 on the 10th in Colombo; 20:00 UTC is 01:30 on the 11th.
    write_events(
        &store,
        vec![
            event("a", "shop.order.placed", T0 + 18 * H),
            event("b", "shop.order.placed", T0 + 20 * H),
        ],
    );
    let report = archive(&store, T0 + 3 * D);
    let days: Vec<(String, u64)> = report
        .written
        .iter()
        .map(|f| (f.day.to_string(), f.rows))
        .collect();
    assert_eq!(
        days,
        [("2026-11-10".to_string(), 1), ("2026-11-11".to_string(), 1)]
    );
    assert_eq!(
        ids(&read_file::<Event>(&file(&store, "events", "2026-11-11"))),
        ["b"]
    );
}

#[test]
fn a_day_file_reads_back_with_every_column_intact() {
    let (_dir, store) = store("roundtrip");
    let mut full = event("full", "shop.order.placed", T0 + H);
    full.version = Some("9cf5634".into());
    full.host = Some("web-1".into());
    full.actor = Some("usér 👋".into());
    full.client = Some("web".into());
    full.trace_id = Some("req-81".into());
    full.props = serde_json::from_value(serde_json::json!({
        "items": ["a", "ü"], "nested": {"k": [1, 2.5, null, true]}, "total_cents": 1200, "note": "日本語"
    }))
    .unwrap();
    full.problems = vec!["unknown property `latncy_ms`".into(), "ünïcode".into()];
    let bare = event("bare", "shop.order.placed", T0 + 2 * H);
    let mut err = error("x1", "fp1", "missing key — 42", T0 + H);
    err.stack = Some("at pay()\n  at main()".into());
    err.context = serde_json::from_value(serde_json::json!({"route": "/pay"})).unwrap();
    err.trace_id = Some("req-81".into());
    err.version = Some("v1".into());
    err.host = Some("web-1".into());
    let bare_err = error("x2", "fp1", "plain", T0 + 2 * H);
    let mut line = log("l1", Level::Warn, "slow ⏳", T0 + H);
    line.fields = serde_json::from_value(serde_json::json!({"pid": 81})).unwrap();
    line.host = Some("web-1".into());
    let mut bare_line = log("l2", Level::Fatal, "", T0 + 2 * H);
    bare_line.stream = None;
    let mut labelled = point(
        "power.watts",
        &[("plug", "désk"), ("a", "1")],
        12.5,
        T0 + H,
        false,
    );
    labelled.point.unit = Some("W".into());
    store
        .write(&Batch {
            events: vec![full.clone(), bare.clone()],
            errors: vec![err.clone(), bare_err.clone()],
            logs: vec![line.clone(), bare_line.clone()],
            points: vec![labelled, point("power.watts", &[], -3.0, T0 + 2 * H, false)],
            ..Batch::default()
        })
        .unwrap();
    store.maintain_tiers(Ts(T0 + D + H)).unwrap();
    archive(&store, T0 + D + H);
    assert_eq!(
        read_file::<Event>(&file(&store, "events", "2026-11-10")),
        [full, bare]
    );
    assert_eq!(
        read_file::<ErrorRecord>(&file(&store, "errors", "2026-11-10")),
        [err, bare_err]
    );
    assert_eq!(
        read_file::<LogRecord>(&file(&store, "logs", "2026-11-10")),
        [line, bare_line]
    );
    let samples: Vec<SampleRow> = read_file(&file(&store, "samples", "2026-11-10"));
    let got: Vec<(i64, f64, &str, &str)> = samples
        .iter()
        .map(|s| (s.ts, s.value, &*s.name, &*s.labels))
        .collect();
    assert_eq!(
        got,
        [
            (T0 + H, 12.5, "power.watts", r#"{"a":"1","plug":"désk"}"#),
            (T0 + 2 * H, -3.0, "power.watts", "{}")
        ]
    );
}

#[test]
fn archived_samples_feed_queries_and_late_samples_merge() {
    let (_dir, store) = store("samples");
    let day: Vec<MetricPoint> = (0..1440)
        .map(|i| point("g", &[], 1.0, T0 + i * M, false))
        .collect();
    write_points(&store, day);
    // Raw samples wait for the 5-minute tier before they leave.
    let report = archive(&store, T0 + 3 * D + H);
    assert_eq!((report.written.len(), report.evicted), (1, 0));
    store.maintain_tiers(Ts(T0 + 3 * D + H)).unwrap();
    let report = archive(&store, T0 + 3 * D + H);
    assert_eq!((report.written.len(), report.evicted), (0, 1440));
    assert_eq!(count(&store, "SELECT count(*) FROM samples"), 0);
    let q = store
        .query(&MetricQuery {
            name: "g".into(),
            matchers: BTreeMap::new(),
            start: Ts(T0),
            end: Ts(T0 + 2 * H),
            step: None,
            agg: Agg::Avg,
        })
        .unwrap();
    assert_eq!(
        (q.tier, q.step),
        (Tier::FiveMinutes, 5 * M),
        "raw samples of the 10th are no longer in SQLite"
    );
    assert_eq!(q.series[0].points[0].1, Some(1.0));
    // A late sample, and a resend at an existing ts with a new value.
    write_points(
        &store,
        vec![
            point("g", &[], 13.0, T0 + 30_000, false),
            point("g", &[], 7.0, T0 + 2 * M, false),
        ],
    );
    let report = store.maintain_tiers(Ts(T0 + 3 * D + 2 * H)).unwrap();
    assert_eq!(report.redone_5m, 1);
    // From the file: 1, 1, 1, 1 (the 1 at 2 minutes replaced by 7), plus 13 and 7 from SQLite.
    let (avg, n): (f64, i64) = store
        .writer()
        .unwrap()
        .conn
        .query_row("SELECT avg, n FROM samples_5m WHERE ts = ?1", [T0], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!((avg, n), ((1.0 * 4.0 + 13.0 + 7.0) / 6.0, 6));
    let report = archive(&store, T0 + 3 * D + 2 * H);
    assert_eq!(report.written.iter().map(|f| f.rows).collect::<Vec<_>>(), [1441]);
    assert_eq!(report.evicted, 2);
    let filed: Vec<SampleRow> = read_file(&file(&store, "samples", "2026-11-10"));
    let at = |ts: i64| filed.iter().find(|s| s.ts == ts).map(|s| s.value);
    assert_eq!(
        (at(T0 + 30_000), at(T0 + 2 * M)),
        (Some(13.0), Some(7.0)),
        "the newest value wins"
    );
}

#[test]
fn logs_and_errors_follow_their_retention() {
    let (_dir, store) = store_with("log-retention", |c| {
        c.retention.logs = Some(2 * D);
        c.retention.errors = Some(3 * D);
    });
    store
        .write(&Batch {
            logs: vec![
                log("l10", Level::Info, "a", T0 + H),
                log("l11", Level::Info, "b", T0 + D),
                log("l12", Level::Info, "c", T0 + 2 * D + H),
            ],
            errors: vec![error("x10", "fp", "boom", T0 + H)],
            ..Batch::default()
        })
        .unwrap();
    let report = archive(&store, T0 + 3 * D + H);
    assert_eq!(report.written.len(), 4, "logs 10, 11, 12 and errors 10");
    assert_eq!(report.evicted, 2, "the 10th's log and error");
    let report = store.expire(Ts(T0 + 3 * D + H)).unwrap();
    let gone: Vec<String> = report
        .deleted
        .iter()
        .map(|f| format!("{}/{}", f.class, f.day))
        .collect();
    assert_eq!(gone, ["logs/2026-11-10"]);
    assert_eq!(report.rows, 1, "the 11th's line is past 2 days and leaves SQLite");
    let left: Vec<String> = store
        .logs(&LogFilter::default())
        .unwrap()
        .into_iter()
        .map(|l| l.id)
        .collect();
    assert_eq!(left, ["l12"], "the 11th's line is past retention");
    let errors = store.errors(&ErrorFilter::default()).unwrap();
    assert_eq!(
        errors.len(),
        1,
        "the 10th's error is within 3 days, read from its file"
    );
    let report = store.expire(Ts(T0 + 4 * D)).unwrap();
    let gone: Vec<String> = report
        .deleted
        .iter()
        .map(|f| format!("{}/{}", f.class, f.day))
        .collect();
    assert_eq!(gone, ["errors/2026-11-10", "logs/2026-11-11"]);
    assert!(store.errors(&ErrorFilter::default()).unwrap().is_empty());
}
