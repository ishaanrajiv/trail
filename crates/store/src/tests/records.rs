use super::*;

#[test]
fn resending_the_same_ids_stores_once_and_reports_duplicates() {
    let (_dir, store) = store("dupes");
    let batch = Batch {
        events: vec![
            event("e1", "shop.order.placed", T0),
            event("e2", "shop.order.placed", T0 + 1),
        ],
        errors: vec![error("x1", "fp", "boom", T0)],
        logs: vec![log("l1", Level::Info, "started", T0)],
        ..Batch::default()
    };
    let first = store.write(&batch).unwrap();
    assert_eq!(first.events, [true, true]);
    assert_eq!(first.duplicates(), 0);
    let mut again = batch.clone();
    again.events.push(event("e3", "shop.order.placed", T0 + 2));
    let second = store.write(&again).unwrap();
    assert_eq!(second.events, [false, false, true]);
    assert_eq!(second.errors, [false]);
    assert_eq!(second.logs, [false]);
    assert_eq!(second.duplicates(), 4);
    assert_eq!(all_events(&store).len(), 3);
    let stats = store.stats().unwrap();
    let rows: Vec<u64> = stats.stores.iter().map(|s| s.rows).collect();
    assert_eq!(rows, [3, 0, 1, 1], "events, samples, errors, logs");
    assert!(stats.db_bytes > 0);
}

#[test]
fn error_groups_count_reopen_and_keep_the_first_message() {
    let (_dir, store) = store("groups");
    let write = |errors: Vec<ErrorRecord>| {
        store
            .write(&Batch {
                errors,
                ..Batch::default()
            })
            .unwrap()
    };
    write(vec![
        error("a", "fp1", "missing key 42", T0),
        error("b", "fp1", "missing key 77", T0 + 10),
    ]);
    write(vec![error("b", "fp1", "missing key 77", T0 + 10)]); // a resend counts once
    let g = store.error_group("fp1").unwrap().unwrap();
    assert_eq!(
        (g.count, g.message.as_str(), g.status),
        (2, "missing key 42", ErrorStatus::Open)
    );
    assert_eq!((g.first_seen, g.last_seen), (Ts(T0), Ts(T0 + 10)));

    assert!(store.set_error_status("fp1", ErrorStatus::Resolved).unwrap());
    assert!(!store.set_error_status("nope", ErrorStatus::Resolved).unwrap());
    write(vec![error("c", "fp1", "missing key 9", T0 + 20)]);
    let g = store.error_group("fp1").unwrap().unwrap();
    assert_eq!(
        (g.count, g.status, g.last_seen),
        (3, ErrorStatus::Open, Ts(T0 + 20)),
        "reopened"
    );

    let mut other = error("d", "fp2", "timeout", T0 + 30);
    other.source = "till".to_string();
    write(vec![other]);
    store.set_error_status("fp2", ErrorStatus::Ignored).unwrap();
    write(vec![{
        let mut e = error("e", "fp2", "timeout", T0 + 40);
        e.source = "till".to_string();
        e
    }]);
    assert_eq!(
        store.error_group("fp2").unwrap().unwrap().status,
        ErrorStatus::Ignored,
        "ignored stays ignored"
    );

    let all = store.error_groups(&GroupFilter::default()).unwrap();
    assert_eq!(
        all.iter().map(|g| g.fingerprint.as_str()).collect::<Vec<_>>(),
        ["fp2", "fp1"]
    );
    let open = store
        .error_groups(&GroupFilter {
            status: Some(ErrorStatus::Open),
            ..GroupFilter::default()
        })
        .unwrap();
    assert_eq!(open.len(), 1);
    let till = store
        .error_groups(&GroupFilter {
            source: Some("till".into()),
            limit: 1,
            ..GroupFilter::default()
        })
        .unwrap();
    assert_eq!(till[0].fingerprint, "fp2");
}

#[test]
fn events_filter_and_page_with_a_cursor() {
    let (_dir, store) = store("event-filters");
    let mut a = event("a", "shop.order.placed", T0);
    a.actor = Some("user".into());
    a.trace_id = Some("req-1".into());
    let mut b = event("b", "shop.order.failed", T0 + 1);
    b.problems = vec!["unknown property `x`".into()];
    let mut c = event("c", "shop.page.viewed", T0 + 1);
    c.source = "web".into();
    let d = event("d", "shop.order.placed", T0 + 2);
    write_events(&store, vec![a, b, c, d]);
    let ids = |f: EventFilter| {
        store
            .events(&f)
            .unwrap()
            .into_iter()
            .map(|e| e.id)
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(EventFilter::default()), ["d", "c", "b", "a"]);
    assert_eq!(
        ids(EventFilter {
            name: Some("shop.order.placed".into()),
            ..Default::default()
        }),
        ["d", "a"]
    );
    assert_eq!(
        ids(EventFilter {
            name: Some("shop.order.*".into()),
            ..Default::default()
        }),
        ["d", "b", "a"]
    );
    assert_eq!(
        ids(EventFilter {
            source: Some("web".into()),
            ..Default::default()
        }),
        ["c"]
    );
    assert_eq!(
        ids(EventFilter {
            actor: Some("user".into()),
            ..Default::default()
        }),
        ["a"]
    );
    assert_eq!(
        ids(EventFilter {
            trace_id: Some("req-1".into()),
            ..Default::default()
        }),
        ["a"]
    );
    assert_eq!(
        ids(EventFilter {
            mismatched: true,
            ..Default::default()
        }),
        ["b"]
    );
    assert_eq!(
        ids(EventFilter {
            start: Some(Ts(T0 + 1)),
            end: Some(Ts(T0 + 2)),
            ..Default::default()
        }),
        ["c", "b"]
    );
    // Pages: same ts, ordered by id descending.
    let page1 = ids(EventFilter {
        limit: 2,
        ..Default::default()
    });
    assert_eq!(page1, ["d", "c"]);
    let page2 = ids(EventFilter {
        limit: 2,
        before: Some((Ts(T0 + 1), "c".into())),
        ..Default::default()
    });
    assert_eq!(page2, ["b", "a"]);
    let got = store
        .events(&EventFilter {
            mismatched: true,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(got[0].problems, ["unknown property `x`"]);
}

#[test]
fn errors_and_logs_filter() {
    let (_dir, store) = store("err-log-filters");
    let mut fatal = error("x2", "fp2", "disk gone", T0 + 5);
    fatal.severity = Severity::Fatal;
    let mut other_log = log("l3", Level::Error, "payment failed for order 7", T0 + 3);
    other_log.stream = Some("api".into());
    store
        .write(&Batch {
            errors: vec![error("x1", "fp1", "boom", T0), fatal],
            logs: vec![
                log("l1", Level::Debug, "tick", T0),
                log("l2", Level::Warn, "slow payment", T0 + 1),
                other_log,
            ],
            ..Batch::default()
        })
        .unwrap();
    let errors = |f: ErrorFilter| {
        store
            .errors(&f)
            .unwrap()
            .into_iter()
            .map(|e| e.id)
            .collect::<Vec<_>>()
    };
    assert_eq!(errors(ErrorFilter::default()), ["x2", "x1"]);
    assert_eq!(
        errors(ErrorFilter {
            fingerprint: Some("fp1".into()),
            ..Default::default()
        }),
        ["x1"]
    );
    assert_eq!(
        errors(ErrorFilter {
            severity: Some(Severity::Fatal),
            ..Default::default()
        }),
        ["x2"]
    );
    assert_eq!(
        errors(ErrorFilter {
            source: Some("shop".into()),
            end: Some(Ts(T0 + 1)),
            ..Default::default()
        }),
        ["x1"]
    );
    assert_eq!(
        errors(ErrorFilter {
            before: Some((Ts(T0 + 5), "x2".into())),
            ..Default::default()
        }),
        ["x1"]
    );

    let logs = |f: LogFilter| {
        store
            .logs(&f)
            .unwrap()
            .into_iter()
            .map(|l| l.id)
            .collect::<Vec<_>>()
    };
    assert_eq!(logs(LogFilter::default()), ["l3", "l2", "l1"]);
    assert_eq!(
        logs(LogFilter {
            min_level: Some(Level::Warn),
            ..Default::default()
        }),
        ["l3", "l2"]
    );
    assert_eq!(
        logs(LogFilter {
            stream: Some("worker".into()),
            ..Default::default()
        }),
        ["l2", "l1"]
    );
    assert_eq!(
        logs(LogFilter {
            contains: Some("payment".into()),
            ..Default::default()
        }),
        ["l3", "l2"]
    );
    assert_eq!(
        logs(LogFilter {
            source: Some("shop".into()),
            start: Some(Ts(T0 + 1)),
            limit: 1,
            ..Default::default()
        }),
        ["l3"]
    );
    assert_eq!(
        logs(LogFilter {
            before: Some((Ts(T0 + 1), "l2".into())),
            ..Default::default()
        }),
        ["l1"]
    );
}

#[test]
fn problems_are_grouped_listed_and_cleared() {
    let (_dir, store) = store("problems");
    let m = |source: &str, message: &str, seen: i64| Mismatch {
        source: source.into(),
        kind: "event".into(),
        name: "shop.order.placed".into(),
        message: message.into(),
        seen: Ts(seen),
    };
    store
        .write(&Batch {
            mismatches: vec![
                m("shop", "unknown property `x`", T0),
                m("till", "missing `y`", T0 + 5),
            ],
            ..Batch::default()
        })
        .unwrap();
    store
        .write(&Batch {
            mismatches: vec![m("shop", "unknown property `x`", T0 + 10)],
            ..Batch::default()
        })
        .unwrap();
    let all = store.problems(None).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(
        (
            all[0].source.as_str(),
            all[0].count,
            all[0].first_seen,
            all[0].last_seen
        ),
        ("shop", 2, Ts(T0), Ts(T0 + 10))
    );
    assert_eq!(store.problems(Some("till")).unwrap().len(), 1);
    assert_eq!(store.clear_problems("shop").unwrap(), 1);
    assert_eq!(store.problems(None).unwrap()[0].source, "till");
}

#[test]
fn reads_run_alongside_a_held_write_lock() {
    let (_dir, store) = store("concurrent");
    write_events(&store, vec![event("a", "shop.order.placed", T0)]);
    let store = std::sync::Arc::new(store);
    let guard = store.writer().unwrap();
    let reader = {
        let store = std::sync::Arc::clone(&store);
        std::thread::spawn(move || all_events(&store).len())
    };
    assert_eq!(reader.join().unwrap(), 1);
    drop(guard);
}
