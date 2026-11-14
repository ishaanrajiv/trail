use super::*;
use crate::metrics::nice_step;

fn query(store: &Store, name: &str, start: i64, end: i64, step: Option<i64>, agg: Agg) -> MetricResult {
    store
        .query(&MetricQuery {
            name: name.to_string(),
            matchers: BTreeMap::new(),
            start: Ts(start),
            end: Ts(end),
            step,
            agg,
        })
        .unwrap()
}

/// The non-empty bucket values of the first series.
fn values(result: &MetricResult) -> Vec<f64> {
    result.series[0].points.iter().filter_map(|(_, v)| *v).collect()
}

fn tier_row(store: &Store, table: &str, ts: i64) -> (f64, f64, f64, i64) {
    store
        .writer()
        .unwrap()
        .conn
        .query_row(
            &format!("SELECT avg, min, max, n FROM {table} WHERE ts = ?1"),
            [ts],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap()
}

#[test]
fn a_counter_stores_rates_totals_and_increases_across_a_reset() {
    let (dir, store) = store("counter");
    let pts = [1000.0, 4000.0, 7000.0, 10.0, 3010.0]
        .iter()
        .enumerate()
        .map(|(i, v)| {
            point(
                "net.bytes_recv",
                &[("interface", "eth0")],
                *v,
                T0 + i as i64 * 30_000,
                true,
            )
        })
        .collect();
    let written = write_points(&store, pts);
    use PointOutcome::*;
    // The reading that went down (10) is a reset: the counter started again from zero, so its
    // 10 count as new, over the 30 seconds since the reading before.
    assert_eq!(written.points, [Baseline, Stored, Stored, Stored, Stored]);
    let q = query(&store, "net.bytes_recv", T0, T0 + 5 * M, Some(30_000), Agg::Avg);
    assert_eq!(q.tier, Tier::Raw);
    assert_eq!(q.series[0].series.kind, trail_core::envelope::MetricKind::Counter);
    assert_eq!(values(&q), [100.0, 100.0, 10.0 / 30.0, 100.0]);
    // Beside each rate, the raw total and the increase since the reading before.
    let kept: Vec<(f64, f64)> = {
        let w = store.writer().unwrap();
        let mut stmt = w
            .conn
            .prepare("SELECT total, increase FROM samples ORDER BY ts")
            .unwrap();
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    };
    assert_eq!(
        kept,
        [(4000.0, 3000.0), (7000.0, 3000.0), (10.0, 10.0), (3010.0, 3000.0)]
    );
    let rise = query(
        &store,
        "net.bytes_recv",
        T0,
        T0 + 5 * M,
        Some(5 * M),
        Agg::Increase,
    );
    assert_eq!(values(&rise), [9010.0], "exact across the reset");
    // Out of order: a rate needs readings in order.
    let late = write_points(
        &store,
        vec![point(
            "net.bytes_recv",
            &[("interface", "eth0")],
            9999.0,
            T0,
            true,
        )],
    );
    assert_eq!(late.points, [OutOfOrder]);
    // The baseline survives a restart.
    drop(store);
    let store = Store::open(&dir.config()).unwrap();
    let after = write_points(
        &store,
        vec![point(
            "net.bytes_recv",
            &[("interface", "eth0")],
            3310.0,
            T0 + 5 * 30_000,
            true,
        )],
    );
    assert_eq!(after.points, [Stored]);
    let latest = store.latest("net.bytes_recv", &BTreeMap::new(), None).unwrap();
    assert_eq!((latest[0].ts, latest[0].value), (Ts(T0 + 150_000), 10.0));
}

#[test]
fn series_are_listed_by_name_glob_labels_and_source() {
    let (_dir, store) = store("series");
    let mut tv = point("power.watts", &[("plug", "tv")], 30.0, T0, false);
    tv.point.unit = Some("W".into());
    let mut other = point("cpu.usage", &[], 3.0, T0, false);
    other.source = "box".into();
    write_points(
        &store,
        vec![
            point("power.watts", &[("plug", "desk")], 12.0, T0, false),
            tv,
            other,
        ],
    );
    let names = |f: SeriesFilter| {
        store
            .series(&f)
            .unwrap()
            .into_iter()
            .map(|s| format!("{}{:?}", s.name, s.labels))
            .collect::<Vec<_>>()
    };
    assert_eq!(names(SeriesFilter::default()).len(), 3);
    assert_eq!(
        names(SeriesFilter {
            name: Some("power.*".into()),
            ..Default::default()
        })
        .len(),
        2
    );
    let tv = store
        .series(&SeriesFilter {
            name: Some("power.watts".into()),
            matchers: [("plug".to_string(), "tv".to_string())].into(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!((tv.len(), tv[0].unit.as_deref()), (1, Some("W")));
    assert_eq!(
        names(SeriesFilter {
            source: Some("box".into()),
            ..Default::default()
        }),
        ["cpu.usage{}"]
    );
    assert_eq!(store.stats().unwrap().series, 3);
    // Alerts: the newest sample per series, and a window's average.
    write_points(
        &store,
        vec![point("power.watts", &[("plug", "tv")], 50.0, T0 + M, false)],
    );
    let latest = store
        .latest(
            "power.watts",
            &[("plug".to_string(), "tv".to_string())].into(),
            None,
        )
        .unwrap();
    assert_eq!(latest.iter().map(|l| l.value).collect::<Vec<_>>(), [50.0]);
    let id = latest[0].series.id;
    assert_eq!(store.window_avg(id, Ts(T0), Ts(T0 + 2 * M)).unwrap(), Some(40.0));
    assert_eq!(
        store.window_avg(id, Ts(T0 + 5 * M), Ts(T0 + 6 * M)).unwrap(),
        None
    );
}

/// 3 hours of a gauge every 30 s: value = minute index.
fn three_hours(store: &Store) {
    let pts = (0..6 * 60).map(|i| {
        point(
            "cpu.usage",
            &[("cpu", "all")],
            f64::from(i / 2),
            T0 + i64::from(i) * 30_000,
            false,
        )
    });
    write_points(store, pts.collect());
}

#[test]
fn closed_buckets_are_built_after_the_grace() {
    let (_dir, store) = store("tiers");
    three_hours(&store);
    let early = store.maintain_tiers(Ts(T0 + 5 * M + 60_000)).unwrap();
    assert_eq!(
        early.built_5m, 0,
        "the first bucket is closed but still in its grace"
    );
    let on_time = store.maintain_tiers(Ts(T0 + 5 * M + 90_000)).unwrap();
    assert_eq!(on_time.built_5m, 1);
    let report = store.maintain_tiers(Ts(T0 + 3 * H + 2 * M)).unwrap();
    assert_eq!((report.built_5m, report.built_1h), (35, 3));
    assert_eq!(tier_row(&store, "samples_5m", T0), (2.0, 0.0, 4.0, 10));
    assert_eq!(tier_row(&store, "samples_1h", T0), (29.5, 0.0, 59.0, 120));
    assert_eq!(
        tier_row(&store, "samples_1h", T0 + 2 * H),
        (149.5, 120.0, 179.0, 120)
    );
}

#[test]
fn a_late_sample_redoes_its_buckets() {
    let (_dir, store) = store("late-tier");
    write_points(&store, vec![point("g", &[], 10.0, T0 + M, false)]);
    store.maintain_tiers(Ts(T0 + 2 * H)).unwrap();
    assert_eq!(tier_row(&store, "samples_5m", T0), (10.0, 10.0, 10.0, 1));
    write_points(&store, vec![point("g", &[], 20.0, T0 + 2 * M, false)]);
    let report = store.maintain_tiers(Ts(T0 + 2 * H + M)).unwrap();
    assert_eq!((report.redone_5m, report.redone_1h), (1, 1));
    assert_eq!(tier_row(&store, "samples_5m", T0), (15.0, 10.0, 20.0, 2));
    assert_eq!(tier_row(&store, "samples_1h", T0), (15.0, 10.0, 20.0, 2));
    // The same ts again replaces the value (a rollup bucket being rewritten).
    write_points(&store, vec![point("g", &[], 30.0, T0 + 2 * M, false)]);
    store.maintain_tiers(Ts(T0 + 2 * H + 2 * M)).unwrap();
    assert_eq!(tier_row(&store, "samples_1h", T0), (20.0, 10.0, 30.0, 2));
    // A dirty bucket with no readings left is deleted from both tiers.
    {
        let w = store.writer().unwrap();
        w.conn.execute("DELETE FROM samples", []).unwrap();
        w.conn
            .execute("INSERT INTO tier_dirty VALUES ('5m', 1, ?1)", [T0])
            .unwrap();
    }
    let report = store.maintain_tiers(Ts(T0 + 2 * H + 3 * M)).unwrap();
    assert_eq!((report.redone_5m, report.redone_1h), (1, 1));
    assert_eq!(count(&store, "SELECT count(*) FROM samples_5m"), 0);
    assert_eq!(count(&store, "SELECT count(*) FROM samples_1h"), 0);
}

#[test]
fn a_query_picks_raw_then_5m_then_1h_as_the_range_grows() {
    let (_dir, store) = store("query-tiers");
    three_hours(&store);
    store.maintain_tiers(Ts(T0 + 3 * H + 2 * M)).unwrap();
    let raw = query(&store, "cpu.usage", T0, T0 + 2 * H, None, Agg::Avg);
    assert_eq!((raw.tier, raw.step), (Tier::Raw, 30_000));
    assert_eq!(&values(&raw)[..4], [0.0, 0.0, 1.0, 1.0]);
    let five = query(&store, "cpu.usage", T0, T0 + D, None, Agg::Avg);
    assert_eq!((five.tier, five.step), (Tier::FiveMinutes, 5 * M));
    assert_eq!(five.series[0].points[0], (Ts(T0), Some(2.0)));
    assert_eq!(
        five.series[0].points.len(),
        288,
        "every bucket, empty ones as None"
    );
    assert_eq!(five.series[0].points[100].1, None);
    let hour = query(&store, "cpu.usage", T0, T0 + 14 * D, None, Agg::Avg);
    assert_eq!((hour.tier, hour.step), (Tier::Hour, 2 * H));
    assert_eq!(values(&hour), [59.5, 149.5]);
    // Explicit steps, and each aggregation over a tier.
    let q = |agg| values(&query(&store, "cpu.usage", T0, T0 + 3 * H, Some(H), agg));
    assert_eq!(q(Agg::Max), [59.0, 119.0, 179.0]);
    assert_eq!(q(Agg::Min), [0.0, 60.0, 120.0]);
    assert_eq!(q(Agg::Avg), [29.5, 89.5, 149.5]);
    assert_eq!(q(Agg::Sum), [29.5 * 120.0, 89.5 * 120.0, 149.5 * 120.0]);
    assert_eq!(q(Agg::Count), [120.0, 120.0, 120.0]);
    assert_eq!(
        q(Agg::Last),
        [29.5, 89.5, 149.5],
        "over a tier: the newest tier bucket's average"
    );
    // Raw sum and last, at a step finer than the tiers.
    let raw = |agg| values(&query(&store, "cpu.usage", T0, T0 + 2 * M, Some(M), agg));
    assert_eq!(raw(Agg::Sum), [0.0, 2.0]);
    assert_eq!(raw(Agg::Last), [0.0, 1.0]);
    // The part after the last built bucket comes from raw samples.
    write_points(
        &store,
        (0..10)
            .map(|i| {
                point(
                    "cpu.usage",
                    &[("cpu", "all")],
                    500.0,
                    T0 + 3 * H + i * 30_000,
                    false,
                )
            })
            .collect(),
    );
    let tail = query(&store, "cpu.usage", T0, T0 + 4 * H, Some(H), Agg::Max);
    assert_eq!(values(&tail), [59.0, 119.0, 179.0, 500.0]);
    let err = store
        .query(&MetricQuery {
            name: "cpu.usage".into(),
            matchers: BTreeMap::new(),
            start: Ts(T0),
            end: Ts(T0),
            step: None,
            agg: Agg::Avg,
        })
        .unwrap_err();
    assert!(err.fix.contains("start before the end"), "{err}");
}

#[test]
fn tier_retention_deletes_old_rows_and_redoes_old_hours_from_raw() {
    let (_dir, store) = store_with("tier-retention", |c| {
        c.retention.samples_5m = Some(D);
        c.retention.samples_1h = Some(30 * D);
    });
    three_hours(&store);
    store.maintain_tiers(Ts(T0 + 3 * H + 2 * M)).unwrap();
    let report = store.maintain_tiers(Ts(T0 + 2 * D)).unwrap();
    assert_eq!(report.deleted_5m, 36);
    // Past the 5-minute retention the hourly tier answers a 5-minute-or-coarser step.
    let q = query(&store, "cpu.usage", T0, T0 + 3 * H, Some(10 * M), Agg::Avg);
    assert_eq!((q.tier, q.step), (Tier::Hour, H));
    assert_eq!(values(&q), [29.5, 89.5, 149.5]);
    // A late sample for an hour whose 5-minute rows are gone is redone from raw samples.
    write_points(
        &store,
        vec![point(
            "cpu.usage",
            &[("cpu", "all")],
            1000.0,
            T0 + 30 * M + 1,
            false,
        )],
    );
    let report = store.maintain_tiers(Ts(T0 + 2 * D + M)).unwrap();
    assert_eq!(report.redone_1h, 1);
    assert_eq!(tier_row(&store, "samples_1h", T0).3, 121);
    let report = store.maintain_tiers(Ts(T0 + 40 * D)).unwrap();
    assert_eq!(report.deleted_1h, 3);
}

#[test]
fn hourly_buckets_start_on_the_local_hour() {
    let (_dir, store) = store_with("local-hours", |c| {
        c.zone = trail_core::time::Zone::new("Asia/Colombo").unwrap()
    });
    write_points(
        &store,
        vec![
            point("g", &[], 1.0, T0 + 10 * M, false),
            point("g", &[], 3.0, T0 + 40 * M, false),
        ],
    );
    store.maintain_tiers(Ts(T0 + 3 * H)).unwrap();
    // T0 is 05:30 in Colombo: its local hour started at T0 - 30 minutes and ends at T0 + 30.
    assert_eq!(tier_row(&store, "samples_1h", T0 - 30 * M), (1.0, 1.0, 1.0, 1));
    assert_eq!(tier_row(&store, "samples_1h", T0 + 30 * M), (3.0, 3.0, 3.0, 1));
    let q = query(&store, "g", T0 - 30 * M, T0 + 90 * M, Some(H), Agg::Avg);
    assert_eq!(
        q.series[0].points,
        [(Ts(T0 - 30 * M), Some(1.0)), (Ts(T0 + 30 * M), Some(3.0))]
    );
}

#[test]
fn nice_steps() {
    assert_eq!(nice_step(1), 10_000);
    assert_eq!(nice_step(24_000), 30_000);
    assert_eq!(nice_step(288_000), 300_000);
    assert_eq!(nice_step(604_800_001), 2 * 604_800_000);
}

#[test]
fn a_long_backlog_is_built_in_slices_and_ends_complete() {
    let (_dir, store) = store("slices");
    // Forty days, a sample an hour: more than one slice of either tier.
    let points: Vec<MetricPoint> = (0..40 * 24)
        .map(|i| point("g", &[], 1.0, T0 + i * H, false))
        .collect();
    write_points(&store, points);
    let report = store.maintain_tiers(Ts(T0 + 41 * D)).unwrap();
    assert_eq!((report.built_5m, report.built_1h), (960, 960));
    assert_eq!(count(&store, "SELECT count(*) FROM samples_1h"), 960);
    assert_eq!(
        store.maintain_tiers(Ts(T0 + 41 * D)).unwrap(),
        TierReport::default()
    );
}

#[test]
fn a_point_that_would_start_one_series_too_many_is_not_stored() {
    let (_dir, store) = store_with("series-cap", |c| c.limits.series = 2);
    let written = write_points(
        &store,
        vec![
            point("g", &[("host", "a")], 1.0, T0, false),
            point("g", &[("host", "b")], 1.0, T0, false),
            point("g", &[("host", "c")], 1.0, T0, false),
            point("g", &[("host", "a")], 2.0, T0 + M, false),
        ],
    );
    use PointOutcome::*;
    assert_eq!(written.points, [Stored, Stored, TooManySeries, Stored]);
    assert_eq!(count(&store, "SELECT count(*) FROM series"), 2);
}

#[test]
fn a_series_belongs_to_its_source() {
    use PointOutcome::{Baseline, Stored};
    let (_dir, store) = store("series-source");
    let from = |source: &str, value: f64, ts: i64, counter: bool| {
        let mut p = point("net.bytes", &[("interface", "eth0")], value, ts, counter);
        p.source = source.to_string();
        p
    };
    // Two senders with the same name and labels: two series, and each counter its own baseline.
    let first = write_points(
        &store,
        vec![from("shop", 1000.0, T0, true), from("till", 5.0, T0, true)],
    );
    assert_eq!(first.points, [Baseline, Baseline]);
    let next = write_points(
        &store,
        vec![
            from("shop", 1600.0, T0 + 60_000, true),
            from("till", 65.0, T0 + 60_000, true),
        ],
    );
    assert_eq!(next.points, [Stored, Stored]);
    let series = store
        .series(&SeriesFilter {
            name: Some("net.bytes".to_string()),
            ..SeriesFilter::default()
        })
        .unwrap();
    let sources: Vec<&str> = series.iter().map(|s| s.source.as_str()).collect();
    assert_eq!(sources, ["shop", "till"]);
    let mine = store.latest("net.bytes", &BTreeMap::new(), Some("shop")).unwrap();
    assert_eq!(mine.len(), 1);
    assert_eq!(
        mine[0].value, 10.0,
        "600 over a minute, untouched by till's readings"
    );
    let all = store.latest("net.bytes", &BTreeMap::new(), None).unwrap();
    assert_eq!(all.len(), 2);
}

#[test]
fn increases_stay_exact_in_every_tier_and_in_the_day_files() {
    use trail_core::decl::Retention;
    let (_dir, store) = store_with("increase", |c| c.hot_days = 1);
    // Two hours of a counter read every minute, going up by i each time, reset at 01:00.
    let mut total = 0.0;
    let mut expected = 0.0;
    let mut points = Vec::new();
    for i in 0..120 {
        if i == 60 {
            total = 7.0; // restarted: 7 since the reset
        } else {
            total += f64::from(i);
        }
        if i > 0 {
            expected += if i == 60 { 7.0 } else { f64::from(i) };
        }
        points.push(point("net.bytes", &[], total, T0 + i64::from(i) * M, true));
        points.push(point("room.temp", &[], 20.0, T0 + i64::from(i) * M, false));
    }
    write_points(&store, points);
    let rise = |start: i64, end: i64, step: i64| -> Vec<Option<f64>> {
        let q = query(&store, "net.bytes", start, end, Some(step), Agg::Increase);
        q.series[0].points.iter().map(|(_, v)| *v).collect()
    };
    let sum = |v: &[Option<f64>]| v.iter().map(|x| x.unwrap_or(0.0)).sum::<f64>();
    assert_eq!(sum(&rise(T0, T0 + 2 * H, M)), expected, "raw");

    // Built into the tiers, and the raw rows filed and gone from SQLite.
    let now = T0 + 3 * D;
    store.maintain_tiers(Ts(now)).unwrap();
    let report = store.archive(Ts(now), &|_| Retention::Forever).unwrap();
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert_eq!(count(&store, "SELECT count(*) FROM samples"), 0);
    let five = rise(T0, T0 + 2 * H, 5 * M);
    assert_eq!(five.len(), 24);
    assert_eq!(sum(&five), expected, "5m");
    let hourly = rise(T0, T0 + 2 * H, H);
    // Each hour holds the increases of the readings in it: 1..=59, then 7 + 61..=119.
    assert_eq!(hourly, [Some(1770.0), Some(7.0 + 5310.0)]);
    let q = query(&store, "net.bytes", T0, T0 + 2 * H, Some(H), Agg::Increase);
    assert_eq!(q.tier, Tier::Hour);

    // The day file keeps the total and the increase.
    let path = store.day_path("samples", "2026-11-10".parse().unwrap());
    let mut rows = crate::parquet_io::FileRows::<crate::parquet_io::SampleRow>::open(&path, None).unwrap();
    let mut counter = Vec::new();
    while let Some(r) = rows.next_row().unwrap() {
        if &*r.name == "net.bytes" {
            counter.push((r.total, r.increase));
        }
    }
    assert_eq!(counter[0], (Some(1.0), Some(1.0)));
    assert_eq!(counter[59], (Some(7.0), Some(7.0)), "the reset");

    // A reading for a bucket already built (the series was quiet), whose day has left SQLite:
    // the bucket is recomputed from the day file and SQLite, increase included.
    // 01:59:30 is in the 01:55 bucket, whose other readings are only in the day file now.
    let late = write_points(
        &store,
        vec![point(
            "net.bytes",
            &[],
            total + 1000.0,
            T0 + 119 * M + 30_000,
            true,
        )],
    );
    assert_eq!(late.points, [PointOutcome::Stored]);
    store.maintain_tiers(Ts(now + H)).unwrap();
    let five = rise(T0, T0 + 2 * H, 5 * M);
    assert_eq!(five[23], Some(f64::from(115 + 116 + 117 + 118 + 119) + 1000.0));
    assert_eq!(sum(&five), expected + 1000.0);
    assert_eq!(
        rise(T0, T0 + 2 * H, H),
        [Some(1770.0), Some(7.0 + 5310.0 + 1000.0)]
    );

    // A gauge has no increase.
    let q = query(&store, "room.temp", T0, T0 + 2 * H, Some(H), Agg::Increase);
    assert!(q.series[0].points.iter().all(|(_, v)| v.is_none()));
}

#[test]
fn a_counter_reading_from_before_totals_were_kept_gives_no_increase() {
    let (_dir, store) = store("increase-old");
    write_points(
        &store,
        vec![
            point("net.bytes", &[], 100.0, T0, true),
            point("net.bytes", &[], 160.0, T0 + M, true),
        ],
    );
    // As version 1 stored it: the rate alone.
    store
        .writer()
        .unwrap()
        .conn
        .execute_batch(&format!(
            "INSERT INTO samples (series, ts, value) SELECT series, {}, 1.0 FROM samples LIMIT 1",
            T0 + 2 * M
        ))
        .unwrap();
    let q = query(&store, "net.bytes", T0, T0 + 3 * M, Some(M), Agg::Increase);
    let got: Vec<Option<f64>> = q.series[0].points.iter().map(|(_, v)| *v).collect();
    assert_eq!(got, [None, Some(60.0), None]);
    let q = query(&store, "net.bytes", T0, T0 + 5 * M, Some(5 * M), Agg::Increase);
    assert_eq!(q.series[0].points[0].1, None, "part of the bucket is unknown");
}
