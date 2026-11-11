//! Metrics: series, the 5-minute and hourly tiers, and queries over them.
//!
//! A tier bucket is built once it has closed and [`GRACE_MS`] has passed. A sample older than what
//! a tier has covered marks its bucket in `tier_dirty`, and the next maintenance recomputes it,
//! from SQLite and, for days that have left it, the samples day file.

use crate::parquet_io::{FileRows, SampleRow, Skip};
use crate::{Error, Result, Store, StoreKind, Writer, evicted_before, get_mark, set_mark, sql};
use rusqlite::{Connection, OptionalExtension};
use std::collections::{BTreeMap, HashMap};
use trail_core::Ts;
use trail_core::config::glob_match;
use trail_core::envelope::MetricKind;
use trail_core::time::{DAY, Day, Every, HOUR, MINUTE};

/// Raw (ts, value) readings per (series, bucket start).
type BucketValues = BTreeMap<(i64, i64), Vec<(i64, f64)>>;

/// Tiers wait this long after a bucket ends for late samples.
pub const GRACE_MS: i64 = 90_000;
const FIVE_MIN: i64 = 5 * MINUTE;
/// The most one transaction of tier maintenance builds: a day of 5-minute buckets, a month of
/// hourly ones.
const SLICE_5M: i64 = DAY;
const SLICE_1H: i64 = 30 * DAY;
/// Query steps snap to these (ms), so charts on one page line up.
const NICE_STEPS: [i64; 17] = [
    10_000,
    15_000,
    30_000,
    60_000,
    120_000,
    300_000,
    600_000,
    900_000,
    1_800_000,
    3_600_000,
    7_200_000,
    10_800_000,
    21_600_000,
    43_200_000,
    86_400_000,
    172_800_000,
    604_800_000,
];
/// Points per series when no step is given, and the most a query returns.
const DEFAULT_POINTS: i64 = 300;
const MAX_POINTS: i64 = 2000;

/// The smallest nice step at or above `ms`.
pub fn nice_step(ms: i64) -> i64 {
    for step in NICE_STEPS {
        if step >= ms {
            return step;
        }
    }
    let week = NICE_STEPS[NICE_STEPS.len() - 1];
    week * ((ms + week - 1) / week)
}

/// The start of the `step` bucket holding `ts`, shifted by `offset` to local time.
fn floor_to(ts: i64, step: i64, offset: i64) -> i64 {
    (ts + offset).div_euclid(step) * step - offset
}

/// Which table a metric query read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Raw,
    FiveMinutes,
    Hour,
}

impl Tier {
    pub fn as_str(self) -> &'static str {
        match self {
            Tier::Raw => "raw",
            Tier::FiveMinutes => "5m",
            Tier::Hour => "1h",
        }
    }

    fn step(self) -> i64 {
        match self {
            Tier::Raw => 0,
            Tier::FiveMinutes => FIVE_MIN,
            Tier::Hour => HOUR,
        }
    }
}

/// How a bucket's readings become one value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Agg {
    Avg,
    Min,
    Max,
    /// The sum of the readings (over a tier: avg × n).
    Sum,
    /// How many readings.
    Count,
    /// The newest reading (over a tier: the newest tier bucket's average).
    Last,
}

impl Agg {
    pub fn parse(text: &str) -> Option<Agg> {
        match text {
            "avg" => Some(Agg::Avg),
            "min" => Some(Agg::Min),
            "max" => Some(Agg::Max),
            "sum" => Some(Agg::Sum),
            "count" => Some(Agg::Count),
            "last" => Some(Agg::Last),
            _ => None,
        }
    }
}

/// Which series to list. Empty fields match everything.
#[derive(Debug, Clone, Default)]
pub struct SeriesFilter {
    /// An exact name, or a glob with `*`.
    pub name: Option<String>,
    /// Labels that must be equal.
    pub matchers: BTreeMap<String, String>,
    pub source: Option<String>,
}

/// One metric series.
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesInfo {
    pub id: i64,
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub source: String,
    pub unit: Option<String>,
    pub kind: MetricKind,
    pub first_ts: Ts,
    pub last_ts: Ts,
}

/// A metric query: each matching series over [start, end) in buckets of one step.
#[derive(Debug, Clone)]
pub struct MetricQuery {
    /// An exact name, or a glob with `*`.
    pub name: String,
    pub matchers: BTreeMap<String, String>,
    pub start: Ts,
    pub end: Ts,
    /// The bucket length wanted (ms). Rounded up to a nice step, at least `min_step`.
    pub step: Option<i64>,
    pub agg: Agg,
}

/// What a metric query found.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricResult {
    pub step: i64,
    pub tier: Tier,
    pub series: Vec<SeriesPoints>,
}

/// One series' buckets: every bucket start in the range, None where it had no readings.
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesPoints {
    pub series: SeriesInfo,
    pub points: Vec<(Ts, Option<f64>)>,
}

/// A series' newest raw sample. Once its raw samples have left SQLite: the time of its last
/// sample, with the average of its newest tier bucket as the value.
#[derive(Debug, Clone, PartialEq)]
pub struct Latest {
    pub series: SeriesInfo,
    pub ts: Ts,
    pub value: f64,
}

/// What one tier maintenance did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TierReport {
    /// Buckets written going forward.
    pub built_5m: u64,
    pub built_1h: u64,
    /// Buckets recomputed because of late samples.
    pub redone_5m: u64,
    pub redone_1h: u64,
    /// Rows deleted past retention.
    pub deleted_5m: u64,
    pub deleted_1h: u64,
}

/// One bucket being aggregated.
#[derive(Debug, Clone, Copy)]
struct Acc {
    total: f64,
    n: i64,
    min: f64,
    max: f64,
    last_ts: i64,
    last: f64,
}

impl Acc {
    fn new() -> Acc {
        Acc {
            total: 0.0,
            n: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            last_ts: i64::MIN,
            last: 0.0,
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn add(&mut self, ts: i64, avg: f64, min: f64, max: f64, n: i64) {
        self.total += avg * n as f64;
        self.n += n;
        self.min = self.min.min(min);
        self.max = self.max.max(max);
        if ts >= self.last_ts {
            self.last_ts = ts;
            self.last = avg;
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn value(&self, agg: Agg) -> Option<f64> {
        if self.n == 0 {
            return None;
        }
        Some(match agg {
            Agg::Avg => self.total / self.n as f64,
            Agg::Min => self.min,
            Agg::Max => self.max,
            Agg::Sum => self.total,
            Agg::Count => self.n as f64,
            Agg::Last => self.last,
        })
    }
}

fn series_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<SeriesInfo> {
    let labels: String = r.get(2)?;
    let labels = serde_json::from_str(&labels).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let kind: String = r.get(5)?;
    Ok(SeriesInfo {
        id: r.get(0)?,
        name: r.get(1)?,
        labels,
        source: r.get(3)?,
        unit: r.get(4)?,
        kind: MetricKind::parse(&kind).unwrap_or(MetricKind::Gauge),
        first_ts: Ts(r.get(6)?),
        last_ts: Ts(r.get(7)?),
    })
}

fn find_series(conn: &Connection, filter: &SeriesFilter) -> Result<Vec<SeriesInfo>> {
    let exact = filter.name.as_ref().filter(|n| !n.contains('*'));
    let rows: Vec<SeriesInfo> = match exact {
        Some(name) => {
            let mut stmt = conn.prepare_cached(sql::SERIES_BY_NAME)?;
            stmt.query_map([name], series_from_row)?
                .collect::<rusqlite::Result<_>>()?
        }
        None => {
            let mut stmt = conn.prepare_cached(sql::SERIES_ALL)?;
            stmt.query_map([], series_from_row)?
                .collect::<rusqlite::Result<_>>()?
        }
    };
    Ok(rows
        .into_iter()
        .filter(|s| filter.name.as_ref().is_none_or(|n| glob_match(n, &s.name)))
        .filter(|s| filter.source.as_ref().is_none_or(|src| *src == s.source))
        .filter(|s| filter.matchers.iter().all(|(k, v)| s.labels.get(k) == Some(v)))
        .collect())
}

impl Store {
    /// The local time offset (ms) used to align buckets of an hour or more around `at`.
    fn local_offset(&self, at: Ts) -> i64 {
        let start = self.zone.day_start(self.zone.day_of(at)).ms();
        (-start).rem_euclid(DAY)
    }

    fn hour_floor(&self, ts: i64) -> i64 {
        self.zone.floor(Ts(ts), Every::Hour).ms()
    }

    /// Series matching `filter`, by name then labels.
    pub fn series(&self, filter: &SeriesFilter) -> Result<Vec<SeriesInfo>> {
        self.read(|tx| find_series(tx, filter))
    }

    /// The newest reading of every series matching `name` (exact or glob) and `matchers`; see
    /// [`Latest`].
    pub fn latest(&self, name: &str, matchers: &BTreeMap<String, String>) -> Result<Vec<Latest>> {
        let filter = SeriesFilter {
            name: Some(name.to_string()),
            matchers: matchers.clone(),
            source: None,
        };
        self.read(|tx| {
            let mut out = Vec::new();
            for series in find_series(tx, &filter)? {
                let row: Option<(i64, f64)> = tx
                    .prepare_cached(sql::SAMPLE_LATEST)?
                    .query_row([series.id], |r| Ok((r.get(0)?, r.get(1)?)))
                    .optional()?;
                if let Some((ts, value)) = row {
                    out.push(Latest {
                        series,
                        ts: Ts(ts),
                        value,
                    });
                    continue;
                }
                // Its raw samples have left SQLite. The series still exists, and a rule that
                // watches for silence needs to know when it last spoke.
                let mut tiered: Option<f64> = None;
                for text in [sql::TIER_5M_LATEST, sql::TIER_1H_LATEST] {
                    if tiered.is_none() {
                        tiered = tx
                            .prepare_cached(text)?
                            .query_row([series.id], |r| r.get(0))
                            .optional()?;
                    }
                }
                if let Some(value) = tiered {
                    let ts = series.last_ts;
                    out.push(Latest { series, ts, value });
                }
            }
            Ok(out)
        })
    }

    /// The average of one series' raw samples in [start, end); None when it has none there.
    pub fn window_avg(&self, series: i64, start: Ts, end: Ts) -> Result<Option<f64>> {
        self.read(|tx| {
            Ok(tx
                .prepare_cached(sql::SAMPLE_WINDOW_AVG)?
                .query_row((series, start.ms(), end.ms()), |r| r.get(0))?)
        })
    }

    /// Runs a metric query. The tier is the finest that covers the start and the step: raw samples
    /// inside the hot window, then 5-minute, then hourly; the part after a tier's last built bucket
    /// comes from the tier below.
    pub fn query(&self, q: &MetricQuery) -> Result<MetricResult> {
        let (start, end) = (q.start.ms(), q.end.ms());
        if end <= start {
            return Err(Error::input(
                "query",
                format!("end {end} is not after start {start}"),
                "pass a start before the end, in milliseconds since 1970",
            ));
        }
        let span = end - start;
        let floor = self.min_step_ms.max((span + MAX_POINTS - 1) / MAX_POINTS);
        let wanted = q.step.unwrap_or(span / DEFAULT_POINTS);
        let mut step = nice_step(wanted.max(floor));
        let filter = SeriesFilter {
            name: Some(q.name.clone()),
            matchers: q.matchers.clone(),
            source: None,
        };
        self.read(|tx| {
            let raw_from = evicted_before(tx, StoreKind::Samples)?;
            let kept_5m = get_mark(tx, "kept_5m_from")?.unwrap_or(i64::MIN);
            let done_5m = get_mark(tx, "done_5m")?.unwrap_or(i64::MIN);
            let done_1h = get_mark(tx, "done_1h")?.unwrap_or(i64::MIN);
            // A query reads from the start of its first bucket, so that bucket is whole: with an
            // hourly step, a range that starts at 10:20 has a first point for 10:00 to 11:00.
            let offset = self.local_offset(q.start);
            let from = |step: i64| floor_to(start, step, offset);
            let tier = if from(step) >= raw_from && step < FIVE_MIN {
                Tier::Raw
            } else if from(step.max(FIVE_MIN)) >= kept_5m && step < HOUR {
                Tier::FiveMinutes
            } else {
                Tier::Hour
            };
            step = step.max(tier.step());
            let first = from(step);
            let start = first;
            let clamp = |edge: i64, lo: i64| edge.clamp(lo, end);
            let parts: Vec<(&str, i64, i64)> = match tier {
                Tier::Raw => vec![(sql::SAMPLES_RANGE, start, end)],
                Tier::FiveMinutes => {
                    let e5 = clamp(done_5m, start);
                    vec![(sql::TIER_5M_RANGE, start, e5), (sql::SAMPLES_RANGE, e5, end)]
                }
                Tier::Hour => {
                    let e1 = clamp(done_1h, start);
                    let e5 = clamp(done_5m, e1);
                    vec![
                        (sql::TIER_1H_RANGE, start, e1),
                        (sql::TIER_5M_RANGE, e1, e5),
                        (sql::SAMPLES_RANGE, e5, end),
                    ]
                }
            };
            let count = usize::try_from((end - first + step - 1) / step).unwrap_or(0);
            let mut out = Vec::new();
            for series in find_series(tx, &filter)? {
                let mut buckets: Vec<Acc> = vec![Acc::new(); count];
                for (text, lo, hi) in &parts {
                    if lo >= hi {
                        continue;
                    }
                    let mut stmt = tx.prepare_cached(text)?;
                    let raw = *text == sql::SAMPLES_RANGE;
                    let mut rows = stmt.query((series.id, lo, hi))?;
                    while let Some(r) = rows.next()? {
                        let ts: i64 = r.get(0)?;
                        let Some(acc) = usize::try_from((floor_to(ts, step, offset) - first) / step)
                            .ok()
                            .and_then(|i| buckets.get_mut(i))
                        else {
                            continue;
                        };
                        if raw {
                            let v: f64 = r.get(1)?;
                            acc.add(ts, v, v, v, 1);
                        } else {
                            acc.add(ts, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?);
                        }
                    }
                }
                let points = buckets
                    .iter()
                    .enumerate()
                    .map(|(i, acc)| (Ts(first + i as i64 * step), acc.value(q.agg)))
                    .collect();
                out.push(SeriesPoints { series, points });
            }
            Ok(MetricResult {
                step,
                tier,
                series: out,
            })
        })
    }

    // ---------- maintenance ----------

    /// Builds closed 5-minute and hourly buckets, recomputes dirty ones, and deletes tier rows
    /// past `retention.samples_5m` / `samples_1h`. Run it before [`Store::archive`].
    ///
    /// A long backlog (a first run after an import, or after days stopped) is built a slice at a
    /// time, and the write lock is free between slices, so ingest never waits for the whole of it.
    pub fn maintain_tiers(&self, now: Ts) -> Result<TierReport> {
        let mut report = TierReport::default();
        while self.maintain_slice(now, &mut report)? {}
        Ok(report)
    }

    /// One transaction of [`Store::maintain_tiers`]. True when there is more to build.
    fn maintain_slice(&self, now: Ts, report: &mut TierReport) -> Result<bool> {
        let mut guard = self.writer()?;
        let w: &mut Writer = &mut guard;
        let mut more = false;
        let tx = w.begin()?;
        let series: Vec<(i64, i64, i64)> = w_series(&tx)?;
        let raw_from = evicted_before(&tx, StoreKind::Samples)?;
        let kept_5m = get_mark(&tx, "kept_5m_from")?.unwrap_or(i64::MIN);

        // 5 minutes, from raw samples.
        let done_5m = get_mark(&tx, "done_5m")?;
        let done_1h = get_mark(&tx, "done_1h")?;
        let limit_5m = floor_to(now.ms() - GRACE_MS, FIVE_MIN, 0);
        let dirty_5m = dirty(&tx, "5m")?;
        let redo = self.raw_values(&tx, &dirty_5m, raw_from, sql::TIER_5M_HAS, |b| b + FIVE_MIN)?;
        for ((id, b), values) in redo {
            write_bucket(&tx, sql::TIER_5M_PUT, sql::TIER_5M_DELETE, id, b, &values)?;
            report.redone_5m += 1;
            if done_1h.is_some_and(|d| b < d) {
                tx.prepare_cached(sql::DIRTY_MARK)?
                    .execute(("1h", id, self.hour_floor(b)))?;
            }
        }
        tx.prepare_cached(sql::DIRTY_CLEAR)?.execute(["5m"])?;
        let start_5m = done_5m
            .or_else(|| series.iter().map(|s| s.1).min().map(|t| floor_to(t, FIVE_MIN, 0)))
            .unwrap_or(limit_5m);
        if start_5m.saturating_add(SLICE_5M) < limit_5m {
            more = true;
        }
        let limit_5m = limit_5m.min(start_5m.saturating_add(SLICE_5M));
        let done_5m = if start_5m < limit_5m {
            let mut stmt = tx.prepare_cached(sql::TIER_5M_BUILD)?;
            for &(id, first, last) in &series {
                if last >= start_5m && first < limit_5m {
                    report.built_5m += stmt.execute((id, start_5m, limit_5m))? as u64;
                }
            }
            set_mark(&tx, "done_5m", limit_5m)?;
            limit_5m
        } else {
            done_5m.unwrap_or(limit_5m)
        };

        // Hourly, from the 5-minute tier (or raw samples where that has been pruned).
        let limit_1h = self.hour_floor(now.ms() - GRACE_MS).min(self.hour_floor(done_5m));
        let dirty_1h = dirty(&tx, "1h")?;
        let (old, recent): (Vec<_>, Vec<_>) = dirty_1h.into_iter().partition(|(_, b)| *b < kept_5m);
        for (id, b) in recent {
            let hi = self.zone.next(Ts(b), Every::Hour).ms();
            let row: (Option<f64>, Option<i64>, Option<f64>, Option<f64>) = tx
                .prepare_cached(sql::TIER_5M_SUM)?
                .query_row((id, b, hi), |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?;
            match row {
                (Some(total), Some(n), Some(min), Some(max)) if n > 0 => {
                    #[allow(clippy::cast_precision_loss)]
                    let avg = total / n as f64;
                    tx.prepare_cached(sql::TIER_1H_PUT)?
                        .execute((id, b, avg, min, max, n))?;
                }
                _ => {
                    tx.prepare_cached(sql::TIER_1H_DELETE)?.execute((id, b))?;
                }
            }
            report.redone_1h += 1;
        }
        let redo = self.raw_values(&tx, &old, raw_from, sql::TIER_1H_HAS, |b| {
            self.zone.next(Ts(b), Every::Hour).ms()
        })?;
        for ((id, b), values) in redo {
            write_bucket(&tx, sql::TIER_1H_PUT, sql::TIER_1H_DELETE, id, b, &values)?;
            report.redone_1h += 1;
        }
        tx.prepare_cached(sql::DIRTY_CLEAR)?.execute(["1h"])?;
        let start_1h = done_1h
            .or_else(|| series.iter().map(|s| s.1).min().map(|t| self.hour_floor(t)))
            .unwrap_or(limit_1h);
        let sliced = self.hour_floor(start_1h.saturating_add(SLICE_1H));
        if sliced < limit_1h {
            more = true;
        }
        let limit_1h = limit_1h.min(sliced);
        let done_1h = if start_1h < limit_1h {
            let offset = self.zone.hour_offset_ms(Ts(start_1h));
            let mut stmt = tx.prepare_cached(sql::TIER_1H_BUILD)?;
            for &(id, first, last) in &series {
                if last >= start_1h && first < limit_1h {
                    report.built_1h += stmt.execute((id, start_1h, limit_1h, offset))? as u64;
                }
            }
            set_mark(&tx, "done_1h", limit_1h)?;
            limit_1h
        } else {
            done_1h.unwrap_or(limit_1h)
        };

        // Retention. 5-minute rows stay until the hourly tier has them.
        if let Some(keep) = self.retention.samples_5m {
            let cutoff = (now.ms() - keep).min(done_1h);
            if cutoff > kept_5m {
                let mut stmt = tx.prepare_cached(sql::TIER_5M_EXPIRE)?;
                for &(id, _, _) in &series {
                    report.deleted_5m += stmt.execute((id, cutoff))? as u64;
                }
                set_mark(&tx, "kept_5m_from", cutoff)?;
            }
        }
        if let Some(keep) = self.retention.samples_1h {
            let cutoff = now.ms() - keep;
            let mut stmt = tx.prepare_cached(sql::TIER_1H_EXPIRE)?;
            for &(id, _, _) in &series {
                report.deleted_1h += stmt.execute((id, cutoff))? as u64;
            }
        }
        tx.commit()
            .map_err(|e| Error::sql("can't commit tier maintenance", &e))?;
        if report.deleted_5m + report.deleted_1h > 0 {
            guard
                .conn
                .execute_batch("PRAGMA incremental_vacuum")
                .map_err(|e| Error::sql("can't vacuum", &e))?;
        }
        Ok(more)
    }

    /// The raw samples of each (series, bucket start) in `buckets`: SQLite's, over the day file's
    /// where the bucket is older than `raw_from` (SQLite's value wins at the same ts).
    ///
    /// A bucket that is already built (`has` finds its row) and whose day file is gone, deleted
    /// by the cap, is left out: the samples it was built from can't be read any more, and the few
    /// that arrived late would replace a whole bucket with a part of one.
    fn raw_values(
        &self,
        conn: &Connection,
        buckets: &[(i64, i64)],
        raw_from: i64,
        has: &str,
        end_of: impl Fn(i64) -> i64,
    ) -> Result<BucketValues> {
        let mut out: BTreeMap<(i64, i64), BTreeMap<i64, f64>> = BTreeMap::new();
        // Cold buckets, by day: one pass over each day file.
        let mut by_day: HashMap<Day, Vec<(i64, i64, i64)>> = HashMap::new();
        for &(id, b) in buckets {
            out.insert((id, b), BTreeMap::new());
            if b < raw_from {
                by_day
                    .entry(self.zone.day_of(Ts(b)))
                    .or_default()
                    .push((id, b, end_of(b)));
            }
        }
        for (day, wanted) in by_day {
            let path = self.day_path(StoreKind::Samples.as_str(), day);
            let lo = wanted.iter().map(|w| w.0).min().unwrap_or(0);
            let hi = wanted.iter().map(|w| w.0).max().unwrap_or(0);
            let Some(mut rows) =
                FileRows::<SampleRow>::open_if_exists(&path, Some(Skip::Int { column: 0, lo, hi }))?
            else {
                for (id, b, _) in wanted {
                    let built: Option<i64> = conn
                        .prepare_cached(has)?
                        .query_row((id, b), |r| r.get(0))
                        .optional()?;
                    if built.is_some() {
                        out.remove(&(id, b));
                    }
                }
                continue;
            };
            let mut index: HashMap<i64, Vec<(i64, i64)>> = HashMap::new();
            for (id, b, e) in wanted {
                index.entry(id).or_default().push((b, e));
            }
            while let Some(row) = rows.next_row()? {
                if let Some(ranges) = index.get(&row.series) {
                    for (b, e) in ranges {
                        if (*b..*e).contains(&row.ts)
                            && let Some(values) = out.get_mut(&(row.series, *b))
                        {
                            values.insert(row.ts, row.value);
                        }
                    }
                }
            }
        }
        let mut stmt = conn.prepare_cached(sql::SAMPLES_RANGE)?;
        for ((id, b), values) in &mut out {
            let rows = stmt.query_map((*id, *b, end_of(*b)), |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, f64>(1)?))
            })?;
            for row in rows {
                let (ts, v) = row?;
                values.insert(ts, v);
            }
        }
        Ok(out
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().collect()))
            .collect())
    }
}

/// Every series' (id, first_ts, last_ts).
fn w_series(conn: &Connection) -> Result<Vec<(i64, i64, i64)>> {
    let mut stmt = conn.prepare_cached(sql::SERIES_SPANS)?;
    let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// The dirty buckets of one tier.
fn dirty(conn: &Connection, tier: &str) -> Result<Vec<(i64, i64)>> {
    let mut stmt = conn.prepare_cached(sql::DIRTY_LIST)?;
    let rows = stmt.query_map([tier], |r| Ok((r.get(0)?, r.get(1)?)))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Writes one tier bucket from raw values, or deletes it when there are none.
fn write_bucket(
    conn: &Connection,
    put: &str,
    delete: &str,
    id: i64,
    b: i64,
    values: &[(i64, f64)],
) -> Result<()> {
    if values.is_empty() {
        conn.prepare_cached(delete)?.execute((id, b))?;
        return Ok(());
    }
    let mut acc = Acc::new();
    for &(ts, v) in values {
        acc.add(ts, v, v, v, 1);
    }
    let avg = acc.value(Agg::Avg).unwrap_or(0.0);
    conn.prepare_cached(put)?
        .execute((id, b, avg, acc.min, acc.max, acc.n))?;
    Ok(())
}
