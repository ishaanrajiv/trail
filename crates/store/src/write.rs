//! Writes: one batch of records per transaction, error groups, problems.

use crate::records::{error_params, event_params, log_params};
use crate::{Error, Result};
use crate::{SeriesKey, SeriesState, Store, StoreKind, Writer, get_mark, set_mark, sql};
use rusqlite::{OptionalExtension, Transaction};
use std::collections::HashSet;
use trail_core::Ts;
use trail_core::envelope::{ErrorRecord, Event, LogRecord, Point};
use trail_core::time::{Day, Every, MINUTE};

/// The mark that holds the last seq given to an event, error or log.
pub(crate) const SEQ_MARK: &str = "seq";

/// One metric reading with where it came from.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricPoint {
    pub source: String,
    pub point: Point,
    /// A running total: stored as the per-second rate since the previous reading, with the total
    /// and the increase beside it.
    pub counter: bool,
}

/// A record that differed from its declaration, for the drift report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mismatch {
    pub source: String,
    /// `event`, `metric`, … : what kind of record it was.
    pub kind: String,
    /// The record's name.
    pub name: String,
    pub message: String,
    /// When it was seen.
    pub seen: Ts,
}

/// Everything one write stores, in one transaction.
#[derive(Debug, Clone, Default)]
pub struct Batch {
    pub events: Vec<Event>,
    pub points: Vec<MetricPoint>,
    pub errors: Vec<ErrorRecord>,
    pub logs: Vec<LogRecord>,
    pub mismatches: Vec<Mismatch>,
}

/// What became of one metric reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointOutcome {
    Stored,
    /// A counter's first reading (or first since its kind changed): kept only as the next rate's
    /// base.
    Baseline,
    /// A counter reading at or before the previous one: a rate needs readings in order.
    OutOfOrder,
    /// It would have started a new series, and `limits.series` are already kept.
    TooManySeries,
}

impl PointOutcome {
    /// False for a point that was not stored and never will be: the sender must be told.
    pub fn kept(self) -> bool {
        matches!(self, PointOutcome::Stored | PointOutcome::Baseline)
    }
}

/// Per record, in batch order: true when it was newly stored, false for a duplicate: a record
/// whose source already stored its id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Written {
    pub events: Vec<bool>,
    pub points: Vec<PointOutcome>,
    pub errors: Vec<bool>,
    pub logs: Vec<bool>,
}

impl Written {
    /// Records whose source had already stored their id.
    pub fn duplicates(&self) -> usize {
        [&self.events, &self.errors, &self.logs]
            .iter()
            .map(|v| v.iter().filter(|new| !**new).count())
            .sum()
    }
}

/// An error group's triage state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorStatus {
    Open,
    Resolved,
    Ignored,
}

impl ErrorStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorStatus::Open => "open",
            ErrorStatus::Resolved => "resolved",
            ErrorStatus::Ignored => "ignored",
        }
    }

    pub fn parse(text: &str) -> Option<ErrorStatus> {
        match text {
            "open" => Some(ErrorStatus::Open),
            "resolved" => Some(ErrorStatus::Resolved),
            "ignored" => Some(ErrorStatus::Ignored),
            _ => None,
        }
    }
}

/// Repeats of one error, grouped by fingerprint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorGroup {
    pub fingerprint: String,
    pub source: String,
    pub kind: String,
    /// The first occurrence's message.
    pub message: String,
    pub first_seen: Ts,
    pub last_seen: Ts,
    pub count: u64,
    pub status: ErrorStatus,
}

/// One row of the drift report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProblemRow {
    pub source: String,
    pub kind: String,
    pub name: String,
    pub message: String,
    pub first_seen: Ts,
    pub last_seen: Ts,
    pub count: u64,
}

/// Marks days that already have a file when one of their rows arrives.
struct Late<'a> {
    store: &'a Store,
    kind: StoreKind,
    filed: Option<&'a std::collections::BTreeSet<Day>>,
    /// Rows before this may belong to a filed day.
    edge: i64,
    marked: HashSet<Day>,
}

impl<'a> Late<'a> {
    fn new(
        store: &'a Store,
        kind: StoreKind,
        filed: Option<&'a std::collections::BTreeSet<Day>>,
    ) -> Late<'a> {
        let edge = filed
            .and_then(|days| days.last())
            .map_or(i64::MIN, |day| store.zone.day_end(*day).ms());
        Late {
            store,
            kind,
            filed,
            edge,
            marked: HashSet::new(),
        }
    }

    fn note(&mut self, tx: &Transaction<'_>, ts: Ts) -> Result<()> {
        if ts.ms() >= self.edge {
            return Ok(());
        }
        let day = self.store.zone.day_of(ts);
        if self.filed.is_some_and(|days| days.contains(&day)) && self.marked.insert(day) {
            tx.prepare_cached(sql::LATE_MARK)?
                .execute((self.kind.as_str(), day.to_string()))?;
        }
        Ok(())
    }
}

impl Store {
    /// Stores a batch in one transaction. Events, errors and logs are kept once per (source, id).
    pub fn write(&self, batch: &Batch) -> Result<Written> {
        let mut guard = self.writer()?;
        let result = self.write_locked(&mut guard, batch);
        if result.is_err() {
            // The transaction rolled back; the in-memory series may be ahead of it.
            guard.reload()?;
        }
        result
    }

    fn write_locked(&self, w: &mut Writer, batch: &Batch) -> Result<Written> {
        let Writer { conn, series, filed } = w;
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|e| Error::sql("can't start a write", &e))?;
        let mut written = Written::default();
        // One counter for the whole server, given out under the write lock and kept in the same
        // transaction: seq rises in commit order and is never given twice.
        let first_seq = get_mark(&tx, SEQ_MARK)?.unwrap_or(0) + 1;
        let mut seq = first_seq;

        let mut late = Late::new(self, StoreKind::Events, filed.get(&StoreKind::Events));
        {
            let mut stmt = tx.prepare_cached(sql::EVENT_INSERT)?;
            for event in &batch.events {
                let new = stmt.execute(rusqlite::params_from_iter(event_params(event, seq)))? == 1;
                if new {
                    seq += 1;
                    late.note(&tx, event.ts)?;
                }
                written.events.push(new);
            }
        }

        let mut late = Late::new(self, StoreKind::Errors, filed.get(&StoreKind::Errors));
        {
            let mut insert = tx.prepare_cached(sql::ERROR_INSERT)?;
            let mut group = tx.prepare_cached(sql::ERROR_GROUP_UPSERT)?;
            for error in &batch.errors {
                let new = insert.execute(rusqlite::params_from_iter(error_params(error, seq)))? == 1;
                if new {
                    seq += 1;
                    group.execute((
                        &error.fingerprint,
                        &error.source,
                        &error.kind,
                        &error.message,
                        error.ts.ms(),
                    ))?;
                    late.note(&tx, error.ts)?;
                }
                written.errors.push(new);
            }
        }

        let mut late = Late::new(self, StoreKind::Logs, filed.get(&StoreKind::Logs));
        {
            let mut stmt = tx.prepare_cached(sql::LOG_INSERT)?;
            for log in &batch.logs {
                let new = stmt.execute(rusqlite::params_from_iter(log_params(log, seq)))? == 1;
                if new {
                    seq += 1;
                    late.note(&tx, log.ts)?;
                }
                written.logs.push(new);
            }
        }

        if !batch.points.is_empty() {
            let mut late = Late::new(self, StoreKind::Samples, filed.get(&StoreKind::Samples));
            written.points = self.write_points(&tx, series, &batch.points, &mut late)?;
        }

        if seq > first_seq {
            set_mark(&tx, SEQ_MARK, seq - 1)?;
        }

        {
            let mut stmt = tx.prepare_cached(sql::PROBLEM_UPSERT)?;
            for m in &batch.mismatches {
                stmt.execute((&m.source, &m.kind, &m.name, &m.message, m.seen.ms()))?;
            }
        }
        tx.commit().map_err(|e| Error::sql("can't commit a write", &e))?;
        Ok(written)
    }

    fn write_points(
        &self,
        tx: &Transaction<'_>,
        cache: &mut std::collections::HashMap<SeriesKey, SeriesState>,
        points: &[MetricPoint],
        late: &mut Late<'_>,
    ) -> Result<Vec<PointOutcome>> {
        let done_5m = get_mark(tx, "done_5m")?;
        let done_1h = get_mark(tx, "done_1h")?;
        let mut outcomes = Vec::with_capacity(points.len());
        let mut touched: HashSet<SeriesKey> = HashSet::new();
        let mut insert_series = tx.prepare_cached(sql::SERIES_INSERT)?;
        let mut insert_sample = tx.prepare_cached(sql::SAMPLE_UPSERT)?;
        let mut dirty = tx.prepare_cached(sql::DIRTY_MARK)?;
        for mp in points {
            let p = &mp.point;
            let labels = serde_json::to_string(&p.labels).map_err(|e| {
                Error::input(
                    &format!("metric {}", p.name),
                    format!("labels can't be written as JSON: {e}"),
                    "send labels as a string-to-string object",
                )
            })?;
            // A series belongs to its source: another source's reading under the same name and
            // labels is a series of its own.
            let key = (mp.source.clone(), p.name.clone(), labels);
            let ts = p.ts.ms();
            if !cache.contains_key(&key) {
                if cache.len() >= self.max_series {
                    outcomes.push(PointOutcome::TooManySeries);
                    continue;
                }
                let kind = if mp.counter { "counter" } else { "gauge" };
                insert_series.execute((&key.1, &key.2, &key.0, &p.unit, kind, ts, ts))?;
                let id = tx.last_insert_rowid();
                cache.insert(
                    key.clone(),
                    SeriesState {
                        id,
                        counter: mp.counter,
                        unit: p.unit.clone(),
                        first_ts: ts,
                        last_ts: ts,
                        last_raw: None,
                        last_raw_ts: None,
                    },
                );
            }
            let Some(s) = cache.get_mut(&key) else {
                continue;
            };
            touched.insert(key.clone());
            if s.counter != mp.counter {
                // The declaration changed kind: start a fresh baseline.
                s.counter = mp.counter;
                s.last_raw = None;
                s.last_raw_ts = None;
            }
            if p.unit.is_some() && p.unit != s.unit {
                s.unit.clone_from(&p.unit);
            }
            let mut value = p.value;
            let mut total = None;
            let mut increase = None;
            if s.counter {
                if s.last_raw_ts.is_some_and(|prev| ts <= prev) {
                    outcomes.push(PointOutcome::OutOfOrder);
                    continue;
                }
                let previous = s.last_raw.zip(s.last_raw_ts);
                s.last_raw = Some(p.value);
                s.last_raw_ts = Some(ts);
                let Some((prev, prev_ts)) = previous else {
                    outcomes.push(PointOutcome::Baseline);
                    continue;
                };
                // A reading below the one before is a reset: the counter started again from
                // zero, so all of it is new since then (as Prometheus counts it).
                let up = if p.value >= prev { p.value - prev } else { p.value };
                #[allow(clippy::cast_precision_loss)]
                let seconds = (ts - prev_ts) as f64 / 1000.0;
                value = up / seconds;
                total = Some(p.value);
                increase = Some(up);
            }
            insert_sample.execute((s.id, ts, value, total, increase))?;
            s.first_ts = s.first_ts.min(ts);
            s.last_ts = s.last_ts.max(ts);
            if done_5m.is_some_and(|done| ts < done) {
                dirty.execute(("5m", s.id, ts - ts.rem_euclid(5 * MINUTE)))?;
            }
            if done_1h.is_some_and(|done| ts < done) {
                dirty.execute(("1h", s.id, self.zone.floor(p.ts, Every::Hour).ms()))?;
            }
            late.note(tx, p.ts)?;
            outcomes.push(PointOutcome::Stored);
        }
        let mut update = tx.prepare_cached(sql::SERIES_UPDATE)?;
        for key in touched {
            if let Some(s) = cache.get(&key) {
                let kind = if s.counter { "counter" } else { "gauge" };
                update.execute((
                    &s.unit,
                    kind,
                    s.first_ts,
                    s.last_ts,
                    s.last_raw,
                    s.last_raw_ts,
                    s.id,
                ))?;
            }
        }
        Ok(outcomes)
    }

    // ---------- error groups ----------

    /// Sets an error group's status. False when no group has that fingerprint.
    pub fn set_error_status(&self, fingerprint: &str, status: ErrorStatus) -> Result<bool> {
        let w = self.writer()?;
        let n = w
            .conn
            .prepare_cached(sql::ERROR_GROUP_STATUS)?
            .execute((status.as_str(), fingerprint))?;
        Ok(n > 0)
    }

    /// One error group by fingerprint.
    pub fn error_group(&self, fingerprint: &str) -> Result<Option<ErrorGroup>> {
        self.read(|tx| {
            Ok(tx
                .prepare_cached(sql::ERROR_GROUP_ONE)?
                .query_row([fingerprint], crate::records::group_from_row)
                .optional()?)
        })
    }

    // ---------- problems ----------

    /// The drift report: every problem, or one source's, newest first.
    pub fn problems(&self, source: Option<&str>) -> Result<Vec<ProblemRow>> {
        self.read(|tx| {
            let mut stmt = tx.prepare_cached(sql::PROBLEMS_LIST)?;
            let rows = stmt.query_map([source], |r| {
                Ok(ProblemRow {
                    source: r.get(0)?,
                    kind: r.get(1)?,
                    name: r.get(2)?,
                    message: r.get(3)?,
                    first_seen: Ts(r.get(4)?),
                    last_seen: Ts(r.get(5)?),
                    count: u64::try_from(r.get::<_, i64>(6)?).unwrap_or(0),
                })
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// Clears one source's problems (after it publishes new declarations). Returns rows removed.
    pub fn clear_problems(&self, source: &str) -> Result<usize> {
        let w = self.writer()?;
        Ok(w.conn.prepare_cached(sql::PROBLEMS_CLEAR)?.execute([source])?)
    }
}
