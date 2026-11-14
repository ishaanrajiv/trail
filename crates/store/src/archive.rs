//! Day files: writing finished days, merging late rows, evicting rows from SQLite, retention and
//! caps. `spec/storage.md` ("Day files") is the contract.
//!
//! An archive run claims the days it will write under the write lock (so a row that arrives for
//! one while its file is written is marked late), writes each from one read snapshot, then evicts
//! the days that have a verified file, are outside the hot window and got nothing late.

use crate::parquet_io::{ArchRow, DayWriter, FileRows, Kept, Keys, Merge, SampleRow, Seqs, file_keys};
use crate::records::{error_from_row, event_from_row, log_from_row};
use crate::{Error, Result, Store, StoreKind, Writer, evicted_before, get_mark, set_mark, sql};
use rusqlite::{Connection, Row, Transaction};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use trail_core::Ts;
use trail_core::decl::Retention;
use trail_core::time::{DAY, Day};

/// Most rows kept in `problems` and in `error_groups`; [`Store::expire`] drops the ones seen
/// longest ago. A sender whose messages or fingerprints never repeat fills neither without end.
const MAX_PROBLEMS: i64 = 10_000;
const MAX_ERROR_GROUPS: i64 = 50_000;

/// One day file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DayFile {
    pub store: StoreKind,
    /// Its folder under `archive/`: `events`, `events-30d`, `samples`, `errors`, `logs`.
    pub class: String,
    pub day: Day,
    pub rows: u64,
    pub bytes: u64,
    /// The largest seq among its rows; None when they have none (a file written before seq).
    pub max_seq: Option<i64>,
}

/// What one archive run did.
#[derive(Debug, Clone, Default)]
pub struct ArchiveReport {
    /// Files written or rewritten (merged with late rows).
    pub written: Vec<DayFile>,
    /// Rows deleted from SQLite because their day is in a file and outside the hot window.
    pub evicted: u64,
    /// Days that could not be written; their rows stay in SQLite and the next run tries again.
    pub failures: Vec<Error>,
}

/// What one retention and caps run did.
#[derive(Debug, Clone, Default)]
pub struct ExpireReport {
    /// Files deleted, past retention or to fit a cap.
    pub deleted: Vec<DayFile>,
    /// Rows deleted from SQLite (past retention, or of a day deleted to fit a cap).
    pub rows: u64,
    /// Files that could not be removed from disk (their rows are already forgotten).
    pub failures: Vec<Error>,
}

fn days_of(conn: &Connection, text: &str, kind: StoreKind) -> Result<BTreeSet<Day>> {
    let mut stmt = conn.prepare_cached(text)?;
    let rows = stmt.query_map([kind.as_str()], |r| r.get::<_, String>(0))?;
    let mut out = BTreeSet::new();
    for row in rows {
        if let Ok(day) = row?.parse::<Day>() {
            out.insert(day);
        }
    }
    Ok(out)
}

fn min_ts_sql(kind: StoreKind) -> &'static str {
    match kind {
        StoreKind::Events => sql::EVENTS_MIN_TS,
        StoreKind::Errors => sql::ERRORS_MIN_TS,
        StoreKind::Logs => sql::LOGS_MIN_TS,
        StoreKind::Samples => sql::SAMPLES_MIN_TS,
    }
}

fn series_ids(conn: &Connection) -> Result<Vec<i64>> {
    let mut stmt = conn.prepare_cached(sql::SERIES_IDS)?;
    let rows = stmt.query_map([], |r| r.get(0))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

impl Store {
    /// Writes the day file of every finished local day that has rows in SQLite and no file, or rows
    /// newer than its file (merged with it), then evicts rows whose day has a file and is outside
    /// the hot window. `retention` gives an event name's retention class. Run after
    /// [`Store::maintain_tiers`]: raw samples leave only once the 5-minute tier has them.
    pub fn archive(&self, now: Ts, retention: &dyn Fn(&str) -> Retention) -> Result<ArchiveReport> {
        let today = self.zone.day_of(now);
        let today_start = self.zone.day_start(today).ms();
        let hot_start = self.zone.day_start(self.days_before(today, self.hot_days)).ms();
        let mut report = ArchiveReport::default();
        for kind in StoreKind::ALL {
            self.archive_kind(kind, today, today_start, hot_start, retention, &mut report)?;
        }
        if report.evicted > 0 {
            self.vacuum()?;
        }
        Ok(report)
    }

    fn archive_kind(
        &self,
        kind: StoreKind,
        today: Day,
        today_start: i64,
        hot_start: i64,
        retention: &dyn Fn(&str) -> Retention,
        report: &mut ArchiveReport,
    ) -> Result<()> {
        let (days, late, filed, done_5m) = self.read(|tx| {
            Ok((
                self.days_with_rows(tx, kind, today_start)?,
                days_of(tx, sql::LATE_LIST, kind)?,
                days_of(tx, sql::ARCHIVE_STORE_DAYS, kind)?,
                get_mark(tx, "done_5m")?,
            ))
        })?;
        let mut candidates: BTreeSet<Day> = days.iter().copied().collect();
        candidates.extend(late.iter().copied());
        let to_write: Vec<Day> = candidates
            .into_iter()
            .filter(|d| *d < today && (!filed.contains(d) || late.contains(d)))
            .collect();

        self.claim(kind, &to_write)?;

        let mut failed = BTreeSet::new();
        for day in to_write {
            let written = self.read(|tx| self.write_day(tx, kind, day, retention));
            let mut guard = self.writer()?;
            match written {
                Ok(files) => {
                    let tx = guard.begin()?;
                    for f in &files {
                        tx.prepare_cached(sql::ARCHIVE_PUT)?.execute((
                            kind.as_str(),
                            &f.class,
                            day.to_string(),
                            i64::try_from(f.rows).unwrap_or(i64::MAX),
                            i64::try_from(f.bytes).unwrap_or(i64::MAX),
                            f.max_seq,
                        ))?;
                    }
                    // The file holds every row the claim saw. A day marked late again since
                    // (claimed set back to 0) keeps its mark.
                    tx.prepare_cached(sql::LATE_DONE)?
                        .execute((kind.as_str(), day.to_string()))?;
                    tx.commit()
                        .map_err(|e| Error::sql("can't record a day file", &e))?;
                    report.written.extend(files);
                }
                Err(e) => {
                    failed.insert(day);
                    if filed.contains(&day) {
                        // Still late: its file lacks rows SQLite has.
                        guard
                            .conn
                            .prepare_cached(sql::LATE_MARK)?
                            .execute((kind.as_str(), day.to_string()))?;
                    } else if let Some(set) = guard.filed.get_mut(&kind) {
                        set.remove(&day);
                    }
                    report.failures.push(e);
                }
            }
        }

        // Evict. Raw samples also wait for the 5-minute tier.
        let limit = match kind {
            StoreKind::Samples => {
                let tiered = done_5m.map_or(i64::MIN, |d| self.zone.day_start(self.zone.day_of(Ts(d))).ms());
                hot_start.min(tiered)
            }
            _ => hot_start,
        };
        if limit == i64::MIN {
            return Ok(());
        }
        let mut guard = self.writer()?;
        let Writer { conn, filed: mem, .. } = &mut *guard;
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|e| Error::sql("can't start a write", &e))?;
        let pending = days_of(&tx, sql::LATE_LIST, kind)?;
        let filed_now = mem.get(&kind).cloned().unwrap_or_default();
        for day in days {
            if self.zone.day_end(day).ms() <= limit
                && filed_now.contains(&day)
                && !pending.contains(&day)
                && !failed.contains(&day)
            {
                report.evicted += self.delete_day_rows(&tx, kind, day)?;
            }
        }
        if limit > evicted_before(&tx, kind)? {
            set_mark(&tx, kind.evicted_key(), limit)?;
        }
        tx.commit()
            .map_err(|e| Error::sql("can't evict archived rows", &e))?;
        Ok(())
    }

    /// Claims days for writing: from here a new row for one of them is marked late, and a late
    /// mark already there is kept as claimed until the day's new file is in place. If the run
    /// dies before that, the mark is still there and the next run writes the day again.
    pub(crate) fn claim(&self, kind: StoreKind, days: &[Day]) -> Result<()> {
        if days.is_empty() {
            return Ok(());
        }
        let mut guard = self.writer()?;
        let claimed = (|| {
            let Writer { conn, filed: mem, .. } = &mut *guard;
            let tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(|e| Error::sql("can't start a write", &e))?;
            for day in days {
                mem.entry(kind).or_default().insert(*day);
                tx.prepare_cached(sql::LATE_CLAIM)?
                    .execute((kind.as_str(), day.to_string()))?;
            }
            tx.commit()
                .map_err(|e| Error::sql("can't claim days to archive", &e))
        })();
        if let Err(e) = claimed {
            guard.reload()?;
            return Err(e);
        }
        Ok(())
    }

    /// The local days before `before` that have rows of `kind` in SQLite, oldest first.
    fn days_with_rows(&self, conn: &Connection, kind: StoreKind, before: i64) -> Result<Vec<Day>> {
        let ids = if kind == StoreKind::Samples {
            series_ids(conn)?
        } else {
            Vec::new()
        };
        let mut stmt = conn.prepare_cached(min_ts_sql(kind))?;
        let mut out = Vec::new();
        let mut cursor = i64::MIN;
        loop {
            let first: Option<i64> = if kind == StoreKind::Samples {
                let mut best: Option<i64> = None;
                for id in &ids {
                    let ts: Option<i64> = stmt.query_row((id, cursor, before), |r| r.get(0))?;
                    best = match (best, ts) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (a, b) => a.or(b),
                    };
                }
                best
            } else {
                stmt.query_row((cursor, before), |r| r.get(0))?
            };
            let Some(ts) = first else { break };
            let day = self.zone.day_of(Ts(ts));
            out.push(day);
            cursor = self.zone.day_end(day).ms();
        }
        Ok(out)
    }

    /// Deletes one day's rows of `kind` from SQLite. Returns how many.
    fn delete_day_rows(&self, conn: &Connection, kind: StoreKind, day: Day) -> Result<u64> {
        let (lo, hi) = (self.zone.day_start(day).ms(), self.zone.day_end(day).ms());
        let n = match kind {
            StoreKind::Events => conn.prepare_cached(sql::EVENTS_DELETE_RANGE)?.execute((lo, hi))?,
            StoreKind::Errors => conn.prepare_cached(sql::ERRORS_DELETE_RANGE)?.execute((lo, hi))?,
            StoreKind::Logs => conn.prepare_cached(sql::LOGS_DELETE_RANGE)?.execute((lo, hi))?,
            StoreKind::Samples => {
                let mut stmt = conn.prepare_cached(sql::SAMPLES_DELETE_RANGE)?;
                let mut n = 0;
                for id in series_ids(conn)? {
                    n += stmt.execute((id, lo, hi))?;
                }
                n
            }
        };
        Ok(n as u64)
    }

    /// Writes (or rewrites, merged) one day's file(s) of `kind` from one read snapshot.
    fn write_day(
        &self,
        tx: &Transaction<'_>,
        kind: StoreKind,
        day: Day,
        retention: &dyn Fn(&str) -> Retention,
    ) -> Result<Vec<DayFile>> {
        let (lo, hi) = (self.zone.day_start(day).ms(), self.zone.day_end(day).ms());
        let existing: BTreeSet<String> = {
            let mut stmt = tx.prepare_cached(sql::ARCHIVE_DAY_CLASSES)?;
            let rows = stmt.query_map((kind.as_str(), day.to_string()), |r| r.get::<_, String>(0))?;
            rows.collect::<rusqlite::Result<_>>()?
        };
        let mut old_keys = Keys::default();
        if kind != StoreKind::Samples {
            for class in &existing {
                let path = self.day_path(class, day);
                if path.exists() {
                    old_keys.extend(file_keys(&path)?);
                }
            }
        }
        // The seq SQLite holds for records the files hold too: a file written before seq
        // existed gives its rows none, and they get SQLite's here.
        let seqs_sql = match kind {
            StoreKind::Events => Some(sql::EVENT_SEQS_DAY),
            StoreKind::Errors => Some(sql::ERROR_SEQS_DAY),
            StoreKind::Logs => Some(sql::LOG_SEQS_DAY),
            StoreKind::Samples => None,
        };
        let mut seqs = Seqs::default();
        if let Some(text) = seqs_sql.filter(|_| !existing.is_empty()) {
            let mut stmt = tx.prepare_cached(text)?;
            let rows = stmt.query_map((lo, hi), |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get(2)?))
            })?;
            for row in rows {
                let (source, id, seq) = row?;
                if old_keys.contains(&source, &id) {
                    seqs.insert(source, id, seq);
                }
            }
        }
        let mut files = Vec::new();
        match kind {
            StoreKind::Events => {
                let class_of = |name: &str| match retention(name) {
                    Retention::Forever => "events".to_string(),
                    Retention::Days(n) => format!("events-{n}d"),
                };
                let mut by_name: HashMap<String, String> = HashMap::new();
                {
                    let mut stmt = tx.prepare_cached(sql::EVENT_NAMES_RANGE)?;
                    for name in stmt.query_map((lo, hi), |r| r.get::<_, String>(0))? {
                        let name = name?;
                        let class = class_of(&name);
                        by_name.insert(name, class);
                    }
                }
                let mut classes: BTreeSet<String> = existing.clone();
                classes.extend(by_name.values().cloned());
                let select = sql::EVENTS_DAY;
                for class in classes {
                    let accept =
                        |e: &trail_core::envelope::Event| by_name.get(&e.name).is_some_and(|c| *c == class);
                    let file = self.write_id_day(
                        tx,
                        (kind, &class, day),
                        select,
                        event_from_row,
                        existing.contains(&class),
                        &accept,
                        &old_keys,
                        &seqs,
                    )?;
                    files.extend(file);
                }
            }
            StoreKind::Errors => {
                let select = sql::ERRORS_DAY;
                let class = kind.as_str();
                files.extend(self.write_id_day(
                    tx,
                    (kind, class, day),
                    select,
                    error_from_row,
                    existing.contains(class),
                    &|_| true,
                    &old_keys,
                    &seqs,
                )?);
            }
            StoreKind::Logs => {
                let select = sql::LOGS_DAY;
                let class = kind.as_str();
                files.extend(self.write_id_day(
                    tx,
                    (kind, class, day),
                    select,
                    log_from_row,
                    existing.contains(class),
                    &|_| true,
                    &old_keys,
                    &seqs,
                )?);
            }
            StoreKind::Samples => {
                let class = kind.as_str();
                let path = self.day_path(class, day);
                let old = if existing.contains(class) {
                    FileRows::open_if_exists(&path, None)?
                } else {
                    None
                };
                let mut merge = Merge {
                    old,
                    out: DayWriter::<SampleRow>::create(&path)?,
                    new_wins: true,
                    fill: &Seqs::default(),
                };
                let series: Vec<(i64, String, String)> = {
                    let mut stmt = tx.prepare_cached(sql::SERIES_NAMES)?;
                    let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
                    rows.collect::<rusqlite::Result<_>>()?
                };
                let mut stmt = tx.prepare_cached(sql::SAMPLES_RANGE)?;
                for (id, name, labels) in series {
                    let (name, labels): (Arc<str>, Arc<str>) = (Arc::from(name), Arc::from(labels));
                    let mut rows = stmt.query((id, lo, hi))?;
                    while let Some(r) = rows.next()? {
                        merge.push(SampleRow {
                            series: id,
                            ts: r.get(0)?,
                            value: r.get(1)?,
                            name: Arc::clone(&name),
                            labels: Arc::clone(&labels),
                        })?;
                    }
                }
                let out = merge.finish()?;
                if out.rows() == 0 {
                    out.abandon();
                } else {
                    let done = out.finish(&path)?;
                    files.push(DayFile {
                        store: kind,
                        class: class.to_string(),
                        day,
                        rows: done.rows,
                        bytes: done.bytes,
                        max_seq: done.max_seq,
                    });
                }
            }
        }
        Ok(files)
    }

    /// Writes one day file of a store keyed by (source, id): the old file's rows (if `existing`)
    /// merged with SQLite's rows that `accept` takes and the old files lack.
    #[allow(clippy::too_many_arguments)]
    fn write_id_day<T: ArchRow + Kept>(
        &self,
        tx: &Transaction<'_>,
        (kind, class, day): (StoreKind, &str, Day),
        select: &str,
        map: fn(&Row<'_>) -> rusqlite::Result<T>,
        existing: bool,
        accept: &dyn Fn(&T) -> bool,
        old_keys: &Keys,
        seqs: &Seqs,
    ) -> Result<Option<DayFile>> {
        let (lo, hi) = (self.zone.day_start(day).ms(), self.zone.day_end(day).ms());
        let path = self.day_path(class, day);
        let old = if existing {
            FileRows::open_if_exists(&path, None)?
        } else {
            None
        };
        let mut merge = Merge {
            old,
            out: DayWriter::<T>::create(&path)?,
            new_wins: false,
            fill: seqs,
        };
        let mut stmt = tx.prepare_cached(select)?;
        let mut rows = stmt.query((lo, hi))?;
        while let Some(r) = rows.next()? {
            let record = map(r)?;
            if accept(&record) && !old_keys.contains(record.source(), record.id()) {
                merge.push(record)?;
            }
        }
        let out = merge.finish()?;
        if out.rows() == 0 {
            out.abandon();
            return Ok(None);
        }
        let done = out.finish(&path)?;
        Ok(Some(DayFile {
            store: kind,
            class: class.to_string(),
            day,
            rows: done.rows,
            bytes: done.bytes,
            max_seq: done.max_seq,
        }))
    }

    /// Deletes day files past retention (`events-<n>d` after n days, logs and errors per
    /// `config.retention`) and SQLite's logs and errors past it; then, per store, the oldest days
    /// until its files fit `config.caps`.
    pub fn expire(&self, now: Ts) -> Result<ExpireReport> {
        let files: Vec<DayFile> = self.read(|tx| {
            let mut stmt = tx.prepare_cached(sql::ARCHIVE_ALL)?;
            let rows = stmt.query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, Option<i64>>(5)?,
                ))
            })?;
            let mut out = Vec::new();
            for row in rows {
                let (store, class, day, rows, bytes, max_seq) = row?;
                if let (Some(store), Ok(day)) = (StoreKind::parse(&store), day.parse::<Day>()) {
                    out.push(DayFile {
                        store,
                        class,
                        day,
                        rows: u64::try_from(rows).unwrap_or(0),
                        bytes: u64::try_from(bytes).unwrap_or(0),
                        max_seq,
                    });
                }
            }
            Ok(out)
        })?;
        let past = |day: Day, keep: Option<i64>| {
            keep.is_some_and(|k| self.zone.day_end(day).ms().saturating_add(k) <= now.ms())
        };
        let mut doomed: Vec<DayFile> = Vec::new();
        let mut kept: Vec<DayFile> = Vec::new();
        for f in files {
            let expired = match f.store {
                StoreKind::Events => f
                    .class
                    .strip_prefix("events-")
                    .and_then(|c| c.strip_suffix('d'))
                    .and_then(|n| n.parse::<i64>().ok())
                    .is_some_and(|n| past(f.day, Some(n * DAY))),
                StoreKind::Logs => past(f.day, self.retention.logs),
                StoreKind::Errors => past(f.day, self.retention.errors),
                StoreKind::Samples => false,
            };
            if expired { doomed.push(f) } else { kept.push(f) }
        }
        // Caps: the oldest days go first, all of a day's classes together.
        let mut capped: Vec<(StoreKind, Day)> = Vec::new();
        for kind in StoreKind::ALL {
            let cap = match kind {
                StoreKind::Events => self.caps.events,
                StoreKind::Samples => self.caps.metrics,
                StoreKind::Errors => self.caps.errors,
                StoreKind::Logs => self.caps.logs,
            };
            let Some(cap) = cap else { continue };
            let mut by_day: BTreeMap<Day, Vec<usize>> = BTreeMap::new();
            let mut total = 0u64;
            for (i, f) in kept.iter().enumerate().filter(|(_, f)| f.store == kind) {
                by_day.entry(f.day).or_default().push(i);
                total += f.bytes;
            }
            for (day, indexes) in by_day {
                if total <= cap {
                    break;
                }
                for i in indexes {
                    total = total.saturating_sub(kept[i].bytes);
                    doomed.push(kept[i].clone());
                }
                capped.push((kind, day));
            }
        }

        // Files first, then their rows in `archive_days`: a file that can't be deleted keeps its
        // row, so the next run tries again, and a run that dies in between leaves a row for a
        // missing file, which reads skip and the next run clears.
        let mut report = ExpireReport::default();
        for f in doomed {
            let path = self.day_path(&f.class, f.day);
            match std::fs::remove_file(&path) {
                Ok(()) => report.deleted.push(f),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => report.deleted.push(f),
                Err(e) => report
                    .failures
                    .push(Error::io(&path, "can't delete an expired day file", &e)),
            }
        }
        {
            let mut guard = self.writer()?;
            let tx = guard.begin()?;
            for f in &report.deleted {
                tx.prepare_cached(sql::ARCHIVE_DELETE)?.execute((
                    f.store.as_str(),
                    &f.class,
                    f.day.to_string(),
                ))?;
                // Another class of the same day may still have a file that waits for late rows.
                tx.prepare_cached(sql::LATE_UNFILED)?
                    .execute((f.store.as_str(), f.day.to_string()))?;
            }
            for (kind, day) in &capped {
                report.rows += self.delete_day_rows(&tx, *kind, *day)?;
            }
            if let Some(keep) = self.retention.logs {
                report.rows += tx.prepare_cached(sql::LOGS_EXPIRE)?.execute([now.ms() - keep])? as u64;
            }
            if let Some(keep) = self.retention.errors {
                report.rows += tx
                    .prepare_cached(sql::ERRORS_EXPIRE)?
                    .execute([now.ms() - keep])? as u64;
            }
            // What a sender can make without end: a problem per distinct message, an error group
            // per distinct fingerprint. The ones seen longest ago go.
            report.rows += tx.prepare_cached(sql::PROBLEMS_TRIM)?.execute([MAX_PROBLEMS])? as u64;
            report.rows += tx.prepare_cached(sql::GROUPS_TRIM)?.execute([MAX_ERROR_GROUPS])? as u64;
            tx.commit()
                .map_err(|e| Error::sql("can't record expired day files", &e))?;
            guard.reload()?;
        }
        if !report.deleted.is_empty() || report.rows > 0 {
            self.vacuum()?;
        }
        Ok(report)
    }

    /// Gives the free pages left by deleted rows back to the file system.
    fn vacuum(&self) -> Result<()> {
        let guard = self.writer()?;
        guard
            .conn
            .execute_batch("PRAGMA incremental_vacuum")
            .map_err(|e| Error::sql("can't vacuum", &e))
    }
}
