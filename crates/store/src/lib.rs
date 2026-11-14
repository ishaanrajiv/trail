//! Trail's store: SQLite for recent records, the metric tiers and Trail's own tables, and one
//! Parquet file per finished local day for older records. `spec/storage.md` is the contract.
//!
//! [`Store`] is opened once and shared behind an `Arc`. Writes go through one connection behind a
//! mutex; reads use a small pool of read-only connections, so a query does not wait for a write.
//! Maintenance ([`Store::maintain_tiers`], [`Store::archive`], [`Store::expire`]) takes `now`
//! from the caller: the store never reads the clock.
//!
//! - `write`: batches of records, error groups, problems, the registry and state.
//! - `records`: reading events, errors and logs from SQLite and the day files.
//! - `metrics`: series, counters, the 5-minute and hourly tiers and metric queries.
//! - `archive`: writing day files, evicting rows, retention and caps.
//! - `parquet_io`: the day files' columns, written and read with parquet's low-level API.

mod archive;
mod error;
mod metrics;
mod migrate;
mod parquet_io;
mod records;
mod sql;
mod write;

#[cfg(test)]
mod tests;

pub use archive::{ArchiveReport, DayFile, ExpireReport};
pub use error::{Error, Result};
pub use metrics::{
    Agg, Latest, MetricQuery, MetricResult, SeriesFilter, SeriesInfo, SeriesPoints, Tier, TierReport,
};
pub use records::{ErrorFilter, EventFilter, GroupFilter, LogFilter, Tail};
pub use write::{Batch, ErrorGroup, ErrorStatus, MetricPoint, Mismatch, PointOutcome, ProblemRow, Written};

use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;
use trail_core::config::Config;
use trail_core::decl::Declarations;
use trail_core::time::{Day, Zone};

/// The state key the timezone the data was written under is kept at.
const ZONE_KEY: &str = "timezone";

/// Read-only connections kept open for reuse.
const READERS: usize = 4;
const BUSY_TIMEOUT: Duration = Duration::from_secs(10);

/// One kind of record kept in day files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum StoreKind {
    Events,
    Samples,
    Errors,
    Logs,
}

impl StoreKind {
    /// Every store, in the order maintenance visits them.
    pub const ALL: [StoreKind; 4] = [
        StoreKind::Events,
        StoreKind::Samples,
        StoreKind::Errors,
        StoreKind::Logs,
    ];

    /// The name used in `archive_days.store` and as the archive folder.
    pub fn as_str(self) -> &'static str {
        match self {
            StoreKind::Events => "events",
            StoreKind::Samples => "samples",
            StoreKind::Errors => "errors",
            StoreKind::Logs => "logs",
        }
    }

    fn parse(text: &str) -> Option<StoreKind> {
        StoreKind::ALL.into_iter().find(|k| k.as_str() == text)
    }

    /// The mark below which this store's rows may be in day files only.
    fn evicted_key(self) -> &'static str {
        match self {
            StoreKind::Events => "evicted_before:events",
            StoreKind::Samples => "evicted_before:samples",
            StoreKind::Errors => "evicted_before:errors",
            StoreKind::Logs => "evicted_before:logs",
        }
    }
}

/// Row counts and sizes, per store and overall.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stats {
    pub stores: Vec<StoreStats>,
    /// trail.db plus its write-ahead log, in bytes.
    pub db_bytes: u64,
    pub series: u64,
}

/// One store's rows in SQLite and its day files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreStats {
    pub store: StoreKind,
    pub rows: u64,
    pub files: u64,
    pub file_bytes: u64,
}

/// A metric series as the writer keeps it in memory.
#[derive(Debug, Clone)]
pub(crate) struct SeriesState {
    pub id: i64,
    pub counter: bool,
    pub unit: Option<String>,
    pub first_ts: i64,
    pub last_ts: i64,
    pub last_raw: Option<f64>,
    pub last_raw_ts: Option<i64>,
}

/// A series' identity: (source, name, labels JSON).
pub(crate) type SeriesKey = (String, String, String);

/// Everything behind the write lock.
pub(crate) struct Writer {
    pub conn: Connection,
    /// (source, name, labels JSON) to its series.
    pub series: HashMap<SeriesKey, SeriesState>,
    /// Days per store that have a day file, or are being written: a row for one is late.
    pub filed: HashMap<StoreKind, BTreeSet<Day>>,
}

/// Trail's store. Open it once; it is `Send + Sync`.
pub struct Store {
    data_dir: PathBuf,
    db_path: PathBuf,
    zone: Zone,
    hot_days: u32,
    min_step_ms: i64,
    retention: trail_core::config::Retention,
    caps: trail_core::config::Caps,
    writer: Mutex<Writer>,
    readers: Mutex<Vec<Connection>>,
    /// Read connections kept open between queries: one per request worker.
    max_readers: usize,
    /// `limits.series`.
    max_series: usize,
}

impl Store {
    /// Opens (creating if needed) `config.data_dir` and its `trail.db`.
    pub fn open(config: &Config) -> Result<Store> {
        let data_dir = config.data_dir.clone();
        std::fs::create_dir_all(&data_dir).map_err(|e| Error::io(&data_dir, "can't create data_dir", &e))?;
        let db_path = data_dir.join("trail.db");
        let mut conn = Connection::open(&db_path).map_err(|e| Error::sql("can't open trail.db", &e))?;
        conn.busy_timeout(BUSY_TIMEOUT)
            .map_err(|e| Error::sql("can't set the busy timeout", &e))?;
        let version = migrate::prepare(&mut conn)?;
        if config.sync_full {
            conn.execute_batch("PRAGMA synchronous = FULL")
                .map_err(|e| Error::sql("can't set sync = \"full\"", &e))?;
        }
        // A folder written under another zone is refused before anything in it changes.
        if version > 0 {
            check_zone(&conn, config)?;
        }
        migrate::upgrade(&mut conn, &data_dir, version, &config.zone)?;
        check_zone(&conn, config)?;
        let mut writer = Writer {
            conn,
            series: HashMap::new(),
            filed: HashMap::new(),
        };
        writer.reload()?;
        Ok(Store {
            data_dir,
            db_path,
            zone: config.zone.clone(),
            hot_days: config.hot_days,
            min_step_ms: config.min_step_ms.max(1),
            retention: config.retention,
            caps: config.caps,
            writer: Mutex::new(writer),
            readers: Mutex::new(Vec::new()),
            max_readers: config.workers.max(READERS),
            max_series: config.limits.series,
        })
    }

    /// The folder this store keeps its data in.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub(crate) fn writer(&self) -> Result<MutexGuard<'_, Writer>> {
        self.writer.lock().map_err(|_| {
            Error::new(
                "store",
                "a write failed part-way and left the write lock poisoned",
                "restart Trail; the database is consistent (SQLite rolled the write back)",
            )
        })
    }

    /// Runs `f` in one read transaction (one snapshot) on a pooled read-only connection.
    pub(crate) fn read<T>(&self, f: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        let pooled = self.readers.lock().ok().and_then(|mut pool| pool.pop());
        let mut conn = match pooled {
            Some(conn) => conn,
            None => {
                let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
                let conn = Connection::open_with_flags(&self.db_path, flags)
                    .map_err(|e| Error::sql("can't open a read connection", &e))?;
                conn.busy_timeout(BUSY_TIMEOUT)
                    .map_err(|e| Error::sql("can't set the busy timeout", &e))?;
                conn
            }
        };
        let result = {
            let tx = conn
                .transaction()
                .map_err(|e| Error::sql("can't start a read", &e))?;
            f(&tx)
        };
        if let Ok(mut pool) = self.readers.lock()
            && pool.len() < self.max_readers
        {
            pool.push(conn);
        }
        result
    }

    // ---------- registry ----------

    /// Every source's published declarations, by source, and the rows that could not be read
    /// (each says how to fix it). One bad source never hides the others.
    pub fn registry(&self) -> Result<(Vec<Declarations>, Vec<Error>)> {
        self.read(|tx| {
            let mut stmt = tx.prepare_cached(sql::REGISTRY_ALL)?;
            let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
            let mut good = Vec::new();
            let mut bad = Vec::new();
            for row in rows {
                let (source, text) = row?;
                let value: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(value) => value,
                    Err(e) => {
                        bad.push(Error::new(
                            format!("registry: {source}"),
                            format!("the stored declarations are not JSON: {e}"),
                            format!("publish `{source}` again with `trail publish <folder>`"),
                        ));
                        continue;
                    }
                };
                match Declarations::from_json(value) {
                    Ok(decl) => good.push(decl),
                    Err(problems) => {
                        let text: Vec<String> = problems.iter().map(ToString::to_string).collect();
                        bad.push(Error::new(
                            format!("registry: {source}"),
                            format!(
                                "the stored declarations no longer pass the check: {}",
                                text.join("; ")
                            ),
                            format!("fix them and publish `{source}` again with `trail publish <folder>`"),
                        ));
                    }
                }
            }
            Ok((good, bad))
        })
    }

    /// Stores (or replaces) one source's declarations.
    pub fn put_declarations(&self, decl: &Declarations) -> Result<()> {
        let text = decl.to_json().to_string();
        let w = self.writer()?;
        w.conn
            .prepare_cached(sql::REGISTRY_PUT)?
            .execute((&decl.source, &text))?;
        Ok(())
    }

    /// Deletes one source's declarations. False when there were none.
    pub fn delete_declarations(&self, source: &str) -> Result<bool> {
        let w = self.writer()?;
        let n = w.conn.prepare_cached(sql::REGISTRY_DELETE)?.execute([source])?;
        Ok(n > 0)
    }

    // ---------- state ----------

    /// A value from the key/value state table.
    pub fn state(&self, key: &str) -> Result<Option<String>> {
        self.read(|tx| {
            Ok(tx
                .prepare_cached(sql::STATE_GET)?
                .query_row([key], |r| r.get(0))
                .optional()?)
        })
    }

    /// Sets a value in the key/value state table.
    pub fn set_state(&self, key: &str, value: &str) -> Result<()> {
        let w = self.writer()?;
        w.conn.prepare_cached(sql::STATE_SET)?.execute((key, value))?;
        Ok(())
    }

    // ---------- stats ----------

    /// Rows in SQLite, day files and their bytes per store; the database size; the series count.
    pub fn stats(&self) -> Result<Stats> {
        let (stores, series) = self.read(|tx| {
            let mut stores = Vec::new();
            for kind in StoreKind::ALL {
                let count = match kind {
                    StoreKind::Events => sql::COUNT_EVENTS,
                    StoreKind::Samples => sql::COUNT_SAMPLES,
                    StoreKind::Errors => sql::COUNT_ERRORS,
                    StoreKind::Logs => sql::COUNT_LOGS,
                };
                let rows: i64 = tx.prepare_cached(count)?.query_row([], |r| r.get(0))?;
                let (files, bytes): (i64, i64) = tx
                    .prepare_cached(sql::ARCHIVE_TOTALS)?
                    .query_row([kind.as_str()], |r| Ok((r.get(0)?, r.get(1)?)))?;
                stores.push(StoreStats {
                    store: kind,
                    rows: u64::try_from(rows).unwrap_or(0),
                    files: u64::try_from(files).unwrap_or(0),
                    file_bytes: u64::try_from(bytes).unwrap_or(0),
                });
            }
            let series: i64 = tx
                .prepare_cached(sql::COUNT_SERIES)?
                .query_row([], |r| r.get(0))?;
            Ok((stores, u64::try_from(series).unwrap_or(0)))
        })?;
        let mut db_bytes = 0;
        for suffix in ["", "-wal"] {
            let mut path = self.db_path.clone().into_os_string();
            path.push(suffix);
            if let Ok(meta) = std::fs::metadata(&path) {
                db_bytes += meta.len();
            }
        }
        Ok(Stats {
            stores,
            db_bytes,
            series,
        })
    }

    // ---------- helpers ----------

    /// The day `n` days before `day`.
    pub(crate) fn days_before(&self, day: Day, n: u32) -> Day {
        let mut d = day;
        for _ in 0..n {
            d = self.zone.day_of(self.zone.day_start(d).minus(1));
        }
        d
    }

    /// The archive folder for a class (`events`, `events-30d`, `samples`, …).
    pub(crate) fn class_dir(&self, class: &str) -> PathBuf {
        self.data_dir.join("archive").join(class)
    }

    pub(crate) fn day_path(&self, class: &str, day: Day) -> PathBuf {
        self.class_dir(class).join(format!("{day}.parquet"))
    }
}

/// Days are local days: file names, the hot window and hourly buckets all follow the zone. A
/// folder written under one zone can't be read under another. Stores the zone when none is.
fn check_zone(conn: &Connection, config: &Config) -> Result<()> {
    let zone: Option<String> = conn
        .query_row(sql::STATE_GET, [ZONE_KEY], |r| r.get(0))
        .optional()
        .map_err(|e| Error::sql("can't read the stored timezone", &e))?;
    match zone {
        None => {
            conn.execute(sql::STATE_SET, (ZONE_KEY, config.zone.name()))
                .map_err(|e| Error::sql("can't store the timezone", &e))?;
            Ok(())
        }
        Some(stored) if stored == config.zone.name() => Ok(()),
        Some(stored) => Err(Error::new(
            "trail.toml: timezone",
            format!(
                "this data was written with timezone `{stored}`, and the config now says `{}`",
                config.zone.name()
            ),
            format!(
                "set timezone = \"{stored}\" again; day files and hourly buckets are cut at local midnight and the local hour, so a folder can't change zone. To use another zone, start with an empty data_dir"
            ),
        )),
    }
}

/// Reads a mark; None when it was never set.
pub(crate) fn get_mark(conn: &Connection, key: &str) -> Result<Option<i64>> {
    Ok(conn
        .prepare_cached(sql::MARK_GET)?
        .query_row([key], |r| r.get(0))
        .optional()?)
}

pub(crate) fn set_mark(conn: &Connection, key: &str, value: i64) -> Result<()> {
    conn.prepare_cached(sql::MARK_SET)?.execute((key, value))?;
    Ok(())
}

/// The mark below which a store's rows may have left SQLite. `i64::MIN` when none have.
pub(crate) fn evicted_before(conn: &Connection, kind: StoreKind) -> Result<i64> {
    Ok(get_mark(conn, kind.evicted_key())?.unwrap_or(i64::MIN))
}

impl Writer {
    /// Reloads the in-memory series and filed days from the database (after open or a failed write).
    pub(crate) fn reload(&mut self) -> Result<()> {
        let mut series = HashMap::new();
        {
            let mut stmt = self.conn.prepare_cached(sql::SERIES_STATE_ALL)?;
            let rows = stmt.query_map([], |r| {
                Ok((
                    (
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                    ),
                    SeriesState {
                        id: r.get(0)?,
                        counter: r.get::<_, String>(4)? == "counter",
                        unit: r.get(5)?,
                        first_ts: r.get(6)?,
                        last_ts: r.get(7)?,
                        last_raw: r.get(8)?,
                        last_raw_ts: r.get(9)?,
                    },
                ))
            })?;
            for row in rows {
                let (key, state) = row?;
                series.insert(key, state);
            }
        }
        let mut filed: HashMap<StoreKind, BTreeSet<Day>> = HashMap::new();
        {
            let mut stmt = self.conn.prepare_cached(sql::ARCHIVE_FILED_DAYS)?;
            let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
            for row in rows {
                let (store, day) = row?;
                if let (Some(kind), Ok(day)) = (StoreKind::parse(&store), day.parse::<Day>()) {
                    filed.entry(kind).or_default().insert(day);
                }
            }
        }
        self.series = series;
        self.filed = filed;
        Ok(())
    }

    /// Starts a write transaction.
    pub(crate) fn begin(&mut self) -> Result<Transaction<'_>> {
        self.conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| Error::sql("can't start a write", &e))
    }
}
