//! Reading events, errors and logs: SQLite for the hot days, the day files for older ones.
//!
//! Rows at or after a store's `evicted_before` mark are all in SQLite. Before it, a day's rows are
//! in its day file(s), plus any late rows SQLite holds until the next archive merges them; a query
//! reads both and keeps one row per (source, id).

use crate::parquet_io::{ArchRow, FileRows, Kept, Keys, Skip, file_keys};
use crate::write::{ErrorGroup, ErrorStatus};
use crate::{Result, Store, StoreKind, evicted_before, sql};
use rusqlite::types::{Type, Value};
use rusqlite::{Row, Transaction};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::path::PathBuf;
use trail_core::Ts;
use trail_core::envelope::{ErrorRecord, Event, Level, LogRecord, Object, Severity};
use trail_core::time::Day;

/// The most rows one query returns.
pub const MAX_LIMIT: usize = 10_000;

/// Which events to return. Empty fields match everything.
#[derive(Debug, Clone, Default)]
pub struct EventFilter {
    pub source: Option<String>,
    /// An exact name, or a prefix pattern ending in `.*` (`shop.order.*`).
    pub name: Option<String>,
    pub actor: Option<String>,
    pub trace_id: Option<String>,
    /// From this time (inclusive).
    pub start: Option<Ts>,
    /// To this time (exclusive).
    pub end: Option<Ts>,
    /// Only events that differed from their declaration.
    pub mismatched: bool,
    /// Only events older than this: the last row of the previous page.
    pub before: Option<Before>,
    /// Only events stored after this seq, oldest seq first, instead of newest first.
    pub after_seq: Option<i64>,
    /// At most this many (0 means 100).
    pub limit: usize,
}

/// Which errors to return.
#[derive(Debug, Clone, Default)]
pub struct ErrorFilter {
    pub source: Option<String>,
    pub fingerprint: Option<String>,
    pub severity: Option<Severity>,
    pub start: Option<Ts>,
    pub end: Option<Ts>,
    pub before: Option<Before>,
    /// Only records stored after this seq, oldest seq first, instead of newest first.
    pub after_seq: Option<i64>,
    pub limit: usize,
}

/// Which error groups to return.
#[derive(Debug, Clone, Default)]
pub struct GroupFilter {
    pub source: Option<String>,
    pub status: Option<ErrorStatus>,
    pub limit: usize,
}

/// Which log lines to return.
#[derive(Debug, Clone, Default)]
pub struct LogFilter {
    pub source: Option<String>,
    pub stream: Option<String>,
    /// This level and above.
    pub min_level: Option<Level>,
    /// A substring of the message (case-sensitive).
    pub contains: Option<String>,
    pub start: Option<Ts>,
    pub end: Option<Ts>,
    pub before: Option<Before>,
    /// Only records stored after this seq, oldest seq first, instead of newest first.
    pub after_seq: Option<i64>,
    pub limit: usize,
}

/// Where the previous page of a newest-first read ended: its last row's `ts`, `id` and `source`.
/// Rows are ordered by (ts, id, source), newest first, and the next page holds only rows after
/// it in that order. Without the source (a position made before a record's identity included
/// it), rows with the same `ts` and `id` count as already given.
pub type Before = (Ts, String, Option<String>);

/// A page read in arrival order: rows with a seq above the one asked for, oldest first, and the
/// seq to ask after next time. Asking again with `next_seq` gives every record stored since,
/// once, whatever its `ts` and wherever it is kept by then.
#[derive(Debug, Clone, PartialEq)]
pub struct Tail<T> {
    pub rows: Vec<T>,
    pub next_seq: i64,
}

fn limit_of(limit: usize) -> usize {
    if limit == 0 { 100 } else { limit.min(MAX_LIMIT) }
}

// ---------- rows ----------

fn json_object(text: &str, column: usize) -> rusqlite::Result<Object> {
    serde_json::from_str(text)
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(column, Type::Text, Box::new(e)))
}

pub(crate) fn event_from_row(r: &Row<'_>) -> rusqlite::Result<Event> {
    let problems: Option<String> = r.get(11)?;
    let problems = match problems {
        Some(text) => serde_json::from_str(&text)
            .map_err(|e| rusqlite::Error::FromSqlConversionFailure(11, Type::Text, Box::new(e)))?,
        None => Vec::new(),
    };
    Ok(Event {
        id: r.get(0)?,
        name: r.get(1)?,
        ts: Ts(r.get(2)?),
        received_at: Ts(r.get(3)?),
        source: r.get(4)?,
        version: r.get(5)?,
        host: r.get(6)?,
        actor: r.get(7)?,
        client: r.get(8)?,
        trace_id: r.get(9)?,
        props: json_object(&r.get::<_, String>(10)?, 10)?,
        problems,
        seq: r.get(12)?,
    })
}

/// A JSON object as text. Objects always serialize.
pub(crate) fn object_text(object: &Object) -> String {
    serde_json::to_string(object).unwrap_or_else(|_| "{}".to_string())
}

/// `problems` as stored: a JSON list, or None for a clean event.
pub(crate) fn problems_text(problems: &[String]) -> Option<String> {
    if problems.is_empty() {
        None
    } else {
        serde_json::to_string(problems).ok()
    }
}

/// An event's insert parameters, with the seq it is stored under.
pub(crate) fn event_params(e: &Event, seq: i64) -> Vec<Value> {
    vec![
        Value::Text(e.id.clone()),
        Value::Text(e.name.clone()),
        Value::Integer(e.ts.ms()),
        Value::Integer(e.received_at.ms()),
        Value::Text(e.source.clone()),
        opt(&e.version),
        opt(&e.host),
        opt(&e.actor),
        opt(&e.client),
        opt(&e.trace_id),
        Value::Text(object_text(&e.props)),
        problems_text(&e.problems).map_or(Value::Null, Value::Text),
        Value::Integer(seq),
    ]
}

fn opt(text: &Option<String>) -> Value {
    text.clone().map_or(Value::Null, Value::Text)
}

pub(crate) fn error_from_row(r: &Row<'_>) -> rusqlite::Result<ErrorRecord> {
    let severity: String = r.get(6)?;
    Ok(ErrorRecord {
        id: r.get(0)?,
        ts: Ts(r.get(1)?),
        received_at: Ts(r.get(2)?),
        source: r.get(3)?,
        version: r.get(4)?,
        host: r.get(5)?,
        severity: Severity::parse(&severity).unwrap_or_default(),
        kind: r.get(7)?,
        message: r.get(8)?,
        fingerprint: r.get(9)?,
        stack: r.get(10)?,
        context: json_object(&r.get::<_, String>(11)?, 11)?,
        trace_id: r.get(12)?,
        seq: r.get(13)?,
    })
}

pub(crate) fn error_params(e: &ErrorRecord, seq: i64) -> Vec<Value> {
    vec![
        Value::Text(e.id.clone()),
        Value::Integer(e.ts.ms()),
        Value::Integer(e.received_at.ms()),
        Value::Text(e.source.clone()),
        opt(&e.version),
        opt(&e.host),
        Value::Text(e.severity.as_str().to_string()),
        Value::Text(e.kind.clone()),
        Value::Text(e.message.clone()),
        Value::Text(e.fingerprint.clone()),
        opt(&e.stack),
        Value::Text(object_text(&e.context)),
        opt(&e.trace_id),
        Value::Integer(seq),
    ]
}

pub(crate) fn log_from_row(r: &Row<'_>) -> rusqlite::Result<LogRecord> {
    let level: String = r.get(6)?;
    Ok(LogRecord {
        id: r.get(0)?,
        ts: Ts(r.get(1)?),
        received_at: Ts(r.get(2)?),
        source: r.get(3)?,
        host: r.get(4)?,
        stream: r.get(5)?,
        level: Level::parse(&level).unwrap_or_default(),
        message: r.get(7)?,
        fields: json_object(&r.get::<_, String>(8)?, 8)?,
        seq: r.get(9)?,
    })
}

pub(crate) fn log_params(l: &LogRecord, seq: i64) -> Vec<Value> {
    vec![
        Value::Text(l.id.clone()),
        Value::Integer(l.ts.ms()),
        Value::Integer(l.received_at.ms()),
        Value::Text(l.source.clone()),
        opt(&l.host),
        opt(&l.stream),
        Value::Text(l.level.as_str().to_string()),
        Value::Text(l.message.clone()),
        Value::Text(object_text(&l.fields)),
        Value::Integer(seq),
    ]
}

pub(crate) fn group_from_row(r: &Row<'_>) -> rusqlite::Result<ErrorGroup> {
    let status: String = r.get(7)?;
    Ok(ErrorGroup {
        fingerprint: r.get(0)?,
        source: r.get(1)?,
        kind: r.get(2)?,
        message: r.get(3)?,
        first_seen: Ts(r.get(4)?),
        last_seen: Ts(r.get(5)?),
        count: u64::try_from(r.get::<_, i64>(6)?).unwrap_or(0),
        status: ErrorStatus::parse(&status).unwrap_or(ErrorStatus::Open),
    })
}

// ---------- building queries ----------

/// A WHERE clause and its arguments, built from a filter.
struct Where {
    sql: String,
    args: Vec<Value>,
}

impl Where {
    fn new() -> Where {
        Where {
            sql: " WHERE 1 = 1".to_string(),
            args: Vec::new(),
        }
    }

    fn and(&mut self, clause: &str, args: impl IntoIterator<Item = Value>) {
        self.sql.push_str(" AND ");
        self.sql.push_str(clause);
        self.args.extend(args);
    }

    fn text(&mut self, clause: &str, value: &Option<String>) {
        if let Some(v) = value {
            self.and(clause, [Value::Text(v.clone())]);
        }
    }

    fn range(&mut self, lo: i64, hi: i64) {
        if lo > i64::MIN {
            self.and("ts >= ?", [Value::Integer(lo)]);
        }
        if hi < i64::MAX {
            self.and("ts < ?", [Value::Integer(hi)]);
        }
    }

    fn before(&mut self, before: &Option<Before>) {
        if let Some((ts, id, source)) = before {
            match source {
                Some(source) => self.and(
                    "(ts < ? OR (ts = ? AND (id < ? OR (id = ? AND source < ?))))",
                    [
                        Value::Integer(ts.ms()),
                        Value::Integer(ts.ms()),
                        Value::Text(id.clone()),
                        Value::Text(id.clone()),
                        Value::Text(source.clone()),
                    ],
                ),
                None => self.and(
                    "(ts < ? OR (ts = ? AND id < ?))",
                    [
                        Value::Integer(ts.ms()),
                        Value::Integer(ts.ms()),
                        Value::Text(id.clone()),
                    ],
                ),
            }
        }
    }

    /// Runs `SELECT <cols> FROM <table><where> AND seq > after ORDER BY seq LIMIT n`.
    fn oldest_seq<T>(
        &self,
        tx: &Transaction<'_>,
        select: &str,
        after: i64,
        limit: usize,
        map: fn(&Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<Vec<T>> {
        let text = format!("{select}{} AND seq > ? ORDER BY seq LIMIT {limit}", self.sql);
        let mut stmt = tx.prepare(&text)?;
        let args = self.args.iter().cloned().chain([Value::Integer(after)]);
        let rows = stmt.query_map(rusqlite::params_from_iter(args), map)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Runs `SELECT <cols> FROM <table><where> ORDER BY ts DESC, id DESC, source DESC LIMIT n`.
    fn newest<T>(
        &self,
        tx: &Transaction<'_>,
        select: &str,
        limit: usize,
        map: fn(&Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<Vec<T>> {
        let text = format!(
            "{select}{} ORDER BY ts DESC, id DESC, source DESC LIMIT {limit}",
            self.sql
        );
        let mut stmt = tx.prepare(&text)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(self.args.iter()), map)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

/// How an event name is matched.
#[derive(Debug, Clone)]
enum NameMatch {
    Exact(String),
    /// Everything starting with this (it ends in `.`).
    Prefix(String),
}

impl NameMatch {
    fn of(name: &str) -> NameMatch {
        match name.strip_suffix('*') {
            Some(prefix) => NameMatch::Prefix(prefix.to_string()),
            None => NameMatch::Exact(name.to_string()),
        }
    }

    fn matches(&self, name: &str) -> bool {
        match self {
            NameMatch::Exact(n) => n == name,
            NameMatch::Prefix(p) => name.starts_with(p.as_str()),
        }
    }

    fn sql(&self, w: &mut Where) {
        match self {
            NameMatch::Exact(n) => w.and("name = ?", [Value::Text(n.clone())]),
            // Names are ASCII, so everything starting with p sorts below p + U+10FFFF.
            NameMatch::Prefix(p) => w.and(
                "name >= ? AND name < ?",
                [Value::Text(p.clone()), Value::Text(format!("{p}\u{10FFFF}"))],
            ),
        }
    }

    /// Row groups whose `name` column can't hold a match are skipped.
    fn groups(&self) -> Skip {
        match self {
            NameMatch::Exact(n) => Skip::Bytes {
                column: "name",
                lo: n.as_bytes().to_vec(),
                hi: n.as_bytes().to_vec(),
            },
            NameMatch::Prefix(p) => {
                let mut hi = p.as_bytes().to_vec();
                hi.push(0xFF);
                Skip::Bytes {
                    column: "name",
                    lo: p.as_bytes().to_vec(),
                    hi,
                }
            }
        }
    }
}

/// Row groups whose `source` column can't hold `source` are skipped.
fn source_groups(source: &Option<String>) -> Option<Skip> {
    source.as_ref().map(|s| Skip::Bytes {
        column: "source",
        lo: s.as_bytes().to_vec(),
        hi: s.as_bytes().to_vec(),
    })
}

fn before_ok(ts: Ts, id: &str, source: &str, before: &Option<Before>) -> bool {
    match before {
        None => true,
        Some((bts, bid, bsource)) => {
            ts < *bts
                || (ts == *bts
                    && (id < bid.as_str()
                        || (id == bid.as_str() && bsource.as_ref().is_some_and(|b| source < b.as_str()))))
        }
    }
}

fn newest_first<T: Kept>(a: &T, b: &T) -> Ordering {
    b.ts()
        .cmp(&a.ts())
        .then_with(|| b.id().cmp(a.id()))
        .then_with(|| b.source().cmp(a.source()))
}

impl EventFilter {
    fn keep(&self, name: &Option<NameMatch>, e: &Event, lo: i64, hi: i64) -> bool {
        (lo..hi).contains(&e.ts.ms())
            && self.source.as_ref().is_none_or(|s| *s == e.source)
            && name.as_ref().is_none_or(|n| n.matches(&e.name))
            && self.actor.as_ref().is_none_or(|a| Some(a) == e.actor.as_ref())
            && self
                .trace_id
                .as_ref()
                .is_none_or(|t| Some(t) == e.trace_id.as_ref())
            && (!self.mismatched || !e.problems.is_empty())
            && before_ok(e.ts, &e.id, &e.source, &self.before)
    }

    fn sql(&self, name: &Option<NameMatch>, lo: i64, hi: i64) -> Where {
        let mut w = Where::new();
        w.text("source = ?", &self.source);
        if let Some(n) = name {
            n.sql(&mut w);
        }
        w.text("actor = ?", &self.actor);
        w.text("trace_id = ?", &self.trace_id);
        if self.mismatched {
            w.and("problems IS NOT NULL", []);
        }
        w.range(lo, hi);
        w.before(&self.before);
        w
    }
}

impl ErrorFilter {
    fn keep(&self, e: &ErrorRecord, lo: i64, hi: i64) -> bool {
        (lo..hi).contains(&e.ts.ms())
            && self.source.as_ref().is_none_or(|s| *s == e.source)
            && self.fingerprint.as_ref().is_none_or(|f| *f == e.fingerprint)
            && self.severity.is_none_or(|s| s == e.severity)
            && before_ok(e.ts, &e.id, &e.source, &self.before)
    }

    fn sql(&self, lo: i64, hi: i64) -> Where {
        let mut w = Where::new();
        w.text("source = ?", &self.source);
        w.text("fingerprint = ?", &self.fingerprint);
        if let Some(s) = self.severity {
            w.and("severity = ?", [Value::Text(s.as_str().to_string())]);
        }
        w.range(lo, hi);
        w.before(&self.before);
        w
    }
}

impl LogFilter {
    fn keep(&self, l: &LogRecord, lo: i64, hi: i64) -> bool {
        (lo..hi).contains(&l.ts.ms())
            && self.source.as_ref().is_none_or(|s| *s == l.source)
            && self.stream.as_ref().is_none_or(|s| Some(s) == l.stream.as_ref())
            && self.min_level.is_none_or(|m| l.level >= m)
            && self
                .contains
                .as_ref()
                .is_none_or(|c| l.message.contains(c.as_str()))
            && before_ok(l.ts, &l.id, &l.source, &self.before)
    }

    fn sql(&self, lo: i64, hi: i64) -> Where {
        let mut w = Where::new();
        w.text("source = ?", &self.source);
        w.text("stream = ?", &self.stream);
        if let Some(min) = self.min_level {
            let levels: Vec<Value> = [
                Level::Trace,
                Level::Debug,
                Level::Info,
                Level::Warn,
                Level::Error,
                Level::Fatal,
            ]
            .into_iter()
            .filter(|l| *l >= min)
            .map(|l| Value::Text(l.as_str().to_string()))
            .collect();
            let marks = vec!["?"; levels.len()].join(", ");
            w.and(&format!("level IN ({marks})"), levels);
        }
        if let Some(c) = &self.contains {
            w.and("instr(message, ?) > 0", [Value::Text(c.clone())]);
        }
        w.range(lo, hi);
        w.before(&self.before);
        w
    }
}

// ---------- the reads ----------

impl Store {
    /// The day files of `kind` for days in [lo, hi), newest day first.
    pub(crate) fn file_days(
        &self,
        tx: &Transaction<'_>,
        kind: StoreKind,
        lo: i64,
        hi: i64,
    ) -> Result<Vec<(Day, Vec<PathBuf>)>> {
        if lo >= hi {
            return Ok(Vec::new());
        }
        let first = if lo == i64::MIN {
            "0000-00-00".to_string()
        } else {
            self.zone.day_of(Ts(lo)).to_string()
        };
        let last = self.zone.day_of(Ts(hi.saturating_sub(1))).to_string();
        let mut stmt = tx.prepare_cached(sql::ARCHIVE_DAYS_RANGE)?;
        let rows = stmt.query_map((kind.as_str(), first, last), |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?;
        let mut out: Vec<(Day, Vec<PathBuf>)> = Vec::new();
        for row in rows {
            let (class, day) = row?;
            let Ok(day) = day.parse::<Day>() else { continue };
            let path = self.day_path(&class, day);
            match out.last_mut() {
                Some((d, paths)) if *d == day => paths.push(path),
                _ => out.push((day, vec![path])),
            }
        }
        Ok(out)
    }

    /// The newest `limit` rows: SQLite's hot rows, then day by day back through the files.
    #[allow(clippy::too_many_arguments)]
    fn newest_rows<T: Kept + crate::parquet_io::ArchRow>(
        &self,
        tx: &Transaction<'_>,
        kind: StoreKind,
        lo: i64,
        hi: i64,
        limit: usize,
        query: &dyn Fn(i64, i64, usize) -> Result<Vec<T>>,
        keep: &dyn Fn(&T) -> bool,
        groups: Option<Skip>,
    ) -> Result<Vec<T>> {
        let edge = evicted_before(tx, kind)?;
        let mut out = query(lo.max(edge), hi, limit)?;
        if out.len() >= limit || lo >= edge {
            out.truncate(limit);
            return Ok(out);
        }
        let remaining = limit - out.len();
        let cold_hi = hi.min(edge);
        let mut found = query(lo, cold_hi, remaining)?;
        let mut late = Keys::default();
        for r in &found {
            late.insert(r.source().to_string(), r.id().to_string(), None);
        }
        for (day, paths) in self.file_days(tx, kind, lo, cold_hi)? {
            for path in paths {
                let Some(mut rows) = FileRows::<T>::open_if_exists(&path, groups.clone())? else {
                    continue;
                };
                while let Some(row) = rows.next_row()? {
                    if keep(&row) && !late.contains(row.source(), row.id()) {
                        found.push(row);
                        if found.len() >= 2 * remaining + 1024 {
                            found.sort_by(newest_first);
                            found.truncate(remaining);
                        }
                    }
                }
            }
            found.sort_by(newest_first);
            found.truncate(remaining);
            let day_start = self.zone.day_start(day).ms();
            if found.len() >= remaining && found.last().is_some_and(|r| r.ts() >= day_start) {
                break;
            }
        }
        out.extend(found);
        Ok(out)
    }

    /// The rows with a seq above `after`, oldest seq first: SQLite's, then the day files whose
    /// largest seq is above it (`archive_days.max_seq`), each read only where its row groups'
    /// seq statistics allow.
    ///
    /// SQLite is read first, so its snapshot is fixed before any file is opened: a day file is
    /// renamed into place before the rows it holds leave SQLite, so a row is always in one or the
    /// other. A row in both carries the same seq and is taken once. A row in SQLite for a day that
    /// has left it, whose file already holds the same record under another seq, is a resend that
    /// the next archive run drops; it is skipped, and `next_seq` moves past it.
    #[allow(clippy::too_many_arguments)]
    fn tail_rows<T: Kept + ArchRow>(
        &self,
        tx: &Transaction<'_>,
        kind: StoreKind,
        after: i64,
        (lo, hi): (i64, i64),
        limit: usize,
        query: &dyn Fn(i64, usize) -> Result<Vec<T>>,
        keep: &dyn Fn(&T) -> bool,
        groups: Option<Skip>,
    ) -> Result<Tail<T>> {
        let edge = evicted_before(tx, kind)?;
        // The last seq given out as this read's view of SQLite saw it. A day file is read
        // after that view was taken and may have been rewritten since with records stored
        // later; taking one would move the position past records stored between the two that
        // this view can't see. Nothing above the ceiling is taken; the next read gets it.
        let ceiling = crate::get_mark(tx, crate::write::SEQ_MARK)?.unwrap_or(0);
        let hot = query(after, limit)?;
        // Past the last row a full page holds, a source may have more that weren't read.
        let mut horizon = if hot.len() >= limit {
            hot.last().and_then(|r| r.seq()).unwrap_or(i64::MAX)
        } else {
            i64::MAX
        };
        let mut cold: Vec<T> = Vec::new();
        let mut dropped = false;
        let by_seq = |a: &T, b: &T| a.seq().cmp(&b.seq());
        for path in self.files_after(tx, kind, after, lo, hi)? {
            let seqs = Skip::Int {
                column: "seq",
                lo: after.saturating_add(1),
                hi: i64::MAX,
            };
            let Some(rows) = FileRows::<T>::open_if_exists(&path, Some(seqs))? else {
                continue;
            };
            let mut rows = match &groups {
                Some(skip) => rows.also(skip.clone()),
                None => rows,
            };
            while let Some(row) = rows.next_row()? {
                if row.seq().is_some_and(|s| s > after && s <= ceiling) && keep(&row) {
                    cold.push(row);
                    if cold.len() >= 2 * limit + 1024 {
                        cold.sort_by(by_seq);
                        cold.truncate(limit);
                        dropped = true;
                    }
                }
            }
        }
        cold.sort_by(by_seq);
        if cold.len() > limit {
            cold.truncate(limit);
            dropped = true;
        }
        if dropped {
            horizon = horizon.min(cold.last().and_then(|r| r.seq()).unwrap_or(i64::MAX));
        }

        // Both lists by seq; a row in both (filed, not yet evicted) is taken once.
        let mut candidates: Vec<(T, bool)> = hot.into_iter().map(|r| (r, true)).collect();
        candidates.extend(cold.into_iter().map(|r| (r, false)));
        candidates.sort_by(|a, b| by_seq(&a.0, &b.0));
        candidates.dedup_by(|a, b| a.0.seq() == b.0.seq());

        let mut filed: HashMap<Day, Keys> = HashMap::new();
        let mut rows = Vec::new();
        let mut next_seq = after;
        for (row, in_sqlite) in candidates {
            let Some(seq) = row.seq() else { continue };
            if seq > horizon || rows.len() >= limit {
                break;
            }
            next_seq = seq;
            if in_sqlite && row.ts() < edge {
                let day = self.zone.day_of(Ts(row.ts()));
                let keys = match filed.entry(day) {
                    std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                    std::collections::hash_map::Entry::Vacant(e) => e.insert(self.day_keys(tx, kind, day)?),
                };
                if keys
                    .seq(row.source(), row.id())
                    .is_some_and(|filed| filed != Some(seq))
                {
                    continue;
                }
            }
            rows.push(row);
        }
        Ok(Tail { rows, next_seq })
    }

    /// The day files of `kind` for days in [lo, hi) that hold a seq above `after`.
    fn files_after(
        &self,
        tx: &Transaction<'_>,
        kind: StoreKind,
        after: i64,
        lo: i64,
        hi: i64,
    ) -> Result<Vec<PathBuf>> {
        if lo >= hi {
            return Ok(Vec::new());
        }
        let first = if lo == i64::MIN {
            "0000-00-00".to_string()
        } else {
            self.zone.day_of(Ts(lo)).to_string()
        };
        let last = self.zone.day_of(Ts(hi.saturating_sub(1))).to_string();
        let mut stmt = tx.prepare_cached(sql::ARCHIVE_DAYS_AFTER_SEQ)?;
        let rows = stmt.query_map((kind.as_str(), after, first, last), |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (class, day) = row?;
            if let Ok(day) = day.parse::<Day>() {
                out.push(self.day_path(&class, day));
            }
        }
        Ok(out)
    }

    /// Every record identity the day's files of `kind` hold, with the seq each was filed under.
    fn day_keys(&self, tx: &Transaction<'_>, kind: StoreKind, day: Day) -> Result<Keys> {
        let classes: Vec<String> = {
            let mut stmt = tx.prepare_cached(sql::ARCHIVE_DAY_CLASSES)?;
            let rows = stmt.query_map((kind.as_str(), day.to_string()), |r| r.get(0))?;
            rows.collect::<rusqlite::Result<_>>()?
        };
        let mut keys = Keys::default();
        for class in classes {
            let path = self.day_path(&class, day);
            if path.exists() {
                keys.extend(file_keys(&path)?);
            }
        }
        Ok(keys)
    }

    /// Events matching `filter` stored after `filter.after_seq` (0 when unset), oldest seq first.
    pub fn events_tail(&self, filter: &EventFilter) -> Result<Tail<Event>> {
        let limit = limit_of(filter.limit);
        let name = filter.name.as_deref().map(NameMatch::of);
        let lo = filter.start.map_or(i64::MIN, Ts::ms);
        let hi = filter.end.map_or(i64::MAX, Ts::ms);
        let after = filter.after_seq.unwrap_or(0);
        let select = sql::EVENTS_SELECT;
        self.read(|tx| {
            let query = |a: i64, n: usize| {
                filter
                    .sql(&name, lo, hi)
                    .oldest_seq(tx, select, a, n, event_from_row)
            };
            let keep = |e: &Event| filter.keep(&name, e, lo, hi);
            let groups = name.as_ref().map(NameMatch::groups);
            self.tail_rows(
                tx,
                StoreKind::Events,
                after,
                (lo, hi),
                limit,
                &query,
                &keep,
                groups,
            )
        })
    }

    /// Errors matching `filter` stored after `filter.after_seq` (0 when unset), oldest seq first.
    pub fn errors_tail(&self, filter: &ErrorFilter) -> Result<Tail<ErrorRecord>> {
        let limit = limit_of(filter.limit);
        let lo = filter.start.map_or(i64::MIN, Ts::ms);
        let hi = filter.end.map_or(i64::MAX, Ts::ms);
        let after = filter.after_seq.unwrap_or(0);
        let select = sql::ERRORS_SELECT;
        self.read(|tx| {
            let query = |a: i64, n: usize| filter.sql(lo, hi).oldest_seq(tx, select, a, n, error_from_row);
            let keep = |e: &ErrorRecord| filter.keep(e, lo, hi);
            let groups = source_groups(&filter.source);
            self.tail_rows(
                tx,
                StoreKind::Errors,
                after,
                (lo, hi),
                limit,
                &query,
                &keep,
                groups,
            )
        })
    }

    /// Log lines matching `filter` stored after `filter.after_seq` (0 when unset), oldest seq first.
    pub fn logs_tail(&self, filter: &LogFilter) -> Result<Tail<LogRecord>> {
        let limit = limit_of(filter.limit);
        let lo = filter.start.map_or(i64::MIN, Ts::ms);
        let hi = filter.end.map_or(i64::MAX, Ts::ms);
        let after = filter.after_seq.unwrap_or(0);
        let select = sql::LOGS_SELECT;
        self.read(|tx| {
            let query = |a: i64, n: usize| filter.sql(lo, hi).oldest_seq(tx, select, a, n, log_from_row);
            let keep = |l: &LogRecord| filter.keep(l, lo, hi);
            let groups = source_groups(&filter.source);
            self.tail_rows(tx, StoreKind::Logs, after, (lo, hi), limit, &query, &keep, groups)
        })
    }

    /// Events matching `filter`, newest first (by ts, then id); with `after_seq`, as
    /// [`Store::events_tail`] gives them.
    pub fn events(&self, filter: &EventFilter) -> Result<Vec<Event>> {
        if filter.after_seq.is_some() {
            return Ok(self.events_tail(filter)?.rows);
        }
        let limit = limit_of(filter.limit);
        let name = filter.name.as_deref().map(NameMatch::of);
        let lo = filter.start.map_or(i64::MIN, Ts::ms);
        let hi = filter.end.map_or(i64::MAX, Ts::ms);
        let select = sql::EVENTS_SELECT;
        self.read(|tx| {
            let query =
                |a: i64, b: i64, n: usize| filter.sql(&name, a, b).newest(tx, select, n, event_from_row);
            let keep = |e: &Event| filter.keep(&name, e, lo, hi);
            let groups = name.as_ref().map(NameMatch::groups);
            self.newest_rows(tx, StoreKind::Events, lo, hi, limit, &query, &keep, groups)
        })
    }

    /// Errors matching `filter`, newest first; with `after_seq`, as [`Store::errors_tail`].
    pub fn errors(&self, filter: &ErrorFilter) -> Result<Vec<ErrorRecord>> {
        if filter.after_seq.is_some() {
            return Ok(self.errors_tail(filter)?.rows);
        }
        let limit = limit_of(filter.limit);
        let lo = filter.start.map_or(i64::MIN, Ts::ms);
        let hi = filter.end.map_or(i64::MAX, Ts::ms);
        let select = sql::ERRORS_SELECT;
        self.read(|tx| {
            let query = |a: i64, b: i64, n: usize| filter.sql(a, b).newest(tx, select, n, error_from_row);
            let keep = |e: &ErrorRecord| filter.keep(e, lo, hi);
            let groups = source_groups(&filter.source);
            self.newest_rows(tx, StoreKind::Errors, lo, hi, limit, &query, &keep, groups)
        })
    }

    /// Log lines matching `filter`, newest first; with `after_seq`, as [`Store::logs_tail`].
    pub fn logs(&self, filter: &LogFilter) -> Result<Vec<LogRecord>> {
        if filter.after_seq.is_some() {
            return Ok(self.logs_tail(filter)?.rows);
        }
        let limit = limit_of(filter.limit);
        let lo = filter.start.map_or(i64::MIN, Ts::ms);
        let hi = filter.end.map_or(i64::MAX, Ts::ms);
        let select = sql::LOGS_SELECT;
        self.read(|tx| {
            let query = |a: i64, b: i64, n: usize| filter.sql(a, b).newest(tx, select, n, log_from_row);
            let keep = |l: &LogRecord| filter.keep(l, lo, hi);
            let groups = source_groups(&filter.source);
            self.newest_rows(tx, StoreKind::Logs, lo, hi, limit, &query, &keep, groups)
        })
    }

    /// Error groups matching `filter`, most recently seen first.
    pub fn error_groups(&self, filter: &GroupFilter) -> Result<Vec<ErrorGroup>> {
        let mut w = Where::new();
        w.text("source = ?", &filter.source);
        if let Some(status) = filter.status {
            w.and("status = ?", [Value::Text(status.as_str().to_string())]);
        }
        let text = format!(
            "{}{} ORDER BY last_seen DESC, fingerprint LIMIT {}",
            sql::GROUPS_SELECT,
            w.sql,
            limit_of(filter.limit)
        );
        self.read(|tx| {
            let mut stmt = tx.prepare(&text)?;
            let rows = stmt.query_map(rusqlite::params_from_iter(w.args.iter()), group_from_row)?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// The events in SQLite that arrived at or after `since`, newest arrival first. For catching
    /// up after a restart on what was stored but not yet acted on. Rows are read in the order
    /// they were inserted, newest first, and reading stops at the first one that arrived
    /// earlier, so the cost is the events since `since` and not the size of the table.
    pub fn events_received_since(&self, since: Ts) -> Result<Vec<Event>> {
        self.read(|tx| {
            let mut found = Vec::new();
            let mut stmt = tx.prepare_cached(sql::EVENTS_NEWEST)?;
            let rows = stmt.query_map((), event_from_row)?;
            for row in rows {
                let event = row?;
                if event.received_at < since {
                    break;
                }
                found.push(event);
            }
            Ok(found)
        })
    }

    /// Calls `visit` once for every event named `name` in [start, end), hot and archived. Order:
    /// archived days oldest first, each in file order (by ts); then SQLite's rows by ts (the hot
    /// days, and late rows of archived days not yet merged into their file). Returns the count.
    pub fn scan_events(&self, name: &str, start: Ts, end: Ts, visit: &mut dyn FnMut(Event)) -> Result<u64> {
        let (lo, hi) = (start.ms(), end.ms());
        self.read(|tx| {
            let mut count = 0u64;
            let edge = evicted_before(tx, StoreKind::Events)?;
            // Records in both a file and SQLite: visited from the file, skipped in SQLite.
            let mut both = Keys::default();
            let mut days = self.file_days(tx, StoreKind::Events, lo, hi.min(edge))?;
            days.reverse();
            let groups = NameMatch::Exact(name.to_string()).groups();
            for (day, paths) in days {
                let day_lo = self.zone.day_start(day).ms().max(lo);
                let day_hi = self.zone.day_end(day).ms().min(hi);
                let mut late = Keys::default();
                {
                    let mut stmt = tx.prepare_cached(sql::EVENT_KEYS_BY_NAME)?;
                    let rows = stmt.query_map((name, day_lo, day_hi), |r| {
                        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                    })?;
                    for row in rows {
                        let (source, id) = row?;
                        late.insert(source, id, None);
                    }
                }
                for path in paths {
                    let Some(mut rows) = FileRows::<Event>::open_if_exists(&path, Some(groups.clone()))?
                    else {
                        continue;
                    };
                    while let Some(e) = rows.next_row()? {
                        if e.name == name && (day_lo..day_hi).contains(&e.ts.ms()) {
                            if late.contains(&e.source, &e.id) {
                                both.insert(e.source.clone(), e.id.clone(), None);
                            }
                            count += 1;
                            visit(e);
                        }
                    }
                }
            }
            let mut stmt = tx.prepare_cached(sql::EVENTS_BY_NAME)?;
            let rows = stmt.query_map((name, lo, hi), event_from_row)?;
            for row in rows {
                let e = row?;
                if !both.contains(&e.source, &e.id) {
                    count += 1;
                    visit(e);
                }
            }
            Ok(count)
        })
    }
}
