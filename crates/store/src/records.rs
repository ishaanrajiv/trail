//! Reading events, errors and logs: SQLite for the hot days, the day files for older ones.
//!
//! Rows at or after a store's `evicted_before` mark are all in SQLite. Before it, a day's rows are
//! in its day file(s), plus any late rows SQLite holds until the next archive merges them; a query
//! reads both and keeps one row per (source, id).

use crate::parquet_io::{FileRows, Kept, Keys, Skip};
use crate::write::{ErrorGroup, ErrorStatus};
use crate::{Result, Store, StoreKind, evicted_before, sql};
use rusqlite::types::{Type, Value};
use rusqlite::{Row, Transaction};
use std::cmp::Ordering;
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
    /// Only events older than this (ts, id): the last row of the previous page.
    pub before: Option<(Ts, String)>,
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
    pub before: Option<(Ts, String)>,
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
    pub before: Option<(Ts, String)>,
    pub limit: usize,
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

pub(crate) fn event_params(e: &Event) -> Vec<Value> {
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
    })
}

pub(crate) fn error_params(e: &ErrorRecord) -> Vec<Value> {
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
    })
}

pub(crate) fn log_params(l: &LogRecord) -> Vec<Value> {
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

    fn before(&mut self, before: &Option<(Ts, String)>) {
        if let Some((ts, id)) = before {
            self.and(
                "(ts < ? OR (ts = ? AND id < ?))",
                [
                    Value::Integer(ts.ms()),
                    Value::Integer(ts.ms()),
                    Value::Text(id.clone()),
                ],
            );
        }
    }

    /// Runs `SELECT <cols> FROM <table><where> ORDER BY ts DESC, id DESC LIMIT n`.
    fn newest<T>(
        &self,
        tx: &Transaction<'_>,
        select: &str,
        limit: usize,
        map: fn(&Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<Vec<T>> {
        let text = format!("{select}{} ORDER BY ts DESC, id DESC LIMIT {limit}", self.sql);
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

fn before_ok(ts: Ts, id: &str, before: &Option<(Ts, String)>) -> bool {
    match before {
        None => true,
        Some((bts, bid)) => ts < *bts || (ts == *bts && id < bid.as_str()),
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
            && before_ok(e.ts, &e.id, &self.before)
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
            && before_ok(e.ts, &e.id, &self.before)
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
            && before_ok(l.ts, &l.id, &self.before)
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
            late.insert(r.source().to_string(), r.id().to_string());
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

    /// Events matching `filter`, newest first (by ts, then id).
    pub fn events(&self, filter: &EventFilter) -> Result<Vec<Event>> {
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

    /// Errors matching `filter`, newest first.
    pub fn errors(&self, filter: &ErrorFilter) -> Result<Vec<ErrorRecord>> {
        let limit = limit_of(filter.limit);
        let lo = filter.start.map_or(i64::MIN, Ts::ms);
        let hi = filter.end.map_or(i64::MAX, Ts::ms);
        let select = sql::ERRORS_SELECT;
        self.read(|tx| {
            let query = |a: i64, b: i64, n: usize| filter.sql(a, b).newest(tx, select, n, error_from_row);
            let keep = |e: &ErrorRecord| filter.keep(e, lo, hi);
            let groups = filter.source.as_ref().map(|s| Skip::Bytes {
                column: "source",
                lo: s.as_bytes().to_vec(),
                hi: s.as_bytes().to_vec(),
            });
            self.newest_rows(tx, StoreKind::Errors, lo, hi, limit, &query, &keep, groups)
        })
    }

    /// Log lines matching `filter`, newest first.
    pub fn logs(&self, filter: &LogFilter) -> Result<Vec<LogRecord>> {
        let limit = limit_of(filter.limit);
        let lo = filter.start.map_or(i64::MIN, Ts::ms);
        let hi = filter.end.map_or(i64::MAX, Ts::ms);
        let select = sql::LOGS_SELECT;
        self.read(|tx| {
            let query = |a: i64, b: i64, n: usize| filter.sql(a, b).newest(tx, select, n, log_from_row);
            let keep = |l: &LogRecord| filter.keep(l, lo, hi);
            let groups = filter.source.as_ref().map(|s| Skip::Bytes {
                column: "source",
                lo: s.as_bytes().to_vec(),
                hi: s.as_bytes().to_vec(),
            });
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
                        late.insert(source, id);
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
                                both.insert(e.source.clone(), e.id.clone());
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
