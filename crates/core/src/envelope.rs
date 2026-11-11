//! What a sender posts, and the records Trail keeps.
//!
//! A batch is one JSON object (`IngestBatch`). Each item in it is read on its own: a malformed
//! item is refused with a [`Problem`] that says how to fix it, and the rest of the batch is stored.
//! `spec/ingest.md` is the same contract in prose.

use crate::ids::uuid7;
use crate::names::{is_event_name, is_id, is_metric_name, is_source};
use crate::problem::Problem;
use crate::time::{DAY, Ts};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

/// Properties, context and fields are JSON objects with sorted keys.
pub type Object = Map<String, Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricKind {
    /// A reading: stored as sent.
    Gauge,
    /// A running total: stored as a per-second rate, so every tier averages the same way.
    Counter,
}

impl MetricKind {
    pub fn as_str(self) -> &'static str {
        match self {
            MetricKind::Gauge => "gauge",
            MetricKind::Counter => "counter",
        }
    }

    pub fn parse(text: &str) -> Option<MetricKind> {
        match text {
            "gauge" => Some(MetricKind::Gauge),
            "counter" => Some(MetricKind::Counter),
            _ => None,
        }
    }
}

/// How bad an error is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Warning,
    #[default]
    Error,
    Fatal,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Warning => "warning",
            Severity::Error => "error",
            Severity::Fatal => "fatal",
        }
    }

    pub fn parse(text: &str) -> Option<Severity> {
        match text {
            "warning" => Some(Severity::Warning),
            "error" => Some(Severity::Error),
            "fatal" => Some(Severity::Fatal),
            _ => None,
        }
    }
}

/// A log line's level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Trace,
    Debug,
    #[default]
    Info,
    Warn,
    Error,
    Fatal,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Trace => "trace",
            Level::Debug => "debug",
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
            Level::Fatal => "fatal",
        }
    }

    pub fn parse(text: &str) -> Option<Level> {
        match text {
            "trace" => Some(Level::Trace),
            "debug" => Some(Level::Debug),
            "info" => Some(Level::Info),
            "warn" | "warning" => Some(Level::Warn),
            "error" => Some(Level::Error),
            "fatal" => Some(Level::Fatal),
            _ => None,
        }
    }
}

// ---------- what arrives ----------

/// The body of `POST /v1/ingest`. Items stay as raw JSON here so each can be read, and refused,
/// on its own.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestBatch {
    /// The sending service.
    pub source: String,
    /// Its deployed version, usually a commit.
    #[serde(default)]
    pub version: Option<String>,
    /// The machine it runs on.
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub events: Vec<Value>,
    #[serde(default)]
    pub metrics: Vec<Value>,
    #[serde(default)]
    pub errors: Vec<Value>,
    #[serde(default)]
    pub logs: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventIn {
    #[serde(default)]
    id: Option<String>,
    name: String,
    #[serde(default)]
    ts: Option<i64>,
    #[serde(default)]
    actor: Option<String>,
    #[serde(default)]
    client: Option<String>,
    #[serde(default)]
    trace_id: Option<String>,
    #[serde(default)]
    props: Object,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricIn {
    name: String,
    value: f64,
    #[serde(default)]
    ts: Option<i64>,
    #[serde(default)]
    labels: BTreeMap<String, String>,
    #[serde(default)]
    unit: Option<String>,
    #[serde(default)]
    kind: Option<MetricKind>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorIn {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    ts: Option<i64>,
    #[serde(default)]
    severity: Option<Severity>,
    /// The error's type: an exception class, an error code.
    kind: String,
    message: String,
    #[serde(default)]
    stack: Option<String>,
    #[serde(default)]
    context: Object,
    #[serde(default)]
    trace_id: Option<String>,
    /// Senders that already group their errors may pass their own fingerprint.
    #[serde(default)]
    fingerprint: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogIn {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    ts: Option<i64>,
    #[serde(default)]
    level: Option<Level>,
    /// Which log within the source: a container, a unit, a file.
    #[serde(default)]
    stream: Option<String>,
    message: String,
    #[serde(default)]
    fields: Object,
}

// ---------- what is kept ----------

/// One thing that happened.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub id: String,
    pub name: String,
    pub ts: Ts,
    pub received_at: Ts,
    pub source: String,
    pub version: Option<String>,
    pub host: Option<String>,
    pub actor: Option<String>,
    pub client: Option<String>,
    pub trace_id: Option<String>,
    pub props: Object,
    /// Where the event differs from its declaration. Empty for a clean event. A mismatched event
    /// is stored and marked, never dropped.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub problems: Vec<String>,
}

/// One reading of a metric.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Point {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
    pub ts: Ts,
    pub unit: Option<String>,
    pub kind: Option<MetricKind>,
}

/// One error a service reported or a collector found.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorRecord {
    pub id: String,
    pub ts: Ts,
    pub received_at: Ts,
    pub source: String,
    pub version: Option<String>,
    pub host: Option<String>,
    pub severity: Severity,
    pub kind: String,
    pub message: String,
    /// Groups repeats of the same error (see `fingerprint`).
    pub fingerprint: String,
    pub stack: Option<String>,
    pub context: Object,
    pub trace_id: Option<String>,
}

/// One log line.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogRecord {
    pub id: String,
    pub ts: Ts,
    pub received_at: Ts,
    pub source: String,
    pub host: Option<String>,
    pub stream: Option<String>,
    pub level: Level,
    pub message: String,
    pub fields: Object,
}

// ---------- reading a batch ----------

/// Size limits on one record. Larger text is cut, not refused, except properties.
pub const MAX_PROPS_BYTES: usize = 64 * 1024;
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024;
pub const MAX_STACK_BYTES: usize = 64 * 1024;
const MAX_LABELS: usize = 32;
const MAX_SHORT_TEXT: usize = 256;

/// What every item in a batch shares.
#[derive(Debug, Clone, Copy)]
pub struct BatchContext<'a> {
    pub source: &'a str,
    pub version: Option<&'a str>,
    pub host: Option<&'a str>,
    pub now: Ts,
}

impl<'a> BatchContext<'a> {
    /// Checks the batch's own fields. An item can only be read once this passes.
    pub fn new(batch: &'a IngestBatch, now: Ts) -> Result<BatchContext<'a>, Problem> {
        if !is_source(&batch.source) {
            return Err(Problem::new(
                "source",
                format!("`{}` is not a source name", batch.source),
                "use lowercase letters, digits and hyphens, starting with a letter, like `shop` or `mail-worker`",
            ));
        }
        for (field, value) in [("version", &batch.version), ("host", &batch.host)] {
            if let Some(text) = value {
                short_text(field, text)?;
            }
        }
        Ok(BatchContext {
            source: &batch.source,
            version: batch.version.as_deref(),
            host: batch.host.as_deref(),
            now,
        })
    }
}

fn short_text(at: &str, text: &str) -> Result<(), Problem> {
    if text.is_empty() || text.len() > MAX_SHORT_TEXT {
        return Err(Problem::new(
            at,
            format!("must be 1 to {MAX_SHORT_TEXT} bytes, got {}", text.len()),
            "send a short identifier, or leave the field out",
        ));
    }
    Ok(())
}

fn read<T: serde::de::DeserializeOwned>(at: &str, raw: Value, shape: &str) -> Result<T, Problem> {
    serde_json::from_value(raw)
        .map_err(|e| Problem::new(at, e.to_string(), format!("send {shape}; see spec/ingest.md")))
}

fn timestamp(at: &str, ts: Option<i64>, now: Ts) -> Result<Ts, Problem> {
    let Some(ts) = ts else { return Ok(now) };
    check_ts(at, ts, now)?;
    Ok(Ts(ts))
}

/// A time a record can be stored at: from the year 2000 to a day ahead of `now`. Anything else
/// is a unit mix-up (seconds or nanoseconds read as milliseconds) or a clock that was never set.
pub fn check_ts(at: &str, ts: i64, now: Ts) -> Result<(), Problem> {
    // 2000-01-01, and a day ahead for clocks that run fast.
    if ts < 946_684_800_000 {
        let hint = if ts > 946_684_800 {
            " It looks like seconds: multiply by 1000."
        } else {
            ""
        };
        return Err(Problem::new(
            at,
            format!("ts {ts} is before the year 2000"),
            format!("send milliseconds since 1970, or leave ts out to use the arrival time.{hint}"),
        ));
    }
    if ts > now.ms() + DAY {
        return Err(Problem::new(
            at,
            format!("ts {ts} is more than a day in the future"),
            "send milliseconds since 1970 (not microseconds or nanoseconds), or leave ts out",
        ));
    }
    Ok(())
}

fn record_id(at: &str, id: Option<String>, ts: Ts) -> Result<String, Problem> {
    match id {
        None => Ok(uuid7(ts)),
        Some(id) if is_id(&id) => Ok(id),
        Some(id) => Err(Problem::new(
            at,
            format!("id `{id}` is not usable"),
            "send 1 to 64 letters, digits or `_ - : .` (a UUID works), or leave id out",
        )),
    }
}

fn optional_text(at: &str, field: &str, value: Option<String>) -> Result<Option<String>, Problem> {
    match value {
        None => Ok(None),
        Some(text) => short_text(&format!("{at}.{field}"), &text).map(|()| Some(text)),
    }
}

/// Cuts text to at most `max` bytes on a character boundary.
fn cut(mut text: String, max: usize) -> String {
    if text.len() > max {
        let mut end = max;
        while end > 0 && !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}

/// Reads one event. `at` names the item for the error, like `events[2]`.
pub fn event(ctx: &BatchContext<'_>, at: &str, raw: Value) -> Result<Event, Problem> {
    let item: EventIn = read(at, raw, "an object with `name` and `props`")?;
    if !is_event_name(&item.name) {
        return Err(Problem::new(
            at,
            format!("`{}` is not an event name", item.name),
            "use two or more snake_case parts joined by dots, like `shop.order.placed`",
        ));
    }
    let size = serde_json::to_string(&item.props)
        .map(|s| s.len())
        .unwrap_or(usize::MAX);
    if size > MAX_PROPS_BYTES {
        return Err(Problem::new(
            at,
            format!("props are {size} bytes; the limit is {MAX_PROPS_BYTES}"),
            "send identifiers and numbers in props, not payloads; put long text in a log line",
        ));
    }
    let ts = timestamp(at, item.ts, ctx.now)?;
    Ok(Event {
        id: record_id(at, item.id, ts)?,
        name: item.name,
        ts,
        received_at: ctx.now,
        source: ctx.source.to_string(),
        version: ctx.version.map(str::to_string),
        host: ctx.host.map(str::to_string),
        actor: optional_text(at, "actor", item.actor)?,
        client: optional_text(at, "client", item.client)?,
        trace_id: optional_text(at, "trace_id", item.trace_id)?,
        props: item.props,
        problems: Vec::new(),
    })
}

/// Reads one metric reading.
pub fn point(ctx: &BatchContext<'_>, at: &str, raw: Value) -> Result<Point, Problem> {
    let item: MetricIn = read(at, raw, "an object with `name` and a numeric `value`")?;
    check_point(at, &item.name, item.value, &item.labels)?;
    Ok(Point {
        ts: timestamp(at, item.ts, ctx.now)?,
        name: item.name,
        labels: item.labels,
        value: item.value,
        unit: optional_text(at, "unit", item.unit)?,
        kind: item.kind,
    })
}

/// The checks a reading gets however it arrived (JSON or line protocol).
pub fn check_point(
    at: &str,
    name: &str,
    value: f64,
    labels: &BTreeMap<String, String>,
) -> Result<(), Problem> {
    if !is_metric_name(name) {
        return Err(Problem::new(
            at,
            format!("`{name}` is not a metric name"),
            "use letters, digits and `_ . : -`, starting with a letter, like `cpu.usage_active`",
        ));
    }
    if !value.is_finite() {
        return Err(Problem::new(
            at,
            format!("value of `{name}` is not a finite number"),
            "skip readings that are NaN or infinite",
        ));
    }
    if labels.len() > MAX_LABELS {
        return Err(Problem::new(
            at,
            format!("`{name}` has {} labels; the limit is {MAX_LABELS}", labels.len()),
            "keep labels to what you chart or filter by",
        ));
    }
    for (key, value) in labels {
        if !is_metric_name(key) || value.len() > MAX_SHORT_TEXT {
            return Err(Problem::new(
                at,
                format!("label `{key}` of `{name}` is not usable"),
                format!("label names follow metric naming, and values are at most {MAX_SHORT_TEXT} bytes"),
            ));
        }
    }
    Ok(())
}

/// Reads one error.
pub fn error(ctx: &BatchContext<'_>, at: &str, raw: Value) -> Result<ErrorRecord, Problem> {
    let item: ErrorIn = read(at, raw, "an object with `kind` and `message`")?;
    short_text(&format!("{at}.kind"), &item.kind)?;
    let ts = timestamp(at, item.ts, ctx.now)?;
    let message = cut(item.message, MAX_MESSAGE_BYTES);
    let stack = item.stack.map(|s| cut(s, MAX_STACK_BYTES));
    let fingerprint = match item.fingerprint {
        Some(given) if is_id(&given) => given,
        Some(given) => {
            return Err(Problem::new(
                at,
                format!("fingerprint `{given}` is not usable"),
                "send 1 to 64 letters, digits or `_ - : .`, or leave it out and Trail works one out",
            ));
        }
        None => crate::fingerprint::fingerprint(ctx.source, &item.kind, &message, stack.as_deref()),
    };
    Ok(ErrorRecord {
        id: record_id(at, item.id, ts)?,
        ts,
        received_at: ctx.now,
        source: ctx.source.to_string(),
        version: ctx.version.map(str::to_string),
        host: ctx.host.map(str::to_string),
        severity: item.severity.unwrap_or_default(),
        kind: item.kind,
        message,
        fingerprint,
        stack,
        context: item.context,
        trace_id: optional_text(at, "trace_id", item.trace_id)?,
    })
}

/// Reads one log line.
pub fn log(ctx: &BatchContext<'_>, at: &str, raw: Value) -> Result<LogRecord, Problem> {
    let item: LogIn = read(at, raw, "an object with `message`")?;
    let ts = timestamp(at, item.ts, ctx.now)?;
    Ok(LogRecord {
        id: record_id(at, item.id, ts)?,
        ts,
        received_at: ctx.now,
        source: ctx.source.to_string(),
        host: ctx.host.map(str::to_string),
        stream: optional_text(at, "stream", item.stream)?,
        level: item.level.unwrap_or_default(),
        message: cut(item.message, MAX_MESSAGE_BYTES),
        fields: item.fields,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn batch() -> IngestBatch {
        serde_json::from_value(json!({"source": "shop", "version": "abc123", "host": "box"})).unwrap()
    }

    #[test]
    fn reads_an_event_and_fills_the_envelope() {
        let batch = batch();
        let ctx = BatchContext::new(&batch, Ts(1_794_000_000_000)).unwrap();
        let e = event(
            &ctx,
            "events[0]",
            json!({"name": "shop.order.placed", "actor": "user", "props": {"total_cents": 1200, "b": 1, "a": 2}}),
        )
        .unwrap();
        assert_eq!(e.source, "shop");
        assert_eq!(e.version.as_deref(), Some("abc123"));
        assert_eq!(e.ts, Ts(1_794_000_000_000));
        assert_eq!(e.id.len(), 36);
        assert_eq!(
            serde_json::to_string(&e.props).unwrap(),
            r#"{"a":2,"b":1,"total_cents":1200}"#
        );
    }

    #[test]
    fn refuses_with_a_fix() {
        let batch = batch();
        let ctx = BatchContext::new(&batch, Ts(1_794_000_000_000)).unwrap();
        let unknown = event(&ctx, "events[0]", json!({"nme": "x.y"})).unwrap_err();
        assert!(unknown.message.contains("nme"), "{unknown}");
        let seconds = event(&ctx, "events[1]", json!({"name": "x.y", "ts": 1_794_000_000})).unwrap_err();
        assert!(seconds.fix.contains("multiply by 1000"), "{seconds}");
        let name = event(&ctx, "events[2]", json!({"name": "Order"})).unwrap_err();
        assert!(name.fix.contains("snake_case"));
        let nan = point(
            &ctx,
            "metrics[0]",
            json!({"name": "a.b", "value": 1.0, "labels": {"bad key": "x"}}),
        )
        .unwrap_err();
        assert!(nan.message.contains("bad key"));
    }

    #[test]
    fn a_bad_source_stops_the_batch() {
        let batch: IngestBatch = serde_json::from_value(json!({"source": "Shop"})).unwrap();
        assert!(BatchContext::new(&batch, Ts(0)).is_err());
        assert!(serde_json::from_value::<IngestBatch>(json!({"source": "shop", "evnts": []})).is_err());
    }

    #[test]
    fn errors_get_a_fingerprint_and_long_text_is_cut() {
        let batch = batch();
        let ctx = BatchContext::new(&batch, Ts(1_794_000_000_000)).unwrap();
        let a = error(
            &ctx,
            "errors[0]",
            json!({"kind": "KeyError", "message": "missing key 42"}),
        )
        .unwrap();
        let b = error(
            &ctx,
            "errors[1]",
            json!({"kind": "KeyError", "message": "missing key 77"}),
        )
        .unwrap();
        assert_eq!(a.fingerprint, b.fingerprint);
        assert_eq!(a.severity, Severity::Error);
        let long = log(&ctx, "logs[0]", json!({"message": "é".repeat(MAX_MESSAGE_BYTES)})).unwrap();
        assert!(long.message.len() <= MAX_MESSAGE_BYTES);
        assert_eq!(long.level, Level::Info);
    }
}
