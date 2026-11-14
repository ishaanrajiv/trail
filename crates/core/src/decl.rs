//! A service's declarations: what it sends and what Trail should do with it.
//!
//! Four TOML files sit beside the service's code, in one folder:
//!
//! - `events.toml`: the source's name and every event with its typed properties (required).
//! - `metrics.toml`: the metrics it sends, with unit, kind and labels.
//! - `rollups.toml`: aggregations Trail computes as events arrive.
//! - `alerts.toml`: alert rules on metrics and on single events.
//!
//! `spec/declarations.md` documents the format. [`load_dir`] reads and checks a folder; every
//! problem it reports says where it is and how to fix it. The typed model round-trips through
//! JSON, which is how `trail publish` hands declarations to the server.

use crate::envelope::MetricKind;
use crate::names::{
    ENVELOPE_FIELDS, GROUPABLE_ENVELOPE_FIELDS, is_alert_kind, is_event_name, is_ident, is_metric_name,
    is_service, is_source, secret_word,
};
use crate::problem::{Problem, closest};
use crate::time::{Every, parse_duration};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

// ---------- the typed model ----------

/// Everything one source declares.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Declarations {
    pub source: String,
    /// The service this source belongs to, when it is not the source's own name: an app, its
    /// worker and its container's logs are three sources of one service. See [`Declarations::service`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    #[serde(default)]
    pub events: BTreeMap<String, EventDecl>,
    #[serde(default)]
    pub metrics: BTreeMap<String, MetricDecl>,
    #[serde(default)]
    pub rollups: BTreeMap<String, RollupDecl>,
    #[serde(default)]
    pub alerts: BTreeMap<String, AlertDecl>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventDecl {
    pub description: String,
    pub retention: Retention,
    #[serde(default)]
    pub props: BTreeMap<String, PropType>,
}

/// How long raw events of one name are kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum Retention {
    Forever,
    Days(u32),
}

impl Retention {
    pub fn parse(text: &str) -> Result<Retention, String> {
        match parse_duration(text)? {
            None => Ok(Retention::Forever),
            Some(ms) if ms % crate::time::DAY == 0 => u32::try_from(ms / crate::time::DAY)
                .map(Retention::Days)
                .map_err(|_| format!("`{text}` is too long a retention; write forever")),
            Some(_) => Err(format!(
                "retention `{text}` must be whole days, like 30d or 90d, or forever"
            )),
        }
    }
}

impl fmt::Display for Retention {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Retention::Forever => write!(f, "forever"),
            Retention::Days(days) => write!(f, "{days}d"),
        }
    }
}

impl TryFrom<String> for Retention {
    type Error = String;
    fn try_from(text: String) -> Result<Retention, String> {
        Retention::parse(&text)
    }
}

impl From<Retention> for String {
    fn from(retention: Retention) -> String {
        retention.to_string()
    }
}

/// A property's type. Written `"int"`, `"string?"` (optional), or `{ enum = ["a", "b"] }`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "PropRepr", into = "PropRepr")]
pub struct PropType {
    pub base: Base,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Base {
    String,
    Int,
    Float,
    Bool,
    /// A list of strings, written `string[]`.
    StringList,
    /// Any JSON value. An escape hatch: nothing can aggregate over it.
    Json,
    /// One of a fixed set of strings.
    Enum(Vec<String>),
}

impl Base {
    pub fn name(&self) -> &'static str {
        match self {
            Base::String => "string",
            Base::Int => "int",
            Base::Float => "float",
            Base::Bool => "bool",
            Base::StringList => "string[]",
            Base::Json => "json",
            Base::Enum(_) => "enum",
        }
    }

    fn is_numeric(&self) -> bool {
        matches!(self, Base::Int | Base::Float)
    }

    /// Whether an aggregation may group by this type: few distinct values, each a short scalar.
    fn is_groupable(&self) -> bool {
        matches!(self, Base::String | Base::Int | Base::Bool | Base::Enum(_))
    }
}

impl PropType {
    pub const TYPE_NAMES: &'static str =
        "string, int, float, bool, string[], json, or { enum = [\"a\", \"b\"] }; add ? for optional";

    fn parse_text(text: &str) -> Result<PropType, String> {
        let (name, optional) = match text.strip_suffix('?') {
            Some(name) => (name, true),
            None => (text, false),
        };
        let base = match name {
            "string" => Base::String,
            "int" => Base::Int,
            "float" => Base::Float,
            "bool" => Base::Bool,
            "string[]" => Base::StringList,
            "json" => Base::Json,
            _ => return Err(format!("`{text}` is not a property type")),
        };
        Ok(PropType { base, optional })
    }

    /// Whether a JSON value fits this type. `null` fits only an optional property.
    pub fn accepts(&self, value: &Value) -> bool {
        match (&self.base, value) {
            (_, Value::Null) => self.optional,
            (Base::String, Value::String(_)) | (Base::Bool, Value::Bool(_)) | (Base::Json, _) => true,
            (Base::Int, Value::Number(n)) => n.is_i64() || n.is_u64(),
            (Base::Float, Value::Number(_)) => true,
            (Base::StringList, Value::Array(items)) => items.iter().all(Value::is_string),
            (Base::Enum(values), Value::String(text)) => values.contains(text),
            _ => false,
        }
    }

    /// The type as a reader would write it: `int?`, `enum(card, cash)`.
    pub fn describe(&self) -> String {
        let base = match &self.base {
            Base::Enum(values) => format!("enum({})", values.join(", ")),
            other => other.name().to_string(),
        };
        if self.optional { format!("{base}?") } else { base }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum PropRepr {
    Text(String),
    Enum {
        #[serde(rename = "enum")]
        values: Vec<String>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        optional: bool,
    },
}

impl TryFrom<PropRepr> for PropType {
    type Error = String;
    fn try_from(repr: PropRepr) -> Result<PropType, String> {
        match repr {
            PropRepr::Text(text) => PropType::parse_text(&text),
            PropRepr::Enum { values, optional } => Ok(PropType {
                base: Base::Enum(values),
                optional,
            }),
        }
    }
}

impl From<PropType> for PropRepr {
    fn from(prop: PropType) -> PropRepr {
        match prop.base {
            Base::Enum(values) => PropRepr::Enum {
                values,
                optional: prop.optional,
            },
            other => PropRepr::Text(if prop.optional {
                format!("{}?", other.name())
            } else {
                other.name().to_string()
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricDecl {
    pub description: String,
    #[serde(default)]
    pub unit: Option<String>,
    pub kind: MetricKind,
    #[serde(default)]
    pub labels: Vec<String>,
}

/// An aggregation: Trail computes it as events arrive and writes one metric series per measure
/// and group, named `<source>.<rollup>.<measure>`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RollupDecl {
    pub description: String,
    /// The event it counts.
    pub from: String,
    /// Only events whose properties equal these values.
    #[serde(default, rename = "where")]
    pub filter: BTreeMap<String, Scalar>,
    /// Properties (or `actor`, `client`, `host`) to group by. Each becomes a label.
    #[serde(default)]
    pub by: Vec<String>,
    #[serde(rename = "measure")]
    pub measures: Vec<Measure>,
    pub every: Every,
}

impl RollupDecl {
    /// The metric series this rollup writes, one per measure.
    pub fn series_names(&self, source: &str, rollup: &str) -> Vec<(Measure, String)> {
        self.measures
            .iter()
            .map(|m| (m.clone(), format!("{source}.{rollup}.{}", m.suffix())))
            .collect()
    }
}

/// What an aggregation computes per group and bucket.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum Measure {
    Count,
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
    /// A percentile (50, 90, 95 or 99) of a numeric property.
    Percentile(u8, String),
}

impl Measure {
    pub const FORMS: &'static str =
        "count, sum(prop), avg(prop), min(prop), max(prop), p50(prop), p90(prop), p95(prop) or p99(prop)";

    pub fn parse(text: &str) -> Result<Measure, String> {
        if text == "count" {
            return Ok(Measure::Count);
        }
        let bad = || format!("`{text}` is not a measure; write {}", Measure::FORMS);
        let (function, rest) = text.split_once('(').ok_or_else(bad)?;
        let prop = rest.strip_suffix(')').ok_or_else(bad)?.trim();
        if !is_ident(prop) {
            return Err(bad());
        }
        let prop = prop.to_string();
        match function {
            "sum" => Ok(Measure::Sum(prop)),
            "avg" => Ok(Measure::Avg(prop)),
            "min" => Ok(Measure::Min(prop)),
            "max" => Ok(Measure::Max(prop)),
            "p50" => Ok(Measure::Percentile(50, prop)),
            "p90" => Ok(Measure::Percentile(90, prop)),
            "p95" => Ok(Measure::Percentile(95, prop)),
            "p99" => Ok(Measure::Percentile(99, prop)),
            _ => Err(bad()),
        }
    }

    /// The property it reads. None for `count`.
    pub fn prop(&self) -> Option<&str> {
        match self {
            Measure::Count => None,
            Measure::Sum(p)
            | Measure::Avg(p)
            | Measure::Min(p)
            | Measure::Max(p)
            | Measure::Percentile(_, p) => Some(p),
        }
    }

    /// The end of the series name: `count`, `latency_ms.p95`.
    pub fn suffix(&self) -> String {
        match self {
            Measure::Count => "count".to_string(),
            Measure::Sum(p) => format!("{p}.sum"),
            Measure::Avg(p) => format!("{p}.avg"),
            Measure::Min(p) => format!("{p}.min"),
            Measure::Max(p) => format!("{p}.max"),
            Measure::Percentile(n, p) => format!("{p}.p{n}"),
        }
    }
}

impl fmt::Display for Measure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Measure::Count => write!(f, "count"),
            Measure::Sum(p) => write!(f, "sum({p})"),
            Measure::Avg(p) => write!(f, "avg({p})"),
            Measure::Min(p) => write!(f, "min({p})"),
            Measure::Max(p) => write!(f, "max({p})"),
            Measure::Percentile(n, p) => write!(f, "p{n}({p})"),
        }
    }
}

impl TryFrom<String> for Measure {
    type Error = String;
    fn try_from(text: String) -> Result<Measure, String> {
        Measure::parse(&text)
    }
}

impl From<Measure> for String {
    fn from(measure: Measure) -> String {
        measure.to_string()
    }
}

/// A value a `where` filter compares a property with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Scalar {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

impl Scalar {
    /// Whether a property's JSON value equals this.
    pub fn matches(&self, value: &Value) -> bool {
        match (self, value) {
            (Scalar::Bool(a), Value::Bool(b)) => a == b,
            (Scalar::Int(a), Value::Number(n)) => n.as_i64() == Some(*a),
            #[allow(clippy::float_cmp)]
            (Scalar::Float(a), Value::Number(n)) => n.as_f64() == Some(*a),
            (Scalar::Text(a), Value::String(b)) => a == b,
            _ => false,
        }
    }

    fn fits(&self, base: &Base) -> bool {
        match (self, base) {
            (Scalar::Bool(_), Base::Bool)
            | (Scalar::Int(_), Base::Int | Base::Float)
            | (Scalar::Float(_), Base::Float) => true,
            (Scalar::Text(_), Base::String) => true,
            (Scalar::Text(text), Base::Enum(values)) => values.contains(text),
            _ => false,
        }
    }

    fn describe(&self) -> String {
        match self {
            Scalar::Bool(b) => b.to_string(),
            Scalar::Int(n) => n.to_string(),
            Scalar::Float(n) => n.to_string(),
            Scalar::Text(t) => format!("\"{t}\""),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    Info,
    #[default]
    Warning,
    Severe,
}

impl AlertSeverity {
    pub fn as_str(self) -> &'static str {
        match self {
            AlertSeverity::Info => "info",
            AlertSeverity::Warning => "warning",
            AlertSeverity::Severe => "severe",
        }
    }

    fn parse(text: &str) -> Option<AlertSeverity> {
        match text {
            "info" => Some(AlertSeverity::Info),
            "warning" => Some(AlertSeverity::Warning),
            "severe" => Some(AlertSeverity::Severe),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlertDecl {
    pub severity: AlertSeverity,
    /// The name the receiver knows this rule's alerts by (`shop.queue_backlog`). Trail only
    /// passes it on. None: the receiver has only the severity to go by.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// `{name}` is replaced: a label or `value` for a metric rule, a property for an event rule.
    pub title: String,
    #[serde(default)]
    pub body: String,
    pub rule: AlertRule,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertRule {
    /// Fires per series while its value is past a threshold, or while it has gone quiet.
    Metric {
        metric: String,
        /// Only series whose labels equal these.
        #[serde(default)]
        labels: BTreeMap<String, String>,
        condition: Condition,
        /// The average over this window (ms) has to be past the threshold. None: the latest reading decides.
        #[serde(default)]
        for_ms: Option<i64>,
        #[serde(default)]
        unit: Option<String>,
    },
    /// Fires once for each matching event.
    Event {
        event: String,
        #[serde(default, rename = "where")]
        filter: BTreeMap<String, Scalar>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Condition {
    /// Fires above `threshold`; once firing, clears at or below `clear`.
    Above { threshold: f64, clear: f64 },
    /// Fires below `threshold`; once firing, clears at or above `clear`.
    Below { threshold: f64, clear: f64 },
    /// Fires when the newest reading is older than this (ms).
    Absent { after_ms: i64 },
}

impl Declarations {
    /// The service this source belongs to: `service` from events.toml, else the source's name.
    pub fn service(&self) -> &str {
        self.service.as_deref().unwrap_or(&self.source)
    }

    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    /// These declarations added to `existing` by name: each event, metric, rollup and alert rule
    /// here replaces the one of the same name, and the ones only `existing` has are kept. The
    /// source's own fields (`service`) are these. What `PUT /v1/registry/{source}?keep=true` publishes.
    #[must_use]
    pub fn keeping(self, existing: &Declarations) -> Declarations {
        fn add<T: Clone>(kept: &BTreeMap<String, T>, new: BTreeMap<String, T>) -> BTreeMap<String, T> {
            let mut all = kept.clone();
            all.extend(new);
            all
        }
        Declarations {
            events: add(&existing.events, self.events),
            metrics: add(&existing.metrics, self.metrics),
            rollups: add(&existing.rollups, self.rollups),
            alerts: add(&existing.alerts, self.alerts),
            ..self
        }
    }

    /// Reads declarations sent as JSON and checks them like a folder's.
    pub fn from_json(value: Value) -> Result<Declarations, Vec<Problem>> {
        let decl: Declarations = serde_json::from_value(value).map_err(|e| {
            vec![Problem::new(
                "declarations",
                e.to_string(),
                "publish with `trail publish <folder>`, which builds this from the TOML files",
            )]
        })?;
        let problems = validate(&decl);
        if problems.is_empty() {
            Ok(decl)
        } else {
            Err(problems)
        }
    }
}

// ---------- the TOML files ----------

/// The text of a declarations folder's files.
#[derive(Debug, Clone, Default)]
pub struct Files {
    pub events: String,
    pub metrics: Option<String>,
    pub rollups: Option<String>,
    pub alerts: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEvents {
    source: String,
    #[serde(default)]
    service: Option<String>,
    #[serde(default)]
    events: BTreeMap<String, RawEvent>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEvent {
    #[serde(default)]
    description: String,
    #[serde(default)]
    retention: Option<String>,
    #[serde(default)]
    props: BTreeMap<String, toml::Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMetrics {
    #[serde(default)]
    metrics: BTreeMap<String, RawMetric>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMetric {
    #[serde(default)]
    description: String,
    #[serde(default)]
    unit: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    labels: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRollups {
    #[serde(default)]
    rollups: BTreeMap<String, RawRollup>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRollup {
    #[serde(default)]
    description: String,
    from: String,
    #[serde(default, rename = "where")]
    filter: BTreeMap<String, toml::Value>,
    #[serde(default)]
    by: Vec<String>,
    measure: Vec<String>,
    every: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAlerts {
    #[serde(default)]
    alerts: BTreeMap<String, RawAlert>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAlert {
    #[serde(default)]
    severity: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    title: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    metric: Option<String>,
    #[serde(default, rename = "match")]
    labels: BTreeMap<String, String>,
    #[serde(default)]
    above: Option<f64>,
    #[serde(default)]
    below: Option<f64>,
    #[serde(default)]
    clear: Option<f64>,
    #[serde(default, rename = "for")]
    hold: Option<String>,
    #[serde(default)]
    absent: Option<String>,
    #[serde(default)]
    unit: Option<String>,
    #[serde(default)]
    event: Option<String>,
    #[serde(default, rename = "where")]
    filter: BTreeMap<String, toml::Value>,
}

/// Reads and checks a declarations folder. `events.toml` is required; the other three are optional.
pub fn load_dir(dir: &Path) -> Result<Declarations, Vec<Problem>> {
    let read = |name: &str| std::fs::read_to_string(dir.join(name)).ok();
    let Some(events) = read("events.toml") else {
        return Err(vec![Problem::new(
            dir.display().to_string(),
            "no events.toml in this folder",
            "create events.toml starting with `source = \"<service name>\"`; see spec/declarations.md",
        )]);
    };
    parse(&Files {
        events,
        metrics: read("metrics.toml"),
        rollups: read("rollups.toml"),
        alerts: read("alerts.toml"),
    })
}

/// Parses and checks the text of a folder's files.
pub fn parse(files: &Files) -> Result<Declarations, Vec<Problem>> {
    let mut problems = Vec::new();
    let mut decl = Declarations {
        source: String::new(),
        service: None,
        events: BTreeMap::new(),
        metrics: BTreeMap::new(),
        rollups: BTreeMap::new(),
        alerts: BTreeMap::new(),
    };

    let mut events_parsed = false;
    match toml::from_str::<RawEvents>(&files.events) {
        Err(e) => problems.push(syntax("events.toml", &e)),
        Ok(raw) => {
            events_parsed = true;
            decl.source = raw.source;
            decl.service = raw.service;
            for (name, event) in raw.events {
                let at = format!("events.toml: events.\"{name}\"");
                let retention = match event.retention.as_deref().map(Retention::parse) {
                    None => Retention::Forever,
                    Some(Ok(retention)) => retention,
                    Some(Err(message)) => {
                        problems.push(Problem::new(
                            format!("{at}.retention"),
                            message,
                            "write forever, or whole days like 30d or 90d",
                        ));
                        Retention::Forever
                    }
                };
                let mut props = BTreeMap::new();
                for (prop, value) in event.props {
                    match prop_type(&value) {
                        Ok(parsed) => {
                            props.insert(prop, parsed);
                        }
                        Err(message) => problems.push(Problem::new(
                            format!("{at}.props.{prop}"),
                            message,
                            format!("write one of: {}", PropType::TYPE_NAMES),
                        )),
                    }
                }
                decl.events.insert(
                    name,
                    EventDecl {
                        description: event.description,
                        retention,
                        props,
                    },
                );
            }
        }
    }

    if let Some(text) = &files.metrics {
        match toml::from_str::<RawMetrics>(text) {
            Err(e) => problems.push(syntax("metrics.toml", &e)),
            Ok(raw) => {
                for (name, metric) in raw.metrics {
                    let at = format!("metrics.toml: metrics.\"{name}\"");
                    let kind = match metric.kind.as_deref() {
                        None => MetricKind::Gauge,
                        Some(text) => MetricKind::parse(text).unwrap_or_else(|| {
                            problems.push(Problem::new(
                                format!("{at}.kind"),
                                format!("`{text}` is not a metric kind"),
                                "write gauge (a reading) or counter (a running total)",
                            ));
                            MetricKind::Gauge
                        }),
                    };
                    decl.metrics.insert(
                        name,
                        MetricDecl {
                            description: metric.description,
                            unit: metric.unit,
                            kind,
                            labels: metric.labels,
                        },
                    );
                }
            }
        }
    }

    if let Some(text) = &files.rollups {
        match toml::from_str::<RawRollups>(text) {
            Err(e) => problems.push(syntax("rollups.toml", &e)),
            Ok(raw) => {
                for (name, rollup) in raw.rollups {
                    let at = format!("rollups.toml: rollups.{name}");
                    let mut measures = Vec::new();
                    for text in &rollup.measure {
                        match Measure::parse(text) {
                            Ok(measure) => measures.push(measure),
                            Err(message) => {
                                problems.push(Problem::new(
                                    format!("{at}.measure"),
                                    message,
                                    format!("write {}", Measure::FORMS),
                                ));
                            }
                        }
                    }
                    let every = Every::parse(&rollup.every).unwrap_or_else(|| {
                        problems.push(Problem::new(
                            format!("{at}.every"),
                            format!("`{}` is not a cadence", rollup.every),
                            "write 1m, 5m, 1h or 1d",
                        ));
                        Every::Minute
                    });
                    let filter = scalars(&format!("{at}.where"), rollup.filter, &mut problems);
                    decl.rollups.insert(
                        name,
                        RollupDecl {
                            description: rollup.description,
                            from: rollup.from,
                            filter,
                            by: rollup.by,
                            measures,
                            every,
                        },
                    );
                }
            }
        }
    }

    if let Some(text) = &files.alerts {
        match toml::from_str::<RawAlerts>(text) {
            Err(e) => problems.push(syntax("alerts.toml", &e)),
            Ok(raw) => {
                for (name, alert) in raw.alerts {
                    let at = format!("alerts.toml: alerts.{name}");
                    if let Some(parsed) = alert_decl(&at, alert, &mut problems) {
                        decl.alerts.insert(name, parsed);
                    }
                }
            }
        }
    }

    // Report everything in one pass. Without events.toml nothing else can be checked against it.
    if events_parsed {
        problems.extend(validate(&decl));
    }
    if problems.is_empty() {
        Ok(decl)
    } else {
        Err(problems)
    }
}

fn syntax(file: &str, error: &toml::de::Error) -> Problem {
    Problem::new(
        file,
        error.to_string().trim_end().to_string(),
        "fix the TOML; spec/declarations.md shows every field",
    )
}

fn prop_type(value: &toml::Value) -> Result<PropType, String> {
    match value {
        toml::Value::String(text) => PropType::parse_text(text),
        toml::Value::Table(table) => {
            let unknown: Vec<&str> = table
                .keys()
                .map(String::as_str)
                .filter(|k| !matches!(*k, "enum" | "optional"))
                .collect();
            if !unknown.is_empty() {
                return Err(format!(
                    "unknown key `{}` in a property type",
                    unknown.join("`, `")
                ));
            }
            let values = match table.get("enum") {
                Some(toml::Value::Array(items)) => items
                    .iter()
                    .map(|item| {
                        item.as_str()
                            .map(str::to_string)
                            .ok_or_else(|| "enum values must be strings".to_string())
                    })
                    .collect::<Result<Vec<String>, String>>()?,
                _ => return Err("a table type needs `enum = [\"a\", \"b\"]`".to_string()),
            };
            let optional = match table.get("optional") {
                None => false,
                Some(toml::Value::Boolean(flag)) => *flag,
                Some(_) => return Err("`optional` must be true or false".to_string()),
            };
            Ok(PropType {
                base: Base::Enum(values),
                optional,
            })
        }
        other => Err(format!(
            "a property type is a string or a table, not {}",
            other.type_str()
        )),
    }
}

fn scalars(
    at: &str,
    raw: BTreeMap<String, toml::Value>,
    problems: &mut Vec<Problem>,
) -> BTreeMap<String, Scalar> {
    let mut out = BTreeMap::new();
    for (key, value) in raw {
        let scalar = match value {
            toml::Value::String(text) => Scalar::Text(text),
            toml::Value::Integer(n) => Scalar::Int(n),
            toml::Value::Float(n) => Scalar::Float(n),
            toml::Value::Boolean(b) => Scalar::Bool(b),
            other => {
                problems.push(Problem::new(
                    format!("{at}.{key}"),
                    format!(
                        "a filter compares with a string, number or boolean, not {}",
                        other.type_str()
                    ),
                    "write a single value, like `ok = false` or `method = \"card\"`",
                ));
                continue;
            }
        };
        out.insert(key, scalar);
    }
    out
}

fn alert_decl(at: &str, raw: RawAlert, problems: &mut Vec<Problem>) -> Option<AlertDecl> {
    let severity = match raw.severity.as_deref() {
        None => AlertSeverity::default(),
        Some(text) => AlertSeverity::parse(text).unwrap_or_else(|| {
            problems.push(Problem::new(
                format!("{at}.severity"),
                format!("`{text}` is not a severity"),
                "write info, warning or severe",
            ));
            AlertSeverity::default()
        }),
    };
    let duration = |field: &str, text: &str, problems: &mut Vec<Problem>| match parse_duration(text) {
        Ok(Some(ms)) => Some(ms),
        Ok(None) => {
            problems.push(Problem::new(
                format!("{at}.{field}"),
                "needs a length of time",
                "write a duration like 2m or 10m",
            ));
            None
        }
        Err(message) => {
            problems.push(Problem::new(
                format!("{at}.{field}"),
                message,
                "write a duration like 2m or 10m",
            ));
            None
        }
    };
    let rule = match (raw.metric, raw.event) {
        (Some(_), Some(_)) | (None, None) => {
            problems.push(Problem::new(
                at,
                "an alert watches either a metric or an event",
                "set exactly one of `metric = \"...\"` or `event = \"...\"`",
            ));
            return None;
        }
        (None, Some(event)) => {
            for (field, set) in [
                ("above", raw.above.is_some()),
                ("below", raw.below.is_some()),
                ("clear", raw.clear.is_some()),
                ("for", raw.hold.is_some()),
                ("absent", raw.absent.is_some()),
                ("match", !raw.labels.is_empty()),
                ("unit", raw.unit.is_some()),
            ] {
                if set {
                    problems.push(Problem::new(
                        format!("{at}.{field}"),
                        format!("`{field}` belongs to a metric rule, and this rule watches an event"),
                        "remove it, or filter events with `where = { ... }`",
                    ));
                }
            }
            AlertRule::Event {
                event,
                filter: scalars(&format!("{at}.where"), raw.filter, problems),
            }
        }
        (Some(metric), None) => {
            if !raw.filter.is_empty() {
                problems.push(Problem::new(
                    format!("{at}.where"),
                    "`where` belongs to an event rule, and this rule watches a metric",
                    "narrow a metric rule by labels with `match = { label = \"value\" }`",
                ));
            }
            let set = [raw.above.is_some(), raw.below.is_some(), raw.absent.is_some()]
                .into_iter()
                .filter(|s| *s)
                .count();
            if set != 1 {
                problems.push(Problem::new(
                    at,
                    "a metric rule needs exactly one condition",
                    "set one of `above = <number>`, `below = <number>` or `absent = \"2m\"`",
                ));
                return None;
            }
            let condition = if let Some(threshold) = raw.above {
                Condition::Above {
                    threshold,
                    clear: raw.clear.unwrap_or(threshold),
                }
            } else if let Some(threshold) = raw.below {
                Condition::Below {
                    threshold,
                    clear: raw.clear.unwrap_or(threshold),
                }
            } else {
                for (field, set) in [("clear", raw.clear.is_some()), ("for", raw.hold.is_some())] {
                    if set {
                        problems.push(Problem::new(
                            format!("{at}.{field}"),
                            format!("`{field}` has no meaning with `absent`"),
                            "remove it: an absent rule clears when a reading arrives",
                        ));
                    }
                }
                Condition::Absent {
                    after_ms: duration("absent", raw.absent.as_deref().unwrap_or(""), problems)?,
                }
            };
            let for_ms = match (&condition, raw.hold.as_deref()) {
                (Condition::Absent { .. }, _) | (_, None) => None,
                (_, Some(text)) => duration("for", text, problems),
            };
            AlertRule::Metric {
                metric,
                labels: raw.labels,
                condition,
                for_ms,
                unit: raw.unit,
            }
        }
    };
    Some(AlertDecl {
        severity,
        kind: raw.kind,
        title: raw.title,
        body: raw.body,
        rule,
    })
}

// ---------- the checks ----------

/// Checks declarations for everything the parser can't see: names, references between files,
/// types, and properties that look like secrets. Empty means clean.
pub fn validate(decl: &Declarations) -> Vec<Problem> {
    let mut problems = Vec::new();
    if !is_source(&decl.source) {
        problems.push(Problem::new(
            "events.toml: source",
            format!("`{}` is not a source name", decl.source),
            "use lowercase letters, digits and hyphens, starting with a letter, like `shop` or `mail-worker`",
        ));
    }
    if let Some(service) = &decl.service
        && !is_service(service)
    {
        problems.push(Problem::new(
            "events.toml: service",
            format!("`{service}` is not a service name"),
            "use lowercase letters, digits, `-` and `_`, starting with a letter, like `shop` or `mail_worker`; leave it out when the service is named like the source",
        ));
    }
    for (name, event) in &decl.events {
        check_event(name, event, &mut problems);
    }
    for (name, metric) in &decl.metrics {
        check_metric(name, metric, &mut problems);
    }
    for (name, rollup) in &decl.rollups {
        check_rollup(decl, name, rollup, &mut problems);
    }
    for (name, alert) in &decl.alerts {
        check_alert(decl, name, alert, &mut problems);
    }
    problems
}

fn need_description(at: &str, description: &str, what: &str, problems: &mut Vec<Problem>) {
    if description.trim().is_empty() {
        problems.push(Problem::new(
            format!("{at}.description"),
            format!("{what} has no description"),
            "add `description = \"...\"`: one sentence saying when it happens, for whoever queries it later",
        ));
    }
}

fn check_event(name: &str, event: &EventDecl, problems: &mut Vec<Problem>) {
    let at = format!("events.toml: events.\"{name}\"");
    if !is_event_name(name) {
        problems.push(Problem::new(
            &at,
            format!("`{name}` is not an event name"),
            "use two or more snake_case parts joined by dots, like `shop.order.placed`",
        ));
    }
    need_description(&at, &event.description, "this event", problems);
    for (prop, kind) in &event.props {
        let at = format!("{at}.props.{prop}");
        if !is_ident(prop) {
            problems.push(Problem::new(
                &at,
                format!("`{prop}` is not a property name"),
                "use snake_case, like `latency_ms`",
            ));
        }
        if ENVELOPE_FIELDS.contains(&prop.as_str()) {
            problems.push(Problem::new(
                &at,
                format!("`{prop}` is a field every event already has"),
                "remove it; the client fills the envelope, or pick another name",
            ));
        }
        if let Some(word) = secret_word(prop) {
            problems.push(Problem::new(
                &at,
                format!("`{prop}` looks like it holds a secret (`{word}`), and events are kept for years"),
                "record an id or a boolean instead, like `otp_used = \"bool\"`",
            ));
        }
        if let Base::Enum(values) = &kind.base {
            let unique: BTreeSet<&String> = values.iter().collect();
            if values.is_empty()
                || unique.len() != values.len()
                || values.iter().any(|v| v.is_empty() || v.len() > 64)
            {
                problems.push(Problem::new(
                    &at,
                    "an enum needs one or more distinct, non-empty values of at most 64 bytes",
                    "list each allowed value once, like `{ enum = [\"card\", \"cash\"] }`",
                ));
            }
        }
    }
}

fn check_metric(name: &str, metric: &MetricDecl, problems: &mut Vec<Problem>) {
    let at = format!("metrics.toml: metrics.\"{name}\"");
    if !is_metric_name(name) {
        problems.push(Problem::new(
            &at,
            format!("`{name}` is not a metric name"),
            "use letters, digits and `_ . : -`, starting with a letter, like `queue.depth`",
        ));
    }
    need_description(&at, &metric.description, "this metric", problems);
    let unique: BTreeSet<&String> = metric.labels.iter().collect();
    if unique.len() != metric.labels.len() {
        problems.push(Problem::new(
            format!("{at}.labels"),
            "a label is listed twice",
            "list each label once",
        ));
    }
    for label in &metric.labels {
        if !is_ident(label) {
            problems.push(Problem::new(
                format!("{at}.labels"),
                format!("`{label}` is not a label name"),
                "use snake_case, like `room` or `queue`",
            ));
        }
    }
}

/// The type of a field an aggregation or filter names: a property, or an envelope field.
fn field_type<'a>(event: &'a EventDecl, field: &str) -> Option<std::borrow::Cow<'a, PropType>> {
    if let Some(prop) = event.props.get(field) {
        return Some(std::borrow::Cow::Borrowed(prop));
    }
    GROUPABLE_ENVELOPE_FIELDS.contains(&field).then_some({
        std::borrow::Cow::Owned(PropType {
            base: Base::String,
            optional: true,
        })
    })
}

fn unknown_field(at: String, event_name: &str, event: &EventDecl, field: &str) -> Problem {
    let known: Vec<&str> = event
        .props
        .keys()
        .map(String::as_str)
        .chain(GROUPABLE_ENVELOPE_FIELDS)
        .collect();
    let hint = closest(field, known.iter().copied())
        .map(|c| format!(" Did you mean `{c}`?"))
        .unwrap_or_default();
    Problem::new(
        at,
        format!("`{field}` is not a property of `{event_name}`"),
        format!("use one of: {}.{hint}", known.join(", ")),
    )
}

fn check_filter(
    at: &str,
    event_name: &str,
    event: &EventDecl,
    filter: &BTreeMap<String, Scalar>,
    problems: &mut Vec<Problem>,
) {
    for (field, value) in filter {
        let at = format!("{at}.where.{field}");
        match field_type(event, field) {
            None => problems.push(unknown_field(at, event_name, event, field)),
            Some(kind) if !value.fits(&kind.base) => problems.push(Problem::new(
                at,
                format!(
                    "`{field}` is {}, and {} is not",
                    kind.describe(),
                    value.describe()
                ),
                format!("compare with a value of type {}", kind.describe()),
            )),
            Some(_) => {}
        }
    }
}

fn find_event<'a>(
    decl: &'a Declarations,
    at: &str,
    key: &str,
    name: &str,
    problems: &mut Vec<Problem>,
) -> Option<&'a EventDecl> {
    let found = decl.events.get(name);
    if found.is_none() {
        let hint = closest(name, decl.events.keys().map(String::as_str))
            .map(|c| format!(" Did you mean `{c}`?"))
            .unwrap_or_default();
        problems.push(Problem::new(
            format!("{at}.{key}"),
            format!("`{name}` is not an event `{}` declares", decl.source),
            format!("declare it in events.toml, or name a declared event.{hint}"),
        ));
    }
    found
}

fn check_rollup(decl: &Declarations, name: &str, rollup: &RollupDecl, problems: &mut Vec<Problem>) {
    let at = format!("rollups.toml: rollups.{name}");
    if !is_ident(name) {
        problems.push(Problem::new(
            &at,
            format!("`{name}` is not a rollup name"),
            "use snake_case, like `device_actions`",
        ));
    }
    need_description(&at, &rollup.description, "this rollup", problems);
    if rollup.measures.is_empty() {
        problems.push(Problem::new(
            format!("{at}.measure"),
            "a rollup needs something to compute",
            format!("list one or more of {}", Measure::FORMS),
        ));
    }
    let unique: BTreeSet<&Measure> = rollup.measures.iter().collect();
    if unique.len() != rollup.measures.len() {
        problems.push(Problem::new(
            format!("{at}.measure"),
            "a measure is listed twice",
            "list each measure once",
        ));
    }
    let unique: BTreeSet<&String> = rollup.by.iter().collect();
    if unique.len() != rollup.by.len() {
        problems.push(Problem::new(
            format!("{at}.by"),
            "a field is listed twice",
            "list each field once",
        ));
    }
    if rollup.by.len() > 4 {
        problems.push(Problem::new(
            format!("{at}.by"),
            format!(
                "{} fields is too many to group by: every combination becomes a series",
                rollup.by.len()
            ),
            "group by at most 4 fields, each with few distinct values",
        ));
    }
    for (_, series) in rollup.series_names(&decl.source, name) {
        if decl.metrics.contains_key(&series) {
            problems.push(Problem::new(
                &at,
                format!("this rollup writes the series `{series}`, which metrics.toml also declares"),
                "rename the rollup or the metric",
            ));
        }
    }
    let Some(event) = find_event(decl, &at, "from", &rollup.from, problems) else {
        return;
    };
    check_filter(&at, &rollup.from, event, &rollup.filter, problems);
    for field in &rollup.by {
        let at = format!("{at}.by");
        match field_type(event, field) {
            None => problems.push(unknown_field(at, &rollup.from, event, field)),
            Some(kind) if !kind.base.is_groupable() => problems.push(Problem::new(
                at,
                format!("`{field}` is {}, which can't be grouped by", kind.describe()),
                "group by a string, int, bool or enum property with few distinct values",
            )),
            Some(_) => {}
        }
    }
    for measure in &rollup.measures {
        let Some(prop) = measure.prop() else { continue };
        let at = format!("{at}.measure");
        match event.props.get(prop) {
            None => problems.push(unknown_field(at, &rollup.from, event, prop)),
            Some(kind) if !kind.base.is_numeric() => problems.push(Problem::new(
                at,
                format!("`{measure}` needs a number, and `{prop}` is {}", kind.describe()),
                "measure an int or float property, or use `count`",
            )),
            Some(_) => {}
        }
    }
}

/// The `{name}` placeholders in a title or body.
pub fn placeholders(text: &str) -> Vec<&str> {
    let mut found = Vec::new();
    let mut rest = text;
    while let Some(open) = rest.find('{') {
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else { break };
        found.push(&after[..close]);
        rest = &after[close + 1..];
    }
    found
}

fn check_alert(decl: &Declarations, name: &str, alert: &AlertDecl, problems: &mut Vec<Problem>) {
    let at = format!("alerts.toml: alerts.{name}");
    if !is_ident(name) {
        problems.push(Problem::new(
            &at,
            format!("`{name}` is not an alert name"),
            "use snake_case, like `queue_backlog`",
        ));
    }
    if let Some(kind) = &alert.kind
        && !is_alert_kind(kind)
    {
        problems.push(Problem::new(
            format!("{at}.kind"),
            format!("`{kind}` is not an alert kind"),
            "write two or more snake_case parts joined by dots, like `shop.queue_backlog`",
        ));
    }
    if alert.title.trim().is_empty() {
        problems.push(Problem::new(
            format!("{at}.title"),
            "this alert has no title",
            "add `title = \"...\"`: what a person sees on their phone when it fires",
        ));
    }
    match &alert.rule {
        AlertRule::Metric {
            metric,
            labels,
            condition,
            for_ms: _,
            unit: _,
        } => {
            if !is_metric_name(metric) {
                problems.push(Problem::new(
                    format!("{at}.metric"),
                    format!("`{metric}` is not a metric name"),
                    "name the metric series to watch",
                ));
            }
            match *condition {
                Condition::Above { threshold, clear } if clear > threshold => problems.push(Problem::new(
                    format!("{at}.clear"),
                    format!(
                        "clear ({clear}) is above the threshold ({threshold}), so the alert could never clear"
                    ),
                    "set clear at or below `above`",
                )),
                Condition::Below { threshold, clear } if clear < threshold => problems.push(Problem::new(
                    format!("{at}.clear"),
                    format!(
                        "clear ({clear}) is below the threshold ({threshold}), so the alert could never clear"
                    ),
                    "set clear at or above `below`",
                )),
                _ => {}
            }
            let known: Vec<&str> = labels
                .keys()
                .map(String::as_str)
                .chain(["value", "name"])
                .collect();
            for text in [&alert.title, &alert.body] {
                for placeholder in placeholders(text) {
                    // Labels the rule doesn't pin are still valid: each matching series supplies its own.
                    if !is_metric_name(placeholder) {
                        problems.push(Problem::new(
                            format!("{at}.title"),
                            format!("`{{{placeholder}}}` is not a label name"),
                            format!("use a label of the metric, or one of: {}", known.join(", ")),
                        ));
                    }
                }
            }
        }
        AlertRule::Event {
            event: event_name,
            filter,
        } => {
            let Some(event) = find_event(decl, &at, "event", event_name, problems) else {
                return;
            };
            check_filter(&at, event_name, event, filter, problems);
            for text in [&alert.title, &alert.body] {
                for placeholder in placeholders(text) {
                    if field_type(event, placeholder).is_none() && placeholder != "source" {
                        problems.push(unknown_field(
                            format!("{at}.title"),
                            event_name,
                            event,
                            placeholder,
                        ));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const EVENTS: &str = r#"
source = "shop"

[events."shop.order.placed"]
description = "A customer placed an order"
retention = "forever"
[events."shop.order.placed".props]
order_id = "string"
total_cents = "int"
latency_ms = "int?"
method = { enum = ["card", "cash"] }
items = "string[]"

[events."shop.page.viewed"]
description = "A page was shown"
retention = "30d"
props.path = "string"
"#;

    const ROLLUPS: &str = r#"
[rollups.orders]
description = "Orders by payment method"
from = "shop.order.placed"
where = { method = "card" }
by = ["method", "client"]
measure = ["count", "sum(total_cents)", "p95(latency_ms)"]
every = "1m"
"#;

    const ALERTS: &str = r#"
[alerts.queue_backlog]
metric = "shop.queue.depth"
match = { queue = "email" }
above = 100
clear = 80
for = "10m"
severity = "warning"
kind = "shop.queue_backlog"
title = "The {queue} queue is backing up"

[alerts.worker_quiet]
metric = "shop.queue.depth"
absent = "2m"
title = "The queue worker stopped reporting"

[alerts.big_order]
event = "shop.order.placed"
where = { method = "cash" }
severity = "info"
title = "Cash order {order_id}"
"#;

    const METRICS: &str = r#"
[metrics."shop.queue.depth"]
description = "Jobs waiting in a queue"
unit = "jobs"
labels = ["queue"]
"#;

    fn files() -> Files {
        Files {
            events: EVENTS.to_string(),
            metrics: Some(METRICS.to_string()),
            rollups: Some(ROLLUPS.to_string()),
            alerts: Some(ALERTS.to_string()),
        }
    }

    fn problems_of(files: &Files) -> String {
        parse(files)
            .unwrap_err()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn parses_a_full_folder() {
        let decl = parse(&files()).unwrap();
        assert_eq!(decl.source, "shop");
        let placed = &decl.events["shop.order.placed"];
        assert_eq!(placed.retention, Retention::Forever);
        assert_eq!(
            placed.props["latency_ms"],
            PropType {
                base: Base::Int,
                optional: true
            }
        );
        assert_eq!(decl.events["shop.page.viewed"].retention, Retention::Days(30));
        let orders = &decl.rollups["orders"];
        assert_eq!(orders.every, Every::Minute);
        assert_eq!(orders.filter["method"], Scalar::Text("card".to_string()));
        let names: Vec<String> = orders
            .series_names("shop", "orders")
            .into_iter()
            .map(|(_, n)| n)
            .collect();
        assert_eq!(
            names,
            [
                "shop.orders.count",
                "shop.orders.total_cents.sum",
                "shop.orders.latency_ms.p95"
            ]
        );
        assert!(matches!(
            decl.alerts["queue_backlog"].rule,
            AlertRule::Metric { condition: Condition::Above { threshold, clear }, for_ms: Some(600_000), .. } if threshold == 100.0 && clear == 80.0
        ));
        assert!(matches!(
            decl.alerts["worker_quiet"].rule,
            AlertRule::Metric {
                condition: Condition::Absent { after_ms: 120_000 },
                ..
            }
        ));
        assert_eq!(decl.metrics["shop.queue.depth"].kind, MetricKind::Gauge);
        assert_eq!(
            decl.alerts["queue_backlog"].kind.as_deref(),
            Some("shop.queue_backlog")
        );
        assert_eq!(decl.alerts["worker_quiet"].kind, None);
    }

    #[test]
    fn a_source_names_its_service_or_is_its_own() {
        let decl = parse(&files()).unwrap();
        assert_eq!(decl.service, None);
        assert_eq!(decl.service(), "shop");
        assert!(decl.to_json().get("service").is_none());

        let mut files = files();
        files.events = EVENTS
            .replace(
                "source = \"shop\"",
                "source = \"shop-worker\"\nservice = \"shop\"",
            )
            .replace("\"shop.", "\"shop_worker.");
        files.metrics = None;
        files.rollups = None;
        files.alerts = None;
        let worker = parse(&files).unwrap();
        assert_eq!(worker.service(), "shop");
        let json = worker.to_json();
        assert_eq!(json["service"], json!("shop"));
        assert_eq!(Declarations::from_json(json).unwrap(), worker);

        for bad in ["Shop", "shop.web", "-shop", "", "shop web"] {
            files.events = format!("source = \"shop\"\nservice = \"{bad}\"\n");
            let text = problems_of(&files);
            assert!(text.contains("events.toml: service"), "{bad}: {text}");
        }
        files.events = "source = \"shop\"\nservice = \"mail_worker-2\"\n".to_string();
        assert_eq!(parse(&files).unwrap().service(), "mail_worker-2");
    }

    #[test]
    fn an_alert_kind_is_a_dotted_name_and_optional_in_json() {
        let decl = parse(&files()).unwrap();
        let json = decl.to_json();
        assert_eq!(
            json["alerts"]["queue_backlog"]["kind"],
            json!("shop.queue_backlog")
        );
        // A rule without one is sent as before, without the field.
        assert!(json["alerts"]["worker_quiet"].get("kind").is_none());
        let mut old = json.clone();
        old["alerts"]["queue_backlog"]
            .as_object_mut()
            .unwrap()
            .remove("kind");
        assert_eq!(
            Declarations::from_json(old).unwrap().alerts["queue_backlog"].kind,
            None
        );

        for bad in [
            "backlog",
            "Shop.Backlog",
            "shop..backlog",
            "shop.queue-backlog",
            "1shop.backlog",
        ] {
            let mut files = files();
            files.alerts =
                Some(ALERTS.replace("kind = \"shop.queue_backlog\"", &format!("kind = \"{bad}\"")));
            let text = problems_of(&files);
            assert!(text.contains("alerts.queue_backlog.kind"), "{bad}: {text}");
            assert!(text.contains(&format!("`{bad}` is not an alert kind")), "{text}");
        }
        let mut wrong = json;
        wrong["alerts"]["queue_backlog"]["kind"] = json!("nodots");
        assert!(Declarations::from_json(wrong).is_err());
    }

    #[test]
    fn round_trips_through_json() {
        let decl = parse(&files()).unwrap();
        let json = decl.to_json();
        assert_eq!(
            json["events"]["shop.order.placed"]["props"]["latency_ms"],
            json!("int?")
        );
        assert_eq!(
            json["events"]["shop.order.placed"]["props"]["method"],
            json!({"enum": ["card", "cash"]})
        );
        assert_eq!(
            json["rollups"]["orders"]["measure"],
            json!(["count", "sum(total_cents)", "p95(latency_ms)"])
        );
        assert_eq!(Declarations::from_json(json).unwrap(), decl);
    }

    #[test]
    fn unknown_property_gets_a_hint() {
        let mut files = files();
        files.rollups = Some(ROLLUPS.replace("p95(latency_ms)", "p95(latncy_ms)"));
        let text = problems_of(&files);
        assert!(
            text.contains("`latncy_ms` is not a property of `shop.order.placed`"),
            "{text}"
        );
        assert!(text.contains("Did you mean `latency_ms`?"), "{text}");
    }

    #[test]
    fn types_are_checked_across_files() {
        let mut files = files();
        files.rollups = Some(
            ROLLUPS
                .replace("sum(total_cents)", "sum(order_id)")
                .replace("by = [\"method\", \"client\"]", "by = [\"items\"]")
                .replace("where = { method = \"card\" }", "where = { method = \"cheque\" }"),
        );
        let text = problems_of(&files);
        assert!(text.contains("`sum(order_id)` needs a number"), "{text}");
        assert!(
            text.contains("`items` is string[], which can't be grouped by"),
            "{text}"
        );
        assert!(
            text.contains("`method` is enum(card, cash), and \"cheque\" is not"),
            "{text}"
        );
    }

    #[test]
    fn secrets_and_envelope_names_are_refused() {
        let mut files = files();
        files.events = EVENTS.replace(
            "props.path = \"string\"",
            "props.session_token = \"string\"\nprops.actor = \"string\"",
        );
        let text = problems_of(&files);
        assert!(text.contains("looks like it holds a secret (`token`)"), "{text}");
        assert!(
            text.contains("`actor` is a field every event already has"),
            "{text}"
        );
    }

    #[test]
    fn syntax_and_shape_errors_name_the_file() {
        let mut files = files();
        files.events = EVENTS.replace(
            "description = \"A page was shown\"",
            "descripton = \"A page was shown\"",
        );
        let text = problems_of(&files);
        assert!(text.starts_with("events.toml:"), "{text}");
        assert!(text.contains("descripton"), "{text}");

        let mut files = self::files();
        files.events = EVENTS.replace("total_cents = \"int\"", "total_cents = \"integer\"");
        let text = problems_of(&files);
        assert!(text.contains("`integer` is not a property type"), "{text}");
        assert!(text.contains("string, int, float"), "{text}");
    }

    #[test]
    fn alerts_are_checked() {
        let mut files = files();
        files.alerts = Some(
            ALERTS
                .replace("clear = 80", "clear = 120")
                .replace("event = \"shop.order.placed\"", "event = \"shop.order.plcaed\"")
                .replace("absent = \"2m\"", "absent = \"2m\"\nabove = 3"),
        );
        let text = problems_of(&files);
        assert!(text.contains("could never clear"), "{text}");
        assert!(text.contains("Did you mean `shop.order.placed`?"), "{text}");
        assert!(text.contains("exactly one condition"), "{text}");
    }

    #[test]
    fn descriptions_are_required() {
        let mut files = files();
        files.events = EVENTS.replace("description = \"A page was shown\"\n", "");
        assert!(problems_of(&files).contains("this event has no description"));
    }

    #[test]
    fn prop_types_accept_matching_json() {
        let optional_int = PropType {
            base: Base::Int,
            optional: true,
        };
        assert!(optional_int.accepts(&json!(3)));
        assert!(optional_int.accepts(&json!(null)));
        assert!(!optional_int.accepts(&json!(3.5)));
        assert!(!optional_int.accepts(&json!("3")));
        let method = PropType {
            base: Base::Enum(vec!["card".into(), "cash".into()]),
            optional: false,
        };
        assert!(method.accepts(&json!("card")));
        assert!(!method.accepts(&json!("cheque")));
        assert!(!method.accepts(&json!(null)));
        assert!(
            PropType {
                base: Base::StringList,
                optional: false
            }
            .accepts(&json!(["a", "b"]))
        );
        assert!(
            PropType {
                base: Base::Float,
                optional: false
            }
            .accepts(&json!(2))
        );
        assert_eq!(placeholders("Order {order_id} by {actor}"), ["order_id", "actor"]);
    }
}
