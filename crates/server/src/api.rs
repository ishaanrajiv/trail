//! The HTTP API: one table of routes, and the function that answers them. `spec/api.md` documents
//! each route; a test keeps the two in step. `GET /v1` serves the table, so a caller can ask the
//! server what it offers.
//!
//! `route` takes plain values and returns plain values, so tests call it without a socket.

use crate::ingest::refusal_body;
use crate::{App, OWN_SOURCE, lock};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use trail_core::decl::Declarations;
use trail_core::envelope::{Level, Severity};
use trail_core::lineproto::Precision;
use trail_core::problem::closest;
use trail_core::time::{HOUR, parse_duration};
use trail_core::{Problem, Ts};
use trail_store::{
    Agg, ErrorFilter, ErrorStatus, EventFilter, GroupFilter, LogFilter, MetricQuery, SeriesFilter, SeriesInfo,
};

/// One route: what it is for and the query parameters it takes.
#[derive(Debug, Clone, Copy)]
pub struct Route {
    pub method: &'static str,
    pub path: &'static str,
    pub summary: &'static str,
    pub params: &'static [(&'static str, &'static str)],
}

const SINCE: (&str, &str) = (
    "since",
    "start of the range: ms since 1970, or how long ago (`-1h`, `-7d`)",
);
const UNTIL: (&str, &str) = ("until", "end of the range, same forms. Default now");
const LIMIT: (&str, &str) = ("limit", "most rows to return (default 100, at most 1000)");
const BEFORE: (&str, &str) = ("before", "the `next` value of the previous page, for older rows");
const LABEL: (&str, &str) = (
    "label.<name>",
    "only series whose label equals this, e.g. `label.queue=email`",
);

pub const ROUTES: &[Route] = &[
    Route {
        method: "GET",
        path: "/v1",
        summary: "This list of routes",
        params: &[],
    },
    Route {
        method: "GET",
        path: "/v1/health",
        summary: "Whether Trail is up",
        params: &[],
    },
    Route {
        method: "POST",
        path: "/v1/ingest",
        summary: "Send events, metrics, errors and logs (spec/ingest.md)",
        params: &[],
    },
    Route {
        method: "POST",
        path: "/v1/write",
        summary: "Send metrics as InfluxDB line protocol; also at /api/v2/write and /write",
        params: &[
            (
                "source",
                "the sending source. Default: the bucket or db, else `influx`",
            ),
            ("precision", "timestamp unit: ns (default), us, ms, s"),
            ("bucket", "InfluxDB 2 clients send this; used as the source"),
            ("db", "InfluxDB 1 clients send this; used as the source"),
            ("org", "ignored"),
        ],
    },
    Route {
        method: "GET",
        path: "/v1/registry",
        summary: "The sources that have published declarations",
        params: &[],
    },
    Route {
        method: "GET",
        path: "/v1/registry/{source}",
        summary: "One source's declarations",
        params: &[],
    },
    Route {
        method: "PUT",
        path: "/v1/registry/{source}",
        summary: "Publish a source's declarations (what `trail publish` sends)",
        params: &[],
    },
    Route {
        method: "DELETE",
        path: "/v1/registry/{source}",
        summary: "Remove a source's declarations",
        params: &[],
    },
    Route {
        method: "GET",
        path: "/v1/events",
        summary: "Events, newest first",
        params: &[
            ("source", "only this source"),
            ("name", "an event name, or a prefix like `shop.order.*`"),
            ("actor", "only this actor"),
            ("trace_id", "only this trace"),
            (
                "mismatched",
                "`1` for only events that differ from their declaration",
            ),
            SINCE,
            UNTIL,
            LIMIT,
            BEFORE,
        ],
    },
    Route {
        method: "GET",
        path: "/v1/metrics/series",
        summary: "The metric series that exist",
        params: &[
            ("name", "a metric name, or a pattern with `*`"),
            ("source", "only this source"),
            LABEL,
        ],
    },
    Route {
        method: "GET",
        path: "/v1/metrics/query",
        summary: "A metric over time, one list of points per series",
        params: &[
            ("name", "a metric name, or a pattern with `*` (required)"),
            LABEL,
            SINCE,
            UNTIL,
            (
                "step",
                "bucket length, like `5m`. Default: about 300 points over the range",
            ),
            (
                "agg",
                "how a bucket's readings become one value: avg (default), min, max, sum, count, last",
            ),
        ],
    },
    Route {
        method: "GET",
        path: "/v1/metrics/latest",
        summary: "The newest reading of each matching series",
        params: &[("name", "a metric name, or a pattern with `*` (required)"), LABEL],
    },
    Route {
        method: "GET",
        path: "/v1/errors",
        summary: "Errors, newest first",
        params: &[
            ("source", "only this source"),
            ("fingerprint", "only this group"),
            ("severity", "warning, error or fatal"),
            SINCE,
            UNTIL,
            LIMIT,
            BEFORE,
        ],
    },
    Route {
        method: "GET",
        path: "/v1/errors/groups",
        summary: "Errors grouped by fingerprint, most recent first",
        params: &[
            ("source", "only this source"),
            ("status", "open, resolved or ignored"),
            LIMIT,
        ],
    },
    Route {
        method: "POST",
        path: "/v1/errors/groups/{fingerprint}",
        summary: "Set a group's status: body `{\"status\": \"resolved\"}`",
        params: &[],
    },
    Route {
        method: "GET",
        path: "/v1/logs",
        summary: "Log lines, newest first",
        params: &[
            ("source", "only this source"),
            ("stream", "only this stream"),
            (
                "level",
                "this level and above: trace, debug, info, warn, error, fatal",
            ),
            ("q", "only lines whose message contains this text"),
            SINCE,
            UNTIL,
            LIMIT,
            BEFORE,
        ],
    },
    Route {
        method: "GET",
        path: "/v1/alerts",
        summary: "What is firing, and notices waiting for the webhook",
        params: &[],
    },
    Route {
        method: "GET",
        path: "/v1/rollups",
        summary: "The rollups being computed and the series each writes",
        params: &[],
    },
    Route {
        method: "POST",
        path: "/v1/rollups/{source}/{name}/rebuild",
        summary: "Recompute a rollup from the raw events",
        params: &[SINCE],
    },
    Route {
        method: "GET",
        path: "/v1/problems",
        summary: "Records that differed from their declaration, grouped",
        params: &[("source", "only this source")],
    },
    Route {
        method: "GET",
        path: "/v1/stats",
        summary: "Rows, files and sizes per store",
        params: &[],
    },
];

/// A request, without the socket.
#[derive(Debug, Clone, Default)]
pub struct Request {
    pub method: String,
    /// The path without the query string.
    pub path: String,
    /// Query parameters, decoded, in order.
    pub query: Vec<(String, String)>,
    /// The bearer token, when one was sent.
    pub token: Option<String>,
    pub body: Vec<u8>,
}

/// An answer: a status and a JSON body (none for 204).
#[derive(Debug, Clone, PartialEq)]
pub struct Reply {
    pub status: u16,
    pub body: Option<Value>,
}

impl Reply {
    fn ok(body: Value) -> Reply {
        Reply {
            status: 200,
            body: Some(body),
        }
    }

    pub(crate) fn problem(status: u16, problem: &Problem) -> Reply {
        Reply {
            status,
            body: Some(json!({ "error": problem })),
        }
    }
}

type Answer = Result<Reply, Reply>;

fn bad(at: &str, message: impl Into<String>, fix: impl Into<String>) -> Reply {
    Reply::problem(400, &Problem::new(at, message, fix))
}

fn failed(error: &trail_store::Error) -> Reply {
    Reply::problem(500, &error.problem())
}

/// Splits a path into its parts and matches it against a route pattern, returning the `{…}` parts.
fn matches<'a>(pattern: &str, path: &'a str) -> Option<Vec<&'a str>> {
    let want: Vec<&str> = pattern.trim_matches('/').split('/').collect();
    let got: Vec<&str> = path.trim_matches('/').split('/').collect();
    if want.len() != got.len() {
        return None;
    }
    let mut captured = Vec::new();
    for (want, got) in want.iter().zip(&got) {
        if want.starts_with('{') {
            if got.is_empty() {
                return None;
            }
            captured.push(*got);
        } else if want != got {
            return None;
        }
    }
    Some(captured)
}

/// The query parameters of one request.
struct Query<'a> {
    pairs: &'a [(String, String)],
    now: Ts,
}

impl Query<'_> {
    fn get(&self, name: &str) -> Option<&str> {
        self.pairs
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
            .filter(|value| !value.is_empty())
    }

    fn text(&self, name: &str) -> Option<String> {
        self.get(name).map(str::to_string)
    }

    fn labels(&self) -> BTreeMap<String, String> {
        self.pairs
            .iter()
            .filter_map(|(key, value)| {
                key.strip_prefix("label.")
                    .map(|name| (name.to_string(), value.clone()))
            })
            .collect()
    }

    /// A time: ms since 1970, or `-1h` for that long before now.
    fn time(&self, name: &str) -> Result<Option<Ts>, Reply> {
        let Some(text) = self.get(name) else {
            return Ok(None);
        };
        let wrong = || {
            bad(
                name,
                format!("`{text}` is not a time"),
                "write milliseconds since 1970 (1794000000000), or how long ago, like -15m, -1h or -7d",
            )
        };
        if text == "now" {
            return Ok(Some(self.now));
        }
        if let Some(ago) = text.strip_prefix('-') {
            return match parse_duration(ago) {
                Ok(Some(ms)) => Ok(Some(self.now.minus(ms))),
                _ => Err(wrong()),
            };
        }
        match text.parse::<i64>() {
            // Ten digits is seconds: the commonest mistake.
            Ok(n) if n > 0 && n < 100_000_000_000 => Err(bad(
                name,
                format!("`{text}` looks like seconds"),
                format!("times are in milliseconds: send {}", n.saturating_mul(1000)),
            )),
            Ok(n) => Ok(Some(Ts(n))),
            Err(_) => Err(wrong()),
        }
    }

    fn limit(&self) -> Result<usize, Reply> {
        match self.get("limit") {
            None => Ok(100),
            Some(text) => match text.parse::<usize>() {
                Ok(n) if (1..=1000).contains(&n) => Ok(n),
                _ => Err(bad(
                    "limit",
                    format!("`{text}` is not a limit"),
                    "write a number from 1 to 1000",
                )),
            },
        }
    }

    /// The `before` cursor: `<ts>:<id>`.
    fn before(&self) -> Result<Option<(Ts, String)>, Reply> {
        let Some(text) = self.get("before") else {
            return Ok(None);
        };
        text.split_once(':')
            .and_then(|(ts, id)| ts.parse::<i64>().ok().map(|ts| (Ts(ts), id.to_string())))
            .map(Some)
            .ok_or_else(|| {
                bad(
                    "before",
                    format!("`{text}` is not a page cursor"),
                    "pass the `next` value from the previous response unchanged",
                )
            })
    }
}

/// Refuses a query parameter the route does not take, naming the one probably meant.
fn check_params(route: &Route, query: &[(String, String)]) -> Result<(), Reply> {
    let takes_labels = route.params.iter().any(|(name, _)| *name == "label.<name>");
    for (key, _) in query {
        if route.params.iter().any(|(name, _)| name == key) || (takes_labels && key.starts_with("label.")) {
            continue;
        }
        let names: Vec<&str> = route.params.iter().map(|(name, _)| *name).collect();
        let hint = closest(key, names.iter().copied())
            .map(|c| format!("did you mean `{c}`? "))
            .unwrap_or_default();
        let listed = if names.is_empty() {
            "it takes no parameters".to_string()
        } else {
            format!("it takes: {}", names.join(", "))
        };
        return Err(bad(
            key,
            format!("`{} {}` has no parameter `{key}`", route.method, route.path),
            format!("{hint}{listed}"),
        ));
    }
    Ok(())
}

fn series_json(series: &SeriesInfo) -> Value {
    json!({
        "name": series.name, "labels": series.labels, "source": series.source,
        "unit": series.unit, "kind": series.kind.as_str(),
        "first_ts": series.first_ts, "last_ts": series.last_ts,
    })
}

fn cursor<T>(rows: &[T], limit: usize, key: impl Fn(&T) -> (Ts, &str)) -> Value {
    match rows.last() {
        Some(last) if rows.len() >= limit => {
            let (ts, id) = key(last);
            json!(format!("{ts}:{id}"))
        }
        _ => Value::Null,
    }
}

/// Answers one request.
pub fn route(app: &App, request: &Request, now: Ts) -> Reply {
    // InfluxDB clients write to these two; they are the same as /v1/write.
    let path = match request.path.trim_end_matches('/') {
        "/api/v2/write" | "/write" => "/v1/write",
        "" => "/v1",
        other => other,
    };
    let mut found = None;
    let mut path_exists = false;
    for route in ROUTES {
        if let Some(parts) = matches(route.path, path) {
            path_exists = true;
            if route.method == request.method {
                found = Some((route, parts));
                break;
            }
        }
    }
    let Some((route, parts)) = found else {
        let fix = "`GET /v1` lists every route with its parameters";
        return if path_exists {
            Reply::problem(
                405,
                &Problem::new(
                    path,
                    format!("`{}` is not a method this path takes", request.method),
                    fix,
                ),
            )
        } else {
            let paths: Vec<&str> = ROUTES.iter().map(|r| r.path).collect();
            let hint = closest(path, paths.iter().copied())
                .map(|c| format!("did you mean `{c}`? "))
                .unwrap_or_default();
            Reply::problem(404, &Problem::new(path, "no such path", format!("{hint}{fix}")))
        };
    };
    let open = matches!(route.path, "/v1/health" | "/v1/ingest" | "/v1/write");
    // Changing what a source declares (and with it its rollups and alert rules) is a write as
    // that source: where the source has a write token, that token is the one that counts.
    let owned = matches!(
        (route.method, route.path),
        ("PUT" | "DELETE", "/v1/registry/{source}") | ("POST", "/v1/rollups/{source}/{name}/rebuild")
    ) && parts
        .first()
        .is_some_and(|source| app.config.auth.tokens.contains_key(*source));
    if owned
        && let Some(source) = parts.first()
        && let Err(refusal) = app.may_write(source, request.token.as_deref())
    {
        return Reply {
            status: refusal.status(),
            body: Some(refusal_body(&refusal)),
        };
    }
    if !open
        && !owned
        && let Some(expected) = app.config.auth.read_token.as_deref()
        && request.token.as_deref() != Some(expected)
    {
        return Reply::problem(
            401,
            &Problem::new(
                "Authorization",
                "this server needs its read token",
                "send `Authorization: Bearer <token>` with `read_token` from [auth] in trail.toml",
            ),
        );
    }
    if let Err(reply) = check_params(route, &request.query) {
        return reply;
    }
    let query = Query {
        pairs: &request.query,
        now,
    };
    match answer(app, route, &parts, &query, request, now) {
        Ok(reply) | Err(reply) => reply,
    }
}

#[allow(clippy::too_many_lines)]
fn answer(app: &App, route: &Route, parts: &[&str], query: &Query<'_>, request: &Request, now: Ts) -> Answer {
    let token = request.token.as_deref();
    match (route.method, route.path) {
        ("GET", "/v1") => Ok(Reply::ok(json!({
            "name": "trail",
            "version": env!("CARGO_PKG_VERSION"),
            "routes": ROUTES.iter().map(|r| json!({
                "method": r.method, "path": r.path, "summary": r.summary,
                "params": r.params.iter().map(|(name, about)| json!({"name": name, "about": about})).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        }))),
        ("GET", "/v1/health") => Ok(Reply::ok(json!({
            "ok": true, "version": env!("CARGO_PKG_VERSION"), "started": app.started,
        }))),
        ("POST", "/v1/ingest") => match app.ingest(&request.body, token, now) {
            Ok(outcome) => Ok(Reply::ok(serde_json::to_value(outcome).unwrap_or(Value::Null))),
            Err(refusal) => Err(Reply {
                status: refusal.status(),
                body: Some(refusal_body(&refusal)),
            }),
        },
        ("POST", "/v1/write") => {
            let source = query
                .get("source")
                .or_else(|| query.get("bucket"))
                .or_else(|| query.get("db"))
                .unwrap_or("influx");
            let precision = Precision::parse(query.get("precision").unwrap_or("ns"));
            let body = String::from_utf8_lossy(&request.body);
            match app.write_lines(source, &body, precision, token, now) {
                Err(refusal) => Err(Reply {
                    status: refusal.status(),
                    body: Some(refusal_body(&refusal)),
                }),
                Ok((_, lines)) if lines.is_empty() => Ok(Reply {
                    status: 204,
                    body: None,
                }),
                Ok((stored, lines)) => Err(Reply {
                    status: 400,
                    body: Some(json!({
                        "error": Problem::new(
                            "body",
                            format!("{} lines could not be read; the other {stored} readings were stored", lines.len()),
                            "fix the lines listed in `lines`; the format is `measurement,tag=value field=1.5 timestamp`",
                        ),
                        "lines": lines.iter().map(|l| json!({"line": l.line, "message": l.message})).collect::<Vec<_>>(),
                    })),
                }),
            }
        }
        ("GET", "/v1/registry") => {
            let registry = app.registry();
            let sources: Vec<Value> = registry
                .sources()
                .map(|d| {
                    json!({"source": d.source, "events": d.events.len(), "metrics": d.metrics.len(),
                           "rollups": d.rollups.len(), "alerts": d.alerts.len()})
                })
                .collect();
            Ok(Reply::ok(json!({ "sources": sources })))
        }
        ("GET", "/v1/registry/{source}") => {
            let registry = app.registry();
            match registry.get(parts[0]) {
                Some(decl) => Ok(Reply::ok(decl.to_json())),
                None => {
                    let known: Vec<&str> = registry.sources().map(|d| d.source.as_str()).collect();
                    let hint = closest(parts[0], known.iter().copied())
                        .map(|c| format!("did you mean `{c}`? "))
                        .unwrap_or_default();
                    Err(Reply::problem(
                        404,
                        &Problem::new(
                            "source",
                            format!("`{}` has published no declarations", parts[0]),
                            format!("{hint}`GET /v1/registry` lists the sources that have"),
                        ),
                    ))
                }
            }
        }
        ("PUT", "/v1/registry/{source}") => {
            let value: Value = serde_json::from_slice(&request.body).map_err(|e| {
                bad(
                    "body",
                    format!("not JSON: {e}"),
                    "publish with `trail publish <folder>`, which builds the body from the TOML files",
                )
            })?;
            let decl = Declarations::from_json(value).map_err(|problems| Reply {
                status: 400,
                body: Some(json!({"error": problems.first(), "problems": problems})),
            })?;
            if decl.source != parts[0] {
                return Err(bad(
                    "source",
                    format!(
                        "the body declares source `{}` but the path says `{}`",
                        decl.source, parts[0]
                    ),
                    format!("PUT to /v1/registry/{}", decl.source),
                ));
            }
            let summary = json!({"source": decl.source, "events": decl.events.len(), "metrics": decl.metrics.len(),
                                 "rollups": decl.rollups.len(), "alerts": decl.alerts.len()});
            app.publish(decl, now).map_err(|problems| Reply {
                status: 409,
                body: Some(json!({"error": problems.first(), "problems": problems})),
            })?;
            Ok(Reply::ok(summary))
        }
        ("DELETE", "/v1/registry/{source}") => {
            if parts[0] == OWN_SOURCE {
                return Err(bad("source", "`trail` is the server's own source", ""));
            }
            let removed = app
                .unpublish(parts[0], now)
                .map_err(|p| Reply::problem(500, &p))?;
            Ok(Reply::ok(json!({ "removed": removed })))
        }
        ("GET", "/v1/events") => {
            let limit = query.limit()?;
            let filter = EventFilter {
                source: query.text("source"),
                name: query.text("name"),
                actor: query.text("actor"),
                trace_id: query.text("trace_id"),
                start: query.time("since")?,
                end: query.time("until")?,
                mismatched: matches!(query.get("mismatched"), Some("1" | "true")),
                before: query.before()?,
                limit,
            };
            let events = app.store.events(&filter).map_err(|e| failed(&e))?;
            let next = cursor(&events, limit, |e| (e.ts, e.id.as_str()));
            Ok(Reply::ok(json!({ "events": events, "next": next })))
        }
        ("GET", "/v1/metrics/series") => {
            let filter = SeriesFilter {
                name: query.text("name"),
                matchers: query.labels(),
                source: query.text("source"),
            };
            let series = app.store.series(&filter).map_err(|e| failed(&e))?;
            Ok(Reply::ok(
                json!({ "series": series.iter().map(series_json).collect::<Vec<_>>() }),
            ))
        }
        ("GET", "/v1/metrics/query") => {
            let name = query.text("name").ok_or_else(|| {
                bad(
                    "name",
                    "which metric?",
                    "add `name=<metric>`; `GET /v1/metrics/series` lists them",
                )
            })?;
            let end = query.time("until")?.unwrap_or(now);
            let start = query.time("since")?.unwrap_or(end.minus(HOUR));
            if start >= end {
                return Err(bad(
                    "since",
                    "the range is empty: `since` is not before `until`",
                    "swap them, or widen the range",
                ));
            }
            let step = match query.get("step") {
                None => None,
                Some(text) => match parse_duration(text) {
                    Ok(Some(ms)) => Some(ms),
                    _ => {
                        return Err(bad(
                            "step",
                            format!("`{text}` is not a step"),
                            "write a length of time, like 30s, 5m or 1h",
                        ));
                    }
                },
            };
            let agg = match query.get("agg") {
                None => Agg::Avg,
                Some(text) => Agg::parse(text).ok_or_else(|| {
                    bad(
                        "agg",
                        format!("`{text}` is not an aggregation"),
                        "write avg, min, max, sum, count or last",
                    )
                })?,
            };
            let result = app
                .store
                .query(&MetricQuery {
                    name,
                    matchers: query.labels(),
                    start,
                    end,
                    step,
                    agg,
                })
                .map_err(|e| failed(&e))?;
            let series: Vec<Value> = result
                .series
                .iter()
                .map(|s| {
                    let mut value = series_json(&s.series);
                    if let Value::Object(map) = &mut value {
                        map.insert("points".to_string(), json!(s.points));
                    }
                    value
                })
                .collect();
            Ok(Reply::ok(json!({
                "start": start, "end": end, "step": result.step, "tier": result.tier.as_str(), "series": series,
            })))
        }
        ("GET", "/v1/metrics/latest") => {
            let name = query.text("name").ok_or_else(|| {
                bad(
                    "name",
                    "which metric?",
                    "add `name=<metric>`; `GET /v1/metrics/series` lists them",
                )
            })?;
            let latest = app.store.latest(&name, &query.labels()).map_err(|e| failed(&e))?;
            let rows: Vec<Value> = latest
                .iter()
                .map(|l| {
                    let mut value = series_json(&l.series);
                    if let Value::Object(map) = &mut value {
                        map.insert("ts".to_string(), json!(l.ts));
                        map.insert("value".to_string(), json!(l.value));
                    }
                    value
                })
                .collect();
            Ok(Reply::ok(json!({ "series": rows })))
        }
        ("GET", "/v1/errors") => {
            let limit = query.limit()?;
            let severity = match query.get("severity") {
                None => None,
                Some(text) => Some(Severity::parse(text).ok_or_else(|| {
                    bad(
                        "severity",
                        format!("`{text}` is not a severity"),
                        "write warning, error or fatal",
                    )
                })?),
            };
            let filter = ErrorFilter {
                source: query.text("source"),
                fingerprint: query.text("fingerprint"),
                severity,
                start: query.time("since")?,
                end: query.time("until")?,
                before: query.before()?,
                limit,
            };
            let errors = app.store.errors(&filter).map_err(|e| failed(&e))?;
            let next = cursor(&errors, limit, |e| (e.ts, e.id.as_str()));
            Ok(Reply::ok(json!({ "errors": errors, "next": next })))
        }
        ("GET", "/v1/errors/groups") => {
            let status = match query.get("status") {
                None => None,
                Some(text) => Some(ErrorStatus::parse(text).ok_or_else(|| {
                    bad(
                        "status",
                        format!("`{text}` is not a status"),
                        "write open, resolved or ignored",
                    )
                })?),
            };
            let groups = app
                .store
                .error_groups(&GroupFilter {
                    source: query.text("source"),
                    status,
                    limit: query.limit()?,
                })
                .map_err(|e| failed(&e))?;
            let rows: Vec<Value> = groups.iter().map(group_json).collect();
            Ok(Reply::ok(json!({ "groups": rows })))
        }
        ("POST", "/v1/errors/groups/{fingerprint}") => {
            let wrong = || {
                bad(
                    "body",
                    "send the new status",
                    "send `{\"status\": \"resolved\"}`: open, resolved or ignored",
                )
            };
            let body: Value = serde_json::from_slice(&request.body).map_err(|_| wrong())?;
            let status = body
                .get("status")
                .and_then(Value::as_str)
                .and_then(ErrorStatus::parse)
                .ok_or_else(wrong)?;
            if !app
                .store
                .set_error_status(parts[0], status)
                .map_err(|e| failed(&e))?
            {
                return Err(Reply::problem(
                    404,
                    &Problem::new(
                        "fingerprint",
                        format!("no error group `{}`", parts[0]),
                        "`GET /v1/errors/groups` lists them",
                    ),
                ));
            }
            let group = app.store.error_group(parts[0]).map_err(|e| failed(&e))?;
            Ok(Reply::ok(group.as_ref().map_or(Value::Null, group_json)))
        }
        ("GET", "/v1/logs") => {
            let limit = query.limit()?;
            let min_level = match query.get("level") {
                None => None,
                Some(text) => Some(Level::parse(text).ok_or_else(|| {
                    bad(
                        "level",
                        format!("`{text}` is not a level"),
                        "write trace, debug, info, warn, error or fatal",
                    )
                })?),
            };
            let filter = LogFilter {
                source: query.text("source"),
                stream: query.text("stream"),
                min_level,
                contains: query.text("q"),
                start: query.time("since")?,
                end: query.time("until")?,
                before: query.before()?,
                limit,
            };
            let logs = app.store.logs(&filter).map_err(|e| failed(&e))?;
            let next = cursor(&logs, limit, |l| (l.ts, l.id.as_str()));
            Ok(Reply::ok(json!({ "logs": logs, "next": next })))
        }
        ("GET", "/v1/alerts") => {
            let alerts = lock(&app.alerts);
            let registry = app.registry();
            let rules: Vec<Value> = registry
                .alerts()
                .map(|(source, name, alert)| {
                    json!({"source": source, "alert": name, "severity": alert.severity.as_str(),
                           "title": alert.title, "rule": alert.rule})
                })
                .collect();
            Ok(Reply::ok(json!({
                "firing": alerts.firing.values().collect::<Vec<_>>(),
                "notices": alerts.notices,
                "webhook": app.config.alerts.webhook.is_some(),
                "rules": rules,
            })))
        }
        ("GET", "/v1/rollups") => {
            let rollups = lock(&app.rollups);
            let rows: Vec<Value> = rollups
                .specs()
                .map(|spec| {
                    json!({
                        "source": spec.source, "rollup": spec.name, "description": spec.decl.description,
                        "from": spec.decl.from, "by": spec.decl.by, "every": spec.decl.every.as_str(),
                        "series": spec.decl.series_names(&spec.source, &spec.name)
                            .into_iter().map(|(measure, name)| json!({"measure": measure.to_string(), "name": name}))
                            .collect::<Vec<_>>(),
                    })
                })
                .collect();
            Ok(Reply::ok(json!({ "rollups": rows })))
        }
        ("POST", "/v1/rollups/{source}/{name}/rebuild") => {
            let id = format!("{}.{}", parts[0], parts[1]);
            let rebuilt = app
                .rebuild_rollup(&id, query.time("since")?, now)
                .map_err(|p| Reply::problem(if p.at == "rollup" { 404 } else { 500 }, &p))?;
            Ok(Reply::ok(serde_json::to_value(rebuilt).unwrap_or(Value::Null)))
        }
        ("GET", "/v1/problems") => {
            let rows = app.store.problems(query.get("source")).map_err(|e| failed(&e))?;
            let rows: Vec<Value> = rows
                .iter()
                .map(|p| {
                    json!({"source": p.source, "kind": p.kind, "name": p.name, "message": p.message,
                           "first_seen": p.first_seen, "last_seen": p.last_seen, "count": p.count})
                })
                .collect();
            Ok(Reply::ok(json!({ "problems": rows })))
        }
        ("GET", "/v1/stats") => {
            let stats = app.store.stats().map_err(|e| failed(&e))?;
            let stores: Vec<Value> = stats
                .stores
                .iter()
                .map(|s| json!({"store": s.store.as_str(), "rows": s.rows, "files": s.files, "file_bytes": s.file_bytes}))
                .collect();
            Ok(Reply::ok(json!({
                "stores": stores, "db_bytes": stats.db_bytes, "series": stats.series,
                "timezone": app.config.zone.name(), "hot_days": app.config.hot_days,
                "data_dir": app.config.data_dir.display().to_string(),
                "open_rollup_buckets": lock(&app.rollups).open_buckets(),
            })))
        }
        _ => Err(Reply::problem(
            500,
            &Problem::new(
                route.path,
                "this route is listed but not answered",
                "report this as a bug in trail-server",
            ),
        )),
    }
}

fn group_json(group: &trail_store::ErrorGroup) -> Value {
    json!({
        "fingerprint": group.fingerprint, "source": group.source, "kind": group.kind,
        "message": group.message, "first_seen": group.first_seen, "last_seen": group.last_seen,
        "count": group.count, "status": group.status.as_str(),
    })
}
