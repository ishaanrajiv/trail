//! The write path: a request body becomes checked records, one store transaction, and a response
//! that says what happened to each record. `spec/ingest.md` is the contract.

use crate::{App, OWN_SOURCE, lock};
use serde::Serialize;
use serde_json::Value;
use trail_core::envelope::{self, BatchContext, IngestBatch, MetricKind, Point};
use trail_core::lineproto::{self, LineError, Precision};
use trail_core::names::is_source;
use trail_core::{Problem, Ts};
use trail_store::{Batch, MetricPoint, Mismatch, PointOutcome, Written};

/// Why a whole request was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// 400: the body can't be read. A retry cannot succeed.
    Bad(Problem),
    /// 401: this source needs a token.
    Unauthorized(Problem),
    /// 413: too many records.
    TooLarge(Problem),
    /// 500: the store failed. The sender keeps the batch and retries.
    Failed(Problem),
}

impl Refusal {
    pub fn status(&self) -> u16 {
        match self {
            Refusal::Bad(_) => 400,
            Refusal::Unauthorized(_) => 401,
            Refusal::TooLarge(_) => 413,
            Refusal::Failed(_) => 500,
        }
    }

    pub fn problem(&self) -> &Problem {
        match self {
            Refusal::Bad(p) | Refusal::Unauthorized(p) | Refusal::TooLarge(p) | Refusal::Failed(p) => p,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Counts {
    pub events: usize,
    pub metrics: usize,
    pub errors: usize,
    pub logs: usize,
}

/// A record that was stored but differs from its declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Marked {
    pub at: String,
    pub message: String,
}

/// The body of a `200` from `POST /v1/ingest`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Outcome {
    pub accepted: Counts,
    pub duplicates: usize,
    pub rejected: Vec<Problem>,
    pub mismatched: Vec<Marked>,
}

impl App {
    /// Whether `token` may write as `source`. Sources without a configured token write freely.
    pub(crate) fn may_write(&self, source: &str, token: Option<&str>) -> Result<(), Refusal> {
        match self.config.auth.tokens.get(source) {
            None => Ok(()),
            Some(expected) if token == Some(expected.as_str()) => Ok(()),
            Some(_) => Err(Refusal::Unauthorized(Problem::new(
                "Authorization",
                format!("source `{source}` needs its write token"),
                format!(
                    "send `Authorization: Bearer <token>` with the token set for `{source}` under [auth.tokens] in trail.toml"
                ),
            ))),
        }
    }

    /// Refuses a batch sent as Trail's own source, which only Trail writes.
    fn not_own(source: &str) -> Result<(), Refusal> {
        if source == OWN_SOURCE {
            return Err(Refusal::Bad(Problem::new(
                "source",
                format!("`{OWN_SOURCE}` is the source Trail reports on itself with"),
                "send as your own source name",
            )));
        }
        Ok(())
    }

    /// Why `source` may not write a name another source owns, when that owner's writes are
    /// guarded: it has a write token, or it is Trail itself. Without a token the record is
    /// stored and marked instead, like any other mismatch.
    fn guarded(&self, source: &str, owner: Option<&str>, at: &str, name: &str) -> Option<Problem> {
        let owner = owner.filter(|owner| *owner != source)?;
        if owner != OWN_SOURCE && !self.config.auth.tokens.contains_key(owner) {
            return None;
        }
        Some(Problem::new(
            at,
            format!("`{name}` belongs to source `{owner}`, and only `{owner}` may write it"),
            format!("send it as `{owner}` with its token, or use a name of your own"),
        ))
    }

    /// Handles the body of `POST /v1/ingest`.
    pub fn ingest(&self, body: &[u8], token: Option<&str>, now: Ts) -> Result<Outcome, Refusal> {
        let batch: IngestBatch = serde_json::from_slice(body).map_err(|e| {
            Refusal::Bad(Problem::new(
                "body",
                format!("not an ingest batch: {e}"),
                "send a JSON object with `source` and any of `events`, `metrics`, `errors`, `logs`; see spec/ingest.md",
            ))
        })?;
        let ctx = BatchContext::new(&batch, now).map_err(Refusal::Bad)?;
        App::not_own(ctx.source)?;
        self.may_write(ctx.source, token)?;
        let items = batch.events.len() + batch.metrics.len() + batch.errors.len() + batch.logs.len();
        if items > self.config.limits.batch_items {
            return Err(Refusal::TooLarge(Problem::new(
                "body",
                format!(
                    "{items} records in one batch; the limit is {}",
                    self.config.limits.batch_items
                ),
                "send smaller batches",
            )));
        }

        let mut outcome = Outcome::default();
        let mut write = Batch::default();
        // Where each stored reading came from, to report on it after the write.
        let mut point_at: Vec<String> = Vec::new();
        {
            let registry = self.registry();
            for (index, raw) in batch.events.iter().enumerate() {
                let at = format!("events[{index}]");
                match envelope::event(&ctx, &at, raw.clone()) {
                    Err(problem) => outcome.rejected.push(problem),
                    Ok(event)
                        if let Some(problem) =
                            self.guarded(ctx.source, registry.event_owner(&event.name), &at, &event.name) =>
                    {
                        outcome.rejected.push(problem);
                    }
                    Ok(mut event) => {
                        event.problems = registry.check_event(ctx.source, &event.name, &event.props);
                        for message in &event.problems {
                            self.mark(
                                &mut outcome,
                                &mut write,
                                &at,
                                ctx.source,
                                "event",
                                &event.name,
                                message,
                                now,
                            );
                        }
                        write.events.push(event);
                    }
                }
            }
            for (index, raw) in batch.metrics.iter().enumerate() {
                let at = format!("metrics[{index}]");
                match envelope::point(&ctx, &at, raw.clone()) {
                    Err(problem) => outcome.rejected.push(problem),
                    Ok(point)
                        if let Some(problem) = self.guarded(
                            ctx.source,
                            registry.metric_owner(&point.name),
                            &at,
                            &point.name,
                        ) =>
                    {
                        outcome.rejected.push(problem);
                    }
                    Ok(point) => {
                        for message in registry.check_metric(ctx.source, &point.name, &point.labels) {
                            self.mark(
                                &mut outcome,
                                &mut write,
                                &at,
                                ctx.source,
                                "metric",
                                &point.name,
                                &message,
                                now,
                            );
                        }
                        write.points.push(self.metric_point(&registry, ctx.source, point));
                        point_at.push(at);
                    }
                }
            }
        }
        for (index, raw) in batch.errors.iter().enumerate() {
            match envelope::error(&ctx, &format!("errors[{index}]"), raw.clone()) {
                Err(problem) => outcome.rejected.push(problem),
                Ok(error) => write.errors.push(error),
            }
        }
        for (index, raw) in batch.logs.iter().enumerate() {
            match envelope::log(&ctx, &format!("logs[{index}]"), raw.clone()) {
                Err(problem) => outcome.rejected.push(problem),
                Ok(log) => write.logs.push(log),
            }
        }

        let written = self.commit(&write, now).map_err(Refusal::Failed)?;
        for (at, stored) in point_at.iter().zip(&written.points) {
            outcome
                .rejected
                .extend(not_stored(at, *stored, self.config.limits.series));
        }
        outcome.accepted = Counts {
            events: written.events.iter().filter(|new| **new).count(),
            metrics: written.points.iter().filter(|outcome| outcome.kept()).count(),
            errors: written.errors.iter().filter(|new| **new).count(),
            logs: written.logs.iter().filter(|new| **new).count(),
        };
        outcome.duplicates = written.duplicates();
        self.meters.count_ingest(ctx.source, &outcome, body.len());
        Ok(outcome)
    }

    /// Handles a line protocol body (`POST /v1/write`). Good lines are stored even when others
    /// are bad; the bad ones come back.
    pub fn write_lines(
        &self,
        source: &str,
        body: &str,
        precision: Precision,
        token: Option<&str>,
        now: Ts,
    ) -> Result<(usize, Vec<LineError>), Refusal> {
        if !is_source(source) {
            return Err(Refusal::Bad(Problem::new(
                "source",
                format!("`{source}` is not a source name"),
                "add `?source=<name>` to the URL: lowercase letters, digits and hyphens, starting with a letter",
            )));
        }
        App::not_own(source)?;
        self.may_write(source, token)?;
        let lines = body.lines().count();
        if lines > self.config.limits.batch_items {
            return Err(Refusal::TooLarge(Problem::new(
                "body",
                format!(
                    "{lines} lines in one body; the limit is {}",
                    self.config.limits.batch_items
                ),
                "send smaller batches (in Telegraf, lower metric_batch_size), or raise limits.batch_items in trail.toml",
            )));
        }
        let (points, mut bad) = lineproto::parse_numbered(body, precision, now);
        let mut outcome = Outcome::default();
        let mut write = Batch::default();
        let mut point_line: Vec<usize> = Vec::new();
        {
            let registry = self.registry();
            for (line, point) in points {
                let at = format!("line {line}");
                // A reading that parsed but can't be stored is a bad line too: say so.
                let refused = envelope::check_point(&at, &point.name, point.value, &point.labels)
                    .err()
                    .or_else(|| envelope::check_ts(&at, point.ts.ms(), now).err())
                    .or_else(|| self.guarded(source, registry.metric_owner(&point.name), &at, &point.name));
                if let Some(problem) = refused {
                    bad.push(LineError {
                        line,
                        message: format!("{}; {}", problem.message, problem.fix),
                    });
                    outcome.rejected.push(problem);
                    continue;
                }
                for message in registry.check_metric(source, &point.name, &point.labels) {
                    self.mark(
                        &mut outcome,
                        &mut write,
                        &at,
                        source,
                        "metric",
                        &point.name,
                        &message,
                        now,
                    );
                }
                write.points.push(self.metric_point(&registry, source, point));
                point_line.push(line);
            }
        }
        let written = self.commit(&write, now).map_err(Refusal::Failed)?;
        for (line, stored) in point_line.iter().zip(&written.points) {
            let Some(problem) = not_stored(&format!("line {line}"), *stored, self.config.limits.series)
            else {
                continue;
            };
            bad.push(LineError {
                line: *line,
                message: format!("{}; {}", problem.message, problem.fix),
            });
            outcome.rejected.push(problem);
        }
        bad.sort_by_key(|error| error.line);
        outcome.accepted.metrics = written.points.iter().filter(|outcome| outcome.kept()).count();
        self.meters.count_ingest(source, &outcome, body.len());
        Ok((outcome.accepted.metrics, bad))
    }

    /// Notes one mismatch in the response and for the drift report.
    #[allow(clippy::too_many_arguments)]
    fn mark(
        &self,
        outcome: &mut Outcome,
        write: &mut Batch,
        at: &str,
        source: &str,
        kind: &str,
        name: &str,
        message: &str,
        now: Ts,
    ) {
        outcome.mismatched.push(Marked {
            at: at.to_string(),
            message: message.to_string(),
        });
        write.mismatches.push(Mismatch {
            source: source.to_string(),
            kind: kind.to_string(),
            name: name.to_string(),
            message: message.to_string(),
            seen: now,
        });
    }

    /// A reading with what its declaration adds: the unit, and whether it is a running total.
    fn metric_point(
        &self,
        registry: &trail_core::registry::Registry,
        source: &str,
        mut point: Point,
    ) -> MetricPoint {
        let declared = registry
            .get(source)
            .and_then(|decl| decl.metrics.get(&point.name));
        if point.unit.is_none() {
            point.unit = declared.and_then(|metric| metric.unit.clone());
        }
        let kind = point.kind.or(declared.map(|metric| metric.kind)).unwrap_or(
            if self.config.is_counter(&point.name) {
                MetricKind::Counter
            } else {
                MetricKind::Gauge
            },
        );
        point.kind = Some(kind);
        MetricPoint {
            source: source.to_string(),
            counter: kind == MetricKind::Counter,
            point,
        }
    }

    /// Stores a batch and feeds its new events to the rollups and the event alerts. The rollup
    /// lock is held across the write so a rollup being rebuilt never sees an event twice.
    pub fn commit(&self, batch: &Batch, now: Ts) -> Result<Written, Problem> {
        if batch.events.is_empty()
            && batch.points.is_empty()
            && batch.errors.is_empty()
            && batch.logs.is_empty()
            && batch.mismatches.is_empty()
        {
            return Ok(Written::default());
        }
        let written = {
            // Only a batch with events needs the rollups: logs, errors and readings go straight
            // to the store's own write lock.
            let mut rollups = (!batch.events.is_empty()).then(|| lock(&self.rollups));
            let written = self.store.write(batch).map_err(|e| e.problem())?;
            let mut stored = 0;
            for (event, new) in batch.events.iter().zip(&written.events) {
                if *new && let Some(rollups) = rollups.as_mut() {
                    stored += 1;
                    rollups.observe(event, &self.config.zone, self.config.limits.rollup_groups);
                }
            }
            // Counted under the rollup lock, so the rollup timer's reading of it is exact.
            self.events_seen
                .fetch_add(stored, std::sync::atomic::Ordering::Relaxed);
            written
        };
        for (event, new) in batch.events.iter().zip(&written.events) {
            if *new {
                self.notice_event(event, now);
            }
        }
        Ok(written)
    }
}

/// Why a point was not stored, for the sender; None when it was.
fn not_stored(at: &str, outcome: PointOutcome, series: usize) -> Option<Problem> {
    match outcome {
        PointOutcome::Stored | PointOutcome::Baseline => None,
        // A rate is the change between two readings in order, so this one can't be placed.
        PointOutcome::OutOfOrder => Some(Problem::new(
            at,
            "this counter reading is older than the newest one stored for its series, so it was not stored",
            "send a counter's readings in time order from one sender; a gauge has no such rule",
        )),
        PointOutcome::TooManySeries => Some(Problem::new(
            at,
            format!(
                "this would start a new series, and Trail already keeps {series} (limits.series), so it was not stored"
            ),
            "a label whose values never repeat (an id, a path, a time) makes a series per value: drop that label, or send it as an event; raise limits.series in trail.toml if the series are all meant",
        )),
    }
}

/// The JSON body for a refused request.
pub fn refusal_body(refusal: &Refusal) -> Value {
    serde_json::json!({ "error": refusal.problem() })
}
