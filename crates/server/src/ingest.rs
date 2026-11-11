//! The write path: a request body becomes checked records, one store transaction, and a response
//! that says what happened to each record. `spec/ingest.md` is the contract.

use crate::{App, lock};
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
    fn may_write(&self, source: &str, token: Option<&str>) -> Result<(), Refusal> {
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
        {
            let registry = self.registry();
            for (index, raw) in batch.events.iter().enumerate() {
                let at = format!("events[{index}]");
                match envelope::event(&ctx, &at, raw.clone()) {
                    Err(problem) => outcome.rejected.push(problem),
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
        outcome.accepted = Counts {
            events: written.events.iter().filter(|new| **new).count(),
            metrics: written
                .points
                .iter()
                .filter(|outcome| **outcome != PointOutcome::OutOfOrder)
                .count(),
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
        self.may_write(source, token)?;
        let (points, bad) = lineproto::parse(body, precision, now);
        let mut outcome = Outcome::default();
        let mut write = Batch::default();
        {
            let registry = self.registry();
            for (index, point) in points.into_iter().enumerate() {
                if let Err(problem) =
                    envelope::check_point(&format!("point {index}"), &point.name, point.value, &point.labels)
                {
                    outcome.rejected.push(problem);
                    continue;
                }
                for message in registry.check_metric(source, &point.name, &point.labels) {
                    let at = format!("metrics[{index}]");
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
            }
        }
        let written = self.commit(&write, now).map_err(Refusal::Failed)?;
        outcome.accepted.metrics = written
            .points
            .iter()
            .filter(|outcome| **outcome != PointOutcome::OutOfOrder)
            .count();
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
            let mut rollups = lock(&self.rollups);
            let written = self.store.write(batch).map_err(|e| e.problem())?;
            for (event, new) in batch.events.iter().zip(&written.events) {
                if *new {
                    rollups.observe(event, &self.config.zone, self.config.limits.rollup_groups);
                }
            }
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

/// The JSON body for a refused request.
pub fn refusal_body(refusal: &Refusal) -> Value {
    serde_json::json!({ "error": refusal.problem() })
}
