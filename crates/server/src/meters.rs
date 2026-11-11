//! Trail's own metrics, sent to itself under the source `trail`. `crates/server/trail/` declares
//! them, so they are documented and checked like any other source's.

use crate::ingest::Outcome;
use crate::lock;
use std::collections::BTreeMap;
use std::sync::Mutex;
use trail_core::Ts;
use trail_core::envelope::{MetricKind, Point};
use trail_store::MetricPoint;

type Key = (String, BTreeMap<String, String>);

/// Running totals since start. They are written as counters, so each is stored as a rate.
#[derive(Debug, Default)]
pub struct Meters {
    totals: Mutex<BTreeMap<Key, f64>>,
}

impl Meters {
    pub fn add(&self, name: &str, labels: &[(&str, &str)], amount: f64) {
        let key = (
            name.to_string(),
            labels
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        );
        *lock(&self.totals).entry(key).or_insert(0.0) += amount;
    }

    pub(crate) fn count_ingest(&self, source: &str, outcome: &Outcome, bytes: usize) {
        #[allow(clippy::cast_precision_loss)]
        for (kind, n) in [
            ("event", outcome.accepted.events),
            ("metric", outcome.accepted.metrics),
            ("error", outcome.accepted.errors),
            ("log", outcome.accepted.logs),
        ] {
            if n > 0 {
                self.add(
                    "trail.ingest.records",
                    &[("source", source), ("kind", kind)],
                    n as f64,
                );
            }
        }
        #[allow(clippy::cast_precision_loss)]
        for (name, n) in [
            ("trail.ingest.duplicates", outcome.duplicates),
            ("trail.ingest.rejected", outcome.rejected.len()),
            ("trail.ingest.mismatched", outcome.mismatched.len()),
            ("trail.ingest.bytes", bytes),
        ] {
            if n > 0 {
                self.add(name, &[("source", source)], n as f64);
            }
        }
    }

    /// Every total as a counter reading at `now`.
    pub fn readings(&self, now: Ts) -> Vec<MetricPoint> {
        lock(&self.totals)
            .iter()
            .map(|((name, labels), total)| counter(name, labels.clone(), *total, now))
            .collect()
    }
}

fn counter(name: &str, labels: BTreeMap<String, String>, value: f64, now: Ts) -> MetricPoint {
    MetricPoint {
        source: "trail".to_string(),
        point: Point {
            name: name.to_string(),
            labels,
            value,
            ts: now,
            unit: None,
            kind: Some(MetricKind::Counter),
        },
        counter: true,
    }
}

/// A gauge reading under the source `trail`.
pub fn gauge(name: &str, labels: &[(&str, &str)], value: f64, now: Ts) -> MetricPoint {
    MetricPoint {
        source: "trail".to_string(),
        point: Point {
            name: name.to_string(),
            labels: labels
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            value,
            ts: now,
            unit: None,
            kind: Some(MetricKind::Gauge),
        },
        counter: false,
    }
}
