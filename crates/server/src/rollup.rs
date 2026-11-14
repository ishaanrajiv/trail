//! Rollups: the aggregations services declare in `rollups.toml`, computed as events arrive.
//!
//! Each rollup keeps its open buckets in memory, one accumulator per group. Every few seconds the
//! buckets that changed are written as metric samples stamped with the bucket's start, so a
//! bucket's value is visible while it is still filling and is simply rewritten as it grows. A
//! bucket leaves memory a grace period after it ends. An event that arrives for a bucket that has
//! already left (a phone that was offline) marks that bucket dirty, and it is recomputed from the
//! raw events. Nothing here is the only copy of anything: after a restart the open buckets are
//! rebuilt from the raw events too.

use crate::sketch::Sketch;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use trail_core::Ts;
use trail_core::decl::{Measure, RollupDecl};
use trail_core::envelope::{Event, MetricKind, Point};
use trail_core::registry::Registry;
use trail_core::time::{MINUTE, Zone};
use trail_store::{MetricPoint, Store};

/// How long after a bucket ends it stays in memory, taking events that arrive a little late.
pub const GRACE: i64 = MINUTE;
/// Label values longer than this are cut.
const MAX_LABEL: usize = 200;

/// One declared rollup, ready to run.
#[derive(Debug, Clone, PartialEq)]
pub struct Spec {
    /// `<source>.<rollup>`.
    pub id: String,
    pub source: String,
    pub name: String,
    pub decl: RollupDecl,
    /// The distinct properties its measures read.
    props: Vec<String>,
    /// Which of those need a percentile sketch.
    sketched: Vec<bool>,
}

impl Spec {
    fn new(source: &str, name: &str, decl: &RollupDecl) -> Spec {
        let mut props: Vec<String> = Vec::new();
        for measure in &decl.measures {
            if let Some(prop) = measure.prop()
                && !props.iter().any(|p| p == prop)
            {
                props.push(prop.to_string());
            }
        }
        let sketched = props
            .iter()
            .map(|prop| {
                decl.measures
                    .iter()
                    .any(|m| matches!(m, Measure::Percentile(_, p) if p == prop))
            })
            .collect();
        Spec {
            id: format!("{source}.{name}"),
            source: source.to_string(),
            name: name.to_string(),
            decl: decl.clone(),
            props,
            sketched,
        }
    }

    /// Whether an event passes the rollup's `where`.
    /// Whether this rollup counts the event: sent by the source that owns the rollup, and
    /// passing its `where`. An event of that name sent by another source is stored and marked,
    /// and counted nowhere.
    fn takes(&self, event: &Event) -> bool {
        event.source == self.source
            && self
                .decl
                .filter
                .iter()
                .all(|(prop, want)| event.props.get(prop).is_some_and(|value| want.matches(value)))
    }

    /// The event's value for each `by` field. A missing one groups under the empty string.
    fn group(&self, event: &Event) -> Vec<String> {
        self.decl
            .by
            .iter()
            .map(|field| {
                let text = match event.props.get(field) {
                    Some(serde_json::Value::String(text)) => text.clone(),
                    Some(serde_json::Value::Null) => String::new(),
                    Some(other) => other.to_string(),
                    None => match field.as_str() {
                        "actor" => event.actor.clone().unwrap_or_default(),
                        "client" => event.client.clone().unwrap_or_default(),
                        "host" => event.host.clone().unwrap_or_default(),
                        _ => String::new(),
                    },
                };
                if text.len() > MAX_LABEL {
                    text.chars().take(MAX_LABEL).collect()
                } else {
                    text
                }
            })
            .collect()
    }
}

/// What one property's readings add up to in one group.
#[derive(Debug, Clone, PartialEq)]
struct Stat {
    n: u64,
    sum: f64,
    min: f64,
    max: f64,
    sketch: Option<Sketch>,
}

/// One group's running totals in one bucket.
#[derive(Debug, Clone, PartialEq)]
struct Acc {
    count: u64,
    stats: Vec<Stat>,
}

/// One bucket of one rollup.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Bucket {
    groups: HashMap<Vec<String>, Acc>,
    /// Events left out because the bucket already had the most groups allowed.
    pub overflow: u64,
    changed: bool,
}

impl Bucket {
    fn add(&mut self, spec: &Spec, event: &Event, max_groups: usize) {
        let key = spec.group(event);
        if !self.groups.contains_key(&key) && self.groups.len() >= max_groups {
            self.overflow += 1;
            return;
        }
        let acc = self.groups.entry(key).or_insert_with(|| Acc {
            count: 0,
            stats: spec
                .sketched
                .iter()
                .map(|sketched| Stat {
                    n: 0,
                    sum: 0.0,
                    min: f64::INFINITY,
                    max: f64::NEG_INFINITY,
                    sketch: sketched.then(Sketch::default),
                })
                .collect(),
        });
        acc.count += 1;
        for (prop, stat) in spec.props.iter().zip(acc.stats.iter_mut()) {
            let Some(value) = event.props.get(prop).and_then(serde_json::Value::as_f64) else {
                continue;
            };
            if !value.is_finite() {
                continue;
            }
            stat.n += 1;
            stat.sum += value;
            stat.min = stat.min.min(value);
            stat.max = stat.max.max(value);
            if let Some(sketch) = stat.sketch.as_mut() {
                sketch.add(value);
            }
        }
        self.changed = true;
    }

    /// The bucket as metric samples stamped `start`: one per group and measure.
    fn points(&self, spec: &Spec, start: Ts) -> Vec<MetricPoint> {
        let mut points = Vec::new();
        for (key, acc) in &self.groups {
            let labels: BTreeMap<String, String> =
                spec.decl.by.iter().cloned().zip(key.iter().cloned()).collect();
            for measure in &spec.decl.measures {
                let stat = measure
                    .prop()
                    .and_then(|prop| spec.props.iter().position(|p| p == prop))
                    .and_then(|index| acc.stats.get(index));
                #[allow(clippy::cast_precision_loss)]
                let value = match (measure, stat) {
                    (Measure::Count, _) => Some(acc.count as f64),
                    (_, None) => None,
                    (_, Some(stat)) if stat.n == 0 => None,
                    (Measure::Sum(_), Some(stat)) => Some(stat.sum),
                    (Measure::Avg(_), Some(stat)) => Some(stat.sum / stat.n as f64),
                    (Measure::Min(_), Some(stat)) => Some(stat.min),
                    (Measure::Max(_), Some(stat)) => Some(stat.max),
                    (Measure::Percentile(p, _), Some(stat)) => stat
                        .sketch
                        .as_ref()
                        .and_then(|sketch| sketch.quantile(f64::from(*p) / 100.0)),
                };
                let Some(value) = value.filter(|v| v.is_finite()) else {
                    continue;
                };
                points.push(MetricPoint {
                    source: spec.source.clone(),
                    point: Point {
                        name: format!("{}.{}", spec.id, measure.suffix()),
                        labels: labels.clone(),
                        value,
                        ts: start,
                        unit: None,
                        kind: Some(MetricKind::Gauge),
                    },
                    counter: false,
                });
            }
        }
        points
    }
}

/// Computes one bucket of a rollup from the raw events.
fn compute(
    store: &Store,
    zone: &Zone,
    spec: &Spec,
    start: Ts,
    max_groups: usize,
) -> trail_store::Result<Bucket> {
    let end = zone.next(start, spec.decl.every);
    let (_, mut buckets) = compute_range(store, zone, spec, start, end, max_groups)?;
    Ok(buckets.remove(&start).unwrap_or_default())
}

/// Computes every bucket of a rollup that has events in [start, end), from the raw events.
/// Returns how many events it read, and the buckets by their start.
pub fn compute_range(
    store: &Store,
    zone: &Zone,
    spec: &Spec,
    start: Ts,
    end: Ts,
    max_groups: usize,
) -> trail_store::Result<(u64, BTreeMap<Ts, Bucket>)> {
    let mut buckets: BTreeMap<Ts, Bucket> = BTreeMap::new();
    let mut events = 0u64;
    store.scan_events(&spec.decl.from, start, end, &mut |event| {
        if spec.takes(&event) {
            events += 1;
            buckets
                .entry(zone.floor(event.ts, spec.decl.every))
                .or_default()
                .add(spec, &event, max_groups);
        }
    })?;
    Ok((events, buckets))
}

/// A bucket's samples, for writing.
pub fn points_of(spec: &Spec, start: Ts, bucket: &Bucket) -> Vec<MetricPoint> {
    bucket.points(spec, start)
}

/// Counts the aggregator keeps for Trail's own metrics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tally {
    /// Events that arrived for a bucket that had already left memory, per rollup.
    pub late: BTreeMap<String, u64>,
    /// Events left out of a bucket that was at its group limit, per rollup.
    pub overflow: BTreeMap<String, u64>,
}

/// Every rollup's open buckets.
#[derive(Debug, Default)]
pub struct Aggregator {
    specs: BTreeMap<String, Spec>,
    by_event: HashMap<String, Vec<String>>,
    open: BTreeMap<(String, i64), Bucket>,
    /// Per rollup: buckets that start before this have been written and have left memory.
    watermark: HashMap<String, i64>,
    dirty: BTreeSet<(String, i64)>,
    tally: Tally,
}

impl Aggregator {
    pub fn new() -> Aggregator {
        Aggregator::default()
    }

    pub fn spec(&self, id: &str) -> Option<&Spec> {
        self.specs.get(id)
    }

    pub fn specs(&self) -> impl Iterator<Item = &Spec> {
        self.specs.values()
    }

    /// Brings the rollups in line with the registry. A rollup that is new or changed starts with
    /// its current bucket rebuilt from the raw events and its previous bucket marked dirty, and a
    /// changed one keeps the buckets it already owed or had open (moved to where its buckets
    /// start now, when `every` changed), so a restart or a publish loses nothing. Call it holding the lock ingest
    /// takes, so no event is both scanned here and fed in afterwards.
    pub fn sync(
        &mut self,
        registry: &Registry,
        store: &Store,
        zone: &Zone,
        now: Ts,
        max_groups: usize,
    ) -> trail_store::Result<()> {
        let wanted: BTreeMap<String, Spec> = registry
            .rollups()
            .map(|(source, name, decl)| {
                let spec = Spec::new(source, name, decl);
                (spec.id.clone(), spec)
            })
            .collect();
        let gone: Vec<String> = self
            .specs
            .keys()
            .filter(|id| wanted.get(*id) != self.specs.get(*id))
            .cloned()
            .collect();
        for id in &gone {
            let before = self.specs.remove(id);
            self.watermark.remove(id);
            // Only a rollup no longer declared has nothing left to recompute.
            let (Some(before), Some(after)) = (before, wanted.get(id)) else {
                self.open.retain(|(owner, _), _| owner != id);
                self.dirty.retain(|(owner, _)| owner != id);
                continue;
            };
            // A rollup that is still declared, changed or not, still owes the buckets late
            // events dirtied, and the buckets it had open: they are recomputed from the raw
            // events under what it says now. Its buckets may start elsewhere now (`every`
            // changed), so each one owed becomes the buckets that cover the same time under the
            // new `every`: the recompute reads and writes whole buckets at their own starts.
            let mut owed: BTreeSet<i64> = BTreeSet::new();
            owed.extend(
                self.dirty
                    .iter()
                    .filter(|(owner, _)| owner == id)
                    .map(|(_, start)| *start),
            );
            owed.extend(
                self.open
                    .keys()
                    .filter(|(owner, _)| owner == id)
                    .map(|(_, start)| *start),
            );
            self.open.retain(|(owner, _), _| owner != id);
            self.dirty.retain(|(owner, _)| owner != id);
            for start in owed {
                let end = zone.next(Ts(start), before.decl.every);
                let mut at = zone.floor(Ts(start), after.decl.every);
                while at < end {
                    self.dirty.insert((id.clone(), at.ms()));
                    at = zone.next(at, after.decl.every);
                }
            }
        }
        for (id, spec) in wanted {
            if self.specs.contains_key(&id) {
                continue;
            }
            let current = zone.floor(now, spec.decl.every);
            let mut bucket = compute(store, zone, &spec, current, max_groups)?;
            bucket.changed = !bucket.groups.is_empty();
            self.open.insert((id.clone(), current.ms()), bucket);
            self.watermark.insert(id.clone(), current.ms());
            let previous = zone.floor(current.minus(1), spec.decl.every);
            self.dirty.insert((id.clone(), previous.ms()));
            self.specs.insert(id, spec);
        }
        self.by_event.clear();
        for spec in self.specs.values() {
            self.by_event
                .entry(spec.decl.from.clone())
                .or_default()
                .push(spec.id.clone());
        }
        Ok(())
    }

    /// Feeds one newly stored event to every rollup that reads it.
    pub fn observe(&mut self, event: &Event, zone: &Zone, max_groups: usize) {
        let Some(ids) = self.by_event.get(&event.name) else {
            return;
        };
        for id in ids {
            let Some(spec) = self.specs.get(id) else {
                continue;
            };
            if !spec.takes(event) {
                continue;
            }
            let start = zone.floor(event.ts, spec.decl.every).ms();
            if start < self.watermark.get(id).copied().unwrap_or(i64::MIN) {
                self.dirty.insert((id.clone(), start));
                *self.tally.late.entry(id.clone()).or_insert(0) += 1;
                continue;
            }
            let bucket = self.open.entry((id.clone(), start)).or_default();
            let before = bucket.overflow;
            bucket.add(spec, event, max_groups);
            if bucket.overflow > before {
                *self.tally.overflow.entry(id.clone()).or_insert(0) += 1;
            }
        }
    }

    /// The samples of every bucket that changed since the last call. Buckets that ended more than
    /// the grace period ago leave memory.
    pub fn flush(&mut self, now: Ts, zone: &Zone) -> Vec<(String, Ts, Vec<MetricPoint>)> {
        let mut points = Vec::new();
        let mut closed = Vec::new();
        for ((id, start), bucket) in &mut self.open {
            let Some(spec) = self.specs.get(id) else {
                closed.push((id.clone(), *start, i64::MIN));
                continue;
            };
            if bucket.changed {
                points.push((id.clone(), Ts(*start), bucket.points(spec, Ts(*start))));
                bucket.changed = false;
            }
            let end = zone.next(Ts(*start), spec.decl.every).ms();
            if end.saturating_add(GRACE) <= now.ms() {
                closed.push((id.clone(), *start, end));
            }
        }
        for (id, start, end) in closed {
            self.open.remove(&(id.clone(), start));
            let mark = self.watermark.entry(id).or_insert(i64::MIN);
            *mark = (*mark).max(end);
        }
        points
    }

    /// The buckets waiting to be recomputed from raw events, taken off the list. Take them before
    /// scanning: an event that arrives during the scan puts its bucket back on the list.
    pub fn take_dirty(&mut self) -> Vec<(Spec, Ts)> {
        std::mem::take(&mut self.dirty)
            .into_iter()
            .filter_map(|(id, start)| self.specs.get(&id).map(|spec| (spec.clone(), Ts(start))))
            .collect()
    }

    /// Puts a bucket back on the dirty list (its recompute failed).
    /// Marks the closed bucket an already-stored event belongs to for recomputing. Used at
    /// start to catch up on events that were stored but whose rollup was not written before
    /// Trail stopped. The open bucket needs nothing: it was read from the events at start.
    pub fn catch_up(&mut self, event: &Event, zone: &Zone) {
        let Some(ids) = self.by_event.get(&event.name) else {
            return;
        };
        for id in ids {
            let Some(spec) = self.specs.get(id).filter(|spec| spec.takes(event)) else {
                continue;
            };
            let start = zone.floor(event.ts, spec.decl.every).ms();
            if start < self.watermark.get(id).copied().unwrap_or(i64::MIN) {
                self.dirty.insert((id.clone(), start));
            }
        }
    }

    /// Marks every open bucket of a rollup to be written again at the next flush. After a
    /// rebuild wrote samples computed from a scan, this puts the live values back on top:
    /// events that arrived during the scan are in the open buckets and may not be in the scan.
    pub fn rewrite_open(&mut self, id: &str) {
        for ((owner, _), bucket) in &mut self.open {
            if owner == id {
                bucket.changed = true;
            }
        }
    }

    pub fn mark_dirty(&mut self, id: &str, start: Ts) {
        self.dirty.insert((id.to_string(), start.ms()));
    }

    pub fn tally(&self) -> &Tally {
        &self.tally
    }

    pub fn open_buckets(&self) -> usize {
        self.open.len()
    }
}
