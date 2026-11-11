//! Every source's published declarations, and the check each incoming record gets against them.
//!
//! The check never rejects: a record that doesn't match its declaration is stored and marked, so
//! nothing is lost and drift shows up in a report. A source that has published nothing is not
//! checked at all, which is how collectors such as Telegraf send freely.

use crate::decl::{AlertDecl, Declarations, EventDecl, Retention, RollupDecl};
use crate::envelope::Object;
use crate::problem::{Problem, closest};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Registry {
    sources: BTreeMap<String, Declarations>,
    /// Event name to the source that declares it.
    event_owners: BTreeMap<String, String>,
    /// Metric name to the source that declares it or whose rollup writes it.
    metric_owners: BTreeMap<String, String>,
}

/// Every metric name a source's declarations lay claim to: the metrics it declares and the
/// series its rollups write.
fn metric_names(decl: &Declarations) -> impl Iterator<Item = String> + '_ {
    let rolled = decl.rollups.iter().flat_map(|(name, rollup)| {
        rollup
            .series_names(&decl.source, name)
            .into_iter()
            .map(|(_, series)| series)
    });
    decl.metrics.keys().cloned().chain(rolled)
}

impl Registry {
    pub fn new() -> Registry {
        Registry::default()
    }

    pub fn from_sources(sources: impl IntoIterator<Item = Declarations>) -> Registry {
        let mut registry = Registry {
            sources: sources.into_iter().map(|d| (d.source.clone(), d)).collect(),
            ..Registry::default()
        };
        registry.index();
        registry
    }

    /// Rebuilds the name-to-owner maps after the sources change.
    fn index(&mut self) {
        self.event_owners.clear();
        self.metric_owners.clear();
        for decl in self.sources.values() {
            for name in decl.events.keys() {
                self.event_owners
                    .entry(name.clone())
                    .or_insert_with(|| decl.source.clone());
            }
            for name in metric_names(decl) {
                self.metric_owners
                    .entry(name)
                    .or_insert_with(|| decl.source.clone());
            }
        }
    }

    /// The source an event name belongs to, when any source declares it.
    pub fn event_owner(&self, name: &str) -> Option<&str> {
        self.event_owners.get(name).map(String::as_str)
    }

    /// The source a metric name belongs to: the one that declares it, or whose rollup writes it.
    pub fn metric_owner(&self, name: &str) -> Option<&str> {
        self.metric_owners.get(name).map(String::as_str)
    }

    /// Checks that `decl` can join the registry: an event name belongs to one source only, and a
    /// rollup's series must not collide with another source's.
    pub fn conflicts(&self, decl: &Declarations) -> Vec<Problem> {
        let mut problems = Vec::new();
        for (source, other) in self.sources.iter().filter(|(source, _)| **source != decl.source) {
            for name in decl.events.keys().filter(|name| other.events.contains_key(*name)) {
                problems.push(Problem::new(
                    format!("events.toml: events.\"{name}\""),
                    format!("`{name}` is already declared by source `{source}`"),
                    "an event name belongs to one source; rename this one, or have that source send it",
                ));
            }
            let theirs: std::collections::BTreeSet<String> = metric_names(other).collect();
            for name in decl.metrics.keys().filter(|name| theirs.contains(*name)) {
                problems.push(Problem::new(
                    format!("metrics.toml: metrics.\"{name}\""),
                    format!("`{name}` already belongs to source `{source}`, which declares it or writes it from a rollup"),
                    "a metric name belongs to one source; rename this one, or have that source send it",
                ));
            }
            for (rollup, declared) in &decl.rollups {
                for (_, series) in declared.series_names(&decl.source, rollup) {
                    if theirs.contains(&series) {
                        problems.push(Problem::new(
                            format!("rollups.toml: rollups.{rollup}"),
                            format!("it writes `{series}`, which already belongs to source `{source}`"),
                            "a metric name belongs to one source; rename this rollup, or that source's metric",
                        ));
                    }
                }
            }
        }
        problems
    }

    /// Adds or replaces one source's declarations.
    pub fn set(&mut self, decl: Declarations) {
        self.sources.insert(decl.source.clone(), decl);
        self.index();
    }

    pub fn remove(&mut self, source: &str) -> Option<Declarations> {
        let removed = self.sources.remove(source);
        self.index();
        removed
    }

    pub fn get(&self, source: &str) -> Option<&Declarations> {
        self.sources.get(source)
    }

    pub fn sources(&self) -> impl Iterator<Item = &Declarations> {
        self.sources.values()
    }

    /// The declaration of an event name, and the source that owns it.
    pub fn event(&self, name: &str) -> Option<(&str, &EventDecl)> {
        let owner = self.sources.get(self.event_owners.get(name)?)?;
        owner.events.get(name).map(|event| (owner.source.as_str(), event))
    }

    /// How long raw events of this name are kept. Undeclared events are kept forever.
    pub fn retention(&self, event_name: &str) -> Retention {
        self.event(event_name)
            .map(|(_, e)| e.retention)
            .unwrap_or(Retention::Forever)
    }

    /// Every rollup, with its source and name.
    pub fn rollups(&self) -> impl Iterator<Item = (&str, &str, &RollupDecl)> {
        self.sources.values().flat_map(|d| {
            d.rollups
                .iter()
                .map(move |(name, r)| (d.source.as_str(), name.as_str(), r))
        })
    }

    /// Every alert rule, with its source and name.
    pub fn alerts(&self) -> impl Iterator<Item = (&str, &str, &AlertDecl)> {
        self.sources.values().flat_map(|d| {
            d.alerts
                .iter()
                .map(move |(name, a)| (d.source.as_str(), name.as_str(), a))
        })
    }

    /// Where an event differs from its declaration. Empty for a clean event, and for an event
    /// no source has declared when its own source has published nothing.
    pub fn check_event(&self, source: &str, name: &str, props: &Object) -> Vec<String> {
        let foreign = |owner: &str| {
            format!(
                "`{name}` is declared by source `{owner}`, not `{source}`; send it from `{owner}` or declare a new name"
            )
        };
        let Some(decl) = self.sources.get(source) else {
            return self.event_owner(name).map(foreign).into_iter().collect();
        };
        let Some(event) = decl.events.get(name) else {
            return vec![match self.event_owner(name) {
                Some(owner) => foreign(owner),
                None => {
                    let hint = closest(name, decl.events.keys().map(String::as_str))
                        .map(|c| format!(" Did you mean `{c}`?"))
                        .unwrap_or_default();
                    format!("undeclared event `{name}`; add it to `{source}`'s events.toml.{hint}")
                }
            }];
        };
        let mut problems = Vec::new();
        for (prop, value) in props {
            match event.props.get(prop) {
                None => {
                    let declared: Vec<&str> = event.props.keys().map(String::as_str).collect();
                    let hint = closest(prop, declared.iter().copied())
                        .map(|c| format!(" Did you mean `{c}`?"))
                        .unwrap_or_default();
                    problems.push(format!(
                        "unknown property `{prop}` on `{name}`; declared: {}.{hint}",
                        declared.join(", ")
                    ));
                }
                Some(kind) if !kind.accepts(value) => {
                    problems.push(format!(
                        "property `{prop}` of `{name}` should be {}, got {}",
                        kind.describe(),
                        json_kind(value)
                    ));
                }
                Some(_) => {}
            }
        }
        for (prop, kind) in &event.props {
            if !kind.optional && !props.contains_key(prop) {
                problems.push(format!(
                    "`{name}` is missing required property `{prop}` ({})",
                    kind.describe()
                ));
            }
        }
        problems
    }

    /// Where a metric reading differs from its declaration. Empty when clean, and for sources that
    /// have published nothing.
    pub fn check_metric(&self, source: &str, name: &str, labels: &BTreeMap<String, String>) -> Vec<String> {
        if let Some(owner) = self.metric_owner(name).filter(|owner| *owner != source) {
            return vec![format!(
                "`{name}` belongs to source `{owner}`, not `{source}`; send it from `{owner}` or use a new name"
            )];
        }
        let Some(decl) = self.sources.get(source) else {
            return Vec::new();
        };
        let Some(metric) = decl.metrics.get(name) else {
            let hint = closest(name, decl.metrics.keys().map(String::as_str))
                .map(|c| format!(" Did you mean `{c}`?"))
                .unwrap_or_default();
            return vec![format!(
                "undeclared metric `{name}`; add it to `{source}`'s metrics.toml.{hint}"
            )];
        };
        let mut problems = Vec::new();
        for label in labels.keys().filter(|label| !metric.labels.contains(label)) {
            problems.push(format!(
                "unknown label `{label}` on `{name}`; declared: {}",
                metric.labels.join(", ")
            ));
        }
        for label in metric.labels.iter().filter(|label| !labels.contains_key(*label)) {
            problems.push(format!("`{name}` is missing label `{label}`"));
        }
        problems
    }
}

fn json_kind(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(_) => "a bool".to_string(),
        serde_json::Value::Number(n) if n.is_f64() => format!("the float {n}"),
        serde_json::Value::Number(n) => format!("the int {n}"),
        serde_json::Value::String(text) => {
            let short: String = text.chars().take(24).collect();
            format!("the string \"{short}\"")
        }
        serde_json::Value::Array(_) => "a list".to_string(),
        serde_json::Value::Object(_) => "an object".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decl::{Files, parse};
    use serde_json::json;

    fn shop() -> Declarations {
        parse(&Files {
            events: r#"
source = "shop"
[events."shop.order.placed"]
description = "A customer placed an order"
retention = "90d"
props.order_id = "string"
props.total_cents = "int"
props.latency_ms = "int?"
"#
            .to_string(),
            metrics: Some(
                r#"
[metrics."shop.queue.depth"]
description = "Jobs waiting"
labels = ["queue"]
"#
                .to_string(),
            ),
            rollups: None,
            alerts: None,
        })
        .unwrap()
    }

    fn props(value: serde_json::Value) -> Object {
        value.as_object().cloned().unwrap()
    }

    #[test]
    fn clean_event_passes() {
        let registry = Registry::from_sources([shop()]);
        let none = registry.check_event(
            "shop",
            "shop.order.placed",
            &props(json!({"order_id": "o1", "total_cents": 1200})),
        );
        assert!(none.is_empty(), "{none:?}");
        assert_eq!(registry.retention("shop.order.placed"), Retention::Days(90));
        assert_eq!(registry.retention("anything.else"), Retention::Forever);
    }

    #[test]
    fn mismatches_state_the_fix() {
        let registry = Registry::from_sources([shop()]);
        let found = registry.check_event(
            "shop",
            "shop.order.placed",
            &props(json!({"order_id": 7, "latncy_ms": 3})),
        );
        assert_eq!(
            found,
            [
                "unknown property `latncy_ms` on `shop.order.placed`; declared: latency_ms, order_id, total_cents. Did you mean `latency_ms`?",
                "property `order_id` of `shop.order.placed` should be string, got the int 7",
                "`shop.order.placed` is missing required property `total_cents` (int)",
            ]
        );
        let undeclared = registry.check_event("shop", "shop.order.place", &Object::new());
        assert!(
            undeclared[0].contains("Did you mean `shop.order.placed`?"),
            "{undeclared:?}"
        );
    }

    #[test]
    fn sources_without_declarations_are_not_checked() {
        let registry = Registry::from_sources([shop()]);
        assert!(
            registry
                .check_event("telegraf", "anything.at_all", &Object::new())
                .is_empty()
        );
        assert!(
            registry
                .check_metric("telegraf", "cpu.usage", &BTreeMap::new())
                .is_empty()
        );
    }

    #[test]
    fn metrics_are_checked() {
        let registry = Registry::from_sources([shop()]);
        let labels = BTreeMap::from([("queue".to_string(), "email".to_string())]);
        assert!(
            registry
                .check_metric("shop", "shop.queue.depth", &labels)
                .is_empty()
        );
        let wrong = BTreeMap::from([("que".to_string(), "email".to_string())]);
        assert_eq!(registry.check_metric("shop", "shop.queue.depth", &wrong).len(), 2);
        assert!(registry.check_metric("shop", "shop.queue.dpth", &labels)[0].contains("Did you mean"));
    }

    #[test]
    fn an_event_name_belongs_to_one_source() {
        let registry = Registry::from_sources([shop()]);
        let mut other = shop();
        other.source = "billing".to_string();
        let conflicts = registry.conflicts(&other);
        assert_eq!(conflicts.len(), 2);
        assert!(conflicts[0].message.contains("already declared by source `shop`"));
        assert!(registry.conflicts(&shop()).is_empty());
        let wrong_sender = Registry::from_sources([shop(), {
            let mut d = shop();
            d.source = "billing".to_string();
            d.events.clear();
            d.metrics.clear();
            d
        }]);
        let found = wrong_sender.check_event("billing", "shop.order.placed", &Object::new());
        assert!(found[0].contains("declared by source `shop`"), "{found:?}");
        // A source that has published nothing is not checked, except against what others own.
        let stranger = registry.check_event("telegraf", "shop.order.placed", &Object::new());
        assert!(stranger[0].contains("declared by source `shop`"), "{stranger:?}");
        assert!(
            registry
                .check_event("telegraf", "cron.job.ran", &Object::new())
                .is_empty()
        );
    }

    fn with_rollup() -> Declarations {
        parse(&Files {
            events: "source = \"shop\"\n[events.\"shop.order.placed\"]\ndescription = \"An order\"\nprops.total_cents = \"int\"\n"
                .to_string(),
            metrics: None,
            rollups: Some(
                "[rollups.orders]\ndescription = \"Orders\"\nfrom = \"shop.order.placed\"\nmeasure = [\"count\"]\nevery = \"1m\"\n"
                    .to_string(),
            ),
            alerts: None,
        })
        .unwrap()
    }

    #[test]
    fn a_metric_name_belongs_to_one_source_and_so_does_what_a_rollup_writes() {
        let registry =
            Registry::from_sources([with_rollup(), shop_metrics_only("billing", "billing.invoices")]);
        assert_eq!(registry.metric_owner("shop.orders.count"), Some("shop"));
        assert_eq!(registry.metric_owner("billing.invoices"), Some("billing"));
        assert_eq!(registry.metric_owner("cpu.usage"), None);
        // Sent by anyone else, declared or not, it is marked.
        for sender in ["billing", "telegraf"] {
            let found = registry.check_metric(sender, "shop.orders.count", &BTreeMap::new());
            assert!(found[0].contains("belongs to source `shop`"), "{found:?}");
        }
        assert!(
            registry
                .check_metric("telegraf", "cpu.usage", &BTreeMap::new())
                .is_empty()
        );

        // Publishing a metric a rollup already writes is refused, and so is the reverse.
        let clash = shop_metrics_only("billing", "shop.orders.count");
        let found = registry.conflicts(&clash);
        assert!(
            found[0].message.contains("already belongs to source `shop`"),
            "{found:?}"
        );
        let taken = Registry::from_sources([shop_metrics_only("billing", "shop.orders.count")]);
        let found = taken.conflicts(&with_rollup());
        assert!(found[0].at.contains("rollups.orders"), "{found:?}");
        assert!(found[0].message.contains("shop.orders.count"), "{found:?}");
        // Removing a source frees its names.
        let mut registry = registry;
        registry.remove("shop");
        assert_eq!(registry.metric_owner("shop.orders.count"), None);
        assert_eq!(registry.event_owner("shop.order.placed"), None);
    }

    fn shop_metrics_only(source: &str, metric: &str) -> Declarations {
        parse(&Files {
            events: format!("source = \"{source}\"\n"),
            metrics: Some(format!("[metrics.\"{metric}\"]\ndescription = \"A number\"\n")),
            rollups: None,
            alerts: None,
        })
        .unwrap()
    }
}
