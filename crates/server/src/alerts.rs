//! Alerts: the rules services declare in `alerts.toml`, evaluated on a timer, and the webhook
//! they are posted to. `spec/alerts.md` is the contract.
//!
//! Metric rules are states. Each evaluation works out the complete set of what is firing, and the
//! webhook gets that whole set (a `snapshot`) whenever it changes and every few minutes besides,
//! so a receiver that restarted or missed a post is put right by the next one. Event rules are
//! one-off `notice`s, queued until the webhook takes them.

use crate::{App, lock};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;
use trail_core::Ts;
use trail_core::decl::{AlertRule, Condition};
use trail_core::envelope::Event;
use trail_core::time::{DAY, HOUR, MINUTE};

/// A snapshot is posted again this often even when nothing changed.
pub const RESEND: i64 = 5 * MINUTE;
/// Above and below rules skip a series whose newest reading is older than this.
pub const STALE: i64 = DAY;
/// An absent rule stops firing for a series that has been silent this long: it is gone, not late.
pub const RETIRED: i64 = 7 * DAY;
/// An event older than this raises no notice: a phone sending last week's queue is not news.
pub const NOTICE_AGE: i64 = HOUR;
/// Most notices kept while the webhook is unreachable. The oldest go first.
const MAX_NOTICES: usize = 1000;
const WEBHOOK_TIMEOUT: Duration = Duration::from_secs(5);

/// One series a metric rule is firing for.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Firing {
    /// Stable for as long as it fires: `<source>.<alert>{label=value,…}`.
    pub key: String,
    pub source: String,
    pub alert: String,
    pub severity: String,
    pub title: String,
    pub body: String,
    pub labels: BTreeMap<String, String>,
    /// The value that decided it. None for an `absent` rule.
    pub value: Option<f64>,
    pub since: Ts,
}

/// One event an event rule matched.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Notice {
    /// `<source>.<alert>:<event id>`: the same event never makes two notices.
    pub id: String,
    pub source: String,
    pub alert: String,
    pub severity: String,
    pub title: String,
    pub body: String,
    pub ts: Ts,
    pub event: Value,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Alerts {
    pub firing: BTreeMap<String, Firing>,
    pub notices: VecDeque<Notice>,
    /// The keys in the last snapshot the webhook accepted.
    #[serde(default)]
    posted: Option<Vec<String>>,
    #[serde(default)]
    posted_at: Ts,
    #[serde(skip)]
    pub failures: u64,
}

const STATE_KEY: &str = "alerts";

impl Alerts {
    pub fn load(store: &trail_store::Store) -> Alerts {
        store
            .state(STATE_KEY)
            .ok()
            .flatten()
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default()
    }
}

/// `3`, `3.5`, `0.25`: at most two decimals, fewer for large numbers, no trailing zeros.
pub fn format_value(value: f64) -> String {
    let decimals = if value.abs() >= 100.0 {
        0
    } else if value.abs() >= 10.0 {
        1
    } else {
        2
    };
    let text = format!("{value:.decimals$}");
    if text.contains('.') {
        text.trim_end_matches('0').trim_end_matches('.').to_string()
    } else {
        text
    }
}

/// Replaces each `{name}` with what `lookup` gives. Unknown names are left as written.
fn fill(template: &str, lookup: &dyn Fn(&str) -> Option<String>) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else {
            out.push_str(&rest[open..]);
            return out;
        };
        let name = &after[..close];
        match lookup(name) {
            Some(value) => out.push_str(&value),
            None => {
                out.push('{');
                out.push_str(name);
                out.push('}');
            }
        }
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    out
}

fn key_of(source: &str, alert: &str, labels: &BTreeMap<String, String>) -> String {
    let labels: Vec<String> = labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
    format!("{source}.{alert}{{{}}}", labels.join(","))
}

impl App {
    /// Queues a notice for each event rule this event matches.
    pub(crate) fn notice_event(&self, event: &Event, now: Ts) {
        if event.ts.ms() < now.ms() - NOTICE_AGE {
            return;
        }
        let mut found = Vec::new();
        {
            let registry = self.registry();
            for (source, name, alert) in registry.alerts() {
                let AlertRule::Event {
                    event: wanted,
                    filter,
                } = &alert.rule
                else {
                    continue;
                };
                if wanted != &event.name
                    || !filter
                        .iter()
                        .all(|(prop, want)| event.props.get(prop).is_some_and(|value| want.matches(value)))
                {
                    continue;
                }
                let lookup = |name: &str| -> Option<String> {
                    match event.props.get(name)? {
                        Value::String(text) => Some(text.clone()),
                        other => Some(other.to_string()),
                    }
                };
                found.push(Notice {
                    id: format!("{source}.{name}:{}", event.id),
                    source: source.to_string(),
                    alert: name.to_string(),
                    severity: alert.severity.as_str().to_string(),
                    title: fill(&alert.title, &lookup),
                    body: fill(&alert.body, &lookup),
                    ts: event.ts,
                    event: json!({"id": event.id, "name": event.name, "props": event.props,
                                  "actor": event.actor, "client": event.client, "host": event.host}),
                });
            }
        }
        if found.is_empty() {
            return;
        }
        let mut alerts = lock(&self.alerts);
        for notice in found {
            if alerts.notices.len() >= MAX_NOTICES {
                alerts.notices.pop_front();
            }
            alerts.notices.push_back(notice);
        }
    }

    /// Evaluates every metric rule and returns what is firing now.
    pub fn evaluate_alerts(&self, now: Ts) -> Result<Vec<Firing>, trail_core::Problem> {
        let previous: BTreeMap<String, Firing> = lock(&self.alerts).firing.clone();
        let mut firing: BTreeMap<String, Firing> = BTreeMap::new();
        {
            let registry = self.registry();
            for (source, name, alert) in registry.alerts() {
                let AlertRule::Metric {
                    metric,
                    labels,
                    condition,
                    for_ms,
                    unit: _,
                } = &alert.rule
                else {
                    continue;
                };
                for latest in self.store.latest(metric, labels).map_err(|e| e.problem())? {
                    let key = key_of(source, name, &latest.series.labels);
                    let was = previous.get(&key);
                    let age = now.ms() - latest.ts.ms();
                    let (on, value) = match condition {
                        Condition::Absent { after_ms } => (age > *after_ms && age <= RETIRED, None),
                        Condition::Above { .. } | Condition::Below { .. } if age > STALE => (false, None),
                        Condition::Above { threshold, clear } | Condition::Below { threshold, clear } => {
                            let value = match for_ms {
                                Some(window) => self
                                    .store
                                    .window_avg(latest.series.id, now.minus(*window), now.plus(1))
                                    .map_err(|e| e.problem())?,
                                None => Some(latest.value),
                            };
                            let above = matches!(condition, Condition::Above { .. });
                            // Firing already: it stays on until the value is back at `clear`.
                            let limit = if was.is_some() { *clear } else { *threshold };
                            let on =
                                value.is_some_and(|value| if above { value > limit } else { value < limit });
                            (on, value)
                        }
                    };
                    if !on {
                        continue;
                    }
                    let lookup = |name: &str| -> Option<String> {
                        if name == "value" {
                            value.map(format_value)
                        } else {
                            latest.series.labels.get(name).cloned()
                        }
                    };
                    firing.insert(
                        key.clone(),
                        Firing {
                            key,
                            source: source.to_string(),
                            alert: name.to_string(),
                            severity: alert.severity.as_str().to_string(),
                            title: fill(&alert.title, &lookup),
                            body: fill(&alert.body, &lookup),
                            labels: latest.series.labels.clone(),
                            value,
                            since: was.map_or(now, |f| f.since),
                        },
                    );
                }
            }
        }
        let list: Vec<Firing> = firing.values().cloned().collect();
        lock(&self.alerts).firing = firing;
        Ok(list)
    }

    /// Posts what the webhook has not had yet: the snapshot when it changed or is due again, then
    /// the queued notices in order. Stops at the first failure and tries again next time.
    pub fn deliver_alerts(&self, now: Ts) {
        let (snapshot, notices) = {
            let alerts = lock(&self.alerts);
            let keys: Vec<String> = alerts.firing.keys().cloned().collect();
            let due = alerts.posted.as_ref() != Some(&keys) || now.ms() - alerts.posted_at.ms() >= RESEND;
            let snapshot = due.then(|| {
                (
                    keys,
                    json!({"type": "snapshot", "sent_at": now,
                           "firing": alerts.firing.values().collect::<Vec<_>>()}),
                )
            });
            (snapshot, alerts.notices.iter().cloned().collect::<Vec<_>>())
        };
        let Some(url) = self.config.alerts.webhook.as_deref() else {
            // Nowhere to send: notices are kept for `GET /v1/alerts` up to the limit.
            return;
        };
        if let Some((keys, body)) = snapshot {
            if let Err(message) = post(url, &body) {
                self.webhook_failed(&message);
                return;
            }
            let mut alerts = lock(&self.alerts);
            alerts.posted = Some(keys);
            alerts.posted_at = now;
        }
        for notice in notices {
            let mut body = serde_json::to_value(&notice).unwrap_or(Value::Null);
            if let Value::Object(map) = &mut body {
                map.insert("type".to_string(), json!("notice"));
            }
            if let Err(message) = post(url, &body) {
                self.webhook_failed(&message);
                return;
            }
            let mut alerts = lock(&self.alerts);
            if alerts.notices.front().is_some_and(|front| front.id == notice.id) {
                alerts.notices.pop_front();
            }
        }
    }

    fn webhook_failed(&self, message: &str) {
        let first = {
            let mut alerts = lock(&self.alerts);
            alerts.failures += 1;
            alerts.failures == 1
        };
        self.meters.add("trail.webhook.failures", &[], 1.0);
        if first {
            self.log(
                trail_core::envelope::Level::Warn,
                &format!("the alert webhook is not taking posts: {message}"),
            );
        }
    }

    /// One alert pass: evaluate, deliver, and save the state so a restart remembers what was
    /// firing and what is still queued.
    pub fn tick_alerts(&self, now: Ts) {
        if let Err(problem) = self.evaluate_alerts(now) {
            self.log(
                trail_core::envelope::Level::Error,
                &format!("alert rules could not be evaluated: {problem}"),
            );
        }
        self.deliver_alerts(now);
        let text = serde_json::to_string(&*lock(&self.alerts)).unwrap_or_default();
        let unchanged = lock(&self.alerts_saved).as_str() == text;
        if !unchanged && self.store.set_state(STATE_KEY, &text).is_ok() {
            *lock(&self.alerts_saved) = text;
        }
    }
}

/// Posts JSON to the webhook. Any 2xx is success.
fn post(url: &str, body: &Value) -> Result<(), String> {
    let config = ureq::Agent::config_builder()
        .timeout_global(Some(WEBHOOK_TIMEOUT))
        .http_status_as_error(false)
        .build();
    let agent = ureq::Agent::new_with_config(config);
    let response = agent
        .post(url)
        .header("Content-Type", "application/json")
        .send(body.to_string())
        .map_err(|e| e.to_string())?;
    let status = response.status().as_u16();
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(format!("it answered {status}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_read_naturally() {
        assert_eq!(format_value(3.0), "3");
        assert_eq!(format_value(3.456), "3.46");
        assert_eq!(format_value(42.26), "42.3");
        assert_eq!(format_value(1234.6), "1235");
        assert_eq!(format_value(0.5), "0.5");
    }

    #[test]
    fn placeholders_fill() {
        let lookup = |name: &str| (name == "queue").then(|| "email".to_string());
        assert_eq!(fill("The {queue} queue", &lookup), "The email queue");
        assert_eq!(fill("{other} stays, {queue", &lookup), "{other} stays, {queue");
    }
}
