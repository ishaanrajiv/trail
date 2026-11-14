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
/// An event that was this much older than its arrival when it arrived raises no notice: a phone
/// sending last week's queue is not news.
pub const NOTICE_AGE: i64 = HOUR;
/// Most notices queued for the webhook. While it is reachable but behind, Trail stops reading
/// events at this many and carries on as it drains; with no webhook, the oldest go first.
pub const MAX_NOTICES: usize = 1000;
/// Events read from the store at a time when looking for ones event rules match.
const NOTICE_PAGE: usize = 1000;
const WEBHOOK_TIMEOUT: Duration = Duration::from_secs(5);

/// One series a metric rule is firing for.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Firing {
    /// Stable for as long as it fires: `<source>.<alert>{label=value,…}`.
    pub key: String,
    pub source: String,
    pub alert: String,
    pub severity: String,
    /// The rule's `kind`, when it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
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
    /// The rule's `kind`, when it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub title: String,
    pub body: String,
    pub ts: Ts,
    /// `service`: the service of the rule's source.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    pub event: Value,
}

/// A series a rule with `for` would fire for, but whose readings do not yet cover the window.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Waiting {
    pub key: String,
    pub source: String,
    pub alert: String,
    pub labels: BTreeMap<String, String>,
    /// The average over the readings the window has so far.
    pub value: Option<f64>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Alerts {
    pub firing: BTreeMap<String, Firing>,
    /// Series held back from firing because their readings do not yet cover the `for` window.
    /// Worked out again at every evaluation, so never saved.
    #[serde(skip)]
    pub waiting: Vec<Waiting>,
    pub notices: VecDeque<Notice>,
    /// The keys in the last snapshot the webhook accepted.
    #[serde(default)]
    posted: Option<Vec<String>>,
    #[serde(default)]
    posted_at: Ts,
    /// The ids of notices the webhook has taken, newest last, so an event seen again at a
    /// restart is not sent twice.
    #[serde(default)]
    sent: VecDeque<String>,
    /// The seq of the last event the event rules have looked at. Saved with the queue, so each
    /// event is looked at once, and a restart carries on after it. None in a state saved before
    /// Trail kept it: [`App::open`] starts it at the last seq stored.
    #[serde(default)]
    pub noticed_seq: Option<i64>,
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

/// The end of the stretch a reading must fall in for a `for` window ending `now` to count as
/// covered: the window's first fifth, or the `for` before it (which starts at `now - 2 * window`).
fn covered_by(now: Ts, window: i64) -> Ts {
    now.minus(window - window / 5)
}

/// `labels` with the service of the rule's source added as `service`, unless they have one
/// already (a metric labelled `service=…` keeps its own).
fn service_label(
    registry: &trail_core::registry::Registry,
    source: &str,
    mut labels: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    if !labels.contains_key("service") {
        let service = registry.get(source).map_or(source, |decl| decl.service());
        labels.insert("service".to_string(), service.to_string());
    }
    labels
}

fn key_of(source: &str, alert: &str, labels: &BTreeMap<String, String>) -> String {
    let labels: Vec<String> = labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
    format!("{source}.{alert}{{{}}}", labels.join(","))
}

impl App {
    /// The notices the event rules make of one event. None for an event that was more than
    /// [`NOTICE_AGE`] old when it arrived: how late it arrived is what counts, not how long ago
    /// that was, so an event stored while the webhook was down still gets its notice.
    fn notices_for(&self, event: &Event) -> Vec<Notice> {
        if event.received_at.ms() - event.ts.ms() > NOTICE_AGE {
            return Vec::new();
        }
        let mut found = Vec::new();
        let registry = self.registry();
        // An event sent under a name another source owns is marked, and alerts no one.
        if registry
            .event_owner(&event.name)
            .is_some_and(|owner| owner != event.source)
        {
            return found;
        }
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
                kind: alert.kind.clone(),
                title: fill(&alert.title, &lookup),
                body: fill(&alert.body, &lookup),
                ts: event.ts,
                labels: service_label(&registry, source, BTreeMap::new()),
                event: json!({"id": event.id, "name": event.name, "props": event.props,
                              "actor": event.actor, "client": event.client, "host": event.host}),
            });
        }
        found
    }

    /// Looks at the events stored since the last one looked at, in the order they were stored,
    /// and queues a notice for each event rule one matches. With a webhook, stops while the queue
    /// is full: the events wait in the store, and are read once the webhook has taken some.
    pub(crate) fn notice_new_events(&self) -> Result<(), trail_core::Problem> {
        let has_rules = self
            .registry()
            .alerts()
            .any(|(_, _, alert)| matches!(alert.rule, AlertRule::Event { .. }));
        if !has_rules {
            // Nothing to match: what is stored now is looked at, and passed.
            let last = self.store.last_seq().map_err(|e| e.problem())?;
            let mut alerts = lock(&self.alerts);
            alerts.noticed_seq = Some(alerts.noticed_seq.unwrap_or(0).max(last));
            return Ok(());
        }
        let holds = self.config.alerts.webhook.is_some();
        loop {
            let after = {
                let alerts = lock(&self.alerts);
                if holds && alerts.notices.len() >= MAX_NOTICES {
                    return Ok(());
                }
                alerts.noticed_seq.unwrap_or(0)
            };
            let tail = self
                .store
                .events_tail(&trail_store::EventFilter {
                    after_seq: Some(after),
                    limit: NOTICE_PAGE,
                    ..trail_store::EventFilter::default()
                })
                .map_err(|e| e.problem())?;
            if tail.next_seq <= after {
                return Ok(());
            }
            let made: Vec<(Option<i64>, Vec<Notice>)> = tail
                .rows
                .iter()
                .map(|event| (event.seq, self.notices_for(event)))
                .collect();
            let mut alerts = lock(&self.alerts);
            for (seq, notices) in made {
                if holds && alerts.notices.len() >= MAX_NOTICES {
                    return Ok(());
                }
                for notice in notices {
                    // The same event never makes two notices, however often it is looked at.
                    if alerts.sent.contains(&notice.id)
                        || alerts.notices.iter().any(|queued| queued.id == notice.id)
                    {
                        continue;
                    }
                    if !holds && alerts.notices.len() >= MAX_NOTICES {
                        alerts.notices.pop_front();
                    }
                    alerts.notices.push_back(notice);
                }
                if let Some(seq) = seq {
                    alerts.noticed_seq = Some(seq);
                }
            }
            // Past rows the page skipped (a copy the archive is about to drop) too.
            alerts.noticed_seq = Some(alerts.noticed_seq.unwrap_or(0).max(tail.next_seq));
        }
    }

    /// Evaluates every metric rule and returns what is firing now.
    pub fn evaluate_alerts(&self, now: Ts) -> Result<Vec<Firing>, trail_core::Problem> {
        let previous: BTreeMap<String, Firing> = lock(&self.alerts).firing.clone();
        let mut firing: BTreeMap<String, Firing> = BTreeMap::new();
        let mut waiting: Vec<Waiting> = Vec::new();
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
                // Only the series of the source that owns the name, when one declares it: another
                // source's readings under that name neither fire nor clear the owner's rule.
                let owner = registry.metric_owner(metric);
                for latest in self
                    .store
                    .latest(metric, labels, owner)
                    .map_err(|e| e.problem())?
                {
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
                            let past =
                                value.is_some_and(|value| if above { value > limit } else { value < limit });
                            // A rule with `for` starts firing only once the series was already
                            // reporting when the window began: otherwise its first reading alone
                            // would be "the average over the window". Clearing is never held back.
                            let held = match for_ms {
                                Some(window) if past && was.is_none() => !self
                                    .store
                                    .has_reading(
                                        latest.series.id,
                                        now.minus(2 * window),
                                        covered_by(now, *window),
                                    )
                                    .map_err(|e| e.problem())?,
                                _ => false,
                            };
                            if held {
                                waiting.push(Waiting {
                                    key: key.clone(),
                                    source: source.to_string(),
                                    alert: name.to_string(),
                                    labels: latest.series.labels.clone(),
                                    value,
                                });
                            }
                            (past && !held, value)
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
                            kind: alert.kind.clone(),
                            title: fill(&alert.title, &lookup),
                            body: fill(&alert.body, &lookup),
                            labels: service_label(&registry, source, latest.series.labels.clone()),
                            value,
                            since: was.map_or(now, |f| f.since),
                        },
                    );
                }
            }
        }
        let list: Vec<Firing> = firing.values().cloned().collect();
        let mut alerts = lock(&self.alerts);
        alerts.firing = firing;
        alerts.waiting = waiting;
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
            match post(url, &body) {
                Ok(()) => {
                    let mut alerts = lock(&self.alerts);
                    alerts.posted = Some(keys);
                    alerts.posted_at = now;
                }
                // The receiver is there and said no to this body: the notices still get their go.
                Err(Refused::ForGood(message)) => self.webhook_failed(&message),
                Err(Refused::ForNow(message)) => {
                    self.webhook_failed(&message);
                    return;
                }
            }
        }
        for notice in notices {
            let mut body = serde_json::to_value(&notice).unwrap_or(Value::Null);
            if let Value::Object(map) = &mut body {
                map.insert("type".to_string(), json!("notice"));
            }
            match post(url, &body) {
                Ok(()) => {}
                // Sending it again would get the same answer, and it would hold back every
                // notice behind it: it is dropped, and the failure is counted and logged.
                Err(Refused::ForGood(message)) => {
                    self.webhook_failed(&message);
                    self.log(
                        trail_core::envelope::Level::Warn,
                        &format!("notice `{}` was dropped: the alert webhook {message}", notice.id),
                    );
                }
                Err(Refused::ForNow(message)) => {
                    self.webhook_failed(&message);
                    return;
                }
            }
            let mut alerts = lock(&self.alerts);
            if alerts.notices.front().is_some_and(|front| front.id == notice.id) {
                alerts.notices.pop_front();
            }
            if alerts.sent.len() >= MAX_NOTICES {
                alerts.sent.pop_front();
            }
            alerts.sent.push_back(notice.id);
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

    /// One alert pass: evaluate, queue notices for the events stored since the last pass, save,
    /// deliver, and save again, so a restart remembers what was firing, what is still queued and
    /// which events have been looked at.
    pub fn tick_alerts(&self, now: Ts) {
        if let Err(problem) = self.evaluate_alerts(now) {
            self.log(
                trail_core::envelope::Level::Error,
                &format!("alert rules could not be evaluated: {problem}"),
            );
        }
        if let Err(problem) = self.notice_new_events() {
            self.log(
                trail_core::envelope::Level::Error,
                &format!("events could not be matched against the event rules: {problem}"),
            );
        }
        // The queue and the mark are saved together before anything is posted: a stop from
        // here on loses no notice, and repeats at most the ones posted but not yet saved.
        self.save_alerts();
        self.deliver_alerts(now);
        self.save_alerts();
    }

    /// Writes the alert state to the store when it differs from what was last written.
    pub(crate) fn save_alerts(&self) {
        let text = serde_json::to_string(&*lock(&self.alerts)).unwrap_or_default();
        let unchanged = lock(&self.alerts_saved).as_str() == text;
        if !unchanged && self.store.set_state(STATE_KEY, &text).is_ok() {
            *lock(&self.alerts_saved) = text;
        }
    }
}

/// Why a post to the webhook did not succeed.
enum Refused {
    /// No answer, a `5xx`, a 408 or a 429: the same post can succeed later.
    ForNow(String),
    /// Any other status: the receiver does not want this body, now or later.
    ForGood(String),
}

/// Posts JSON to the webhook. Any 2xx is success.
fn post(url: &str, body: &Value) -> Result<(), Refused> {
    let config = ureq::Agent::config_builder()
        .timeout_global(Some(WEBHOOK_TIMEOUT))
        .http_status_as_error(false)
        .build();
    let agent = ureq::Agent::new_with_config(config);
    let response = agent
        .post(url)
        .header("Content-Type", "application/json")
        .send(body.to_string())
        .map_err(|e| Refused::ForNow(e.to_string()))?;
    let status = response.status().as_u16();
    match status {
        200..300 => Ok(()),
        408 | 429 | 500.. => Err(Refused::ForNow(format!("it answered {status}"))),
        _ => Err(Refused::ForGood(format!("answered {status}"))),
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
