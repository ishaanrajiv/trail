//! The server's configuration, `trail.toml`. Every field has a default, so an empty file (or no
//! file) runs a working server on loopback. `spec/config.md` documents each field.

use crate::problem::Problem;
use crate::time::{Zone, parse_bytes, parse_duration};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// `trail.toml` as written.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
struct RawConfig {
    listen: Option<String>,
    data_dir: Option<PathBuf>,
    timezone: Option<String>,
    hot_days: Option<u32>,
    min_step: Option<String>,
    counters: Vec<String>,
    retention: RawRetention,
    caps: RawCaps,
    limits: RawLimits,
    alerts: RawAlerts,
    auth: RawAuth,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
struct RawRetention {
    samples_5m: Option<String>,
    samples_1h: Option<String>,
    logs: Option<String>,
    errors: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
struct RawCaps {
    events: Option<String>,
    metrics: Option<String>,
    errors: Option<String>,
    logs: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
struct RawLimits {
    body_bytes: Option<usize>,
    batch_items: Option<usize>,
    rollup_groups: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
struct RawAlerts {
    webhook: Option<String>,
    interval: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
struct RawAuth {
    read_token: Option<String>,
    tokens: BTreeMap<String, String>,
}

/// The configuration the server runs with: defaults filled in, text parsed.
#[derive(Debug, Clone)]
pub struct Config {
    /// Address to listen on. Loopback by default: put a reverse proxy in front to reach it from elsewhere.
    pub listen: String,
    /// Where the database and the day files live.
    pub data_dir: PathBuf,
    /// Decides where a day and an hour start.
    pub zone: Zone,
    /// How many finished local days stay in SQLite beside today.
    pub hot_days: u32,
    /// The collection interval: metric queries never use a finer step (ms).
    pub min_step_ms: i64,
    /// Glob patterns of metrics that are running totals, stored as per-second rates.
    pub counters: Vec<String>,
    pub retention: Retention,
    pub caps: Caps,
    pub limits: Limits,
    pub alerts: Alerts,
    pub auth: Auth,
}

/// How long each tier is kept (ms). None keeps forever. Raw events follow their declaration;
/// raw samples are kept until the metrics archive reaches its cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    pub samples_5m: Option<i64>,
    pub samples_1h: Option<i64>,
    pub logs: Option<i64>,
    pub errors: Option<i64>,
}

/// The size (bytes) each store's day files may reach before the oldest are deleted. None: no cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Caps {
    pub events: Option<u64>,
    pub metrics: Option<u64>,
    pub errors: Option<u64>,
    pub logs: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Largest request body accepted.
    pub body_bytes: usize,
    /// Most records in one batch.
    pub batch_items: usize,
    /// Most groups one rollup keeps per bucket; the rest are counted as overflow.
    pub rollup_groups: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Alerts {
    /// Where alert snapshots and notices are posted. None: alerts are evaluated but not sent.
    pub webhook: Option<String>,
    /// How often rules are evaluated (ms).
    pub interval_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Auth {
    /// Required as a bearer token on read endpoints when set.
    pub read_token: Option<String>,
    /// Per-source bearer tokens for writes. Empty: writes need no token.
    pub tokens: BTreeMap<String, String>,
}

impl Config {
    /// The defaults, with data in `data_dir`.
    pub fn default_in(data_dir: &Path) -> Config {
        Config {
            listen: "127.0.0.1:8200".to_string(),
            data_dir: data_dir.to_path_buf(),
            zone: Zone::utc(),
            hot_days: 2,
            min_step_ms: 30_000,
            counters: Vec::new(),
            retention: Retention {
                samples_5m: Some(90 * crate::time::DAY),
                samples_1h: None,
                logs: Some(90 * crate::time::DAY),
                errors: None,
            },
            caps: Caps {
                events: Some(20_000_000_000),
                metrics: Some(2_000_000_000),
                errors: Some(1_000_000_000),
                logs: Some(5_000_000_000),
            },
            limits: Limits {
                body_bytes: 8 * 1024 * 1024,
                batch_items: 10_000,
                rollup_groups: 1000,
            },
            alerts: Alerts {
                webhook: None,
                interval_ms: 30_000,
            },
            auth: Auth::default(),
        }
    }

    /// Reads `trail.toml`. Every problem says which field and how to fix it.
    pub fn load(path: &Path) -> Result<Config, Vec<Problem>> {
        let file = path.display().to_string();
        let text = std::fs::read_to_string(path).map_err(|e| {
            vec![Problem::new(
                &file,
                format!("can't read the config: {e}"),
                "pass --config <path to trail.toml>, or omit it to use the defaults",
            )]
        })?;
        Config::parse(&text, &file)
    }

    /// Parses the text of a `trail.toml`. `file` names it in problems.
    pub fn parse(text: &str, file: &str) -> Result<Config, Vec<Problem>> {
        let raw: RawConfig = toml::from_str(text).map_err(|e| {
            vec![Problem::new(
                file,
                e.to_string().trim_end().to_string(),
                "fix the TOML; spec/config.md lists every field",
            )]
        })?;
        let mut problems = Vec::new();
        let mut config = Config::default_in(Path::new("./trail-data"));
        if let Some(listen) = raw.listen {
            if listen.parse::<std::net::SocketAddr>().is_err() {
                problems.push(Problem::new(
                    format!("{file}: listen"),
                    format!("`{listen}` is not an address"),
                    "write an IP address and a port, like 127.0.0.1:8200",
                ));
            }
            config.listen = listen;
        }
        if let Some(dir) = raw.data_dir {
            config.data_dir = dir;
        }
        if let Some(name) = raw.timezone {
            match Zone::new(&name) {
                Ok(zone) => config.zone = zone,
                Err(message) => problems.push(Problem::new(
                    format!("{file}: timezone"),
                    message,
                    "write an IANA name like UTC or Asia/Colombo",
                )),
            }
        }
        if let Some(days) = raw.hot_days {
            if days == 0 || days > 30 {
                problems.push(Problem::new(
                    format!("{file}: hot_days"),
                    format!("{days} is out of range"),
                    "write 1 to 30; the default is 2",
                ));
            }
            config.hot_days = days;
        }
        let duration =
            |field: &str, text: Option<String>, problems: &mut Vec<Problem>| -> Option<Option<i64>> {
                let text = text?;
                match parse_duration(&text) {
                    Ok(value) => Some(value),
                    Err(message) => {
                        problems.push(Problem::new(
                            format!("{file}: {field}"),
                            message,
                            "write a duration like 30s, 90d or forever",
                        ));
                        None
                    }
                }
            };
        if let Some(step) = duration("min_step", raw.min_step, &mut problems) {
            match step {
                Some(ms) => config.min_step_ms = ms,
                None => problems.push(Problem::new(
                    format!("{file}: min_step"),
                    "needs a length of time",
                    "write a duration like 30s",
                )),
            }
        }
        if let Some(value) = duration("retention.samples_5m", raw.retention.samples_5m, &mut problems) {
            config.retention.samples_5m = value;
        }
        if let Some(value) = duration("retention.samples_1h", raw.retention.samples_1h, &mut problems) {
            config.retention.samples_1h = value;
        }
        if let Some(value) = duration("retention.logs", raw.retention.logs, &mut problems) {
            config.retention.logs = value;
        }
        if let Some(value) = duration("retention.errors", raw.retention.errors, &mut problems) {
            config.retention.errors = value;
        }
        if let Some(interval) = duration("alerts.interval", raw.alerts.interval, &mut problems) {
            match interval {
                Some(ms) => config.alerts.interval_ms = ms,
                None => problems.push(Problem::new(
                    format!("{file}: alerts.interval"),
                    "needs a length of time",
                    "write a duration like 30s",
                )),
            }
        }
        let cap = |field: &str, text: Option<String>, problems: &mut Vec<Problem>| -> Option<Option<u64>> {
            let text = text?;
            if matches!(text.trim().to_ascii_lowercase().as_str(), "none" | "off" | "0") {
                return Some(None);
            }
            match parse_bytes(&text) {
                Ok(bytes) => Some(Some(bytes)),
                Err(message) => {
                    problems.push(Problem::new(
                        format!("{file}: caps.{field}"),
                        message,
                        "write a size like 500MB or 2GB, or none",
                    ));
                    None
                }
            }
        };
        if let Some(value) = cap("events", raw.caps.events, &mut problems) {
            config.caps.events = value;
        }
        if let Some(value) = cap("metrics", raw.caps.metrics, &mut problems) {
            config.caps.metrics = value;
        }
        if let Some(value) = cap("errors", raw.caps.errors, &mut problems) {
            config.caps.errors = value;
        }
        if let Some(value) = cap("logs", raw.caps.logs, &mut problems) {
            config.caps.logs = value;
        }
        if let Some(bytes) = raw.limits.body_bytes {
            config.limits.body_bytes = bytes;
        }
        if let Some(items) = raw.limits.batch_items {
            config.limits.batch_items = items;
        }
        if let Some(groups) = raw.limits.rollup_groups {
            config.limits.rollup_groups = groups;
        }
        for (field, value) in [
            ("body_bytes", config.limits.body_bytes),
            ("batch_items", config.limits.batch_items),
            ("rollup_groups", config.limits.rollup_groups),
        ] {
            if value == 0 {
                problems.push(Problem::new(
                    format!("{file}: limits.{field}"),
                    "must be more than zero",
                    "remove the line to use the default",
                ));
            }
        }
        config.counters = raw.counters;
        if let Some(url) = raw.alerts.webhook.filter(|url| !url.is_empty()) {
            if url.starts_with("https://") {
                problems.push(Problem::new(
                    format!("{file}: alerts.webhook"),
                    "Trail posts alerts over plain HTTP, and this address is https",
                    "point it at a receiver on this machine or network, like http://127.0.0.1:9000/hooks/trail; put a proxy in between if the receiver needs TLS",
                ));
            } else if !url.starts_with("http://") {
                problems.push(Problem::new(
                    format!("{file}: alerts.webhook"),
                    format!("`{url}` is not a URL"),
                    "write the address alerts are posted to, like http://127.0.0.1:9000/hooks/trail",
                ));
            }
            config.alerts.webhook = Some(url);
        }
        config.auth.read_token = raw.auth.read_token.filter(|token| !token.is_empty());
        for (source, token) in raw.auth.tokens {
            if !crate::names::is_source(&source) || token.len() < 16 {
                problems.push(Problem::new(
                    format!("{file}: auth.tokens.{source}"),
                    "a write token needs a valid source name and at least 16 characters",
                    "write `<source> = \"<a long random string>\"`",
                ));
            }
            config.auth.tokens.insert(source, token);
        }
        if problems.is_empty() {
            Ok(config)
        } else {
            Err(problems)
        }
    }

    /// Whether a metric name is a running total, by the `counters` patterns.
    pub fn is_counter(&self, name: &str) -> bool {
        self.counters.iter().any(|pattern| glob_match(pattern, name))
    }
}

/// Matches `*` (any run of characters) and `?` (one character).
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    let (mut p, mut t) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while t < text.len() {
        if p < pattern.len() && (pattern[p] == '?' || pattern[p] == text[t]) {
            p += 1;
            t += 1;
        } else if p < pattern.len() && pattern[p] == '*' {
            star = Some((p, t));
            p += 1;
        } else if let Some((star_p, star_t)) = star {
            p = star_p + 1;
            t = star_t + 1;
            star = Some((star_p, star_t + 1));
        } else {
            return false;
        }
    }
    pattern[p..].iter().all(|c| *c == '*')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_file_is_the_defaults() {
        let config = Config::parse("", "trail.toml").unwrap();
        assert_eq!(config.listen, "127.0.0.1:8200");
        assert_eq!(config.hot_days, 2);
        assert_eq!(config.retention.logs, Some(90 * crate::time::DAY));
        assert_eq!(config.caps.metrics, Some(2_000_000_000));
        assert!(config.alerts.webhook.is_none());
        assert!(config.auth.tokens.is_empty());
    }

    #[test]
    fn reads_every_section() {
        let config = Config::parse(
            r#"
listen = "127.0.0.1:9000"
data_dir = "/var/lib/trail"
timezone = "Asia/Colombo"
hot_days = 3
min_step = "10s"
counters = ["net.bytes_*", "diskio.*_bytes"]

[retention]
samples_5m = "30d"
samples_1h = "forever"
logs = "14d"

[caps]
events = "50GB"
logs = "none"

[limits]
batch_items = 500

[alerts]
webhook = "http://127.0.0.1:8190/alerts"
interval = "1m"

[auth]
read_token = "read-me-please-0123456789"
[auth.tokens]
home = "0123456789abcdef0123"
"#,
            "trail.toml",
        )
        .unwrap();
        assert_eq!(config.zone.name(), "Asia/Colombo");
        assert_eq!(config.min_step_ms, 10_000);
        assert_eq!(config.retention.samples_5m, Some(30 * crate::time::DAY));
        assert_eq!(config.retention.samples_1h, None);
        assert_eq!(config.caps.events, Some(50_000_000_000));
        assert_eq!(config.caps.logs, None);
        assert_eq!(config.limits.batch_items, 500);
        assert_eq!(config.alerts.interval_ms, 60_000);
        assert!(config.is_counter("net.bytes_recv"));
        assert!(config.is_counter("diskio.read_bytes"));
        assert!(!config.is_counter("cpu.usage"));
        assert_eq!(config.auth.tokens["home"], "0123456789abcdef0123");
    }

    #[test]
    fn problems_name_the_field() {
        let problems = Config::parse("timezone = \"Mars/Olympus\"\nhot_days = 0\n[caps]\nlogs = \"big\"\n[auth.tokens]\nhome = \"short\"\n", "trail.toml")
            .unwrap_err();
        let text: Vec<String> = problems.iter().map(|p| p.at.clone()).collect();
        assert_eq!(
            text,
            [
                "trail.toml: timezone",
                "trail.toml: hot_days",
                "trail.toml: caps.logs",
                "trail.toml: auth.tokens.home"
            ]
        );
        let unknown = Config::parse("lisen = \"x\"", "trail.toml").unwrap_err();
        assert!(unknown[0].message.contains("lisen"));
    }

    #[test]
    fn globs() {
        assert!(glob_match("net.bytes_*", "net.bytes_sent"));
        assert!(glob_match("*.io_time", "diskio.io_time"));
        assert!(glob_match("a?c", "abc"));
        assert!(!glob_match("a?c", "ac"));
        assert!(glob_match("*", ""));
        assert!(!glob_match("net.*", "cpu.usage"));
    }
}
