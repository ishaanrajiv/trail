//! `trail collect`: long-running collectors that turn logs something else wrote into Trail logs.
//!
//! - `journald` follows the systemd journal (which also holds container output when Docker logs
//!   to journald), one source per unit or container. A line at error priority is also sent as an
//!   error, and a kernel out-of-memory kill or a failed unit as an event under the source
//!   `journal`.
//! - `jsonl` follows a file of JSON lines.
//!
//! Neither keeps a queue of its own. The journal and the file are the durable buffer: a
//! collector remembers how far it has sent (a cursor, an offset) and only moves that on after
//! Trail has answered, so Trail being down just means it catches up later. Record ids are made
//! from the position, so sending a stretch twice stores it once.

use crate::args::Args;
use crate::send::Http;
use crate::{Done, complain};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;
use trail_core::Problem;
use trail_core::names::is_source;
use trail_core::time::parse_rfc3339;

/// Lines sent in one request at most.
const BATCH: usize = 500;
/// Bytes of a followed file read at a time.
const CHUNK: usize = 8 << 20;

pub fn collect(mut raw: Vec<String>) -> Done {
    let kind = if raw.is_empty() {
        String::new()
    } else {
        raw.remove(0)
    };
    match kind.as_str() {
        "journald" => journald(raw),
        "jsonl" => jsonl(raw),
        other => Err(vec![Problem::new(
            "trail collect",
            if other.is_empty() {
                "collect what?".to_string()
            } else {
                format!("`{other}` is not a collector")
            },
            "write `trail collect journald` or `trail collect jsonl <file> --source <name>`",
        )]),
    }
}

/// Where collectors keep their place: systemd's state folder for the unit, else the user's.
fn state_dir() -> PathBuf {
    let from = |name: &str| {
        std::env::var(name)
            .ok()
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
    };
    from("STATE_DIRECTORY")
        .or_else(|| from("XDG_STATE_HOME").map(|dir| dir.join("trail")))
        .or_else(|| from("HOME").map(|dir| dir.join(".local/state/trail")))
        .unwrap_or_else(|| PathBuf::from("."))
}

fn save(path: &Path, text: &str) {
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let temp = path.with_extension("tmp");
    if std::fs::write(&temp, text).is_ok() {
        let _ = std::fs::rename(&temp, path);
    }
}

/// FNV-1a, for ids and state file names. Two seeds give 128 bits for an id.
fn fnv(seed: u64, text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325 ^ seed, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

fn id_for(position: &str) -> String {
    format!(
        "c{:016x}{:016x}",
        fnv(0, position),
        fnv(0x9e37_79b9_7f4a_7c15, position)
    )
}

/// A name made into a valid source: lowercase, other characters to hyphens.
pub(crate) fn source_name(text: &str) -> String {
    let mut name = String::new();
    for c in text.trim().chars() {
        let c = c.to_ascii_lowercase();
        if c.is_ascii_lowercase() || c.is_ascii_digit() {
            name.push(c);
        } else if !name.ends_with('-') && !name.is_empty() {
            name.push('-');
        }
    }
    let name = name.trim_end_matches('-');
    if name.is_empty() {
        return "journal".to_string();
    }
    let name: String = if name.starts_with(|c: char| c.is_ascii_lowercase()) {
        name.to_string()
    } else {
        format!("x-{name}")
    };
    let name: String = name.chars().take(64).collect();
    if is_source(&name) {
        name
    } else {
        "journal".to_string()
    }
}

/// What other loggers call the levels.
pub(crate) fn level_name(value: &Value) -> &'static str {
    if let Some(n) = value.as_i64() {
        // pino and bunyan: 10 trace … 60 fatal.
        return match n {
            i64::MIN..=14 => "trace",
            15..=24 => "debug",
            25..=34 => "info",
            35..=44 => "warn",
            45..=54 => "error",
            _ => "fatal",
        };
    }
    match value.as_str().unwrap_or("").to_ascii_lowercase().as_str() {
        "trace" => "trace",
        "debug" => "debug",
        "warn" | "warning" => "warn",
        "error" | "err" => "error",
        "fatal" | "critical" | "crit" | "panic" | "emerg" | "alert" => "fatal",
        _ => "info",
    }
}

/// A time other loggers wrote: RFC 3339 text, or a number of seconds, ms, µs or ns.
pub(crate) fn time_of(value: &Value) -> Option<i64> {
    let number = match value {
        Value::Number(n) => n.as_f64()?,
        Value::String(text) => match parse_rfc3339(text) {
            Some(ts) => return Some(ts.ms()),
            None => text.trim().parse::<f64>().ok()?,
        },
        _ => return None,
    };
    #[allow(clippy::cast_possible_truncation)]
    let ms = if number < 1.0e11 {
        number * 1000.0
    } else if number < 1.0e14 {
        number
    } else if number < 1.0e17 {
        number / 1000.0
    } else {
        number / 1.0e6
    } as i64;
    Some(ms)
}

/// Records waiting to go, by source.
struct Shipper {
    http: Http,
    host: Option<String>,
    pending: BTreeMap<(String, &'static str), Vec<Value>>,
    count: usize,
}

impl Shipper {
    fn push(&mut self, source: String, record: Value) {
        self.put(source, "logs", record);
    }

    /// Queues a record under its key in the ingest body: `logs`, `errors` or `events`.
    fn put(&mut self, source: String, kind: &'static str, record: Value) {
        self.pending.entry((source, kind)).or_default().push(record);
        self.count += 1;
    }

    /// Sends everything, waiting out a Trail that is down. Returns once it has all been answered.
    fn ship(&mut self) {
        let mut wait = Duration::from_secs(1);
        let mut told = false;
        while let Some(((source, kind), records)) = self.pending.pop_first() {
            let mut body = Map::new();
            body.insert("source".to_string(), json!(source));
            if let Some(host) = &self.host {
                body.insert("host".to_string(), json!(host));
            }
            body.insert(kind.to_string(), Value::Array(records.clone()));
            match self
                .http
                .attempt("POST", "/v1/ingest", Some(&Value::Object(body).to_string()))
            {
                Ok((200, _)) => {
                    wait = Duration::from_secs(1);
                }
                // It can never succeed. Say so and move on rather than stall on it forever.
                Ok((status @ (400 | 401 | 413), reply)) => {
                    for problem in crate::send::problems_in(&reply, status) {
                        complain(&Problem::new(
                            format!("{} {kind} from `{source}` dropped: {}", records.len(), problem.at),
                            problem.message,
                            problem.fix,
                        ));
                    }
                }
                Ok((_, _)) | Err(_) => {
                    if !told {
                        eprintln!(
                            "trail collect: no answer from {}; waiting, nothing is lost",
                            self.http.url()
                        );
                        told = true;
                    }
                    self.pending.insert((source, kind), records);
                    std::thread::sleep(wait);
                    wait = (wait * 2).min(Duration::from_secs(60));
                }
            }
        }
        self.count = 0;
    }
}

fn shipper(args: &Args) -> Shipper {
    Shipper {
        http: Http::new(args),
        host: args.value("host").map(str::to_string),
        pending: BTreeMap::new(),
        count: 0,
    }
}

// ---------- journald ----------

/// A journal field as text. The journal writes non-UTF-8 values as lists of bytes.
fn field(entry: &Value, name: &str) -> Option<String> {
    match entry.get(name)? {
        Value::String(text) => Some(text.clone()),
        Value::Array(bytes) => {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let bytes: Vec<u8> = bytes.iter().filter_map(Value::as_u64).map(|b| b as u8).collect();
            Some(String::from_utf8_lossy(&bytes).into_owned())
        }
        _ => None,
    }
}

/// The source the events below are sent under, whatever unit they are about.
pub(crate) const JOURNAL: &str = "journal";
/// systemd's message id for "unit failed with result …".
const UNIT_FAILED: &str = "d9b373ed55a64feb8242e02dbe79a49c";

/// What one journal entry becomes.
pub(crate) struct Collected {
    /// The source of the log line: `--source`, else the container, unit or program.
    pub source: String,
    pub log: Value,
    /// The same line as an error record, when its priority is error or worse.
    pub error: Option<Value>,
    /// `journal.oom.killed` or `journal.unit.failed`, sent under the source `journal`.
    pub event: Option<Value>,
    pub cursor: Option<String>,
}

/// The pid and name in the kernel's "Out of memory: Killed process 812 (shop-api) …".
fn oom_victim(message: &str) -> Option<(i64, String)> {
    if !message.to_ascii_lowercase().contains("out of memory") {
        return None;
    }
    let rest = message.split_once("Killed process ")?.1;
    let (pid, rest) = rest.split_once(' ')?;
    let name = rest.strip_prefix('(')?.split_once(')')?.0;
    Some((pid.parse().ok()?, name.to_string()))
}

/// One journal entry as a log record, and what else it tells. None for an entry with no message.
pub(crate) fn from_journal(entry: &Value, fixed_source: Option<&str>) -> Option<Collected> {
    let message = field(entry, "MESSAGE").filter(|m| !m.trim().is_empty())?;
    let cursor = field(entry, "__CURSOR");
    let container = field(entry, "CONTAINER_NAME");
    let unit = field(entry, "_SYSTEMD_USER_UNIT").or_else(|| field(entry, "_SYSTEMD_UNIT"));
    let identifier = field(entry, "SYSLOG_IDENTIFIER");
    let trimmed = unit.as_deref().map(|unit| unit.trim_end_matches(".service"));
    // A container's unit is docker.service; its own name says more.
    let named = container
        .as_deref()
        .or(trimmed)
        .or(identifier.as_deref())
        .unwrap_or("journal");
    let source = fixed_source.map_or_else(|| source_name(named), str::to_string);
    let priority = field(entry, "PRIORITY").and_then(|p| p.parse::<u8>().ok());
    let level = match priority {
        Some(0..=2) => "fatal",
        Some(3) => "error",
        Some(4) => "warn",
        Some(7) => "debug",
        _ => "info",
    };
    let ts = field(entry, "__REALTIME_TIMESTAMP")
        .and_then(|micros| micros.parse::<i64>().ok())
        .map(|micros| micros / 1000);
    let mut fields = Map::new();
    for (key, name) in [
        ("unit", "_SYSTEMD_UNIT"),
        ("user_unit", "_SYSTEMD_USER_UNIT"),
        ("pid", "_PID"),
        ("identifier", "SYSLOG_IDENTIFIER"),
    ] {
        if let Some(value) = field(entry, name) {
            fields.insert(key.to_string(), json!(value));
        }
    }
    let position = cursor
        .clone()
        .unwrap_or_else(|| format!("{source}:{ts:?}:{message}"));
    // Every record made from an entry gets an id from the entry's position, so a stretch of
    // the journal sent twice is stored once.
    let stamped = |suffix: &str| {
        let mut record = Map::new();
        record.insert("id".to_string(), json!(id_for(&format!("{position}{suffix}"))));
        if let Some(ts) = ts {
            record.insert("ts".to_string(), json!(ts));
        }
        record
    };

    let error = matches!(priority, Some(0..=3)).then(|| {
        let mut record = stamped("#error");
        record.insert("kind".to_string(), json!(identifier.as_deref().unwrap_or(named)));
        record.insert("message".to_string(), json!(message));
        record.insert("severity".to_string(), json!(level));
        record.insert("context".to_string(), Value::Object(fields.clone()));
        Value::Object(record)
    });

    let event = if field(entry, "_TRANSPORT").as_deref() == Some("kernel")
        && let Some((pid, process)) = oom_victim(&message)
    {
        Some(("journal.oom.killed", json!({"process": process, "pid": pid})))
    } else if field(entry, "MESSAGE_ID").as_deref() == Some(UNIT_FAILED)
        && let Some(failed) = field(entry, "UNIT").or_else(|| field(entry, "USER_UNIT"))
    {
        let result = field(entry, "UNIT_RESULT").unwrap_or_else(|| "unknown".to_string());
        Some(("journal.unit.failed", json!({"unit": failed, "result": result})))
    } else {
        None
    }
    .map(|(name, props)| {
        let mut record = stamped("#event");
        record.insert("name".to_string(), json!(name));
        record.insert("props".to_string(), props);
        Value::Object(record)
    });

    let mut log = stamped("");
    log.insert("level".to_string(), json!(level));
    log.insert("stream".to_string(), json!(named));
    log.insert("message".to_string(), json!(message));
    log.insert("fields".to_string(), Value::Object(fields));
    Some(Collected {
        source,
        log: Value::Object(log),
        error,
        event,
        cursor,
    })
}

/// `trail collect journald`
fn journald(raw: Vec<String>) -> Done {
    let args = Args::parse(
        "collect journald",
        raw,
        &["url", "token", "source", "unit", "state", "host"],
        &["user", "once", "logs-only"],
    )?;
    if let Some(source) = args.value("source")
        && !is_source(source)
    {
        return Err(vec![Problem::new(
            "trail collect journald",
            format!("`{source}` is not a source name"),
            "use lowercase letters, digits and hyphens, or leave --source out to name each unit's lines after the unit",
        )]);
    }
    let state = args
        .value("state")
        .map_or_else(|| state_dir().join("journald.cursor"), PathBuf::from);
    let mut cursor = std::fs::read_to_string(&state)
        .ok()
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty());
    let mut command = Command::new("journalctl");
    command.args(["-o", "json", "--no-pager"]);
    if !args.flag("once") {
        command.arg("-f");
    }
    if args.flag("user") {
        command.arg("--user");
    }
    for unit in args.values("unit") {
        command.args(["-u", unit]);
    }
    match &cursor {
        Some(cursor) => command.arg(format!("--after-cursor={cursor}")),
        // The first run starts from now: the journal's history is not replayed.
        None => command.args(["-n", "0"]),
    };
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| {
            vec![Problem::new(
                "trail collect journald",
                format!("can't run journalctl: {e}"),
                "this collector needs systemd's journalctl on PATH",
            )]
        })?;
    let Some(stdout) = child.stdout.take() else {
        return Err(vec![Problem::new(
            "trail collect journald",
            "journalctl gave no output pipe",
            "",
        )]);
    };
    let (sender, lines) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if sender.send(line).is_err() {
                break;
            }
        }
    });
    let mut shipper = shipper(&args);
    loop {
        let line = lines.recv_timeout(Duration::from_secs(1));
        let ended = matches!(line, Err(mpsc::RecvTimeoutError::Disconnected));
        if let Ok(line) = &line
            && let Ok(entry) = serde_json::from_str::<Value>(line)
            && let Some(found) = from_journal(&entry, args.value("source"))
        {
            if shipper.host.is_none() {
                shipper.host = field(&entry, "_HOSTNAME");
            }
            if !args.flag("logs-only") {
                if let Some(error) = found.error {
                    shipper.put(found.source.clone(), "errors", error);
                }
                if let Some(event) = found.event {
                    shipper.put(JOURNAL.to_string(), "events", event);
                }
            }
            shipper.push(found.source, found.log);
            cursor = found.cursor.or(cursor);
        }
        if shipper.count >= BATCH || (line.is_err() && shipper.count > 0) {
            shipper.ship();
            if let Some(cursor) = &cursor {
                save(&state, cursor);
            }
        }
        if ended {
            break;
        }
    }
    let _ = child.wait();
    if args.flag("once") {
        Ok(())
    } else {
        Err(vec![Problem::new(
            "trail collect journald",
            "journalctl stopped",
            "restart the collector; it carries on from where it had sent to",
        )])
    }
}

// ---------- jsonl ----------

/// One line of a JSON-lines file as a log record. A line that is not a JSON object is the message.
pub(crate) fn from_line(line: &str, position: &str, stream: Option<&str>) -> Value {
    let mut record = Map::new();
    record.insert("id".to_string(), json!(id_for(position)));
    if let Some(stream) = stream {
        record.insert("stream".to_string(), json!(stream));
    }
    let Ok(Value::Object(mut object)) = serde_json::from_str::<Value>(line) else {
        record.insert("message".to_string(), json!(line));
        return Value::Object(record);
    };
    let mut take = |names: &[&str]| names.iter().find_map(|name| object.remove(*name));
    let message = take(&["message", "msg", "event"]);
    let level = take(&["level", "severity", "lvl"]);
    let ts = take(&["ts", "time", "timestamp", "@timestamp"]);
    let message = match message {
        Some(Value::String(text)) => text,
        Some(other) => other.to_string(),
        None => line.to_string(),
    };
    record.insert("message".to_string(), json!(message));
    if let Some(level) = &level {
        record.insert("level".to_string(), json!(level_name(level)));
    }
    if let Some(ms) = ts.as_ref().and_then(time_of) {
        record.insert("ts".to_string(), json!(ms));
    }
    record.insert("fields".to_string(), Value::Object(object));
    Value::Object(record)
}

#[cfg(unix)]
fn inode(meta: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    meta.ino()
}

#[cfg(not(unix))]
fn inode(_: &std::fs::Metadata) -> u64 {
    0
}

/// `trail collect jsonl <file> --source NAME`
fn jsonl(raw: Vec<String>) -> Done {
    let args = Args::parse(
        "collect jsonl",
        raw,
        &["url", "token", "source", "stream", "state", "host"],
        &["once", "from-start"],
    )?;
    args.need("collect jsonl", &["file"])?;
    let file = PathBuf::from(&args.positional[0]);
    let source = args.value("source").filter(|s| is_source(s)).ok_or_else(|| {
        vec![Problem::new(
            "trail collect jsonl",
            "which source are these lines from?",
            "add `--source <name>`: lowercase letters, digits and hyphens",
        )]
    })?;
    let name = file.display().to_string();
    let state = args.value("state").map_or_else(
        || state_dir().join(format!("jsonl-{:016x}.offset", fnv(0, &name))),
        PathBuf::from,
    );
    // The state is "<inode> <offset>": a different inode means the file was rotated.
    let saved: Option<(u64, u64)> = std::fs::read_to_string(&state).ok().and_then(|text| {
        let (ino, offset) = text.trim().split_once(' ')?;
        Some((ino.parse().ok()?, offset.parse().ok()?))
    });
    let mut place = saved;
    let mut shipper = shipper(&args);
    loop {
        if let Ok(mut handle) = std::fs::File::open(&file)
            && let Ok(meta) = handle.metadata()
        {
            let ino = inode(&meta);
            let mut offset = match place {
                Some((was, offset)) if was == ino && offset <= meta.len() => offset,
                // Rotated or truncated: the new file is read from its start.
                Some(_) => 0,
                // First run: only what is written from now on, unless asked for the whole file.
                None if args.flag("from-start") || args.flag("once") => 0,
                None => meta.len(),
            };
            let mut chunk = Vec::new();
            if handle.seek(SeekFrom::Start(offset)).is_ok()
                && handle.take(CHUNK as u64).read_to_end(&mut chunk).is_ok()
            {
                // Only whole lines: the writer may be half-way through the last one.
                // A chunk with no line end at all is one over-long line: it goes as it is.
                let whole = chunk
                    .iter()
                    .rposition(|byte| *byte == b'\n')
                    .map_or(if chunk.len() >= CHUNK { chunk.len() } else { 0 }, |last| {
                        last + 1
                    });
                let before = offset;
                for line in chunk[..whole].split_inclusive(|byte| *byte == b'\n') {
                    let text = String::from_utf8_lossy(line);
                    let text = text.trim_end_matches(['\n', '\r']);
                    if !text.trim().is_empty() {
                        let position = format!("{name}:{ino}:{offset}");
                        shipper.push(
                            source.to_string(),
                            from_line(text, &position, args.value("stream")),
                        );
                    }
                    offset += line.len() as u64;
                    if shipper.count >= BATCH {
                        shipper.ship();
                        save(&state, &format!("{ino} {offset}"));
                    }
                }
                shipper.ship();
                save(&state, &format!("{ino} {offset}"));
                place = Some((ino, offset));
                // More whole lines may be waiting; a half-written last line is left for later.
                if offset > before && offset < meta.len() {
                    continue;
                }
            }
            place = place.or(Some((ino, offset)));
        } else if args.flag("once") {
            return Err(vec![Problem::new(
                name,
                "can't read this file",
                "check the path and that it is readable",
            )]);
        }
        if args.flag("once") {
            return Ok(());
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_entries_become_logs_named_after_their_unit() {
        let entry = json!({
            "MESSAGE": "listening on :8080", "PRIORITY": "6", "_SYSTEMD_UNIT": "Shop_API.service",
            "_PID": "812", "__REALTIME_TIMESTAMP": "1794312000123456", "__CURSOR": "s=abc;i=1",
            "_HOSTNAME": "web-1",
        });
        let found = from_journal(&entry, None).unwrap();
        assert!(found.error.is_none() && found.event.is_none());
        let (source, record, cursor) = (found.source, found.log, found.cursor);
        assert_eq!(source, "shop-api");
        assert_eq!(cursor.as_deref(), Some("s=abc;i=1"));
        assert_eq!(record["ts"], 1_794_312_000_123i64);
        assert_eq!(record["level"], "info");
        assert_eq!(record["stream"], "Shop_API");
        assert_eq!(record["fields"]["pid"], "812");
        // The same entry read again gets the same id, so a resend is stored once.
        assert_eq!(record["id"], from_journal(&entry, None).unwrap().log["id"]);
        assert!(trail_core::names::is_id(record["id"].as_str().unwrap()));

        // A container is named by its container, bytes are decoded, priorities map to levels.
        let entry = json!({"MESSAGE": [104, 105], "PRIORITY": "3", "CONTAINER_NAME": "caddy", "_SYSTEMD_UNIT": "docker.service"});
        let found = from_journal(&entry, None).unwrap();
        let (source, record) = (found.source, found.log);
        assert_eq!(
            (
                source.as_str(),
                record["message"].as_str(),
                record["level"].as_str()
            ),
            ("caddy", Some("hi"), Some("error"))
        );
        assert_eq!(from_journal(&entry, Some("box")).unwrap().source, "box");
        assert!(from_journal(&json!({"MESSAGE": "  "}), None).is_none());
    }

    #[test]
    fn error_lines_are_errors_and_the_journal_reports_kills_and_failed_units() {
        let entry = json!({
            "MESSAGE": "connect to db-1 failed after 3 tries", "PRIORITY": "3", "SYSLOG_IDENTIFIER": "shop-api",
            "_SYSTEMD_UNIT": "shop-api.service", "_PID": "812", "__CURSOR": "s=abc;i=2",
            "__REALTIME_TIMESTAMP": "1794312000123456",
        });
        let found = from_journal(&entry, None).unwrap();
        let error = found.error.unwrap();
        assert_eq!(error["kind"], "shop-api");
        assert_eq!(error["severity"], "error");
        assert_eq!(error["message"], "connect to db-1 failed after 3 tries");
        assert_eq!(error["ts"], 1_794_312_000_123i64);
        assert_eq!(error["context"]["unit"], "shop-api.service");
        // Its own id, the same each time the entry is read.
        assert_ne!(error["id"], found.log["id"]);
        assert_eq!(
            error["id"],
            from_journal(&entry, None).unwrap().error.unwrap()["id"]
        );
        assert!(found.event.is_none());
        // A warning is a log line only.
        let warning = json!({"MESSAGE": "slow", "PRIORITY": "4", "_SYSTEMD_UNIT": "shop-api.service"});
        assert!(from_journal(&warning, None).unwrap().error.is_none());

        let kill = json!({
            "MESSAGE": "Out of memory: Killed process 812 (shop-api) total-vm:912340kB, anon-rss:401232kB",
            "PRIORITY": "3", "_TRANSPORT": "kernel", "SYSLOG_IDENTIFIER": "kernel", "__CURSOR": "s=abc;i=3",
        });
        let found = from_journal(&kill, None).unwrap();
        let event = found.event.unwrap();
        assert_eq!(event["name"], "journal.oom.killed");
        assert_eq!(event["props"], json!({"process": "shop-api", "pid": 812}));
        assert!(trail_core::names::is_id(event["id"].as_str().unwrap()));
        // A cgroup's limit reads the same way; a program that only prints those words does not count.
        let cgroup = json!({"MESSAGE": "Memory cgroup out of memory: Killed process 9 (worker) total-vm:1kB", "_TRANSPORT": "kernel"});
        assert_eq!(
            from_journal(&cgroup, None).unwrap().event.unwrap()["props"]["process"],
            "worker"
        );
        let chatter =
            json!({"MESSAGE": "Out of memory: Killed process 812 (shop-api)", "_TRANSPORT": "stdout"});
        assert!(from_journal(&chatter, None).unwrap().event.is_none());

        let failed = json!({
            "MESSAGE": "mail-worker.service: Failed with result 'exit-code'.", "PRIORITY": "4",
            "MESSAGE_ID": "d9b373ed55a64feb8242e02dbe79a49c", "UNIT": "mail-worker.service",
            "UNIT_RESULT": "exit-code", "SYSLOG_IDENTIFIER": "systemd", "_SYSTEMD_UNIT": "init.scope",
        });
        let event = from_journal(&failed, None).unwrap().event.unwrap();
        assert_eq!(event["name"], "journal.unit.failed");
        assert_eq!(
            event["props"],
            json!({"unit": "mail-worker.service", "result": "exit-code"})
        );
        // A user unit's failure names the unit in another field.
        let user = json!({"MESSAGE": "x", "MESSAGE_ID": "d9b373ed55a64feb8242e02dbe79a49c", "USER_UNIT": "sync.service", "UNIT_RESULT": "timeout"});
        assert_eq!(
            from_journal(&user, None).unwrap().event.unwrap()["props"]["unit"],
            "sync.service"
        );
    }

    #[test]
    fn names_become_sources() {
        assert_eq!(source_name("Shop_API"), "shop-api");
        assert_eq!(source_name("user@1000"), "user-1000");
        assert_eq!(source_name("9front"), "x-9front");
        assert_eq!(source_name("___"), "journal");
    }

    #[test]
    fn json_lines_become_logs() {
        let line = r#"{"time":"2026-11-10T12:00:00Z","level":"WARNING","msg":"slow query","ms":812}"#;
        let record = from_line(line, "app.log:7:0", Some("db"));
        assert_eq!(record["message"], "slow query");
        assert_eq!(record["level"], "warn");
        assert_eq!(record["ts"], 1_794_312_000_000i64);
        assert_eq!(record["stream"], "db");
        assert_eq!(record["fields"], json!({"ms": 812}));
        assert_ne!(record["id"], from_line(line, "app.log:7:90", None)["id"]);

        let record = from_line("plain text line", "app.log:7:0", None);
        assert_eq!(record["message"], "plain text line");
        assert!(record.get("level").is_none());
        assert_eq!(
            from_line(r#"{"level":50,"ts":1794312000}"#, "x", None)["level"],
            "error"
        );
    }

    #[test]
    fn times_in_any_unit() {
        assert_eq!(time_of(&json!(1_794_312_000)), Some(1_794_312_000_000));
        assert_eq!(time_of(&json!(1_794_312_000.5)), Some(1_794_312_000_500));
        assert_eq!(time_of(&json!(1_794_312_000_000i64)), Some(1_794_312_000_000));
        assert_eq!(time_of(&json!(1_794_312_000_000_000i64)), Some(1_794_312_000_000));
        assert_eq!(time_of(&json!("1794312000")), Some(1_794_312_000_000));
        assert_eq!(time_of(&json!("soon")), None);
    }
}
