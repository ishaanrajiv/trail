//! Talking to a running Trail: the commands that send one record from a script, the spool they
//! fall back to, `import`, `get` and `rollup rebuild`.

use crate::args::{Args, SERVER, pairs};
use crate::{Done, out};
use serde_json::{Map, Value, json};
use std::path::{Path, PathBuf};
use std::time::Duration;
use trail_core::{Problem, Ts};

/// Options the commands that send one record share.
const RECORD: [&str; 7] = ["url", "token", "spool", "ts", "id", "version", "host"];

/// A connection to the server.
pub struct Http {
    url: String,
    token: Option<String>,
    agent: ureq::Agent,
}

impl Http {
    pub fn new(args: &Args) -> Http {
        Http::to(args.url(), args.token())
    }

    pub fn to(url: String, token: Option<String>) -> Http {
        let config = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(600)))
            .timeout_connect(Some(Duration::from_secs(5)))
            .http_status_as_error(false)
            .build();
        Http {
            url,
            token,
            agent: ureq::Agent::new_with_config(config),
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Sends a request. `Err` is "no answer": the caller may keep the record and retry.
    pub fn attempt(&self, method: &str, path: &str, body: Option<&str>) -> Result<(u16, String), String> {
        if self.url.starts_with("https://") {
            return Err("this command speaks plain HTTP, and the address is https".to_string());
        }
        let url = format!("{}{path}", self.url);
        let auth = self.token.as_ref().map(|token| format!("Bearer {token}"));
        let response = match method {
            "GET" | "DELETE" => {
                let mut request = if method == "GET" {
                    self.agent.get(&url)
                } else {
                    self.agent.delete(&url)
                };
                if let Some(auth) = &auth {
                    request = request.header("Authorization", auth);
                }
                request.call()
            }
            _ => {
                let mut request = if method == "PUT" {
                    self.agent.put(&url)
                } else {
                    self.agent.post(&url)
                };
                request = request.header("Content-Type", "application/json");
                if let Some(auth) = &auth {
                    request = request.header("Authorization", auth);
                }
                request.send(body.unwrap_or(""))
            }
        };
        let mut response = response.map_err(|e| e.to_string())?;
        let status = response.status().as_u16();
        let text = response
            .body_mut()
            .with_config()
            .limit(1 << 30)
            .read_to_string()
            .map_err(|e| e.to_string())?;
        Ok((status, text))
    }

    /// Sends a request; no answer is a problem that says where it tried.
    pub fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> Result<(u16, String), Vec<Problem>> {
        self.attempt(method, path, body)
            .map_err(|e| vec![self.unreachable(&e)])
    }

    pub fn unreachable(&self, error: &str) -> Problem {
        let fix = if self.url.starts_with("https://") {
            "use the server's http:// address (it listens on loopback by default), set with --url or TRAIL_URL"
        } else {
            "check Trail is running (`trail serve`) and that --url or TRAIL_URL points at it"
        };
        Problem::new(&self.url, format!("no answer from Trail: {error}"), fix)
    }
}

/// The problems in an error response, or one that quotes it.
pub fn problems_in(body: &str, status: u16) -> Vec<Problem> {
    let value: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    let read = |v: &Value| serde_json::from_value::<Problem>(v.clone()).ok();
    if let Some(list) = value.get("problems").and_then(Value::as_array) {
        let problems: Vec<Problem> = list.iter().filter_map(read).collect();
        if !problems.is_empty() {
            return problems;
        }
    }
    if let Some(problem) = value.get("error").and_then(read) {
        return vec![problem];
    }
    let short: String = body.chars().take(300).collect();
    vec![Problem::new(
        "trail",
        format!("the server answered {status}: {short}"),
        "",
    )]
}

fn spool_dir(args: &Args) -> Option<PathBuf> {
    args.value("spool").map(PathBuf::from).or_else(|| {
        std::env::var("TRAIL_SPOOL_DIR")
            .ok()
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
    })
}

fn hostname() -> Option<String> {
    std::env::var("TRAIL_HOST")
        .ok()
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
}

/// The batch-level fields and a record's `id` and `ts`, made here so a resend is stored once.
fn envelope(
    args: &Args,
    command: &str,
    source: &str,
) -> Result<(Map<String, Value>, String, i64), Vec<Problem>> {
    let now = Ts::now();
    let ts = match args.value("ts") {
        None => now.ms(),
        Some(text) => text.parse::<i64>().map_err(|_| {
            vec![Problem::new(
                format!("trail {command}"),
                format!("`--ts {text}` is not a time"),
                "write milliseconds since 1970, or leave it out for now",
            )]
        })?,
    };
    let id = args
        .value("id")
        .map_or_else(|| trail_core::ids::uuid7(now), str::to_string);
    let mut batch = Map::new();
    batch.insert("source".to_string(), json!(source));
    let version = args
        .value("version")
        .map(str::to_string)
        .or_else(|| std::env::var("TRAIL_VERSION").ok().filter(|v| !v.is_empty()));
    if let Some(version) = version {
        batch.insert("version".to_string(), json!(version));
    }
    if let Some(host) = args.value("host").map(str::to_string).or_else(hostname) {
        batch.insert("host".to_string(), json!(host));
    }
    Ok((batch, id, ts))
}

/// Posts one ingest body. With a spool, a body Trail could not take right now is kept for
/// `trail flush`, and the command still succeeds: a cron job should not fail because Trail is
/// restarting.
fn deliver(args: &Args, body: &Value) -> Done {
    let http = Http::new(args);
    let text = body.to_string();
    let kept = match http.attempt("POST", "/v1/ingest", Some(&text)) {
        Ok((200, reply)) => return report(&reply),
        Ok((status @ (400 | 401 | 413), reply)) => return Err(problems_in(&reply, status)),
        Ok((status, _)) => format!("it answered {status}"),
        Err(error) => error,
    };
    let Some(dir) = spool_dir(args) else {
        let mut problem = http.unreachable(&kept);
        problem.fix = format!(
            "{}; set TRAIL_SPOOL_DIR to keep records for `trail flush` when this happens",
            problem.fix
        );
        return Err(vec![problem]);
    };
    let file = dir.join(format!("{}.json", trail_core::ids::uuid7(Ts::now())));
    std::fs::create_dir_all(&dir)
        .and_then(|()| std::fs::write(&file, &text))
        .map_err(|e| {
            vec![Problem::new(
                dir.display().to_string(),
                format!("Trail gave no answer ({kept}) and the record could not be spooled: {e}"),
                "make the spool folder writable",
            )]
        })?;
    eprintln!(
        "trail: no answer ({kept}); kept in {} for `trail flush`",
        file.display()
    );
    Ok(())
}

/// Reads an ingest response: refused records are problems, marked ones are warnings.
fn report(reply: &str) -> Done {
    let value: Value = serde_json::from_str(reply).unwrap_or(Value::Null);
    for marked in value
        .get("mismatched")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        eprintln!(
            "trail: stored, but it differs from its declaration: {}",
            marked.get("message").and_then(Value::as_str).unwrap_or("")
        );
    }
    let rejected: Vec<Problem> = value
        .get("rejected")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|v| serde_json::from_value(v.clone()).ok())
        .collect();
    if rejected.is_empty() {
        Ok(())
    } else {
        Err(rejected)
    }
}

/// `trail event <source> <name> [key=value ...]`
pub fn event(raw: Vec<String>) -> Done {
    let options = [&RECORD[..], &["actor", "client", "trace-id"]].concat();
    let args = Args::parse("event", raw, &options, &[])?;
    args.need("event", &["source", "name"])?;
    let (mut batch, id, ts) = envelope(&args, "event", &args.positional[0])?;
    let mut record = Map::new();
    record.insert("id".to_string(), json!(id));
    record.insert("name".to_string(), json!(args.positional[1]));
    record.insert("ts".to_string(), json!(ts));
    record.insert(
        "props".to_string(),
        Value::Object(pairs("event", &args.positional[2..])?),
    );
    for (option, field) in [("actor", "actor"), ("client", "client"), ("trace-id", "trace_id")] {
        if let Some(value) = args.value(option) {
            record.insert(field.to_string(), json!(value));
        }
    }
    batch.insert("events".to_string(), json!([record]));
    deliver(&args, &Value::Object(batch))
}

/// `trail metric <source> <name> <value> [label=value ...]`
pub fn metric(raw: Vec<String>) -> Done {
    let options = [&RECORD[..], &["unit"]].concat();
    let args = Args::parse("metric", raw, &options, &["counter"])?;
    args.need("metric", &["source", "name", "value"])?;
    let value: f64 = args.positional[2].parse().map_err(|_| {
        vec![Problem::new(
            "trail metric",
            format!("`{}` is not a number", args.positional[2]),
            "write the reading as a number, like 12.5",
        )]
    })?;
    let (mut batch, _, ts) = envelope(&args, "metric", &args.positional[0])?;
    let labels: Map<String, Value> = pairs("metric", &args.positional[3..])?
        .into_iter()
        .map(|(key, value)| {
            let text = match value {
                Value::String(text) => text,
                other => other.to_string(),
            };
            (key, Value::String(text))
        })
        .collect();
    let mut record = Map::new();
    record.insert("name".to_string(), json!(args.positional[1]));
    record.insert("value".to_string(), json!(value));
    record.insert("ts".to_string(), json!(ts));
    record.insert("labels".to_string(), Value::Object(labels));
    if let Some(unit) = args.value("unit") {
        record.insert("unit".to_string(), json!(unit));
    }
    if args.flag("counter") {
        record.insert("kind".to_string(), json!("counter"));
    }
    batch.insert("metrics".to_string(), json!([record]));
    deliver(&args, &Value::Object(batch))
}

/// `trail error <source> <kind> <message> [key=value ...]`
pub fn error(raw: Vec<String>) -> Done {
    let options = [&RECORD[..], &["severity", "stack", "trace-id"]].concat();
    let args = Args::parse("error", raw, &options, &[])?;
    args.need("error", &["source", "kind", "message"])?;
    let (mut batch, id, ts) = envelope(&args, "error", &args.positional[0])?;
    let mut record = Map::new();
    record.insert("id".to_string(), json!(id));
    record.insert("ts".to_string(), json!(ts));
    record.insert("kind".to_string(), json!(args.positional[1]));
    record.insert("message".to_string(), json!(args.positional[2]));
    record.insert(
        "context".to_string(),
        Value::Object(pairs("error", &args.positional[3..])?),
    );
    for (option, field) in [
        ("severity", "severity"),
        ("stack", "stack"),
        ("trace-id", "trace_id"),
    ] {
        if let Some(value) = args.value(option) {
            record.insert(field.to_string(), json!(value));
        }
    }
    batch.insert("errors".to_string(), json!([record]));
    deliver(&args, &Value::Object(batch))
}

/// `trail log <source> <message> [key=value ...]`
pub fn log(raw: Vec<String>) -> Done {
    let options = [&RECORD[..], &["level", "stream"]].concat();
    let args = Args::parse("log", raw, &options, &[])?;
    args.need("log", &["source", "message"])?;
    let (mut batch, id, ts) = envelope(&args, "log", &args.positional[0])?;
    let mut record = Map::new();
    record.insert("id".to_string(), json!(id));
    record.insert("ts".to_string(), json!(ts));
    record.insert("message".to_string(), json!(args.positional[1]));
    record.insert(
        "fields".to_string(),
        Value::Object(pairs("log", &args.positional[2..])?),
    );
    for option in ["level", "stream"] {
        if let Some(value) = args.value(option) {
            record.insert(option.to_string(), json!(value));
        }
    }
    batch.insert("logs".to_string(), json!([record]));
    deliver(&args, &Value::Object(batch))
}

/// The spool's batch files, oldest first (their names are UUIDv7s, which sort by time).
fn spooled(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    files.sort();
    files
}

/// `trail flush`: resends spooled batches, oldest first. Works on any client's spool folder,
/// since they all hold request bodies.
pub fn flush(raw: Vec<String>) -> Done {
    let args = Args::parse("flush", raw, &["url", "token", "spool"], &[])?;
    let Some(dir) = spool_dir(&args) else {
        return Err(vec![Problem::new(
            "trail flush",
            "which spool folder?",
            "set TRAIL_SPOOL_DIR or pass `--spool <folder>`",
        )]);
    };
    let http = Http::new(&args);
    let files = spooled(&dir);
    let mut problems = Vec::new();
    let mut sent = 0;
    for (index, file) in files.iter().enumerate() {
        let Ok(text) = std::fs::read_to_string(file) else {
            continue;
        };
        match http.attempt("POST", "/v1/ingest", Some(&text)) {
            Ok((200, reply)) => {
                sent += 1;
                if let Err(found) = report(&reply) {
                    problems.extend(found);
                }
                let _ = std::fs::remove_file(file);
            }
            // It can never succeed: report it and stop carrying it.
            Ok((status @ (400 | 401 | 413), reply)) => {
                problems.extend(problems_in(&reply, status));
                let _ = std::fs::remove_file(file);
            }
            Ok((status, _)) => {
                problems.push(waiting(
                    &http,
                    &format!("it answered {status}"),
                    files.len() - index,
                ));
                break;
            }
            Err(error) => {
                problems.push(waiting(&http, &error, files.len() - index));
                break;
            }
        }
    }
    out(&format!("sent {sent} of {} spooled batches", files.len()));
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems)
    }
}

fn waiting(http: &Http, error: &str, left: usize) -> Problem {
    let mut problem = http.unreachable(error);
    problem.message = format!("{}; {left} batches are still spooled", problem.message);
    problem
}

/// `trail import <file.jsonl>...`: each line is one ingest body.
pub fn import(raw: Vec<String>) -> Done {
    let args = Args::parse("import", raw, &SERVER, &[])?;
    args.need("import", &["file.jsonl"])?;
    let http = Http::new(&args);
    let mut problems = Vec::new();
    for file in &args.positional {
        let text = std::fs::read_to_string(file).map_err(|e| {
            vec![Problem::new(
                file,
                format!("can't read: {e}"),
                "pass a file of ingest bodies, one JSON object per line",
            )]
        })?;
        let (mut accepted, mut duplicates, mut rejected) = (0u64, 0u64, 0u64);
        for (index, line) in text
            .lines()
            .enumerate()
            .filter(|(_, line)| !line.trim().is_empty())
        {
            let at = format!("{file}:{}", index + 1);
            let (status, reply) = http.request("POST", "/v1/ingest", Some(line)).map_err(|mut found| {
                for problem in &mut found {
                    problem.message = format!("{} (stopped at {at}; lines before it are stored, and importing again is safe when records carry ids)", problem.message);
                }
                found
            })?;
            if status != 200 {
                for problem in problems_in(&reply, status) {
                    problems.push(Problem::new(
                        format!("{at}: {}", problem.at),
                        problem.message,
                        problem.fix,
                    ));
                }
                continue;
            }
            let value: Value = serde_json::from_str(&reply).unwrap_or(Value::Null);
            accepted += value["accepted"]
                .as_object()
                .map_or(0, |counts| counts.values().filter_map(Value::as_u64).sum());
            duplicates += value["duplicates"].as_u64().unwrap_or(0);
            for refused in value["rejected"].as_array().into_iter().flatten() {
                rejected += 1;
                if let Ok(problem) = serde_json::from_value::<Problem>(refused.clone()) {
                    problems.push(Problem::new(
                        format!("{at}: {}", problem.at),
                        problem.message,
                        problem.fix,
                    ));
                }
            }
        }
        out(&format!(
            "{file}: {accepted} stored, {duplicates} already there, {rejected} refused"
        ));
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems)
    }
}

/// `trail get <path>`: prints a route's JSON.
pub fn get(raw: Vec<String>) -> Done {
    let args = Args::parse("get", raw, &SERVER, &[])?;
    args.need("get", &["path"])?;
    let path = &args.positional[0];
    let path = if path.starts_with('/') {
        path.clone()
    } else {
        format!("/{path}")
    };
    let (status, body) = Http::new(&args).request("GET", &path, None)?;
    if !(200..300).contains(&status) {
        return Err(problems_in(&body, status));
    }
    let pretty = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|value| serde_json::to_string_pretty(&value).ok())
        .unwrap_or(body);
    out(&pretty);
    Ok(())
}

/// `trail rollup rebuild <source> <name> [--since T]`
pub fn rollup(raw: Vec<String>) -> Done {
    let args = Args::parse("rollup", raw, &["url", "token", "since"], &[])?;
    if args.positional.first().map(String::as_str) != Some("rebuild") {
        return Err(vec![Problem::new(
            "trail rollup",
            "the only rollup command is `rebuild`",
            "write `trail rollup rebuild <source> <name>`; `trail get /v1/rollups` lists them",
        )]);
    }
    args.need("rollup", &["rebuild", "source", "name"])?;
    let mut path = format!(
        "/v1/rollups/{}/{}/rebuild",
        args.positional[1], args.positional[2]
    );
    if let Some(since) = args.value("since") {
        path = format!("{path}?since={since}");
    }
    let (status, body) = Http::new(&args).request("POST", &path, None)?;
    if status != 200 {
        return Err(problems_in(&body, status));
    }
    let value: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    out(&format!(
        "rebuilt {}: {} events into {} buckets ({} samples)",
        value["rollup"].as_str().unwrap_or(""),
        value["events"],
        value["buckets"],
        value["samples"]
    ));
    Ok(())
}
