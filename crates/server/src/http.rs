//! Trail's HTTP server: a request off a socket in, `api::route`'s reply out.

use crate::App;
use crate::api::{self, Reply, Request};
use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use trail_core::{Problem, Ts};

/// Decodes `%41` and `+` in a query string part.
fn decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
                match hex.and_then(|hex| u8::from_str_radix(hex, 16).ok()) {
                    Some(byte) => {
                        out.push(byte);
                        i += 2;
                    }
                    None => out.push(b'%'),
                }
            }
            other => out.push(other),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Splits a request URL into its path and decoded query parameters.
pub fn split_url(url: &str) -> (String, Vec<(String, String)>) {
    let (path, query) = url.split_once('?').unwrap_or((url, ""));
    let pairs = query
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            (decode(key), decode(value))
        })
        .collect();
    (decode(path), pairs)
}

/// The token in an `Authorization` header.
fn bearer(value: &str) -> Option<String> {
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        // InfluxDB clients send `Token <token>`.
        .or_else(|| value.strip_prefix("Token "))
        .map(|token| token.trim().to_string())
}

// ---------- the server ----------
//
// HTTP/1.1 over std's sockets, and only what Trail's callers use: a request line and headers, a
// body with a length or in chunks, keep-alive, `Expect: 100-continue`. Each connection has a
// thread, which mostly waits. The work of answering a request is done by at most `workers` of
// them at once, so that setting bounds the CPU Trail uses however many callers are connected.
// Every wait has a limit, so a caller that stops sending holds one connection, not a worker.

/// The longest a request line and headers may be.
const MAX_HEAD: usize = 16 * 1024;
/// Connections open at once. One more is answered `503` and closed.
pub const MAX_CONNECTIONS: usize = 256;
/// Bytes asked of the socket at a time.
const CHUNK: usize = 64 * 1024;

/// How long a connection may take over each thing.
#[derive(Debug, Clone, Copy)]
pub struct Patience {
    /// Between requests on a connection kept open.
    pub idle: Duration,
    /// From a request's first byte to the end of its headers.
    pub head: Duration,
    /// For the whole body.
    pub body: Duration,
    /// For the caller to take the response.
    pub write: Duration,
}

impl Default for Patience {
    fn default() -> Patience {
        Patience {
            idle: Duration::from_secs(60),
            head: Duration::from_secs(10),
            body: Duration::from_secs(60),
            write: Duration::from_secs(30),
        }
    }
}

/// Lets `workers` requests be worked on at once; the rest wait their turn. With more than one
/// worker, queries may hold all but one: slow reads never keep ingest or the health check out.
struct Gate {
    /// (workers free, of which queries may still take).
    free: Mutex<(usize, usize)>,
    freed: Condvar,
}

struct Pass<'a> {
    gate: &'a Gate,
    query: bool,
}

impl Gate {
    fn new(workers: usize) -> Gate {
        let workers = workers.max(1);
        Gate {
            free: Mutex::new((workers, (workers - 1).max(1))),
            freed: Condvar::new(),
        }
    }

    fn enter(&self, query: bool) -> Pass<'_> {
        let mut free = crate::lock(&self.free);
        while free.0 == 0 || (query && free.1 == 0) {
            free = self
                .freed
                .wait(free)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        free.0 -= 1;
        if query {
            free.1 -= 1;
        }
        Pass { gate: self, query }
    }
}

impl Drop for Pass<'_> {
    fn drop(&mut self) {
        let mut free = crate::lock(&self.gate.free);
        free.0 += 1;
        if self.query {
            free.1 += 1;
        }
        // Waiters differ in what they wait for, so all of them look again.
        self.gate.freed.notify_all();
    }
}

/// The connections open now, so stopping the server can close them.
#[derive(Default)]
pub(crate) struct Open {
    streams: Mutex<HashMap<u64, TcpStream>>,
}

impl Open {
    /// Hangs up on every caller.
    pub(crate) fn close_all(&self) {
        for stream in crate::lock(&self.streams).values() {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }
}

/// Removes a connection from the open set when its thread ends, however it ends.
struct Seat {
    open: Arc<Open>,
    id: u64,
}

impl Drop for Seat {
    fn drop(&mut self) {
        crate::lock(&self.open.streams).remove(&self.id);
    }
}

/// Why a connection stops being read.
enum Stop {
    /// The caller went away, or was idle for too long. Nothing to say.
    Closed,
    /// The request can't be served. Answer with this, then close.
    Refuse(u16, Problem),
}

fn refuse(status: u16, message: impl Into<String>, fix: &str) -> Stop {
    Stop::Refuse(status, Problem::new("request", message, fix))
}

/// One request off the wire.
struct Incoming {
    method: String,
    target: String,
    token: Option<String>,
    encoding: Option<String>,
    body: Vec<u8>,
    /// Close the connection after answering.
    close: bool,
}

struct Connection {
    stream: TcpStream,
    /// Bytes read from the socket and not yet used.
    buf: Vec<u8>,
}

impl Connection {
    /// Reads whatever arrives next into `buf`. `Refuse(408)` when `deadline` passes first.
    fn more(&mut self, deadline: Instant) -> Result<(), Stop> {
        let slow = || {
            refuse(
                408,
                "the request took too long to arrive",
                "send the whole request without pausing",
            )
        };
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(slow());
        }
        if self.stream.set_read_timeout(Some(left)).is_err() {
            return Err(Stop::Closed);
        }
        let before = self.buf.len();
        self.buf.resize(before + CHUNK, 0);
        let read = self.stream.read(&mut self.buf[before..]);
        self.buf.truncate(before + read.as_ref().map_or(0, |n| *n));
        match read {
            Ok(0) => Err(Stop::Closed),
            Ok(_) => Ok(()),
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => Err(slow()),
            Err(_) => Err(Stop::Closed),
        }
    }

    /// Moves the next `n` bytes into `out`.
    fn take(&mut self, mut n: usize, out: &mut Vec<u8>, deadline: Instant) -> Result<(), Stop> {
        while n > 0 {
            if self.buf.is_empty() {
                self.more(deadline)?;
            }
            let some = n.min(self.buf.len());
            out.extend_from_slice(&self.buf[..some]);
            self.buf.drain(..some);
            n -= some;
        }
        Ok(())
    }

    /// The next line, without its CRLF. For the short lines of a chunked body.
    fn line(&mut self, deadline: Instant) -> Result<String, Stop> {
        loop {
            if let Some(end) = self.buf.windows(2).position(|pair| pair == b"\r\n") {
                let line = String::from_utf8_lossy(&self.buf[..end]).into_owned();
                self.buf.drain(..end + 2);
                return Ok(line);
            }
            if self.buf.len() > 1024 {
                return Err(refuse(
                    400,
                    "a chunk of the body has no size line",
                    "send the body with a Content-Length",
                ));
            }
            self.more(deadline)?;
        }
    }

    /// Reads one request. `Closed` when the caller has no more to send.
    fn next(&mut self, limit: usize, patience: &Patience) -> Result<Incoming, Stop> {
        if self.buf.is_empty() {
            // Between requests, a caller that says nothing is just hung up on.
            self.more(Instant::now() + patience.idle)
                .map_err(|_| Stop::Closed)?;
        }
        let deadline = Instant::now() + patience.head;
        let end = loop {
            let found = self.buf.windows(4).position(|four| four == b"\r\n\r\n");
            if found.unwrap_or(self.buf.len()) > MAX_HEAD {
                return Err(refuse(
                    431,
                    format!("the request line and headers are over {MAX_HEAD} bytes"),
                    "send fewer or shorter headers; put data in the body",
                ));
            }
            if let Some(end) = found {
                break end;
            }
            self.more(deadline)?;
        };
        let malformed = |what: &str| {
            refuse(
                400,
                format!("this is not an HTTP/1.1 request: {what}"),
                "send `METHOD /path HTTP/1.1`, then `Name: value` headers, each line ending CRLF",
            )
        };
        let head = std::str::from_utf8(&self.buf[..end])
            .map_err(|_| malformed("the headers are not UTF-8"))?
            .to_string();
        self.buf.drain(..end + 4);
        let mut lines = head.split("\r\n");
        let mut first = lines.next().unwrap_or_default().split(' ');
        let (Some(method), Some(target), Some(version), None) =
            (first.next(), first.next(), first.next(), first.next())
        else {
            return Err(malformed("the first line is not `METHOD /path HTTP/1.1`"));
        };
        let keeps_alive = match version {
            "HTTP/1.1" => true,
            "HTTP/1.0" => false,
            _ => return Err(malformed("the version is not HTTP/1.1")),
        };
        let mut headers: Vec<(String, &str)> = Vec::new();
        for line in lines {
            let Some((name, value)) = line.split_once(':') else {
                return Err(malformed("a header line has no colon"));
            };
            if name.is_empty() || name.contains(|c: char| c.is_ascii_whitespace()) {
                return Err(malformed("a header name has a space in it"));
            }
            headers.push((name.to_ascii_lowercase(), value.trim()));
        }
        let one = |name: &str| headers.iter().find(|(n, _)| n == name).map(|(_, v)| *v);

        let chunked = match one("transfer-encoding") {
            None => false,
            Some(value) if value.eq_ignore_ascii_case("chunked") => true,
            Some(other) => {
                return Err(refuse(
                    501,
                    format!("Trail does not read a body sent as `{other}`"),
                    "send the body with a Content-Length, or as `Transfer-Encoding: chunked`",
                ));
            }
        };
        let mut length: Option<usize> = None;
        for (_, value) in headers.iter().filter(|(name, _)| name == "content-length") {
            let parsed: usize = value
                .parse()
                .map_err(|_| malformed("Content-Length is not a number"))?;
            if length.is_some_and(|earlier| earlier != parsed) {
                return Err(malformed("two Content-Length headers disagree"));
            }
            length = Some(parsed);
        }
        if chunked && length.is_some() {
            // Which one a proxy in front believed is anyone's guess; take neither.
            return Err(malformed("it has both a Content-Length and a chunked body"));
        }
        let too_large = || {
            Stop::Refuse(
                413,
                Problem::new(
                    "body",
                    format!("the body is over {limit} bytes"),
                    "send smaller batches, or raise limits.body_bytes in trail.toml",
                ),
            )
        };
        if length.is_some_and(|length| length > limit) {
            return Err(too_large());
        }
        if (chunked || length.is_some_and(|length| length > 0))
            && one("expect").is_some_and(|value| value.eq_ignore_ascii_case("100-continue"))
            && self.stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").is_err()
        {
            return Err(Stop::Closed);
        }

        let deadline = Instant::now() + patience.body;
        let mut body = Vec::new();
        if chunked {
            loop {
                let line = self.line(deadline)?;
                let size = line.split(';').next().unwrap_or_default().trim();
                let size = usize::from_str_radix(size, 16)
                    .map_err(|_| malformed("a chunk's size is not a hexadecimal number"))?;
                if size == 0 {
                    // Trailers, which Trail has no use for, up to the empty line.
                    while !self.line(deadline)?.is_empty() {}
                    break;
                }
                // Checked, not added: a size near usize::MAX would wrap a sum past the limit.
                if size > limit.saturating_sub(body.len()) {
                    return Err(too_large());
                }
                self.take(size, &mut body, deadline)?;
                if !self.line(deadline)?.is_empty() {
                    return Err(malformed("a chunk is longer than its size line says"));
                }
            }
        } else if let Some(length) = length {
            body.reserve_exact(length);
            self.take(length, &mut body, deadline)?;
        }

        let close = match one("connection") {
            Some(value) if value.eq_ignore_ascii_case("close") => true,
            Some(value) if value.eq_ignore_ascii_case("keep-alive") => false,
            _ => !keeps_alive,
        };
        Ok(Incoming {
            method: method.to_string(),
            target: target.to_string(),
            token: one("authorization").and_then(bearer),
            encoding: one("content-encoding")
                .map(|value| value.trim().to_ascii_lowercase())
                .filter(|encoding| !encoding.is_empty() && encoding != "identity"),
            body,
            close,
        })
    }

    /// Sends a whole response in one write, so it is never held back waiting for the caller
    /// to acknowledge its first part.
    fn respond(&mut self, reply: Reply, close: bool, with_body: bool) -> std::io::Result<()> {
        let reason = match reply.status {
            200 => "OK",
            204 => "No Content",
            400 => "Bad Request",
            401 => "Unauthorized",
            404 => "Not Found",
            405 => "Method Not Allowed",
            408 => "Request Timeout",
            413 => "Content Too Large",
            415 => "Unsupported Media Type",
            431 => "Request Header Fields Too Large",
            500 => "Internal Server Error",
            501 => "Not Implemented",
            503 => "Service Unavailable",
            _ => "",
        };
        let text = reply.body.map(|body| body.to_string()).unwrap_or_default();
        let mut out = Vec::with_capacity(text.len() + 160);
        out.extend_from_slice(format!("HTTP/1.1 {} {reason}\r\n", reply.status).as_bytes());
        if reply.status != 204 {
            out.extend_from_slice(
                format!(
                    "Content-Type: application/json\r\nContent-Length: {}\r\n",
                    text.len()
                )
                .as_bytes(),
            );
        }
        if close {
            out.extend_from_slice(b"Connection: close\r\n");
        }
        out.extend_from_slice(b"\r\n");
        if with_body {
            out.extend_from_slice(text.as_bytes());
        }
        self.stream.write_all(&out)
    }

    /// Closes after a refusal without cutting the caller off mid-send: a caller still writing
    /// its body would otherwise get a reset and never read the answer.
    fn linger(&mut self) {
        let _ = self.stream.shutdown(Shutdown::Write);
        let until = Instant::now() + Duration::from_secs(2);
        let mut sink = [0u8; 8192];
        while Instant::now() < until {
            let left = until
                .saturating_duration_since(Instant::now())
                .max(Duration::from_millis(1));
            if self.stream.set_read_timeout(Some(left)).is_err() {
                break;
            }
            match self.stream.read(&mut sink) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    }
}

/// Serves one connection until the caller is done with it.
fn converse(app: &App, gate: &Gate, stream: TcpStream, patience: &Patience) {
    // Responses go out whole, so there is nothing for the kernel to wait to batch up.
    let _ = stream.set_nodelay(true);
    let _ = stream.set_write_timeout(Some(patience.write));
    let mut connection = Connection {
        stream,
        buf: Vec::new(),
    };
    loop {
        match connection.next(app.config.limits.body_bytes, patience) {
            Ok(request) => {
                let close = request.close;
                let head_only = request.method == "HEAD";
                let reply = answer(app, gate, request);
                if connection.respond(reply, close, !head_only).is_err() || close {
                    return;
                }
            }
            Err(Stop::Closed) => return,
            Err(Stop::Refuse(status, problem)) => {
                if connection
                    .respond(Reply::problem(status, &problem), true, true)
                    .is_ok()
                {
                    connection.linger();
                }
                return;
            }
        }
    }
}

/// Works out the answer to a request that has arrived whole.
fn answer(app: &App, gate: &Gate, request: Incoming) -> Reply {
    if let Some(encoding) = request.encoding {
        return Reply::problem(
            415,
            &Problem::new(
                "Content-Encoding",
                format!("the body is compressed with `{encoding}`, which Trail does not read"),
                "send it uncompressed: in Telegraf set content_encoding = \"identity\" on the output",
            ),
        );
    }
    let (path, query) = split_url(&request.target);
    let method = if request.method == "HEAD" {
        "GET".to_string()
    } else {
        request.method
    };
    // Reading the request needed no worker; working on it does.
    let asks = method == "GET" && !matches!(path.trim_end_matches('/'), "/v1/health" | "/v1" | "");
    let _pass = gate.enter(asks);
    api::route(
        app,
        &Request {
            method,
            path,
            query,
            token: request.token,
            body: request.body,
        },
        Ts::now(),
    )
}

/// Accepts connections until `stop` is set. Returns the accepting thread.
pub(crate) fn serve(
    app: Arc<App>,
    listener: TcpListener,
    workers: usize,
    patience: Patience,
    open: Arc<Open>,
    stop: Arc<AtomicBool>,
) -> std::io::Result<JoinHandle<()>> {
    let gate = Arc::new(Gate::new(workers));
    std::thread::Builder::new()
        .name("trail-accept".to_string())
        .spawn(move || {
            let mut next_id = 0u64;
            for stream in listener.incoming() {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                let Ok(stream) = stream else {
                    // Out of file descriptors, most likely: wait for some to be returned.
                    std::thread::sleep(Duration::from_millis(50));
                    continue;
                };
                next_id += 1;
                let id = next_id;
                {
                    let mut streams = crate::lock(&open.streams);
                    if streams.len() >= MAX_CONNECTIONS {
                        drop(streams);
                        let mut connection = Connection {
                            stream,
                            buf: Vec::new(),
                        };
                        let _ = connection.stream.set_write_timeout(Some(Duration::from_secs(1)));
                        let busy = Problem::new(
                            "connections",
                            format!("{MAX_CONNECTIONS} connections are open already"),
                            "send over fewer connections and reuse them; try again shortly",
                        );
                        let _ = connection.respond(Reply::problem(503, &busy), true, true);
                        continue;
                    }
                    if let Ok(copy) = stream.try_clone() {
                        streams.insert(id, copy);
                    }
                }
                let seat = Seat {
                    open: open.clone(),
                    id,
                };
                let (app, gate) = (app.clone(), gate.clone());
                // If no thread can be started the seat is dropped here, which gives it back.
                let _ = std::thread::Builder::new()
                    .name("trail-http".to_string())
                    .stack_size(1024 * 1024)
                    .spawn(move || {
                        let _seat = seat;
                        converse(&app, &gate, stream, &patience);
                    });
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_split_and_decode() {
        let (path, query) = split_url("/v1/events?name=shop.order.%2A&q=a+b&flag&label.queue=e%20mail");
        assert_eq!(path, "/v1/events");
        assert_eq!(
            query,
            vec![
                ("name".to_string(), "shop.order.*".to_string()),
                ("q".to_string(), "a b".to_string()),
                ("flag".to_string(), String::new()),
                ("label.queue".to_string(), "e mail".to_string()),
            ]
        );
        assert_eq!(split_url("/v1").1, vec![]);
        assert_eq!(decode("100%"), "100%");
        assert_eq!(decode("%zz"), "%zz");
    }
}
