//! KEPT IDENTICAL IN TWO REPOSITORIES. This file is the same, byte for byte, in the other server
//! that carries it, at the same path. A change to it goes into both in the same piece of work, and
//! a check outside both repositories fails when the two copies differ. So it names neither server
//! and uses only `std` (its tests also `serde_json`, which both depend on): what is particular to
//! one server reaches it through [`Config`] and [`Handler`].
//!
//! HTTP/1.1 over std's sockets, and only what the servers' callers use: a request line and
//! headers, a body with a length or in chunks, keep-alive, `Expect: 100-continue`, `HEAD`. Each
//! connection has a thread, which mostly waits. Working on a request may need a slot first
//! ([`Lane`]): at most `workers` requests in [`Lane::Work`] and one in [`Lane::Write`] are worked
//! on at once, so that setting bounds the CPU used however many callers are connected. Every wait
//! has a limit ([`Patience`]), so a caller that stops sending holds one connection, not a slot.

use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// The longest a request line and headers may be; also the longest a chunked body's trailers
/// may be. Longer is answered `431`.
pub const MAX_HEAD: usize = 16 * 1024;
/// The longest a chunk's size line may be, extensions included. Longer is answered `400`.
pub const MAX_CHUNK_LINE: usize = 1024;
/// The longest a method may be. It is a word in capitals, like `GET`.
pub const MAX_METHOD: usize = 16;
/// Seconds a `503` the transport sends asks the caller to wait before trying again.
pub const RETRY_AFTER: u64 = 5;
/// Bytes asked of the socket at a time.
const READ: usize = 64 * 1024;
/// How long a refused caller's remaining bytes are read and dropped before the connection closes.
const LINGER: Duration = Duration::from_secs(2);
/// How long the `503` for one connection too many may take to write, on the accepting thread.
const BUSY_WRITE: Duration = Duration::from_secs(1);

/// Writes all of `bytes` by `deadline`, or fails. A socket's write timeout limits each write, not
/// the whole: a caller that took a little every so often could hold the connection for ever. So
/// each write may take only what is left.
fn write_by(stream: &mut TcpStream, mut bytes: &[u8], deadline: Instant) -> std::io::Result<()> {
    while !bytes.is_empty() {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(ErrorKind::TimedOut.into());
        }
        stream.set_write_timeout(Some(left))?;
        match stream.write(bytes) {
            Ok(0) => return Err(ErrorKind::WriteZero.into()),
            Ok(n) => bytes = bytes.get(n..).unwrap_or_default(),
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// How long a connection may take over each thing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Patience {
    /// Between requests on a connection kept open. Longer is hung up on, quietly.
    pub idle: Duration,
    /// From a request's first byte to the end of its headers. Longer is answered `408`.
    pub head: Duration,
    /// For the whole body, after the headers. Longer is answered `408`.
    pub body: Duration,
    /// For the caller to take the whole response, from its first byte to its last; also each
    /// `100 Continue`. Longer is hung up on.
    pub write: Duration,
    /// For a slot to work on the request in ([`Lane`]). Longer is answered `503`.
    pub queue: Duration,
}

impl Default for Patience {
    fn default() -> Patience {
        Patience {
            idle: Duration::from_secs(60),
            head: Duration::from_secs(10),
            body: Duration::from_secs(30),
            write: Duration::from_secs(30),
            queue: Duration::from_secs(10),
        }
    }
}

/// What one server tells the transport.
#[derive(Debug, Clone)]
pub struct Config {
    /// Names the threads: `<name>-accept`, and `<name>-http` for each connection.
    pub name: &'static str,
    /// The settings file a refusal's fix names, like `server.toml`.
    pub file: &'static str,
    /// The largest request body, in bytes (`limits.body_bytes` in `file`). Longer is `413`.
    pub body_bytes: usize,
    /// Connections open at once. One more is answered `503` and closed.
    pub max_connections: usize,
    /// Requests in [`Lane::Work`] worked on at once (at least one).
    pub workers: usize,
    /// The stack of each connection's thread, which also runs the handler.
    pub stack_bytes: usize,
    pub patience: Patience,
}

/// What a request waits for before it is worked on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    /// Nothing: answered at once, whatever else is being worked on (a health check).
    Free,
    /// The one writer's slot: one at a time, never behind a request in `Work`.
    Write,
    /// One of the `workers` slots.
    Work,
}

/// One request, read whole. A `HEAD` reaches the handler as a `GET`, and its body is not sent.
#[derive(Debug, Clone)]
pub struct Request {
    pub method: String,
    /// The path without the query string, `%`-decoded.
    pub path: String,
    /// The query parameters, decoded, in order.
    pub query: Vec<(String, String)>,
    /// Lowercased names, values without the spaces and tabs around them, in order.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub peer: SocketAddr,
}

impl Request {
    /// The first header with this lowercase name.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    /// The first query parameter with this name.
    pub fn param(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
}

/// An answer. The body is JSON text, sent with its length; a `204` or `304` sends no body and
/// no `Content-Type` or `Content-Length`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    /// Headers besides the ones the transport writes.
    pub headers: Vec<(&'static str, String)>,
    pub body: String,
}

impl Response {
    pub fn json(status: u16, body: String) -> Response {
        Response {
            status,
            headers: Vec::new(),
            body,
        }
    }
}

/// A request the transport will not pass on, and why, in the servers' three parts: where, what
/// is wrong, what would work. The handler turns it into its own error body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub status: u16,
    pub at: &'static str,
    pub message: String,
    pub fix: String,
}

impl Refusal {
    fn new(status: u16, at: &'static str, message: impl Into<String>, fix: impl Into<String>) -> Refusal {
        Refusal {
            status,
            at,
            message: message.into(),
            fix: fix.into(),
        }
    }
}

/// What a server puts behind the transport.
pub trait Handler: Send + Sync + 'static {
    /// The slot `request` waits for. Called before its body is worked on, with the body read.
    fn lane(&self, request: &Request) -> Lane;
    /// The answer to a request that arrived whole and got its slot.
    fn handle(&self, request: Request) -> Response;
    /// The answer to a request the transport refused, in the server's error shape. A `503`
    /// gets `Retry-After` added when it has none.
    fn refused(&self, refusal: Refusal) -> Response;
}

/// Decodes `%41`, and `+` as a space when `plus` (a query string, not a path).
fn decode(text: &str, plus: bool) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' if plus => out.push(b' '),
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

/// Splits a request target into its path and decoded query parameters.
pub fn split_url(url: &str) -> (String, Vec<(String, String)>) {
    let (path, query) = url.split_once('?').unwrap_or((url, ""));
    let pairs = query
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            (decode(key, true), decode(value, true))
        })
        .collect();
    (decode(path, false), pairs)
}

/// Locks a mutex. A thread that panicked holding it does not take the server down: what these
/// locks guard (counts and sockets) stays usable.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A number of slots, each held by one request at a time.
struct Slots {
    free: Mutex<usize>,
    freed: Condvar,
}

impl Slots {
    fn new(n: usize) -> Slots {
        Slots {
            free: Mutex::new(n),
            freed: Condvar::new(),
        }
    }

    /// Takes a slot, waiting for one until `deadline`. None when none came free in time.
    fn take(&self, deadline: Instant) -> Option<Pass<'_>> {
        let mut free = lock(&self.free);
        while *free == 0 {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return None;
            }
            free = self
                .freed
                .wait_timeout(free, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        *free -= 1;
        Some(Pass { slots: self })
    }
}

/// A slot held; given back when dropped.
struct Pass<'a> {
    slots: &'a Slots,
}

impl Drop for Pass<'_> {
    fn drop(&mut self) {
        *lock(&self.slots.free) += 1;
        self.slots.freed.notify_one();
    }
}

/// The slots of [`Lane::Work`] and [`Lane::Write`]. The two never wait for each other, and
/// [`Lane::Free`] waits for nothing, so a slow request keeps neither a write nor a health
/// check out.
struct Gate {
    work: Slots,
    write: Slots,
}

impl Gate {
    fn new(workers: usize) -> Gate {
        Gate {
            work: Slots::new(workers.max(1)),
            write: Slots::new(1),
        }
    }

    /// The slot a lane needs: `Ok(None)` when it needs none, `Err` when none came free by
    /// `deadline`.
    fn enter(&self, lane: Lane, deadline: Instant) -> Result<Option<Pass<'_>>, ()> {
        let slots = match lane {
            Lane::Free => return Ok(None),
            Lane::Write => &self.write,
            Lane::Work => &self.work,
        };
        slots.take(deadline).map(Some).ok_or(())
    }
}

/// The connections open now, so stopping the server can close them.
#[derive(Default)]
pub struct Open {
    streams: Mutex<HashMap<u64, TcpStream>>,
}

impl Open {
    /// Hangs up on every caller.
    pub fn close_all(&self) {
        for stream in lock(&self.streams).values() {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    /// How many connections are open.
    pub fn count(&self) -> usize {
        lock(&self.streams).len()
    }
}

/// Removes a connection from the open set when its thread ends, however it ends.
struct Seat {
    open: Arc<Open>,
    id: u64,
}

impl Drop for Seat {
    fn drop(&mut self) {
        lock(&self.open.streams).remove(&self.id);
    }
}

/// Why a connection stops being read.
enum Stop {
    /// The caller went away, or was idle for too long. Nothing to say.
    Closed,
    /// The request can't be served. Answer this, then close.
    Refuse(Refusal),
}

fn refuse(status: u16, message: impl Into<String>, fix: impl Into<String>) -> Stop {
    Stop::Refuse(Refusal::new(status, "request", message, fix))
}

fn malformed(what: &str) -> Stop {
    refuse(
        400,
        format!("this is not an HTTP/1.1 request: {what}"),
        "send `METHOD /path HTTP/1.1`, then `Name: value` headers, each line ending CRLF",
    )
}

/// A byte no line of a request may hold: a control character other than a tab. A bare CR or LF
/// is one, so one line can't pass for two.
fn control(byte: u8) -> bool {
    (byte < 0x20 && byte != b'\t') || byte == 0x7f
}

/// A byte a header name may hold: RFC 9110's `tchar`.
fn tchar(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

fn too_slow() -> Stop {
    refuse(
        408,
        "the request took too long to arrive",
        "send the whole request without pausing",
    )
}

/// What reading a connection has cost, for the tests: the work done per byte stays constant
/// however the caller cuts up what it sends.
#[cfg(test)]
#[derive(Debug, Default, Clone, Copy)]
struct Cost {
    /// Reads from the socket that brought bytes.
    reads: usize,
    /// Bytes they brought.
    received: usize,
    /// Times the unused bytes were moved to the front of the buffer.
    compactions: usize,
    /// Bytes those moves copied.
    moved: usize,
    /// Bytes looked at in searches for the end of a line or of the headers.
    searched: usize,
}

struct Connection {
    stream: TcpStream,
    peer: SocketAddr,
    /// Bytes read from the socket: those before `at` are used, the rest not yet.
    buf: Vec<u8>,
    /// Where the unused bytes start. Using bytes moves it on rather than moving them.
    at: usize,
    /// Where the next search for the end of a line or of the headers starts. What lies between
    /// `at` and here has been searched already and holds none.
    searched: usize,
    #[cfg(test)]
    cost: Cost,
}

impl Connection {
    fn new(stream: TcpStream, peer: SocketAddr) -> Connection {
        Connection {
            stream,
            peer,
            buf: Vec::new(),
            at: 0,
            searched: 0,
            #[cfg(test)]
            cost: Cost::default(),
        }
    }

    /// The bytes read and not yet used.
    fn unread(&self) -> &[u8] {
        self.buf.get(self.at..).unwrap_or_default()
    }

    /// Marks the next `n` unused bytes used.
    fn consume(&mut self, n: usize) {
        self.at = (self.at + n).min(self.buf.len());
        if self.at == self.buf.len() {
            self.buf.clear();
            self.at = 0;
        }
        self.searched = self.at;
    }

    /// Where `pattern` starts in the unused bytes. Each call searches only what arrived since
    /// the last one, and what a match could straddle.
    fn find(&mut self, pattern: &[u8]) -> Option<usize> {
        let from = self.searched.max(self.at);
        let found = self
            .buf
            .get(from..)
            .unwrap_or_default()
            .windows(pattern.len())
            .position(|window| window == pattern);
        #[cfg(test)]
        {
            let rest = self.buf.len() - from;
            self.cost.searched += found.map_or(rest, |at| at + pattern.len());
        }
        match found {
            Some(at) => Some(from + at - self.at),
            None => {
                // The last bytes may be the start of a match that the next read completes.
                let tail = pattern.len().saturating_sub(1);
                self.searched = self.buf.len().saturating_sub(tail).max(self.at);
                None
            }
        }
    }

    /// Reads whatever arrives next into `buf`. `Refuse(408)` when `deadline` passes first.
    fn more(&mut self, deadline: Instant) -> Result<(), Stop> {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(too_slow());
        }
        if self.stream.set_read_timeout(Some(left)).is_err() {
            return Err(Stop::Closed);
        }
        // The used bytes are dropped from the front once they are at least as many as the
        // unused ones, so moving the unused ones down copies no more than was used: each byte
        // is moved at most once, on average, however small the pieces it is used in.
        let at = self.at;
        if at > 0 && at >= self.buf.len() - at {
            self.buf.drain(..at);
            self.searched = self.searched.saturating_sub(at);
            self.at = 0;
            #[cfg(test)]
            {
                self.cost.compactions += 1;
                self.cost.moved += self.buf.len();
            }
        }
        let before = self.buf.len();
        self.buf.resize(before + READ, 0);
        let read = self.stream.read(self.buf.get_mut(before..).unwrap_or_default());
        self.buf.truncate(before + read.as_ref().map_or(0, |n| *n));
        match read {
            Ok(0) => Err(Stop::Closed),
            Ok(_n) => {
                #[cfg(test)]
                {
                    self.cost.reads += 1;
                    self.cost.received += _n;
                }
                Ok(())
            }
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                Err(too_slow())
            }
            Err(_) => Err(Stop::Closed),
        }
    }

    /// Moves the next `n` bytes into `out`.
    fn take(&mut self, mut n: usize, out: &mut Vec<u8>, deadline: Instant) -> Result<(), Stop> {
        while n > 0 {
            if self.unread().is_empty() {
                self.more(deadline)?;
            }
            let unread = self.unread();
            let some = n.min(unread.len());
            out.extend_from_slice(unread.get(..some).unwrap_or_default());
            self.consume(some);
            n -= some;
        }
        Ok(())
    }

    /// The next line, without its CRLF; `long()` when it is over `max` bytes. For the short
    /// lines of a chunked body.
    fn line(&mut self, deadline: Instant, max: usize, long: fn() -> Stop) -> Result<String, Stop> {
        loop {
            let found = self.find(b"\r\n");
            let unread = self.unread();
            // Without a CRLF yet, a last `\r` may be the start of one.
            let so_far = found.unwrap_or_else(|| unread.len() - usize::from(unread.ends_with(b"\r")));
            if so_far > max {
                return Err(long());
            }
            if let Some(end) = found {
                let line = unread.get(..end).unwrap_or_default();
                if line.iter().copied().any(control) {
                    return Err(malformed(
                        "a chunk's size line or a trailer has a control character other than a tab in it",
                    ));
                }
                let line = String::from_utf8_lossy(line).into_owned();
                self.consume(end + 2);
                return Ok(line);
            }
            self.more(deadline)?;
        }
    }

    /// Reads one request; the bool is "close after answering". `Closed` when the caller has no
    /// more to send.
    fn next(&mut self, config: &Config) -> Result<(Request, bool), Stop> {
        let patience = &config.patience;
        if self.unread().is_empty() {
            // Between requests, a caller that says nothing is just hung up on.
            self.more(Instant::now() + patience.idle)
                .map_err(|_| Stop::Closed)?;
        }
        let deadline = Instant::now() + patience.head;
        let end = loop {
            let found = self.find(b"\r\n\r\n");
            if found.unwrap_or(self.unread().len()) > MAX_HEAD {
                return Err(refuse(
                    431,
                    format!("the request line and headers are over {MAX_HEAD} bytes"),
                    "send fewer or shorter headers and a shorter URL; put data in the body",
                ));
            }
            if let Some(end) = found {
                break end;
            }
            self.more(deadline)?;
        };
        let head = std::str::from_utf8(self.unread().get(..end).unwrap_or_default())
            .map_err(|_| malformed("the headers are not UTF-8"))?
            .to_string();
        self.consume(end + 4);
        if head.split("\r\n").any(|line| line.bytes().any(control)) {
            return Err(malformed(
                "the request line or a header has a control character other than a tab in it",
            ));
        }
        let mut lines = head.split("\r\n");
        let mut first = lines.next().unwrap_or_default().split(' ');
        let (Some(method), Some(target), Some(version), None) =
            (first.next(), first.next(), first.next(), first.next())
        else {
            return Err(malformed("the first line is not `METHOD /path HTTP/1.1`"));
        };
        if method.is_empty() || method.len() > MAX_METHOD || !method.bytes().all(|b| b.is_ascii_uppercase()) {
            return Err(malformed("the method is not a word in capitals, like GET"));
        }
        if target.is_empty() {
            return Err(malformed("the first line is not `METHOD /path HTTP/1.1`"));
        }
        let keeps_alive = match version {
            "HTTP/1.1" => true,
            "HTTP/1.0" => false,
            _ => return Err(malformed("the version is not HTTP/1.1")),
        };
        let mut headers: Vec<(String, String)> = Vec::new();
        for line in lines {
            let Some((name, value)) = line.split_once(':') else {
                return Err(malformed("a header line has no colon"));
            };
            if name.is_empty() || !name.bytes().all(tchar) {
                return Err(malformed(
                    "a header name has a space or another character a name can't have (only letters, digits and !#$%&'*+-.^_`|~)",
                ));
            }
            headers.push((
                name.to_ascii_lowercase(),
                value.trim_matches([' ', '\t']).to_string(),
            ));
        }
        let all = |name: &str| {
            headers
                .iter()
                .filter(|(n, _)| n == name)
                .map(|(_, v)| v.as_str())
                .collect::<Vec<&str>>()
        };
        let one = |name: &str| headers.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_str());

        // Only a single `chunked`: a list, or a second header, is something a proxy in front
        // may have read differently.
        let chunked = match all("transfer-encoding").as_slice() {
            [] => false,
            [value] if value.eq_ignore_ascii_case("chunked") => true,
            other => {
                return Err(refuse(
                    501,
                    format!("the server does not read a body sent as `{}`", other.join(", ")),
                    "send the body with a Content-Length, or as `Transfer-Encoding: chunked`",
                ));
            }
        };
        let mut length: Option<usize> = None;
        for value in all("content-length") {
            // Digits only: `parse` would also take a sign.
            let parsed: usize = Some(value)
                .filter(|value| !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|value| value.parse().ok())
                .ok_or_else(|| malformed("Content-Length is not a number"))?;
            if length.is_some_and(|earlier| earlier != parsed) {
                return Err(malformed("two Content-Length headers disagree"));
            }
            length = Some(parsed);
        }
        if chunked && length.is_some() {
            // Which one a proxy in front believed is anyone's guess; take neither.
            return Err(malformed("it has both a Content-Length and a chunked body"));
        }
        let limit = config.body_bytes;
        let too_large = || {
            Stop::Refuse(Refusal::new(
                413,
                "body",
                format!("the body is over {limit} bytes"),
                format!(
                    "send a smaller body (split a batch in two), or raise limits.body_bytes in {}",
                    config.file
                ),
            ))
        };
        if length.is_some_and(|length| length > limit) {
            return Err(too_large());
        }
        if (chunked || length.is_some_and(|length| length > 0))
            && one("expect").is_some_and(|value| value.eq_ignore_ascii_case("100-continue"))
            && write_by(
                &mut self.stream,
                b"HTTP/1.1 100 Continue\r\n\r\n",
                Instant::now() + patience.write,
            )
            .is_err()
        {
            return Err(Stop::Closed);
        }

        let deadline = Instant::now() + patience.body;
        let mut body = Vec::new();
        if chunked {
            loop {
                // A body that has arrived already is read without asking the socket for more,
                // so its deadline is checked here too.
                if Instant::now() >= deadline {
                    return Err(too_slow());
                }
                let line = self.line(deadline, MAX_CHUNK_LINE, || {
                    malformed("a chunk's size line is over 1024 bytes")
                })?;
                let size = line.split(';').next().unwrap_or_default().trim();
                if size.is_empty() || !size.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return Err(malformed("a chunk's size is not a hexadecimal number"));
                }
                // Hexadecimal, and too long for a number: larger than any limit.
                let Ok(size) = usize::from_str_radix(size, 16) else {
                    return Err(too_large());
                };
                if size == 0 {
                    // Trailers, which no route uses, up to the empty line; no more than headers.
                    let mut trailers = 0;
                    loop {
                        let line = self.line(deadline, MAX_HEAD, || {
                            refuse(
                                431,
                                format!("the trailers are over {MAX_HEAD} bytes"),
                                "send no trailers",
                            )
                        })?;
                        if line.is_empty() {
                            break;
                        }
                        trailers += line.len() + 2;
                        if trailers > MAX_HEAD {
                            return Err(refuse(
                                431,
                                format!("the trailers are over {MAX_HEAD} bytes"),
                                "send no trailers",
                            ));
                        }
                    }
                    break;
                }
                // Checked, not added: a size near usize::MAX would wrap a sum past the limit.
                if size > limit.saturating_sub(body.len()) {
                    return Err(too_large());
                }
                self.take(size, &mut body, deadline)?;
                let after = self.line(deadline, 0, || {
                    malformed("a chunk is longer than its size line says")
                })?;
                if !after.is_empty() {
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
        let (path, query) = split_url(target);
        Ok((
            Request {
                method: method.to_string(),
                path,
                query,
                headers,
                body,
                peer: self.peer,
            },
            close,
        ))
    }

    /// Sends a whole response at once, so it is never held back waiting for the caller to
    /// acknowledge its first part, and all of it by `deadline`.
    fn respond(
        &mut self,
        response: &Response,
        close: bool,
        with_body: bool,
        deadline: Instant,
    ) -> std::io::Result<()> {
        let reason = match response.status {
            200 => "OK",
            201 => "Created",
            204 => "No Content",
            304 => "Not Modified",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            405 => "Method Not Allowed",
            408 => "Request Timeout",
            409 => "Conflict",
            410 => "Gone",
            413 => "Content Too Large",
            415 => "Unsupported Media Type",
            422 => "Unprocessable Content",
            429 => "Too Many Requests",
            431 => "Request Header Fields Too Large",
            500 => "Internal Server Error",
            501 => "Not Implemented",
            502 => "Bad Gateway",
            503 => "Service Unavailable",
            _ => "",
        };
        let bodiless = matches!(response.status, 204 | 304);
        let mut out = Vec::with_capacity(response.body.len() + 200);
        out.extend_from_slice(format!("HTTP/1.1 {} {reason}\r\n", response.status).as_bytes());
        if !bodiless {
            out.extend_from_slice(
                format!(
                    "Content-Type: application/json\r\nContent-Length: {}\r\n",
                    response.body.len()
                )
                .as_bytes(),
            );
        }
        out.extend_from_slice(b"Cache-Control: no-cache\r\n");
        for (name, value) in &response.headers {
            out.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
        }
        if close {
            out.extend_from_slice(b"Connection: close\r\n");
        }
        out.extend_from_slice(b"\r\n");
        if with_body && !bodiless {
            out.extend_from_slice(response.body.as_bytes());
        }
        write_by(&mut self.stream, &out, deadline)
    }

    /// Closes after a refusal without cutting the caller off mid-send: a caller still writing
    /// its body would otherwise get a reset and never read the answer.
    fn linger(&mut self) {
        let _ = self.stream.shutdown(Shutdown::Write);
        let until = Instant::now() + LINGER;
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

/// The handler's answer to a refusal, with `Retry-After` on a `503`.
fn refused(handler: &dyn Handler, refusal: Refusal) -> Response {
    let mut response = handler.refused(refusal);
    if response.status == 503
        && !response
            .headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("retry-after"))
    {
        response.headers.push(("Retry-After", RETRY_AFTER.to_string()));
    }
    response
}

/// Works out the answer to a request that has arrived whole: its slot, then the handler.
fn answer(handler: &dyn Handler, gate: &Gate, config: &Config, request: Request) -> Response {
    let lane = handler.lane(&request);
    let queue = config.patience.queue;
    // Reading the request needed no slot; working on it does, unless its lane is free.
    let Ok(_pass) = gate.enter(lane, Instant::now() + queue) else {
        let waited = match lane {
            Lane::Write => "the writer's slot, which stayed busy",
            Lane::Work | Lane::Free => "a worker, and none came free",
        };
        return refused(
            handler,
            Refusal::new(
                503,
                "workers",
                format!("the request waited {queue:?} for {waited}"),
                format!(
                    "try again shortly (Retry-After says when); if this is common, raise `workers` in {} or make the slow requests narrower",
                    config.file
                ),
            ),
        );
    };
    handler.handle(request)
}

/// Serves one connection until the caller is done with it.
fn converse(handler: &dyn Handler, gate: &Gate, config: &Config, stream: TcpStream, peer: SocketAddr) {
    // Responses go out whole, so there is nothing for the kernel to wait to batch up.
    let _ = stream.set_nodelay(true);
    let mut connection = Connection::new(stream, peer);
    loop {
        match connection.next(config) {
            Ok((mut request, close)) => {
                let head_only = request.method == "HEAD";
                if head_only {
                    request.method = "GET".to_string();
                }
                let response = answer(handler, gate, config, request);
                let deadline = Instant::now() + config.patience.write;
                if connection
                    .respond(&response, close, !head_only, deadline)
                    .is_err()
                    || close
                {
                    return;
                }
            }
            Err(Stop::Closed) => return,
            Err(Stop::Refuse(refusal)) => {
                let deadline = Instant::now() + config.patience.write;
                if connection
                    .respond(&refused(handler, refusal), true, true, deadline)
                    .is_ok()
                {
                    connection.linger();
                }
                return;
            }
        }
    }
}

/// Accepts connections on `listener` until `stop` is set, each on its own thread, and answers
/// them with `handler`. Returns the accepting thread. To stop it: set `stop`, connect once so
/// the accepting thread sees it, then [`Open::close_all`].
pub fn serve(
    handler: Arc<dyn Handler>,
    listener: TcpListener,
    config: Config,
    open: Arc<Open>,
    stop: Arc<AtomicBool>,
) -> std::io::Result<JoinHandle<()>> {
    let gate = Arc::new(Gate::new(config.workers));
    let config = Arc::new(config);
    std::thread::Builder::new()
        .name(format!("{}-accept", config.name))
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
                // A caller with no address has gone already.
                let Ok(peer) = stream.peer_addr() else {
                    continue;
                };
                next_id += 1;
                let id = next_id;
                {
                    let mut streams = lock(&open.streams);
                    if streams.len() >= config.max_connections {
                        drop(streams);
                        let mut connection = Connection::new(stream, peer);
                        let busy = Refusal::new(
                            503,
                            "connections",
                            format!("{} connections are open already", config.max_connections),
                            "send over fewer connections and reuse them; try again shortly",
                        );
                        let deadline = Instant::now() + BUSY_WRITE;
                        let _ = connection.respond(&refused(handler.as_ref(), busy), true, true, deadline);
                        continue;
                    }
                    // Without a copy in the open set, the connection would be outside the cap
                    // and out of reach of `close_all`. A copy fails only when descriptors have
                    // run out; dropping the connection closes it and gives one back.
                    let Ok(copy) = stream.try_clone() else {
                        continue;
                    };
                    streams.insert(id, copy);
                }
                let seat = Seat {
                    open: open.clone(),
                    id,
                };
                let (handler, gate, config) = (handler.clone(), gate.clone(), config.clone());
                // If no thread can be started the seat is dropped here, which gives it back.
                let _ = std::thread::Builder::new()
                    .name(format!("{}-http", config.name))
                    .stack_size(config.stack_bytes)
                    .spawn(move || {
                        let _seat = seat;
                        converse(handler.as_ref(), &gate, &config, stream, peer);
                    });
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Answers with what it was sent. `/slow…` waits until the test lets it go; `/empty` is a
    /// `204`, `/same` a `304`, `/free/big` [`BIG`] bytes; `/free…` needs no slot, a `POST` the
    /// writer's.
    /// More than the kernel buffers between a server and a caller on one machine hold.
    const BIG: usize = 16 * 1024 * 1024;

    struct Echo {
        held: Mutex<bool>,
        released: Condvar,
    }

    impl Echo {
        fn release(&self) {
            *lock(&self.held) = false;
            self.released.notify_all();
        }
    }

    impl Handler for Echo {
        fn lane(&self, request: &Request) -> Lane {
            if request.path.starts_with("/free") {
                Lane::Free
            } else if request.method == "POST" {
                Lane::Write
            } else {
                Lane::Work
            }
        }

        fn handle(&self, request: Request) -> Response {
            if request.path.starts_with("/slow") {
                let mut held = lock(&self.held);
                while *held {
                    held = self.released.wait(held).unwrap_or_else(PoisonError::into_inner);
                }
            }
            match request.path.as_str() {
                "/empty" => Response::json(204, String::new()),
                "/free/big" => Response::json(200, "x".repeat(BIG)),
                "/same" => {
                    let mut response = Response::json(304, "{}".to_string());
                    response.headers.push(("ETag", "\"1\"".to_string()));
                    response
                }
                _ => Response::json(
                    200,
                    serde_json::json!({
                        "method": request.method,
                        "path": request.path,
                        "query": request.query,
                        "body": String::from_utf8_lossy(&request.body),
                        "x": request.header("x-echo"),
                        "q": request.param("q"),
                        "loopback": request.peer.ip().is_loopback(),
                    })
                    .to_string(),
                ),
            }
        }

        fn refused(&self, refusal: Refusal) -> Response {
            Response::json(
                refusal.status,
                serde_json::json!({"error": {"at": refusal.at, "message": refusal.message, "fix": refusal.fix}})
                    .to_string(),
            )
        }
    }

    struct Running {
        addr: SocketAddr,
        echo: Arc<Echo>,
        open: Arc<Open>,
        stop: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    }

    impl Drop for Running {
        fn drop(&mut self) {
            self.echo.release();
            self.stop.store(true, Ordering::Relaxed);
            let _ = TcpStream::connect(self.addr);
            self.open.close_all();
            if let Some(thread) = self.thread.take() {
                thread.join().unwrap();
            }
        }
    }

    fn quick() -> Config {
        Config {
            name: "test",
            file: "server.toml",
            body_bytes: 4096,
            max_connections: 64,
            workers: 1,
            stack_bytes: 256 * 1024,
            patience: Patience {
                // Long enough that a test thread held up between connecting and sending is not
                // hung up on.
                idle: Duration::from_secs(2),
                head: Duration::from_millis(300),
                body: Duration::from_millis(300),
                write: Duration::from_secs(5),
                queue: Duration::from_millis(300),
            },
        }
    }

    fn start(config: Config) -> Running {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let echo = Arc::new(Echo {
            held: Mutex::new(true),
            released: Condvar::new(),
        });
        let (open, stop) = (Arc::new(Open::default()), Arc::new(AtomicBool::new(false)));
        let thread = serve(echo.clone(), listener, config, open.clone(), stop.clone()).unwrap();
        Running {
            addr,
            echo,
            open,
            stop,
            thread: Some(thread),
        }
    }

    /// Sends raw bytes on a new connection and returns it.
    fn raw(addr: SocketAddr, bytes: &[u8]) -> TcpStream {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        stream.write_all(bytes).unwrap();
        stream
    }

    /// The next response on a connection: its status, head and body.
    fn response(stream: &mut TcpStream) -> (u16, String, String) {
        let mut bytes = Vec::new();
        let mut one = [0u8; 1];
        while !bytes.ends_with(b"\r\n\r\n") {
            assert_eq!(stream.read(&mut one).unwrap(), 1, "closed mid-head: {bytes:?}");
            bytes.push(one[0]);
        }
        let head = String::from_utf8(bytes).unwrap();
        let length: usize = head
            .lines()
            .find_map(|line| line.strip_prefix("Content-Length: "))
            .map_or(0, |n| n.trim().parse().unwrap());
        let mut body = vec![0u8; length];
        stream.read_exact(&mut body).unwrap();
        let status = head.split(' ').nth(1).unwrap().parse().unwrap();
        (status, head, String::from_utf8(body).unwrap())
    }

    /// Sends one request on its own connection: the status, the head, the body, how long it took.
    fn send(addr: SocketAddr, request: &str) -> (u16, String, String, Duration) {
        let started = Instant::now();
        let mut stream = raw(addr, request.as_bytes());
        let (status, head, body) = response(&mut stream);
        (status, head, body, started.elapsed())
    }

    /// True when the server has closed the connection (an end of stream, or a reset); false when
    /// it sent more. A connection left open and silent fails the test: a timeout is not a close.
    fn closed(stream: &mut TcpStream) -> bool {
        match stream.read(&mut [0u8; 16]) {
            Ok(n) => n == 0,
            Err(error) if error.kind() == ErrorKind::ConnectionReset => true,
            Err(error) => panic!("the connection is open and silent, not closed: {error}"),
        }
    }

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
        let (path, query) = split_url("/v1/inbox?source=a%2Cb&class=update+x");
        assert_eq!(path, "/v1/inbox");
        assert_eq!(query[0], ("source".to_string(), "a,b".to_string()));
        assert_eq!(query[1], ("class".to_string(), "update x".to_string()));
        assert_eq!(split_url("/v1").1, vec![]);
        // A `+` in a path is a plus; `%20` is the space.
        assert_eq!(split_url("/a+b%20c").0, "/a+b c");
        assert_eq!(decode("100%", true), "100%");
        assert_eq!(decode("%zz", true), "%zz");
        assert_eq!(decode("%4", true), "%4");
        assert_eq!(decode("a+b", false), "a+b");
    }

    #[test]
    fn one_connection_serves_many_requests() {
        let server = start(quick());
        // Two requests sent at once on one connection are answered in order, and it stays open.
        let mut stream = raw(
            server.addr,
            b"POST /free/in?q=a+b HTTP/1.1\r\nHost: x\r\nX-Echo: hi\r\nContent-Length: 5\r\n\r\nhello\
              GET /free/two HTTP/1.1\r\nHost: x\r\n\r\n",
        );
        let (status, head, body) = response(&mut stream);
        assert_eq!(status, 200, "{body}");
        assert!(head.starts_with("HTTP/1.1 200 OK\r\n"), "{head}");
        assert!(head.contains("Content-Type: application/json\r\n"), "{head}");
        assert!(head.contains("Cache-Control: no-cache\r\n"), "{head}");
        assert!(!head.contains("Connection: close"), "{head}");
        let echoed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(echoed["method"], "POST");
        assert_eq!(echoed["path"], "/free/in");
        assert_eq!(echoed["body"], "hello");
        assert_eq!(echoed["x"], "hi");
        assert_eq!(echoed["q"], "a b");
        assert_eq!(echoed["loopback"], true);
        let (status, _, body) = response(&mut stream);
        assert_eq!((status, body.contains("/free/two")), (200, true), "{body}");

        // HEAD is GET without the body; the length is still stated.
        stream.write_all(b"HEAD /free/x HTTP/1.1\r\n\r\n").unwrap();
        let mut bytes = Vec::new();
        let mut one = [0u8; 1];
        while !bytes.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut one).unwrap();
            bytes.push(one[0]);
        }
        let head = String::from_utf8(bytes).unwrap();
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        assert!(head.contains("Content-Length: "), "{head}");
        // No body follows: the next thing on the connection is the next answer.
        stream.write_all(b"GET /empty HTTP/1.1\r\n\r\n").unwrap();
        let (status, head, body) = response(&mut stream);
        assert_eq!((status, body.as_str()), (204, ""), "{head}");
        assert!(!head.contains("Content-Length"), "{head}");
        stream.write_all(b"GET /same HTTP/1.1\r\n\r\n").unwrap();
        let (status, head, body) = response(&mut stream);
        assert_eq!((status, body.as_str()), (304, ""), "{head}");
        assert!(head.contains("ETag: \"1\"\r\n"), "{head}");
        assert!(!head.contains("Content-Type"), "{head}");

        // An idle connection is hung up on, quietly.
        let started = Instant::now();
        assert!(closed(&mut stream));
        assert!(
            started.elapsed() >= Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
    }

    #[test]
    fn header_values_lose_spaces_and_tabs_only() {
        let server = start(quick());
        let (status, _, body, _) = send(
            server.addr,
            "GET /free HTTP/1.1\r\nX-Echo:\t hi there \t\r\nConnection: close\r\n\r\n",
        );
        assert_eq!(status, 200, "{body}");
        let echoed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(echoed["x"], "hi there");
        // Other whitespace is part of the value: a no-break space, or an ideographic one.
        let (status, _, body, _) = send(
            server.addr,
            "GET /free HTTP/1.1\r\nX-Echo: \u{a0}hi\u{3000}\r\nConnection: close\r\n\r\n",
        );
        assert_eq!(status, 200, "{body}");
        let echoed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(echoed["x"], "\u{a0}hi\u{3000}");
        // Every token character is a name character.
        let (status, _, body, _) = send(
            server.addr,
            "GET /free HTTP/1.1\r\nX!#$%&'*+-.^_`|~09: a\r\nConnection: close\r\n\r\n",
        );
        assert_eq!(status, 200, "{body}");
    }

    #[test]
    fn close_and_http_1_0_end_the_connection() {
        let server = start(quick());
        let mut stream = raw(server.addr, b"GET /free HTTP/1.1\r\nConnection: close\r\n\r\n");
        let (status, head, _) = response(&mut stream);
        assert_eq!(status, 200);
        assert!(head.contains("Connection: close\r\n"), "{head}");
        assert!(closed(&mut stream));
        let mut stream = raw(server.addr, b"GET /free HTTP/1.0\r\n\r\n");
        assert_eq!(response(&mut stream).0, 200);
        assert!(closed(&mut stream));
        let mut stream = raw(
            server.addr,
            b"GET /free HTTP/1.0\r\nConnection: keep-alive\r\n\r\nGET /free HTTP/1.1\r\n\r\n",
        );
        assert_eq!(response(&mut stream).0, 200);
        assert_eq!(response(&mut stream).0, 200);
    }

    #[test]
    fn a_chunked_body_is_read_after_100_continue() {
        let server = start(quick());
        let mut stream = raw(
            server.addr,
            b"POST /free HTTP/1.1\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
        );
        let mut go = [0u8; 25];
        stream.read_exact(&mut go).unwrap();
        assert_eq!(&go, b"HTTP/1.1 100 Continue\r\n\r\n");
        stream
            .write_all(b"5;note=1\r\nhello\r\nA\r\n, world!!!\r\n0\r\nChecksum: none\r\n\r\n")
            .unwrap();
        let (status, _, body) = response(&mut stream);
        assert_eq!(status, 200, "{body}");
        assert!(body.contains(r#""body":"hello, world!!!""#), "{body}");
        assert!(closed(&mut stream), "`Connection: close` closes it");

        // A body exactly at the limit, in chunks, is read.
        let full = "x".repeat(4096);
        let framed = format!(
            "POST /free HTTP/1.1\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n800\r\n{}\r\n800\r\n{}\r\n0\r\n\r\n",
            &full[..2048],
            &full[2048..]
        );
        let (status, _, body, _) = send(server.addr, &framed);
        assert_eq!(status, 200, "{body}");
        // As is one exactly at the limit by its length.
        let sized = format!("POST /free HTTP/1.1\r\nContent-Length: 4096\r\nConnection: close\r\n\r\n{full}");
        assert_eq!(send(server.addr, &sized).0, 200);
    }

    #[test]
    fn what_cannot_be_read_safely_is_refused_and_the_connection_closed() {
        let server = start(quick());
        let long_chunk_line = format!(
            "POST /free HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n1;{}\r\n",
            "x".repeat(2000)
        );
        let long_trailers = format!(
            "POST /free HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n{}",
            "X-Pad: yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy\r\n".repeat(400)
        );
        let long_line = format!("GET /free?{} HTTP/1.1\r\n\r\n", "q".repeat(MAX_HEAD + 10));
        let long_head = format!("GET /free HTTP/1.1\r\nX-Pad: {}\r\n\r\n", "x".repeat(20_000));
        let over = "x".repeat(4097);
        let cases: Vec<(Vec<u8>, u16, &str)> = vec![
            (b"GET /free\r\n\r\n".to_vec(), 400, "METHOD /path HTTP/1.1"),
            (
                b"GET  /free HTTP/1.1\r\n\r\n".to_vec(),
                400,
                "METHOD /path HTTP/1.1",
            ),
            (b"GET /free HTTP/2\r\n\r\n".to_vec(), 400, "HTTP/1.1"),
            (b"get /free HTTP/1.1\r\n\r\n".to_vec(), 400, "capitals"),
            (b"G3T /free HTTP/1.1\r\n\r\n".to_vec(), 400, "capitals"),
            (
                b"GETTINGEVENMORETHINGS /free HTTP/1.1\r\n\r\n".to_vec(),
                400,
                "capitals",
            ),
            (
                b"GET /free HTTP/1.1\r\nNo colon\r\n\r\n".to_vec(),
                400,
                "no colon",
            ),
            (
                b"GET /free HTTP/1.1\r\nBad name: x\r\n\r\n".to_vec(),
                400,
                "space",
            ),
            (b"GET /free HTTP/1.1\r\n folded: x\r\n\r\n".to_vec(), 400, "space"),
            (b"GET /\xff HTTP/1.1\r\n\r\n".to_vec(), 400, "UTF-8"),
            (
                b"POST /free HTTP/1.1\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec(),
                400,
                "both",
            ),
            (
                b"POST /free HTTP/1.1\r\nContent-Length: 2\r\nContent-Length: 3\r\n\r\nabc".to_vec(),
                400,
                "disagree",
            ),
            (
                b"POST /free HTTP/1.1\r\nContent-Length: +2\r\n\r\nab".to_vec(),
                400,
                "not a number",
            ),
            (
                b"POST /free HTTP/1.1\r\nContent-Length: two\r\n\r\n".to_vec(),
                400,
                "not a number",
            ),
            (
                b"POST /free HTTP/1.1\r\nTransfer-Encoding: gzip\r\n\r\n".to_vec(),
                501,
                "chunked",
            ),
            (
                b"POST /free HTTP/1.1\r\nTransfer-Encoding: gzip, chunked\r\n\r\n".to_vec(),
                501,
                "gzip, chunked",
            ),
            (
                b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n"
                    .to_vec(),
                501,
                "chunked, chunked",
            ),
            (
                b"POST /free HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n".to_vec(),
                400,
                "hexadecimal",
            ),
            (
                b"POST /free HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n+5\r\nhello\r\n0\r\n\r\n".to_vec(),
                400,
                "hexadecimal",
            ),
            (
                b"POST /free HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n\r\n".to_vec(),
                400,
                "hexadecimal",
            ),
            (
                b"POST /free HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nhello\r\n0\r\n\r\n".to_vec(),
                400,
                "longer than its size",
            ),
            (long_chunk_line.into_bytes(), 400, "size line is over 1024"),
            // Control characters: a bare LF would let one header line carry two.
            (
                b"POST /free HTTP/1.1\r\nX-Echo: hi\nContent-Length: 5\r\n\r\nhello".to_vec(),
                400,
                "control character",
            ),
            (
                b"GET /free HTTP/1.1\r\nX-Echo: a\x00b\r\n\r\n".to_vec(),
                400,
                "control character",
            ),
            (
                b"GET /free HTTP/1.1\r\nX-Echo: a\rb\r\n\r\n".to_vec(),
                400,
                "control character",
            ),
            (
                b"GET /free HTTP/1.1\r\nX-Echo: a\x7fb\r\n\r\n".to_vec(),
                400,
                "control character",
            ),
            (
                b"GET /fr\x01ee HTTP/1.1\r\n\r\n".to_vec(),
                400,
                "control character",
            ),
            (
                b"GET /free HTTP/1.1\nX-Echo: a\r\n\r\n".to_vec(),
                400,
                "control character",
            ),
            (
                b"GET /free HTTP/1.1\r\nX\x00Echo: a\r\n\r\n".to_vec(),
                400,
                "control character",
            ),
            (
                b"GET /free HTTP/1.1\r\nX(Echo): a\r\n\r\n".to_vec(),
                400,
                "header name",
            ),
            (
                b"GET /free HTTP/1.1\r\nX-\xc3\xa9: a\r\n\r\n".to_vec(),
                400,
                "header name",
            ),
            (
                b"GET /free HTTP/1.1\r\nX-Echo\t: a\r\n\r\n".to_vec(),
                400,
                "header name",
            ),
            (
                b"POST /free HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n5;a\x00b\r\nhello\r\n0\r\n\r\n"
                    .to_vec(),
                400,
                "control character",
            ),
            (
                b"POST /free HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n5;a\nb\r\nhello\r\n0\r\n\r\n"
                    .to_vec(),
                400,
                "control character",
            ),
            (
                b"POST /free HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nX: a\x00\r\n\r\n".to_vec(),
                400,
                "control character",
            ),
            (
                b"POST /free HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nX: a\nY: b\r\n\r\n".to_vec(),
                400,
                "control character",
            ),
            (long_trailers.into_bytes(), 431, "trailers"),
            (long_line.into_bytes(), 431, "over 16384 bytes"),
            (long_head.into_bytes(), 431, "over 16384 bytes"),
            (
                format!("POST /free HTTP/1.1\r\nContent-Length: 4097\r\n\r\n{over}").into_bytes(),
                413,
                "limits.body_bytes in server.toml",
            ),
            (
                b"POST /free HTTP/1.1\r\nContent-Length: 99999999999999\r\n\r\n".to_vec(),
                413,
                "over 4096 bytes",
            ),
        ];
        for (bytes, status, says) in cases {
            let mut stream = raw(server.addr, &bytes);
            let (got, head, body) = response(&mut stream);
            let sent = String::from_utf8_lossy(&bytes[..bytes.len().min(120)]).into_owned();
            assert_eq!(got, status, "{sent}: {body}");
            assert!(body.contains(says), "{sent}: {body}");
            assert!(body.contains(r#""fix""#), "{body}");
            assert!(head.contains("Connection: close\r\n"), "{head}");
            assert!(closed(&mut stream), "{sent}");
        }
    }

    #[test]
    fn a_chunked_body_past_the_limit_is_refused_even_when_its_sizes_would_wrap() {
        let server = start(quick());
        let chunked = |framed: &str| {
            let request = format!(
                "POST /free HTTP/1.1\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{framed}"
            );
            send(server.addr, &request)
        };
        // A small chunk, then a size that wraps `len + size` round to a small number.
        let (status, _, body, _) = chunked("1\r\n{\r\nffffffffffffffff\r\n");
        assert_eq!(status, 413, "{body}");
        assert!(body.contains("limits.body_bytes"), "{body}");
        // A size too long to be a number at all.
        assert_eq!(chunked("1ffffffffffffffffffff\r\n").0, 413);
        // Two chunks that only together pass the limit.
        let two = format!(
            "800\r\n{}\r\n801\r\n{}\r\n0\r\n\r\n",
            "x".repeat(0x800),
            "x".repeat(0x801)
        );
        assert_eq!(chunked(&two).0, 413);
    }

    #[test]
    fn a_caller_that_stalls_is_told_and_holds_no_worker() {
        // One worker, to make any hold-up show.
        let server = start(quick());
        let mut stalled: Vec<TcpStream> = Vec::new();
        for _ in 0..4 {
            // In the body, in the headers, in a chunk.
            stalled.push(raw(
                server.addr,
                b"POST /x HTTP/1.1\r\nContent-Length: 500\r\n\r\n{\"source\":",
            ));
            stalled.push(raw(server.addr, b"GET /x HTTP/1.1\r\nX-Half"));
            stalled.push(raw(
                server.addr,
                b"POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n10\r\nabc",
            ));
        }
        // Everyone else is answered at once, by the one worker and the writer.
        let (status, _, _, took) = send(server.addr, "GET /x HTTP/1.1\r\nConnection: close\r\n\r\n");
        assert_eq!(status, 200);
        assert!(took < Duration::from_millis(250), "a request took {took:?}");
        let post = "POST /x HTTP/1.1\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
        assert_eq!(send(server.addr, post).0, 200);

        // Each stalled caller is told so once its time is up, and hung up on.
        for stream in &mut stalled {
            let (status, _, body) = response(stream);
            assert_eq!(status, 408, "{body}");
            assert!(body.contains("too long"), "{body}");
            assert!(closed(stream));
        }
    }

    #[test]
    fn a_slow_request_keeps_out_neither_a_write_nor_a_free_one() {
        let server = start(quick());
        let free = "GET /free HTTP/1.1\r\nConnection: close\r\n\r\n";
        let work = "GET /x HTTP/1.1\r\nConnection: close\r\n\r\n";
        let write = "POST /x HTTP/1.1\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";

        // The one worker is taken by a slow request.
        let mut slow = raw(server.addr, b"GET /slow HTTP/1.1\r\nConnection: close\r\n\r\n");
        std::thread::sleep(Duration::from_millis(100));
        let (status, _, _, took) = send(server.addr, free);
        assert_eq!(status, 200);
        assert!(
            took < Duration::from_millis(250),
            "a free request waited {took:?}"
        );
        let (status, _, body, took) = send(server.addr, write);
        assert_eq!(status, 200, "{body}");
        assert!(took < Duration::from_millis(250), "a write waited {took:?}");
        // Another request waits its turn, but not for ever: it is told to come back.
        let (status, head, body, took) = send(server.addr, work);
        assert_eq!(status, 503, "{body}");
        assert!(head.contains("Retry-After: 5\r\n"), "{head}");
        assert!(body.contains("workers") && body.contains("server.toml"), "{body}");
        assert!(took >= Duration::from_millis(300), "it gave up after {took:?}");
        // The connection stays open after a busy answer.
        let mut kept = raw(server.addr, b"GET /x HTTP/1.1\r\n\r\n");
        assert_eq!(response(&mut kept).0, 503);
        server.echo.release();
        assert_eq!(response(&mut slow).0, 200);
        kept.write_all(b"GET /x HTTP/1.1\r\n\r\n").unwrap();
        assert_eq!(response(&mut kept).0, 200);
    }

    #[test]
    fn a_slow_write_keeps_out_other_writes_but_not_work() {
        let server = start(quick());
        let work = "GET /x HTTP/1.1\r\nConnection: close\r\n\r\n";
        let write = "POST /x HTTP/1.1\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
        let mut slow = raw(
            server.addr,
            b"POST /slow HTTP/1.1\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
        );
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(send(server.addr, work).0, 200);
        let (status, _, body, _) = send(server.addr, write);
        assert_eq!(status, 503, "{body}");
        assert!(body.contains("writer"), "{body}");
        server.echo.release();
        assert_eq!(response(&mut slow).0, 200);
        assert_eq!(send(server.addr, write).0, 200);
    }

    #[test]
    fn one_connection_too_many_is_told_to_come_back() {
        let server = start(Config {
            max_connections: 2,
            ..quick()
        });
        // Two open connections, each with a request answered so both are surely seated.
        let mut held = Vec::new();
        for _ in 0..2 {
            let mut stream = raw(server.addr, b"GET /free HTTP/1.1\r\n\r\n");
            assert_eq!(response(&mut stream).0, 200);
            held.push(stream);
        }
        let mut third = raw(server.addr, b"GET /free HTTP/1.1\r\n\r\n");
        let (status, head, body) = response(&mut third);
        assert_eq!(status, 503, "{body}");
        assert!(head.contains("Retry-After: 5\r\n"), "{head}");
        assert!(body.contains("2 connections are open already"), "{body}");
        assert!(closed(&mut third));
        assert_eq!(server.open.count(), 2);
        // A connection that ends gives its seat back.
        drop(held.pop());
        let started = Instant::now();
        while server.open.count() > 1 && started.elapsed() < Duration::from_secs(5) {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            send(server.addr, "GET /free HTTP/1.1\r\nConnection: close\r\n\r\n").0,
            200
        );
    }

    #[test]
    fn a_body_in_one_byte_chunks_costs_no_more_per_byte_than_any_other() {
        // `1\r\nx\r\n` over and over: three uses of the buffer for each byte of body. Moving
        // the rest of the buffer down at each would copy it all, about 64 KB, three times per
        // byte. Instead the buffer is compacted at most once per read from the socket, and
        // compacting copies, in all, no more than the socket brought.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let n = 200_000;
        let letter = |i: usize| b"abcdefghijklmnopqrstuvwxyz"[i % 26];
        let sender = std::thread::spawn(move || {
            let mut request = b"POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
            for i in 0..n {
                request.extend_from_slice(b"1\r\n");
                request.push(letter(i));
                request.extend_from_slice(b"\r\n");
            }
            request.extend_from_slice(b"0\r\n\r\n");
            let mut stream = TcpStream::connect(addr).unwrap();
            stream.write_all(&request).unwrap();
            (stream, request.len())
        });
        let (stream, peer) = listener.accept().unwrap();
        let mut connection = Connection::new(stream, peer);
        let config = quick();
        let config = Config {
            body_bytes: n,
            patience: Patience {
                body: Duration::from_secs(30),
                ..config.patience
            },
            ..config
        };
        let Ok((request, close)) = connection.next(&config) else {
            panic!("the request was refused");
        };
        let (_stream, sent) = sender.join().unwrap();
        assert!(!close);
        assert_eq!(request.body.len(), n);
        assert!(request.body.iter().enumerate().all(|(i, b)| *b == letter(i)));
        let cost = connection.cost;
        assert_eq!(cost.received, sent, "{cost:?}");
        assert!(
            cost.reads > 1,
            "it was all read at once, which tests nothing: {cost:?}"
        );
        assert!(cost.compactions <= cost.reads, "{cost:?}");
        assert!(cost.moved <= cost.received, "{cost:?}");
        // Each search starts where the last stopped: only the last three bytes of a read are
        // looked at again, in case a CRLF CRLF straddles it.
        assert!(cost.searched <= cost.received + 3 * cost.reads, "{cost:?}");
    }

    #[test]
    fn headers_that_arrive_in_pieces_are_searched_once() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut head = b"GET /x HTTP/1.1\r\n".to_vec();
        for i in 0..200 {
            head.extend_from_slice(format!("X-Pad-{i}: {}\r\n", "y".repeat(40)).as_bytes());
        }
        head.extend_from_slice(b"\r\n");
        let sent = head.len();
        let sender = std::thread::spawn(move || {
            let mut stream = TcpStream::connect(addr).unwrap();
            stream.set_nodelay(true).unwrap();
            for piece in head.chunks(50) {
                stream.write_all(piece).unwrap();
                std::thread::sleep(Duration::from_micros(200));
            }
            stream
        });
        let (stream, peer) = listener.accept().unwrap();
        let mut connection = Connection::new(stream, peer);
        let config = quick();
        let config = Config {
            patience: Patience {
                head: Duration::from_secs(30),
                ..config.patience
            },
            ..config
        };
        let Ok((request, _)) = connection.next(&config) else {
            panic!("the request was refused");
        };
        let _stream = sender.join().unwrap();
        assert_eq!(request.headers.len(), 200);
        let cost = connection.cost;
        assert_eq!(cost.received, sent, "{cost:?}");
        assert!(
            cost.reads > 10,
            "it arrived in too few pieces to test anything: {cost:?}"
        );
        assert!(cost.searched <= cost.received + 3 * cost.reads, "{cost:?}");
    }

    #[test]
    fn a_chunked_body_already_arrived_is_not_read_past_its_deadline() {
        // No time at all for the body: one that has already arrived whole, in the same packet as
        // the headers, is still refused, because the deadline is checked between chunks and
        // not only when more is read from the socket.
        let config = quick();
        let server = start(Config {
            patience: Patience {
                body: Duration::ZERO,
                ..config.patience
            },
            ..config
        });
        let chunks = "1\r\nx\r\n".repeat(64);
        let request = format!("POST /free HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n{chunks}0\r\n\r\n");
        let (status, _, body, _) = send(server.addr, &request);
        assert_eq!(status, 408, "{body}");
    }

    #[test]
    fn a_response_taken_slowly_is_given_up_on_at_its_deadline() {
        let config = quick();
        let server = start(Config {
            patience: Patience {
                write: Duration::from_millis(500),
                ..config.patience
            },
            ..config
        });
        let mut stream = raw(server.addr, b"GET /free/big HTTP/1.1\r\n\r\n");
        let mut some = [0u8; 4096];
        assert!(stream.read(&mut some).unwrap() > 0);
        assert_eq!(server.open.count(), 1);
        // The caller takes a little at a time, often enough that every single write the server
        // makes gets somewhere, but too slowly to take the whole answer in half a second.
        let started = Instant::now();
        while server.open.count() > 0 && started.elapsed() < Duration::from_secs(5) {
            let _ = stream.read(&mut some);
            std::thread::sleep(Duration::from_millis(5));
        }
        let held = started.elapsed();
        assert!(
            held < Duration::from_millis(1500),
            "the server was still writing after {held:?}; it should give up after 500 ms"
        );
    }

    #[test]
    fn the_default_patience_is_the_documented_one() {
        let patience = Patience::default();
        assert_eq!(patience.idle, Duration::from_secs(60));
        assert_eq!(patience.head, Duration::from_secs(10));
        assert_eq!(patience.body, Duration::from_secs(30));
        assert_eq!(patience.write, Duration::from_secs(30));
        assert_eq!(patience.queue, Duration::from_secs(10));
    }
}
