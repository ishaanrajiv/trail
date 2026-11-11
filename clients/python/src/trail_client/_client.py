"""The Trail client: a queue, a sender thread and a spool. `spec/client.md` is the contract."""

from __future__ import annotations

import atexit
import http.client
import json
import math
import os
import socket
import sys
import threading
import traceback
import urllib.error
import urllib.request
import weakref
from collections import deque
from dataclasses import dataclass
from time import monotonic
from typing import Any, Callable, Literal, Mapping, cast

from ._spool import KINDS, DirSpool, MemorySpool, Spool, count_records, split_body
from ._uuid import now_ms, reset_after_fork, uuid7

DEFAULT_URL = "http://127.0.0.1:8200"
_EXIT_TIMEOUT = 2.0  # what `atexit` and the exception hooks give the queue
_REQUEST_TIMEOUT = 10.0
_MAX_BACKOFF = 60.0
_EVENT, _METRIC, _ERROR, _LOG = 0, 1, 2, 3
_FINAL_DROP = (400, 401, 413)

ProblemKind = Literal["rejected", "mismatched", "dropped", "send"]
# What became of one post: finished with (taken, or dropped because a retry cannot succeed), kept
# to retry, or refused with 413 and to be sent again as two halves.
_Outcome = Literal["done", "keep", "split"]
_NOT_TRAIL_FIX = (
    "check that TRAIL_URL points at Trail itself, and that nothing between the app and Trail "
    "(a proxy, a sign-on page, a captive portal) answers in its place"
)


@dataclass(frozen=True)
class Context:
    """Who and what an event belongs to. Every field is optional."""

    actor: str | None = None
    client: str | None = None
    trace_id: str | None = None
    ts: int | None = None  # ms; default now


@dataclass(frozen=True)
class Problem:
    """Something the client or the server found: where, what is wrong, and what to change."""

    kind: ProblemKind
    at: str
    message: str
    fix: str

    def __str__(self) -> str:
        text = f"{self.kind} {self.at}: {self.message}"
        return f"{text}\n  fix: {self.fix}" if self.fix else text


@dataclass(frozen=True)
class Stats:
    """Record counts since the client started."""

    sent: int = 0  # accepted by Trail (including duplicates)
    spooled: int = 0  # waiting in the spool after a failed send
    dropped: int = 0  # lost: queue or spool full, unserialisable, or refused with 400/401/413
    rejected: int = 0  # refused one by one in a 200 response
    mismatched: int = 0  # stored but marked as differing from their declaration


class TrailError(Exception):
    """Raised by `flush()` in strict mode, listing every problem since the last flush."""

    def __init__(self, problems: list[Problem]) -> None:
        self.problems = problems
        lines = "\n".join(str(problem) for problem in problems)
        super().__init__(f"{len(problems)} Trail problem(s):\n{lines}")


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def _env(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value or None


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """A redirect is answered like any other unexpected status: kept and retried, never followed
    as a GET that would lose the body."""

    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


_clients: weakref.WeakSet[Client] = weakref.WeakSet()


def _after_fork_in_child() -> None:
    reset_after_fork()
    for client in list(_clients):
        client._after_fork_in_child()  # pyright: ignore[reportPrivateUsage] -- module-private hook


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork_in_child)


class Client:
    """Queues records and sends them to Trail in the background. Never raises into the caller
    unless `strict` is on, and then only from `flush()`."""

    def __init__(
        self,
        source: str,
        *,
        url: str | None = None,
        version: str | None = None,
        host: str | None = None,
        token: str | None = None,
        spool_dir: str | os.PathLike[str] | None = None,
        flush_interval: float = 1.0,
        max_batch: int = 200,
        max_queue: int = 10_000,
        max_spool_bytes: int = 50_000_000,
        strict: bool | None = None,
        on_problem: Callable[[Problem], None] | None = None,
    ) -> None:
        self.source = source
        self.disabled = _env_flag("TRAIL_DISABLED")
        self.strict = _env_flag("TRAIL_STRICT") if strict is None else strict
        base = url or _env("TRAIL_URL") or DEFAULT_URL
        self.endpoint = base.rstrip("/") + "/v1/ingest"
        # An empty string passed in code means "leave it out".
        self.version = (version if version is not None else _env("TRAIL_VERSION")) or None
        self.host = (host if host is not None else (_env("TRAIL_HOST") or socket.gethostname())) or None
        self.token = (token if token is not None else _env("TRAIL_TOKEN")) or None
        self.flush_interval = max(0.001, flush_interval)
        self.max_batch = max(1, max_batch)
        self.max_queue = max(1, max_queue)
        self.max_spool_bytes = max(0, max_spool_bytes)
        self._on_problem = on_problem

        self._lock = threading.RLock()
        self._wake = threading.Condition(self._lock)
        self._send_lock = threading.RLock()
        self._queue: deque[tuple[int, str]] = deque()
        self._thread: threading.Thread | None = None
        self._stopping = False
        self._closed = False
        self._pid = os.getpid()
        self._backoff = 0.0
        self._next_attempt = 0.0
        self._failing = False
        self._strict_problems: list[Problem] = []
        self._unreported_drops = {"queue": 0, "spool": 0}
        # Spool entries being posted right now. The spool loop and the size bound leave them alone.
        self._in_flight: set[str] = set()
        self._spool_write_failing = False
        self._sent = self._spooled = self._dropped = self._rejected = self._mismatched = 0
        self._hooks_installed = False
        self._opener = urllib.request.build_opener(_NoRedirect())

        if self.disabled:
            self._spool: Spool = MemorySpool()
            return

        folder = os.fspath(spool_dir) if spool_dir is not None else _env("TRAIL_SPOOL_DIR")
        self._spool = MemorySpool()
        if folder:
            try:
                self._spool = DirSpool(folder)
            except OSError as exc:
                self._problem(
                    "send",
                    "spool_dir",
                    f"cannot use `{folder}` as the spool folder ({exc}); batches that fail wait in memory",
                    "pass a folder the process can create and write, like `/var/lib/shop/trail-spool`",
                )

        ref = weakref.ref(self)

        def at_exit() -> None:
            client = ref()
            if client is not None:
                client.close(_EXIT_TIMEOUT)

        self._at_exit = at_exit
        atexit.register(at_exit)
        _clients.add(self)
        try:
            if self._spool.names():  # a previous run left batches: start sending them now
                with self._lock:
                    self._ensure_thread()
        except Exception as exc:
            self._problem("send", "spool", f"cannot read the spool: {exc}", "check the spool folder's permissions")

    # ---------- recording ----------

    def record_event(self, name: str, props: Mapping[str, Any], context: Context | None = None) -> None:
        """Called by generated code. Application code calls the generated functions instead."""
        if self.disabled:
            return
        try:
            context = context or Context()
            record: dict[str, Any] = {"id": uuid7(), "name": name, "ts": context.ts if context.ts is not None else now_ms()}
            if context.actor is not None:
                record["actor"] = context.actor
            if context.client is not None:
                record["client"] = context.client
            if context.trace_id is not None:
                record["trace_id"] = context.trace_id
            record["props"] = dict(props)
            self._push(_EVENT, record, f"event {name}")
        except Exception as exc:
            self._internal(f"event {name}", exc)

    def record_metric(
        self,
        name: str,
        value: float,
        labels: Mapping[str, str],
        *,
        unit: str | None,
        kind: Literal["gauge", "counter"],
        ts: int | None = None,
    ) -> None:
        """Called by generated code. Application code calls the generated functions instead."""
        if self.disabled:
            return
        try:
            if not _is_finite_number(value):
                self._drop_one(
                    f"metric {name}",
                    f"the value {value!r} is not a finite number",
                    "pass an int or float that is not NaN or infinite",
                )
                return
            record: dict[str, Any] = {"name": name, "value": value, "ts": ts if ts is not None else now_ms()}
            record["labels"] = dict(labels)
            if unit is not None:
                record["unit"] = unit
            record["kind"] = kind
            self._push(_METRIC, record, f"metric {name}")
        except Exception as exc:
            self._internal(f"metric {name}", exc)

    def error(
        self,
        error: BaseException | None = None,
        *,
        kind: str | None = None,
        message: str | None = None,
        severity: Literal["warning", "error", "fatal"] = "error",
        context: Mapping[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> None:
        """Report an exception, or an error described by `kind` and `message`. Called with no
        arguments inside an `except` block, it reports the exception being handled."""
        if self.disabled:
            return
        try:
            exc = error
            if exc is None and kind is None and message is None:
                exc = sys.exc_info()[1]
            stack: str | None = None
            if exc is not None:
                kind = kind or type(exc).__name__
                message = message if message is not None else str(exc)
                if exc.__traceback__ is not None:
                    stack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            record: dict[str, Any] = {
                "id": uuid7(),
                "ts": now_ms(),
                "severity": severity,
                "kind": kind or "Error",
                "message": message or "",
            }
            if stack:
                record["stack"] = stack
            if context is not None:
                record["context"] = dict(context)
            if trace_id is not None:
                record["trace_id"] = trace_id
            self._push(_ERROR, record, f"error {record['kind']}")
        except Exception as exc:
            self._internal("error", exc)

    def log(
        self,
        level: Literal["trace", "debug", "info", "warn", "error", "fatal"],
        message: str,
        *,
        stream: str | None = None,
        fields: Mapping[str, Any] | None = None,
    ) -> None:
        if self.disabled:
            return
        try:
            record: dict[str, Any] = {"id": uuid7(), "ts": now_ms(), "level": level}
            if stream is not None:
                record["stream"] = stream
            record["message"] = message
            if fields is not None:
                record["fields"] = dict(fields)
            self._push(_LOG, record, "log")
        except Exception as exc:
            self._internal("log", exc)

    def _push(self, kind: int, record: dict[str, Any], at: str) -> None:
        try:
            text = _dumps(record)
        except (TypeError, ValueError, RecursionError) as exc:
            self._drop_one(
                at,
                f"cannot be sent as JSON: {exc}",
                "pass only str, int, float (not NaN), bool, None, and lists and dicts of those with str keys",
            )
            return
        with self._lock:
            if self._closed:
                self._dropped += 1
                return
            self._queue.append((kind, text))
            while len(self._queue) > self.max_queue:
                self._queue.popleft()
                self._dropped += 1
                self._unreported_drops["queue"] += 1
            self._ensure_thread()
            if len(self._queue) >= self.max_batch:
                self._wake.notify()

    def _drop_one(self, at: str, message: str, fix: str) -> None:
        with self._lock:
            self._dropped += 1
        self._problem("dropped", at, message + "; the record was dropped", fix)

    def _internal(self, at: str, exc: Exception) -> None:
        try:
            self._problem("dropped", at, f"the client failed: {exc!r}; the record was dropped", "report this as a trail-client bug")
        except Exception:
            pass

    # ---------- problems ----------

    def _problem(self, kind: ProblemKind, at: str, message: str, fix: str) -> None:
        problem = Problem(kind, at, message, fix)
        if self.strict:
            with self._lock:
                self._strict_problems.append(problem)
        if self._on_problem is not None:
            try:
                self._on_problem(problem)
            except Exception:
                pass  # a broken callback must not stop the sender

    def _report_drops(self) -> None:
        with self._lock:
            queue, spool = self._unreported_drops["queue"], self._unreported_drops["spool"]
            self._unreported_drops = {"queue": 0, "spool": 0}
        if queue:
            self._problem(
                "dropped",
                "queue",
                f"{queue} record(s) dropped, oldest first: the queue holds max_queue={self.max_queue}",
                "check that Trail is reachable; set spool_dir so waiting batches go to disk, or raise max_queue",
            )
        if spool:
            self._problem(
                "dropped",
                "spool",
                f"{spool} record(s) dropped, oldest first: the spool holds max_spool_bytes={self.max_spool_bytes}",
                "check that Trail is reachable, or raise max_spool_bytes",
            )

    # ---------- sending ----------

    def _ensure_thread(self) -> None:
        """Start the sender on first use. Called with `_lock` held."""
        if self._thread is None and not self._stopping:
            thread = threading.Thread(target=self._run, name=f"trail-{self.source}", daemon=True)
            self._thread = thread
            thread.start()

    def _sender_should_stop(self) -> bool:
        """The background thread stops between batches once `close()` starts; `close()`'s own
        flush, in the caller's thread, carries on."""
        return self._stopping and threading.current_thread() is self._thread

    def _in_backoff(self) -> bool:
        return monotonic() < self._next_attempt

    def _run(self) -> None:
        while True:
            with self._wake:
                deadline = monotonic() + self.flush_interval
                full_only = True
                while not self._stopping and not (len(self._queue) >= self.max_batch and not self._in_backoff()):
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        full_only = False
                        break
                    self._wake.wait(remaining)
                if self._stopping:
                    return
            try:
                # Woken by a full batch, send only full batches; the rest waits for the interval.
                self._cycle(force=False, deadline=monotonic() + 2 * _REQUEST_TIMEOUT, full_only=full_only)
            except Exception as exc:
                self._internal("sender", exc)

    def _cycle(self, *, force: bool, deadline: float, full_only: bool = False) -> bool:
        """One pass: the spool oldest first, then the queue. True when nothing failed."""
        if not self._send_lock.acquire(timeout=max(0.0, deadline - monotonic())):
            return False
        try:
            if not force and self._in_backoff():
                if self._spool.durable:
                    self._spool_queue()  # keep waiting records on disk, not just in memory
                return False
            return self._drain_spool(deadline) and self._drain_queue(deadline, full_only)
        finally:
            self._send_lock.release()
            self._report_drops()

    def _drain_spool(self, deadline: float) -> bool:
        """Post the spool's entries oldest first. An entry split after a `413` is replaced by its
        two halves, and the pass starts again from the oldest, so the halves go out in this pass
        too. Each split leaves smaller entries, so the passes end."""
        while True:
            try:
                names = self._spool.names()
            except OSError as exc:
                self._problem("send", "spool", f"cannot read the spool: {exc}", "check the spool folder's permissions")
                return False
            split = False
            for name in names:
                if monotonic() >= deadline or self._sender_should_stop():
                    return False
                with self._lock:
                    if name in self._in_flight:
                        continue  # being posted already, further up this thread's stack
                    self._in_flight.add(name)
                try:
                    try:
                        body = self._spool.read(name)
                    except OSError as exc:
                        self._problem("send", f"spool {name}", f"cannot read: {exc}", "check the spool folder's permissions")
                        return False
                    if body is None:
                        continue
                    outcome = self._deliver(body, count_records(body), deadline)
                finally:
                    with self._lock:
                        self._in_flight.discard(name)
                if outcome == "keep":
                    return False
                if outcome == "split":
                    if not self._split(body, replacing=name):
                        return False
                    split = True
                    break
                if not self._remove_sent(name):
                    return False
            if not split:
                return True

    def _remove_sent(self, name: str) -> bool:
        """Delete a spool entry that got a final answer. False when it cannot be deleted (it is
        then sent again later, which the ids make harmless)."""
        try:
            self._spool.remove(name)
        except OSError as exc:
            self._problem("send", f"spool {name}", f"cannot delete a sent batch: {exc}", "check the spool folder's permissions")
            return False
        return True

    def _take(self, full_only: bool = False) -> list[tuple[int, str]]:
        with self._lock:
            if full_only and len(self._queue) < self.max_batch:
                return []
            count = min(self.max_batch, len(self._queue))
            return [self._queue.popleft() for _ in range(count)]

    def _drain_queue(self, deadline: float, full_only: bool = False) -> bool:
        while monotonic() < deadline and not self._sender_should_stop():
            items = self._take(full_only)
            if not items:
                return True
            outcome = self._send_batch(items, deadline)
            if outcome == "keep":
                return False
            if outcome == "split" and not self._drain_spool(deadline):  # the halves are in the spool now
                return False
        with self._lock:
            return not self._queue

    def _send_batch(self, items: list[tuple[int, str]], deadline: float) -> _Outcome:
        """Send one batch from the queue. "done" when it is finished with, "split" when it was
        refused with `413` and its two halves are now in the spool, "keep" when it is kept.

        With a spool folder the batch is written there before it is posted and deleted only after
        a final answer, so a process that dies mid-request leaves it for the next run. Without
        one (or when the write fails) it is posted from memory and kept only if the post fails."""
        body = self._body(items)
        name = self._spool_before_send(body) if self._spool.durable else None
        if name is None:
            outcome = self._deliver(body, len(items), deadline)
            if outcome == "done" or (outcome == "split" and self._split(body, replacing=None)):
                return outcome
            if self._spool.durable:
                self._requeue(items)  # the spool could not be written a moment ago; already reported
            else:
                self._keep(items, body)
            return "keep"
        try:
            outcome = self._deliver(body, len(items), deadline)
        finally:
            with self._lock:
                self._in_flight.discard(name)
        if outcome == "done":
            return "done" if self._remove_sent(name) else "keep"
        if outcome == "split":
            return "split" if self._split(body, replacing=name) else "keep"
        with self._lock:
            self._spooled += len(items)
        self._bound_spool()
        return "keep"

    def _split(self, body: bytes, *, replacing: str | None) -> bool:
        """Put the two halves of a batch refused with `413` in the spool, in place of its own
        entry `replacing` if it has one. The halves are written before that entry is deleted, so
        a crash in between leaves records twice (harmless: they have ids), never lost. False when
        they cannot be written; the batch is then kept whole and tried again later."""
        halves = split_body(body)
        if halves is None:
            return False
        written: list[str] = []
        try:
            for half in halves:
                written.append(self._spool.write(half))
        except OSError as exc:
            for name in written:
                try:
                    self._spool.remove(name)
                except OSError:
                    pass  # a leftover half is sent twice at worst, which the ids make harmless
            self._problem(
                "send",
                "spool",
                f"cannot write the two halves of a batch Trail found too large (413): {exc}; the batch is kept whole",
                "check the spool folder's permissions and free space",
            )
            return False
        return replacing is None or self._remove_sent(replacing)

    def _spool_before_send(self, body: bytes) -> str | None:
        """Write a batch about to be posted to the spool and mark it in flight. None when the
        write fails; the batch is then sent from memory."""
        try:
            name = self._spool.write(body)
        except OSError as exc:
            if not self._spool_write_failing:
                self._spool_write_failing = True
                self._problem(
                    "send",
                    "spool",
                    f"cannot write to the spool: {exc}; batches are sent from memory until it works again",
                    "check the spool folder's permissions and free space",
                )
            return None
        self._spool_write_failing = False
        with self._lock:
            self._in_flight.add(name)
        self._bound_spool()  # makes room by dropping the oldest entries, never the one in flight
        return name

    def _spool_queue(self) -> None:
        while True:
            items = self._take()
            if not items:
                return
            if not self._keep(items, self._body(items)):
                return

    def _keep(self, items: list[tuple[int, str]], body: bytes) -> bool:
        """Put a batch that was not sent into the spool; back in the queue if the spool fails."""
        try:
            self._spool.write(body)
        except OSError as exc:
            self._requeue(items)
            self._problem("send", "spool", f"cannot write to the spool: {exc}", "check the spool folder's permissions and free space")
            return False
        with self._lock:
            self._spooled += len(items)
        self._bound_spool()
        return True

    def _requeue(self, items: list[tuple[int, str]]) -> None:
        """Put a batch back at the front of the queue, dropping the oldest past `max_queue`."""
        with self._lock:
            self._queue.extendleft(reversed(items))
            while len(self._queue) > self.max_queue:
                self._queue.popleft()
                self._dropped += 1
                self._unreported_drops["queue"] += 1

    def _bound_spool(self) -> None:
        """Drop the oldest entries until the spool fits `max_spool_bytes`. An entry in flight is
        never dropped; once its post fails it is an ordinary entry and counts like any other."""
        try:
            total = self._spool.total_bytes()
            if total <= self.max_spool_bytes:
                return
            with self._lock:
                in_flight = set(self._in_flight)
            for name in self._spool.names():
                if total <= self.max_spool_bytes:
                    break
                if name in in_flight:
                    continue
                body = self._spool.read(name) or b""
                self._spool.remove(name)
                total -= len(body)
                count = count_records(body)
                with self._lock:
                    self._dropped += count
                    self._unreported_drops["spool"] += count
        except OSError as exc:
            self._problem("send", "spool", f"cannot trim the spool: {exc}", "check the spool folder's permissions")

    def _body(self, items: list[tuple[int, str]]) -> bytes:
        """The request body for a batch, in the order `spec/ingest.md` shows."""
        parts = [f'"source":{_dumps(self.source)}']
        if self.version is not None:
            parts.append(f'"version":{_dumps(self.version)}')
        if self.host is not None:
            parts.append(f'"host":{_dumps(self.host)}')
        groups: list[list[str]] = [[], [], [], []]
        for kind, text in items:
            groups[kind].append(text)
        for index, name in enumerate(KINDS):
            if groups[index]:
                parts.append(f'"{name}":[{",".join(groups[index])}]')
        return ("{" + ",".join(parts) + "}").encode("utf-8")

    def _post(self, body: bytes, deadline: float) -> tuple[int | None, bytes, str]:
        """Status (None for no answer), response body, and what went wrong."""
        headers = {"Content-Type": "application/json", "User-Agent": "trail-client-python/0.1.0"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        request = urllib.request.Request(self.endpoint, data=body, headers=headers, method="POST")
        timeout = min(_REQUEST_TIMEOUT, max(0.05, deadline - monotonic()))
        try:
            with self._opener.open(request, timeout=timeout) as response:
                status: int = response.status
                return status, response.read(), ""
        except urllib.error.HTTPError as exc:
            try:
                payload = exc.read()
            except Exception:
                payload = b""
            finally:
                exc.close()
            return exc.code, payload, f"HTTP {exc.code}"
        except (OSError, http.client.HTTPException, ValueError) as exc:
            return None, b"", f"no answer from {self.endpoint}: {exc}"

    def _deliver(self, body: bytes, count: int, deadline: float) -> _Outcome:
        """Post one batch. "done" when it is finished with (taken, or dropped because a retry
        cannot succeed), "keep" when it should be kept and retried, "split" when Trail refused it
        as too large (413) and it holds more than one record: the caller sends it as two halves.

        Only a 2xx whose body is Trail's ingest answer means Trail took the batch. A proxy's
        catch-all page, a sign-on wall or a captive portal can answer 200 with HTML."""
        status, payload, failure = self._post(body, deadline)
        if status is not None and 200 <= status < 300:
            answer = _ingest_answer(payload)
            if answer is None:
                self._failed(
                    f"the {status} answer from {self.endpoint} did not come from Trail: its body is not Trail's ingest answer"
                    f" (a JSON object with `accepted`); {_preview(payload)}",
                    _NOT_TRAIL_FIX,
                )
                return "keep"
            self._succeeded()
            self._read_response(answer, count)
            return "done"
        if status == 413 and count > 1:
            self._succeeded()
            return "split"
        if status in _FINAL_DROP:
            self._succeeded()
            with self._lock:
                self._dropped += count
            self._problem("dropped", self.endpoint, f"{count} record(s) refused with {status}: {_server_message(payload)}", _drop_fix(status, self))
            return "done"
        self._failed(failure or f"HTTP {status}")
        return "keep"

    def _read_response(self, data: dict[str, Any], count: int) -> None:
        rejected = _entries(data, "rejected")
        mismatched = _entries(data, "mismatched")
        with self._lock:
            self._sent += max(0, count - len(rejected))
            self._rejected += len(rejected)
            self._mismatched += len(mismatched)
        for at, message, fix in rejected:
            self._problem("rejected", at, message, fix)
        for at, message, fix in mismatched:
            self._problem("mismatched", at, message, fix)

    def _succeeded(self) -> None:
        self._backoff = 0.0
        self._next_attempt = 0.0
        self._failing = False

    def _failed(self, failure: str, fix: str = "check that Trail is running and TRAIL_URL points at it") -> None:
        self._backoff = 1.0 if self._backoff == 0 else min(_MAX_BACKOFF, self._backoff * 2)
        self._next_attempt = monotonic() + self._backoff
        first = not self._failing
        self._failing = True
        if first or self.strict:
            where = "the spool folder" if self._spool.durable else "memory"
            self._problem(
                "send",
                self.endpoint,
                f"{failure}; batches wait in {where} and are retried with backoff",
                fix,
            )

    # ---------- flush, close, stats ----------

    def _pending(self, *, in_flight: bool = True) -> bool:
        """Records waiting in the queue or the spool. `in_flight=False` leaves out spool entries
        being posted right now."""
        with self._lock:
            if self._queue:
                return True
            posting: set[str] = set() if in_flight else set(self._in_flight)
        try:
            return any(name not in posting for name in self._spool.names())
        except OSError:
            return True

    def flush(self, timeout: float = 5.0) -> bool:
        """Send what is queued and spooled now, ignoring backoff. True when both are empty.
        In strict mode, raises `TrailError` listing every problem since the last flush."""
        if self.disabled:
            return True
        deadline = monotonic() + max(0.0, timeout)
        empty = False
        try:
            while True:
                ok = self._cycle(force=True, deadline=deadline)
                # A flush called from `on_problem` runs inside a post; that batch is not its to wait for.
                if not self._pending(in_flight=False) or not ok or monotonic() >= deadline:
                    break
            empty = not self._pending()
        except Exception as exc:
            self._internal("flush", exc)
        if self.strict:
            with self._lock:
                problems, self._strict_problems = self._strict_problems, []
            if not empty and not any(problem.kind == "send" for problem in problems):
                problems.append(Problem("send", self.endpoint, "records are still waiting after flush", "check that Trail is reachable, or pass a longer timeout"))
            if problems:
                raise TrailError(problems)
        return empty

    def close(self, timeout: float = 5.0) -> None:
        """Flush, stop the sender, and write what is left to the spool folder. Safe to call twice."""
        with self._lock:
            if self._closed or self.disabled:
                return
            self._closed = True
        try:
            atexit.unregister(self._at_exit)
        except Exception:
            pass
        with self._wake:
            self._stopping = True
            self._wake.notify_all()
            thread = self._thread
        try:
            self.flush(timeout)
        except TrailError:
            pass
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=0.5)
        locked = self._send_lock.acquire(timeout=0.5)
        try:
            if self._spool.durable:
                self._spool_queue()
            with self._lock:
                left = len(self._queue)
                self._queue.clear()
                self._dropped += left
        finally:
            if locked:
                self._send_lock.release()
        self._report_drops()
        if left:
            self._problem("dropped", "close", f"{left} record(s) were not sent before close", "set spool_dir so they wait on disk for the next run")

    def stats(self) -> Stats:
        with self._lock:
            return Stats(self._sent, self._spooled, self._dropped, self._rejected, self._mismatched)

    # ---------- hooks ----------

    def install_excepthook(self) -> None:
        """Report uncaught exceptions (`sys.excepthook` and `threading.excepthook`), flush briefly,
        then hand them to the hooks that were there before."""
        if self.disabled or self._hooks_installed:
            return
        self._hooks_installed = True
        previous = sys.excepthook
        previous_thread = threading.excepthook

        def excepthook(exc_type: type[BaseException], exc: BaseException, tb: Any) -> None:
            try:
                if not issubclass(exc_type, KeyboardInterrupt):
                    self.error(exc, severity="fatal")
                    self.flush(_EXIT_TIMEOUT)
            except Exception:
                pass
            previous(exc_type, exc, tb)

        def thread_excepthook(args: threading.ExceptHookArgs) -> None:
            try:
                exc = args.exc_value
                if exc is not None and not isinstance(exc, SystemExit):
                    self.error(exc, severity="error")
                    self.flush(_EXIT_TIMEOUT)
            except Exception:
                pass
            previous_thread(args)

        sys.excepthook = excepthook
        threading.excepthook = thread_excepthook

    # ---------- fork ----------

    def _after_fork_in_child(self) -> None:
        """The child starts empty, with fresh locks; the parent still sends what it had queued.
        Its own sender thread starts on first use."""
        self._lock = threading.RLock()
        self._wake = threading.Condition(self._lock)
        self._send_lock = threading.RLock()
        self._queue = deque()
        self._in_flight = set()
        self._thread = None
        self._pid = os.getpid()
        if isinstance(self._spool, MemorySpool):
            self._spool.clear()


def _is_finite_number(value: object) -> bool:
    """Generated code passes typed numbers, but a caller without a type checker may not."""
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _entries(parsed: dict[str, Any], key: str) -> list[tuple[str, str, str]]:
    items: Any = parsed.get(key)
    found: list[tuple[str, str, str]] = []
    if not isinstance(items, list):
        return found
    for item in cast("list[Any]", items):
        if isinstance(item, dict):
            entry = cast("dict[str, Any]", item)
            found.append((str(entry.get("at", "")), str(entry.get("message", "")), str(entry.get("fix", ""))))
    return found


def _ingest_answer(payload: bytes) -> dict[str, Any] | None:
    """The body of a 2xx when it is Trail's answer to `POST /v1/ingest`: a JSON object with
    `accepted` (`spec/ingest.md`). None for anything else, like an HTML page."""
    try:
        parsed: Any = json.loads(payload)
    except ValueError:
        return None
    if not isinstance(parsed, dict) or "accepted" not in parsed:
        return None
    return cast("dict[str, Any]", parsed)


def _preview(payload: bytes) -> str:
    text = " ".join(payload[:80].decode("utf-8", "replace").split())
    return f"it begins `{text}`" if text else "it is empty"


def _server_message(payload: bytes) -> str:
    try:
        parsed: Any = json.loads(payload)
        problem: Any = parsed["error"]
        text = f"{problem['at']}: {problem['message']}"
        return f"{text} (fix: {problem['fix']})" if problem.get("fix") else text
    except Exception:
        return payload[:200].decode("utf-8", "replace") or "no body"


def _drop_fix(status: int, client: Client) -> str:
    if status == 401:
        return f"pass token= or set TRAIL_TOKEN to the write token for source `{client.source}`"
    if status == 413:
        return "the record is over Trail's size limit on its own; send smaller props, messages or fields"
    return "fix what the server names; a retry of the same batch cannot succeed"
