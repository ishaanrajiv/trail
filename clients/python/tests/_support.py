"""A fake Trail for the tests, and the path setup that lets them import `trail_client` from `src`."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
import unittest
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from trail_client import Client, Problem  # noqa: E402


@dataclass
class Received:
    path: str
    headers: dict[str, str]
    raw: bytes
    status: int

    @property
    def body(self) -> dict[str, Any]:
        parsed: dict[str, Any] = json.loads(self.raw)
        return parsed

    def ids(self, kind: str = "events") -> list[str]:
        return [str(item["id"]) for item in self.body.get(kind, [])]


class FakeTrail:
    """`POST /v1/ingest` on a loopback port. `answer()` chooses the reply; `down()` closes the
    port (no answer) and `up()` opens the same port again. `hold()` keeps every request waiting
    for its answer until `release()`; `arrived` counts requests read, answered or not.
    `html` makes a 200 answer a web page instead of Trail's answer, as a proxy's catch-all or a
    sign-on page would. `limit` answers 413 to a request with more records than it, and
    `max_bytes` to a body larger than it."""

    def __init__(self) -> None:
        self.requests: list[Received] = []
        self.status = 200
        self.rejected: list[dict[str, str]] = []
        self.mismatched: list[dict[str, str]] = []
        self.delay = 0.0
        self.html = False
        self.limit: int | None = None
        self.max_bytes: int | None = None
        self.arrived = 0
        self._released = threading.Event()
        self._released.set()
        self._lock = threading.Lock()
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self.port = 0
        self.up()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def answer(self, status: int, *, rejected: list[dict[str, str]] | None = None, mismatched: list[dict[str, str]] | None = None) -> None:
        self.status = status
        self.rejected = rejected or []
        self.mismatched = mismatched or []

    def hold(self) -> None:
        self._released.clear()

    def release(self) -> None:
        self._released.set()

    def up(self) -> None:
        fake = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length)
                with fake._lock:
                    fake.arrived += 1
                fake._released.wait()
                if fake.delay:
                    time.sleep(fake.delay)
                status = fake.status
                if (fake.limit is not None and _records(raw) > fake.limit) or (fake.max_bytes is not None and len(raw) > fake.max_bytes):
                    status = 413
                with fake._lock:
                    fake.requests.append(Received(self.path, {k.lower(): v for k, v in self.headers.items()}, raw, status))
                if status == 200:
                    try:
                        parsed: dict[str, Any] = json.loads(raw)
                    except ValueError:
                        parsed = {}
                    accepted = {kind: len(parsed.get(kind, [])) for kind in ("events", "metrics", "errors", "logs")}
                    reply: dict[str, Any] = {"accepted": accepted, "duplicates": 0, "rejected": fake.rejected, "mismatched": fake.mismatched}
                elif status == 400:
                    reply = {"error": {"at": "source", "message": "`Shop` is not a source name", "fix": "use lowercase letters, like `shop`"}}
                else:
                    reply = {"error": {"at": "", "message": f"status {status}", "fix": ""}}
                data = json.dumps(reply).encode()
                kind = "application/json"
                if status == 200 and fake.html:
                    data, kind = b"<!doctype html><html><body>Sign in to continue</body></html>", "text/html"
                self.send_response(status)
                self.send_header("Content-Type", kind)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, format: str, *args: Any) -> None:
                pass

        server = ThreadingHTTPServer(("127.0.0.1", self.port), Handler)
        server.daemon_threads = True
        self.port = server.server_address[1]
        self._server = server
        self._thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
        self._thread.start()

    def down(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None

    def bodies(self) -> list[dict[str, Any]]:
        with self._lock:
            return [request.body for request in self.requests]

    def count(self) -> int:
        with self._lock:
            return len(self.requests)

    def wait_for(self, count: int, timeout: float = 3.0) -> bool:
        return wait_until(lambda: self.count() >= count, timeout)


def _records(raw: bytes) -> int:
    try:
        parsed: dict[str, Any] = json.loads(raw)
    except ValueError:
        return 0
    return sum(len(parsed.get(kind, [])) for kind in ("events", "metrics", "errors", "logs"))


def read_bytes(path: str) -> bytes:
    with open(path, "rb") as file:
        return file.read()


def read_json(path: str) -> dict[str, Any]:
    parsed: dict[str, Any] = json.loads(read_bytes(path))
    return parsed


def wait_until(condition: Callable[[], bool], timeout: float = 3.0) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if condition():
            return True
        time.sleep(0.01)
    return condition()


class TrailTestCase(unittest.TestCase):
    """Each test gets a fake Trail, a temporary spool folder and a clean TRAIL_* environment."""

    def setUp(self) -> None:
        saved = {key: value for key, value in os.environ.items() if key.startswith("TRAIL_")}
        for key in saved:
            del os.environ[key]

        def restore() -> None:
            for key in [key for key in os.environ if key.startswith("TRAIL_")]:
                del os.environ[key]
            os.environ.update(saved)

        self.addCleanup(restore)
        self.fake = FakeTrail()
        self.addCleanup(self.fake.down)
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.spool = os.path.join(folder.name, "spool")
        self.problems: list[Problem] = []

    def client(self, **options: Any) -> Client:
        settings: dict[str, Any] = {
            "url": self.fake.url,
            "host": "web-1",
            "spool_dir": self.spool,
            "flush_interval": 0.05,
            "on_problem": self.problems.append,
        }
        settings.update(options)
        client = Client("shop", **settings)
        self.addCleanup(client.close, 0.5)
        return client

    def spool_files(self, folder: str | None = None) -> list[str]:
        folder = folder or self.spool
        if not os.path.isdir(folder):
            return []
        return sorted(name for name in os.listdir(folder) if name.endswith(".json") and not name.startswith("."))

    def kinds(self) -> list[str]:
        return [problem.kind for problem in self.problems]
