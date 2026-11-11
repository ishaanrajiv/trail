"""Every row of the behaviour table in `spec/client.md`, against a fake Trail."""

from __future__ import annotations

import math
import os
import re
import socket
import subprocess
import sys
import textwrap
import threading
import time
import unittest
import warnings
from typing import Any

from _support import SRC, FakeTrail, TrailTestCase, read_bytes, read_json, wait_until

from trail_client import Client, Context, TrailError, uuid7

UUID7 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


def run_script(code: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    full = {**{k: v for k, v in os.environ.items() if not k.startswith("TRAIL_")}, **env, "PYTHONPATH": SRC}
    return subprocess.run([sys.executable, "-c", textwrap.dedent(code)], env=full, capture_output=True, text=True, timeout=20)


class Recording(TrailTestCase):
    def test_recording_returns_immediately_while_trail_is_slow(self) -> None:
        self.fake.delay = 0.5
        client = self.client(max_batch=10)
        start = time.monotonic()
        for index in range(500):
            client.record_event("shop.order.placed", {"order_id": f"o-{index}"})
        self.assertLess(time.monotonic() - start, 0.25)

    def test_recording_returns_immediately_while_trail_is_down(self) -> None:
        self.fake.down()
        client = self.client(max_batch=10)
        start = time.monotonic()
        for index in range(500):
            client.record_event("shop.order.placed", {"order_id": f"o-{index}"})
        self.assertLess(time.monotonic() - start, 0.25)

    def test_batches_by_size(self) -> None:
        client = self.client(max_batch=3, flush_interval=30)
        for index in range(7):
            client.record_event("shop.order.placed", {"n": index})
        self.assertTrue(self.fake.wait_for(2))
        time.sleep(0.1)
        self.assertEqual([len(body["events"]) for body in self.fake.bodies()], [3, 3])
        self.assertTrue(client.flush())
        self.assertEqual([len(body["events"]) for body in self.fake.bodies()], [3, 3, 1])

    def test_batches_by_interval(self) -> None:
        client = self.client(flush_interval=0.05)
        client.record_event("shop.order.placed", {"n": 1})
        client.record_event("shop.order.placed", {"n": 2})
        self.assertTrue(self.fake.wait_for(1, timeout=1.0))
        self.assertEqual(len(self.fake.bodies()[0]["events"]), 2)

    def test_ids_are_unique_uuid7_and_ts_is_ms(self) -> None:
        before = int(time.time() * 1000)
        ids = [uuid7() for _ in range(5000)]
        after = int(time.time() * 1000)
        self.assertEqual(len(set(ids)), len(ids))
        self.assertEqual(ids, sorted(ids), "ids from one process sort in the order they were made")
        for value in ids[:50]:
            self.assertRegex(value, UUID7)
            stamp = int(value.replace("-", "")[:12], 16)
            self.assertTrue(before <= stamp <= after + 2, stamp)

        client = self.client()
        client.record_event("shop.order.placed", {})
        client.error(kind="Timeout", message="slow")
        client.log("info", "started")
        self.assertTrue(client.flush())
        body = self.fake.bodies()[0]
        for kind in ("events", "errors", "logs"):
            record = body[kind][0]
            self.assertRegex(record["id"], UUID7)
            self.assertIsInstance(record["ts"], int)
            self.assertTrue(before <= record["ts"] <= int(time.time() * 1000))


class Body(TrailTestCase):
    def test_body_matches_the_ingest_spec(self) -> None:
        client = self.client(version="9cf5634")
        client.record_event(
            "shop.order.placed",
            {"order_id": "o-17", "total_cents": 1200, "items": ["a"]},
            Context(actor="user", client="web", trace_id="req-81", ts=1794000000000),
        )
        client.record_event("shop.order.viewed", {})
        client.record_metric("shop.queue.depth", 12, {"queue": "email"}, unit="jobs", kind="gauge", ts=1794000000000)
        client.record_metric("shop.emails.sent", 3.5, {}, unit=None, kind="counter")
        client.error(kind="KeyError", message="missing key 42", context={"route": "/pay"}, trace_id="req-81")
        client.log("info", "started", stream="worker", fields={"pid": 81})
        client.log("warn", "slow")
        self.assertTrue(client.flush())

        request = self.fake.requests[0]
        self.assertEqual(request.path, "/v1/ingest")
        self.assertEqual(request.headers["content-type"], "application/json")
        self.assertNotIn("authorization", request.headers)
        body = request.body
        self.assertEqual(list(body), ["source", "version", "host", "events", "metrics", "errors", "logs"])
        self.assertEqual((body["source"], body["version"], body["host"]), ("shop", "9cf5634", "web-1"))

        first, second = body["events"]
        self.assertEqual(
            first,
            {"id": first["id"], "name": "shop.order.placed", "ts": 1794000000000, "actor": "user", "client": "web",
             "trace_id": "req-81", "props": {"order_id": "o-17", "total_cents": 1200, "items": ["a"]}},
        )
        self.assertEqual(set(second), {"id", "name", "ts", "props"}, "unset context fields are left out, not null")
        self.assertEqual(
            body["metrics"],
            [
                {"name": "shop.queue.depth", "value": 12, "ts": 1794000000000, "labels": {"queue": "email"}, "unit": "jobs", "kind": "gauge"},
                {"name": "shop.emails.sent", "value": 3.5, "ts": body["metrics"][1]["ts"], "labels": {}, "kind": "counter"},
            ],
        )
        error = body["errors"][0]
        self.assertEqual(
            error,
            {"id": error["id"], "ts": error["ts"], "severity": "error", "kind": "KeyError", "message": "missing key 42",
             "context": {"route": "/pay"}, "trace_id": "req-81"},
        )
        info, warn = body["logs"]
        self.assertEqual(info, {"id": info["id"], "ts": info["ts"], "level": "info", "stream": "worker", "message": "started", "fields": {"pid": 81}})
        self.assertEqual(set(warn), {"id", "ts", "level", "message"})
        self.assertNotIn("null", request.raw.decode())

    def test_batch_fields_left_out_when_unset_and_empty_kinds_omitted(self) -> None:
        client = self.client(host="")  # an empty string leaves the field out
        client.record_metric("shop.queue.depth", 1, {"queue": "email"}, unit="jobs", kind="gauge")
        self.assertTrue(client.flush())
        self.assertEqual(list(self.fake.bodies()[0]), ["source", "metrics"])

    def test_bearer_token_header(self) -> None:
        client = self.client(token="s3cret-token")
        client.record_event("shop.order.placed", {})
        self.assertTrue(client.flush())
        self.assertEqual(self.fake.requests[0].headers["authorization"], "Bearer s3cret-token")


class Errors(TrailTestCase):
    def test_error_from_an_exception(self) -> None:
        client = self.client()

        def charge() -> None:
            raise KeyError("card")

        try:
            charge()
        except KeyError as exc:
            client.error(exc, severity="warning", context={"order_id": "o-1"})
        try:
            raise ValueError("bad total")
        except ValueError:
            client.error()  # the exception being handled
        client.error(kind="Timeout", message="payment provider took 30 s")
        self.assertTrue(client.flush())
        first, second, third = self.fake.bodies()[0]["errors"]
        self.assertEqual((first["kind"], first["message"], first["severity"]), ("KeyError", "'card'", "warning"))
        self.assertIn("Traceback", first["stack"])
        self.assertIn("in charge", first["stack"])
        self.assertEqual(first["context"], {"order_id": "o-1"})
        self.assertEqual((second["kind"], second["message"]), ("ValueError", "bad total"))
        self.assertEqual((third["kind"], third["message"]), ("Timeout", "payment provider took 30 s"))
        self.assertNotIn("stack", third)

    def test_excepthook_reports_and_chains(self) -> None:
        client = self.client()
        seen: list[str] = []
        previous_sys, previous_thread = sys.excepthook, threading.excepthook

        def restore() -> None:
            sys.excepthook, threading.excepthook = previous_sys, previous_thread

        self.addCleanup(restore)
        sys.excepthook = lambda kind, exc, tb: seen.append(f"sys {kind.__name__}")
        threading.excepthook = lambda args: seen.append(f"thread {args.exc_type.__name__}")
        client.install_excepthook()

        try:
            raise RuntimeError("crash in main")
        except RuntimeError as exc:
            sys.excepthook(type(exc), exc, exc.__traceback__)

        def work() -> None:
            raise LookupError("crash in worker")

        worker = threading.Thread(target=work)
        worker.start()
        worker.join()

        self.assertEqual(seen, ["sys RuntimeError", "thread LookupError"])
        errors = [error for body in self.fake.bodies() for error in body.get("errors", [])]
        self.assertEqual([(e["kind"], e["message"], e["severity"]) for e in errors],
                         [("RuntimeError", "crash in main", "fatal"), ("LookupError", "crash in worker", "error")])
        self.assertIn("in work", errors[1]["stack"])

    def test_a_crashing_process_still_reports(self) -> None:
        result = run_script(
            """
            from trail_client import Client
            client = Client("shop", flush_interval=30)
            client.install_excepthook()
            def pay():
                raise ZeroDivisionError("no money")
            pay()
            """,
            {"TRAIL_URL": self.fake.url},
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("ZeroDivisionError: no money", result.stderr, "the default hook still runs")
        errors = [error for body in self.fake.bodies() for error in body.get("errors", [])]
        self.assertEqual(len(errors), 1)
        self.assertEqual((errors[0]["kind"], errors[0]["severity"]), ("ZeroDivisionError", "fatal"))


class NeverThrows(TrailTestCase):
    def test_values_that_are_not_json_are_dropped_and_reported(self) -> None:
        client = self.client()
        loop: dict[str, Any] = {}
        loop["self"] = loop
        client.record_event("shop.order.placed", {"when": object()})
        client.record_event("shop.order.placed", loop)
        client.record_event("shop.order.placed", {"total": math.nan})
        client.record_metric("shop.queue.depth", math.inf, {}, unit=None, kind="gauge")
        client.log("info", "fields", fields={"x": {1, 2}})
        client.record_event("shop.order.placed", {"order_id": "o-1"})
        self.assertTrue(client.flush())
        self.assertEqual(self.kinds(), ["dropped"] * 5)
        self.assertEqual(self.problems[0].at, "event shop.order.placed")
        self.assertEqual(client.stats().dropped, 5)
        self.assertEqual(self.fake.bodies()[0]["events"][0]["props"], {"order_id": "o-1"})

    def test_a_broken_on_problem_callback_is_ignored(self) -> None:
        def broken(problem: object) -> None:
            raise RuntimeError("callback bug")

        client = self.client(on_problem=broken)
        client.record_event("shop.order.placed", {"bad": object()})
        self.fake.answer(500)
        client.record_event("shop.order.placed", {})
        self.assertFalse(client.flush(0.5))

    def test_close_twice_and_record_after_close(self) -> None:
        client = self.client()
        client.record_event("shop.order.placed", {})
        client.close()
        client.close()
        client.record_event("shop.order.placed", {})
        self.assertEqual(self.fake.count(), 1)
        self.assertEqual(client.stats().sent, 1)


class Spooling(TrailTestCase):
    def test_5xx_and_no_answer_spool_one_file_per_batch_and_resend_oldest_first(self) -> None:
        for outage in ("500", "down"):
            with self.subTest(outage=outage):
                self.fake.requests.clear()
                for name in self.spool_files():
                    os.remove(os.path.join(self.spool, name))
                if outage == "500":
                    self.fake.answer(500)
                else:
                    self.fake.down()
                client = self.client(max_batch=2, flush_interval=30)
                client.record_event("shop.order.placed", {"n": 1})
                client.record_event("shop.order.placed", {"n": 2})
                self.assertFalse(client.flush(1.0))
                self.assertEqual(len(self.spool_files()), 1)
                client.record_event("shop.order.placed", {"n": 3})
                client.record_event("shop.order.placed", {"n": 4})
                client.record_event("shop.order.placed", {"n": 5})
                client.close(1.0)  # what the queue still holds goes to the spool, one file per batch
                files = self.spool_files()
                self.assertEqual(len(files), 3)
                spooled = [read_json(os.path.join(self.spool, f)) for f in files]
                self.assertEqual([[e["props"]["n"] for e in body["events"]] for body in spooled], [[1, 2], [3, 4], [5]])
                if outage == "500":
                    self.assertEqual(self.fake.requests[0].raw, read_bytes(os.path.join(self.spool, files[0])),
                                     "a spool file is the request body exactly as posted")
                self.assertIn("send", self.kinds())

                if outage == "down":
                    self.fake.up()
                self.fake.answer(200)
                self.fake.requests.clear()
                again = self.client(max_batch=2, flush_interval=30)  # the next run of the service
                self.assertTrue(again.flush(2.0))
                self.assertEqual(self.fake.bodies(), spooled, "resent oldest first, with the same ids")
                self.assertEqual(self.spool_files(), [])
                self.assertEqual(again.stats().sent, 5)

    def test_a_batch_is_in_the_spool_while_its_request_is_in_flight(self) -> None:
        self.fake.hold()
        self.addCleanup(self.fake.release)
        client = self.client()
        client.record_event("shop.order.placed", {"order_id": "o-1"})
        self.assertTrue(wait_until(lambda: self.fake.arrived == 1))
        files = self.spool_files()
        self.assertEqual(len(files), 1, "written before it was posted")
        spooled = read_bytes(os.path.join(self.spool, files[0]))
        self.assertEqual(client.stats().spooled, 0, "`spooled` counts batches kept after a failed send")

        # The process is killed here. Its next run, on the same folder, sends the batch.
        restarted = FakeTrail()
        self.addCleanup(restarted.down)
        again = self.client(url=restarted.url)
        self.assertTrue(again.flush(2.0))
        self.assertEqual([request.raw for request in restarted.requests], [spooled])
        self.assertEqual(restarted.bodies()[0]["events"][0]["props"], {"order_id": "o-1"})
        self.assertEqual(self.spool_files(), [])

    def test_a_spool_file_in_flight_goes_after_a_final_answer_and_stays_after_a_500(self) -> None:
        for status in (200, 400, 500):
            with self.subTest(status=status):
                self.problems.clear()
                self.fake.requests.clear()
                self.fake.arrived = 0
                self.fake.answer(status)
                self.fake.hold()
                folder = os.path.join(self.spool, str(status))
                client = self.client(spool_dir=folder)
                client.record_event("shop.order.placed", {})
                self.assertTrue(wait_until(lambda: self.fake.arrived == 1))
                self.assertEqual(len(self.spool_files(folder)), 1)
                self.fake.release()
                self.assertTrue(self.fake.wait_for(1))
                if status == 500:
                    self.assertTrue(wait_until(lambda: client.stats().spooled == 1))
                    self.assertEqual(len(self.spool_files(folder)), 1, "kept for a retry")
                    self.assertEqual(self.kinds(), ["send"])
                else:
                    self.assertTrue(wait_until(lambda: self.spool_files(folder) == []))
                    expected = ["dropped"] if status == 400 else []
                    self.assertTrue(wait_until(lambda: self.kinds() == expected), self.kinds())
                    if status == 400:
                        self.assertIn("400", self.problems[0].message)
                        self.assertEqual(client.stats().dropped, 1)
                    else:
                        self.assertEqual(client.stats().sent, 1)
                client.close(0.2)

    def test_a_flush_from_on_problem_does_not_send_the_batch_in_flight_again(self) -> None:
        self.fake.answer(200, mismatched=[{"at": "events[0]", "message": "unknown property"}])
        took: list[float] = []

        def flush_again(problem: object) -> None:
            start = time.monotonic()
            client.flush(1.0)  # False: the batch being answered is still in the spool
            took.append(time.monotonic() - start)

        client = self.client(flush_interval=30, on_problem=flush_again)
        client.record_event("shop.order.placed", {"extra": 1})
        self.assertTrue(client.flush(2.0))
        self.assertEqual(self.fake.count(), 1, "sent once")
        self.assertEqual(len(took), 1)
        self.assertLess(took[0], 0.5, "does not wait out its timeout for the batch in flight")
        self.assertEqual(self.spool_files(), [])

    def test_without_a_spool_folder_nothing_is_written_to_disk(self) -> None:
        self.fake.hold()
        self.addCleanup(self.fake.release)
        parent = os.path.dirname(self.spool)
        client = self.client(spool_dir=None)
        client.record_event("shop.order.placed", {})
        self.assertTrue(wait_until(lambda: self.fake.arrived == 1))
        self.assertEqual(os.listdir(parent), [])
        self.fake.answer(500)
        self.fake.release()
        self.assertTrue(self.fake.wait_for(1))
        self.assertFalse(client.flush(0.3))
        client.close(0.2)
        self.assertEqual(os.listdir(parent), [])

    def test_resend_in_the_same_process_after_trail_returns(self) -> None:
        self.fake.down()
        client = self.client()
        client.record_event("shop.order.placed", {"n": 1})
        self.assertFalse(client.flush(0.5))
        stats = client.stats()
        self.assertEqual((stats.spooled, stats.sent), (1, 0))
        spooled_ids = [e["id"] for e in read_json(os.path.join(self.spool, self.spool_files()[0]))["events"]]
        self.fake.up()
        self.assertTrue(client.flush(2.0))
        self.assertEqual(self.fake.requests[0].ids(), spooled_ids)

    def test_spool_starts_sending_without_new_records(self) -> None:
        self.fake.answer(500)
        first = self.client()
        first.record_event("shop.order.placed", {})
        first.close(0.5)
        self.assertEqual(len(self.spool_files()), 1)
        self.fake.answer(200)
        self.fake.requests.clear()
        self.client()  # records nothing; the sender starts because the spool is not empty
        self.assertTrue(wait_until(lambda: self.spool_files() == [], 2.0))
        self.assertEqual(self.fake.requests[0].status, 200)

    def test_backoff_after_a_failure(self) -> None:
        self.fake.answer(503)
        client = self.client(flush_interval=0.02)
        client.record_event("shop.order.placed", {})
        self.assertTrue(self.fake.wait_for(1))
        time.sleep(0.4)
        self.assertEqual(self.fake.count(), 1, "no retry before the first backoff (1 s) is over")
        self.assertEqual(len(self.spool_files()), 1)

    def test_without_a_spool_folder_failed_batches_wait_in_memory(self) -> None:
        self.fake.down()
        client = self.client(spool_dir=None)
        client.record_event("shop.order.placed", {"n": 1})
        self.assertFalse(client.flush(0.5))
        self.assertFalse(os.path.exists(self.spool))
        self.fake.up()
        self.assertTrue(client.flush(2.0))
        self.assertEqual(self.fake.bodies()[0]["events"][0]["props"], {"n": 1})

    def test_400_401_413_are_dropped_and_reported_not_retried(self) -> None:
        for status in (400, 401, 413):
            with self.subTest(status=status):
                self.problems.clear()
                self.fake.requests.clear()
                self.fake.answer(status)
                client = self.client()
                client.record_event("shop.order.placed", {})
                self.assertTrue(client.flush(1.0))
                self.assertTrue(client.flush(1.0))
                self.assertEqual(self.fake.count(), 1, "not retried")
                self.assertEqual(self.spool_files(), [])
                self.assertEqual(self.kinds(), ["dropped"])
                self.assertIn(str(status), self.problems[0].message)
                self.assertEqual(client.stats().dropped, 1)
                client.close(0.2)

    def test_a_200_that_is_not_trails_answer_keeps_the_batch(self) -> None:
        for folder in ("spool", None):
            with self.subTest(spool_dir=folder):
                self.problems.clear()
                self.fake.requests.clear()
                self.fake.html = True  # a sign-on page or a proxy's catch-all, answering 200
                client = self.client(spool_dir=self.spool if folder else None)
                client.record_event("shop.order.placed", {"order_id": "o-1"})
                self.assertFalse(client.flush(0.5))
                self.assertEqual(self.fake.count(), 1)
                self.assertEqual(client.stats().sent, 0)
                if folder:
                    self.assertEqual(len(self.spool_files()), 1, "kept in the spool")
                send = [p for p in self.problems if p.kind == "send"]
                self.assertEqual(len(send), 1, self.problems)
                self.assertIn("did not come from Trail", send[0].message)
                self.assertIn("Sign in", send[0].message)
                self.assertIn("TRAIL_URL", send[0].fix)

                self.fake.html = False
                self.assertTrue(client.flush(2.0))
                self.assertEqual(self.fake.count(), 2)
                self.assertEqual(self.fake.requests[1].ids(), self.fake.requests[0].ids(), "the same batch, sent again")
                self.assertEqual(client.stats().sent, 1)
                self.assertEqual(self.spool_files(), [])
                client.close(0.2)

    def test_413_splits_the_batch_until_every_record_is_taken(self) -> None:
        for folder in ("spool", None):
            with self.subTest(spool_dir=folder):
                self.problems.clear()
                self.fake.requests.clear()
                self.fake.limit = 3  # more records than this in one request get 413
                client = self.client(spool_dir=self.spool if folder else None, max_batch=10, flush_interval=30)
                for n in range(10):
                    client.record_event("shop.order.placed", {"n": n})
                self.assertTrue(client.flush(3.0))
                taken = [e["props"]["n"] for r in self.fake.requests if r.status == 200 for e in r.body["events"]]
                self.assertEqual(sorted(taken), list(range(10)), "every record taken, each once")
                self.assertTrue(all(len(r.body["events"]) > 3 for r in self.fake.requests if r.status == 413))
                self.assertEqual(len(self.fake.requests[0].body["events"]), 10)
                stats = client.stats()
                self.assertEqual((stats.sent, stats.dropped), (10, 0))
                self.assertEqual(self.problems, [])
                self.assertEqual(self.spool_files(), [])
                client.close(0.2)

    def test_413_drops_a_single_record_that_is_too_large_on_its_own(self) -> None:
        self.fake.max_bytes = 1000
        client = self.client(max_batch=10, flush_interval=30)
        for n in range(5):
            client.record_event("shop.order.placed", {"n": n, "pad": "x" * (2000 if n == 2 else 0)})
        self.assertTrue(client.flush(3.0))
        taken = [e["props"]["n"] for r in self.fake.requests if r.status == 200 for e in r.body["events"]]
        self.assertEqual(sorted(taken), [0, 1, 3, 4])
        self.assertEqual(self.kinds(), ["dropped"], "reported once")
        self.assertIn("413", self.problems[0].message)
        self.assertIn("1 record(s)", self.problems[0].message)
        stats = client.stats()
        self.assertEqual((stats.sent, stats.dropped), (4, 1))
        self.assertEqual(self.spool_files(), [])
        self.assertTrue(client.flush(1.0))
        self.assertEqual(self.kinds(), ["dropped"], "not sent again")

    def test_400_names_the_servers_problem(self) -> None:
        self.fake.answer(400)
        client = self.client()
        client.record_event("shop.order.placed", {})
        client.flush(1.0)
        self.assertIn("`Shop` is not a source name", self.problems[0].message)

    def test_queue_bound_drops_the_oldest_and_counts_them(self) -> None:
        self.fake.down()
        client = self.client(max_queue=5, max_batch=100, flush_interval=30, spool_dir=None)
        for index in range(8):
            client.record_event("shop.order.placed", {"n": index})
        self.assertEqual(client.stats().dropped, 3)
        self.fake.up()
        self.assertTrue(client.flush(2.0))
        self.assertEqual([e["props"]["n"] for e in self.fake.bodies()[0]["events"]], [3, 4, 5, 6, 7])
        dropped = [p for p in self.problems if p.kind == "dropped"]
        self.assertEqual(dropped[0].at, "queue")
        self.assertIn("3 record(s)", dropped[0].message)

    def test_spool_bound_drops_the_oldest_and_counts_them(self) -> None:
        self.fake.down()
        client = self.client(max_batch=1, flush_interval=30, max_spool_bytes=10_000_000)
        for index in range(3):
            client.record_event("shop.order.placed", {"n": index, "pad": "x" * 100})
        client.close(0.5)
        sizes = [os.path.getsize(os.path.join(self.spool, f)) for f in self.spool_files()]
        self.assertEqual(len(sizes), 3)
        for name in self.spool_files():
            os.remove(os.path.join(self.spool, name))

        self.problems.clear()
        bounded = self.client(max_batch=1, flush_interval=30, max_spool_bytes=sizes[0] * 2 + 10)
        for index in range(3):
            bounded.record_event("shop.order.placed", {"n": index, "pad": "x" * 100})
        bounded.close(0.5)
        remaining = [read_json(os.path.join(self.spool, f))["events"][0]["props"]["n"] for f in self.spool_files()]
        self.assertEqual(remaining, [1, 2])
        self.assertEqual(bounded.stats().dropped, 1)
        self.assertIn("spool", [p.at for p in self.problems if p.kind == "dropped"])


class Reports(TrailTestCase):
    def test_rejected_and_mismatched_go_to_on_problem(self) -> None:
        self.fake.answer(
            200,
            rejected=[{"at": "events[1]", "message": "`Order` is not an event name", "fix": "use snake_case parts joined by dots"}],
            mismatched=[{"at": "events[0]", "message": "unknown property `latncy_ms` on `shop.order.placed`"}],
        )
        client = self.client()
        client.record_event("shop.order.placed", {"latncy_ms": 3})
        client.record_event("Order", {})
        self.assertTrue(client.flush())
        self.assertEqual([(p.kind, p.at) for p in self.problems], [("rejected", "events[1]"), ("mismatched", "events[0]")])
        self.assertEqual(self.problems[0].fix, "use snake_case parts joined by dots")
        self.assertEqual(self.problems[1].fix, "")
        stats = client.stats()
        self.assertEqual((stats.sent, stats.rejected, stats.mismatched), (1, 1, 1))

    def test_strict_mode_raises_at_flush(self) -> None:
        self.fake.answer(200, mismatched=[{"at": "events[0]", "message": "unknown property", "fix": "declare it"}])
        client = self.client(strict=True)
        client.record_event("shop.order.placed", {"bad": object()})  # does not raise here
        client.record_event("shop.order.placed", {"extra": 1})
        with self.assertRaises(TrailError) as raised:
            client.flush()
        self.assertEqual([p.kind for p in raised.exception.problems], ["dropped", "mismatched"])
        self.assertIn("unknown property", str(raised.exception))
        self.fake.answer(200)
        client.record_event("shop.order.placed", {})
        self.assertTrue(client.flush(), "problems are cleared once raised")

    def test_strict_mode_raises_when_sending_fails(self) -> None:
        self.fake.down()
        os.environ["TRAIL_STRICT"] = "1"
        client = self.client()
        client.record_event("shop.order.placed", {})
        with self.assertRaises(TrailError) as raised:
            client.flush(0.5)
        self.assertEqual(raised.exception.problems[0].kind, "send")


class Environment(TrailTestCase):
    def test_disabled_is_a_no_op(self) -> None:
        os.environ["TRAIL_DISABLED"] = "1"
        threads = threading.active_count()
        client = self.client()
        client.record_event("shop.order.placed", {})
        client.record_metric("shop.queue.depth", 1, {}, unit=None, kind="gauge")
        client.error(ValueError("x"))
        client.log("info", "x")
        client.install_excepthook()
        self.assertTrue(client.flush())
        client.close()
        self.assertEqual(threading.active_count(), threads)
        self.assertEqual(self.fake.count(), 0)
        self.assertFalse(os.path.exists(self.spool))
        self.assertEqual(client.stats().sent, 0)

    def test_environment_defaults(self) -> None:
        os.environ.update({
            "TRAIL_URL": self.fake.url + "/",
            "TRAIL_VERSION": "9cf5634",
            "TRAIL_HOST": "worker-2",
            "TRAIL_TOKEN": "env-token",
            "TRAIL_SPOOL_DIR": self.spool + "-env",
        })
        client = Client("shop", flush_interval=0.05)
        self.addCleanup(client.close, 0.5)
        self.assertTrue(os.path.isdir(self.spool + "-env"))
        client.record_event("shop.order.placed", {})
        self.assertTrue(client.flush())
        request = self.fake.requests[0]
        self.assertEqual((request.body["version"], request.body["host"]), ("9cf5634", "worker-2"))
        self.assertEqual(request.headers["authorization"], "Bearer env-token")
        self.assertFalse(client.strict)

    def test_defaults_without_environment(self) -> None:
        client = Client("shop")
        self.addCleanup(client.close, 0.1)
        self.assertEqual(client.endpoint, "http://127.0.0.1:8200/v1/ingest")
        self.assertEqual(client.host, socket.gethostname())
        self.assertIsNone(client.version)
        self.assertIsNone(client.token)
        self.assertFalse(client.strict)
        self.assertFalse(client.disabled)

    def test_options_win_over_environment(self) -> None:
        os.environ.update({"TRAIL_HOST": "env-host", "TRAIL_VERSION": "env"})
        client = self.client(version="opt")
        self.assertEqual((client.host, client.version), ("web-1", "opt"))


class Process(TrailTestCase):
    def test_a_short_script_loses_nothing(self) -> None:
        result = run_script(
            """
            from trail_client import Client
            Client("shop", flush_interval=30).record_event("shop.order.placed", {"order_id": "o-1"})
            """,
            {"TRAIL_URL": self.fake.url},
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.fake.bodies()[0]["events"][0]["props"], {"order_id": "o-1"})

    def test_at_exit_what_cannot_be_sent_goes_to_the_spool(self) -> None:
        self.fake.down()
        result = run_script(
            """
            from trail_client import Client
            client = Client("shop", flush_interval=30, max_batch=2)
            for n in range(5):
                client.record_event("shop.order.placed", {"n": n})
            """,
            {"TRAIL_URL": self.fake.url, "TRAIL_SPOOL_DIR": self.spool},
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        files = self.spool_files()
        counts = [len(read_json(os.path.join(self.spool, f))["events"]) for f in files]
        self.assertEqual(sum(counts), 5)
        self.assertTrue(all(count <= 2 for count in counts))

    @unittest.skipUnless(hasattr(os, "fork"), "needs os.fork")
    def test_a_forked_child_sends_with_its_own_thread(self) -> None:
        client = self.client(flush_interval=30)
        client.record_event("shop.order.placed", {"who": "parent"})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            pid = os.fork()
        if pid == 0:
            code = 1
            try:
                client.record_event("shop.order.placed", {"who": "child"})
                code = 0 if client.flush(2.0) else 2
            finally:
                os._exit(code)
        _, status = os.waitpid(pid, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        self.assertTrue(client.flush())
        who = sorted(e["props"]["who"] for body in self.fake.bodies() for e in body["events"])
        self.assertEqual(who, ["child", "parent"], "the child does not resend the parent's queue")


if __name__ == "__main__":
    unittest.main()
