"""The client and the generated emitters against a real Trail.

Skipped unless `TRAIL_LIVE_URL` points at a running server that has the example `shop`
declarations published. `scripts/e2e` starts one and runs this.
"""

from __future__ import annotations

import json
import os
import unittest
import urllib.request
from typing import Any

import _support

_ = _support.SRC  # importing it puts `src` on the path

import shop_events
from trail_client import Client, Context, Problem

URL = os.environ.get("TRAIL_LIVE_URL", "")


def get(path: str) -> dict[str, Any]:
    with urllib.request.urlopen(URL + path, timeout=10) as response:
        parsed: dict[str, Any] = json.loads(response.read())
        return parsed


@unittest.skipUnless(URL, "set TRAIL_LIVE_URL to a running Trail with the shop example published")
class Live(unittest.TestCase):
    def test_everything_a_service_sends_arrives_clean(self) -> None:
        problems: list[Problem] = []
        client = Client("shop", url=URL, version="live-python", on_problem=problems.append)
        events = shop_events.Events(client)
        metrics = shop_events.Metrics(client)
        who = Context(actor="live-python", client="python", trace_id="live-py-1")

        events.shop_order_placed(order_id="py-1", total_cents=1200, method="card", items=["tea"], latency_ms=80, context=who)
        events.shop_order_placed(order_id="py-2", total_cents=300, method="cash", items=[], context=who)
        events.shop_order_failed(order_id="py-3", reason="out-of-stock", detail={"sku": "mug"}, context=who)
        events.shop_page_viewed(path="/", signed_in=True, load_ms=12.5, context=who)
        metrics.shop_queue_depth(7, queue="live-python")
        metrics.shop_emails_sent(3)
        try:
            raise ValueError("live test")
        except ValueError as error:
            client.error(error, context={"route": "/live"})
        client.log("warn", "live test line", stream="live-python", fields={"n": 1})
        self.assertTrue(client.flush(10))
        client.close()
        self.assertEqual(problems, [])

        found = get("/v1/events?actor=live-python")["events"]
        self.assertEqual(sorted(e["name"] for e in found), ["shop.order.failed", "shop.order.placed", "shop.order.placed", "shop.page.viewed"])
        for event in found:
            # Stored exactly as declared: nothing marked.
            self.assertNotIn("problems", event)
            self.assertEqual((event["version"], event["client"], event["trace_id"]), ("live-python", "python", "live-py-1"))
        placed = next(e for e in found if e["props"].get("order_id") == "py-1")
        self.assertEqual(placed["props"], {"order_id": "py-1", "total_cents": 1200, "method": "card", "items": ["tea"], "latency_ms": 80})

        depth = get("/v1/metrics/latest?name=shop.queue.depth&label.queue=live-python")["series"]
        self.assertEqual((depth[0]["value"], depth[0]["unit"]), (7, "jobs"))
        errors = [e for e in get("/v1/errors?source=shop")["errors"] if e["version"] == "live-python"]
        self.assertEqual((errors[0]["kind"], errors[0]["message"], errors[0]["context"]), ("ValueError", "live test", {"route": "/live"}))
        self.assertIn("test_live.py", errors[0]["stack"])
        logs = get("/v1/logs?source=shop&stream=live-python")["logs"]
        self.assertEqual((logs[0]["level"], logs[0]["message"], logs[0]["fields"]), ("warn", "live test line", {"n": 1}))

    def test_a_wrong_record_comes_back_as_a_problem(self) -> None:
        problems: list[Problem] = []
        client = Client("shop", url=URL, on_problem=problems.append)
        # Around the generated code on purpose: this is what it exists to prevent.
        client.record_event("shop.order.placed", {"order_id": "py-bad", "total": 1})
        client.record_event("Not A Name", {})
        self.assertTrue(client.flush(10))
        client.close()
        kinds = sorted(problem.kind for problem in problems)
        self.assertEqual(kinds[0], "mismatched")
        self.assertEqual(kinds[-1], "rejected")
        self.assertTrue(any("total" in problem.message for problem in problems))


if __name__ == "__main__":
    unittest.main()
