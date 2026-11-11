"""The request the client posts for generated code, using a copy of the example in `spec/client.md`."""

from __future__ import annotations

import unittest

from _support import TrailTestCase

import shop_events
from trail_client import Context


class Generated(TrailTestCase):
    def test_generated_emitters_post_the_expected_request(self) -> None:
        client = self.client(version="9cf5634")
        events = shop_events.Events(client)
        metrics = shop_events.Metrics(client)

        events.shop_order_failed(order_id="o-17", reason="payment")
        events.shop_order_failed(
            order_id="o-18",
            reason="out-of-stock",
            detail={"sku": "mug-blue", "wanted": 2},
            context=Context(actor="user", trace_id="req-81"),
        )
        metrics.shop_emails_sent(41)
        metrics.shop_queue_depth(12, queue="email", ts=1794000000000)
        self.assertTrue(client.flush())

        body = self.fake.bodies()[0]
        self.assertEqual(list(body), ["source", "version", "host", "events", "metrics"])
        self.assertEqual(body["source"], shop_events.SOURCE)
        first, second = body["events"]
        self.assertEqual(
            first,
            {"id": first["id"], "name": "shop.order.failed", "ts": first["ts"], "props": {"order_id": "o-17", "reason": "payment"}},
            "an optional property that is not given is left out",
        )
        self.assertEqual(
            second,
            {"id": second["id"], "name": "shop.order.failed", "ts": second["ts"], "actor": "user", "trace_id": "req-81",
             "props": {"order_id": "o-18", "reason": "out-of-stock", "detail": {"sku": "mug-blue", "wanted": 2}}},
        )
        sent, depth = body["metrics"]
        self.assertEqual(sent, {"name": "shop.emails.sent", "value": 41, "ts": sent["ts"], "labels": {}, "kind": "counter"})
        self.assertEqual(
            depth,
            {"name": "shop.queue.depth", "value": 12, "ts": 1794000000000, "labels": {"queue": "email"}, "unit": "jobs", "kind": "gauge"},
        )
        self.assertEqual(self.problems, [])


if __name__ == "__main__":
    unittest.main()
