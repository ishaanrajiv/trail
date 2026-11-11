// The request the client posts for generated code, using a copy of the example in
// `spec/client.md`. `shop_events.ts` imports "trail-client" by name, which resolves to this
// package through its own `exports`.

import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import { events, metrics, SOURCE } from "./shop_events.ts";
import { World } from "./support.ts";

let w: World;
beforeEach(async () => {
  w = await World.create();
});
afterEach(async () => {
  await w.dispose();
});

test("generated emitters post the expected request", async () => {
  const client = w.client({ version: "9cf5634" });
  const shop = events(client);
  const gauges = metrics(client);

  shop.shopOrderFailed({ order_id: "o-17", reason: "payment" });
  shop.shopOrderFailed(
    { order_id: "o-18", reason: "out-of-stock", detail: { sku: "mug-blue", wanted: 2 } },
    { actor: "user", traceId: "req-81" },
  );
  gauges.shopEmailsSent(41);
  gauges.shopQueueDepth(12, { queue: "email" }, 1794000000000);
  assert.equal(await client.flush(), true);

  const body = w.fake.bodies()[0] ?? {};
  assert.deepEqual(Object.keys(body), ["source", "version", "host", "events", "metrics"]);
  assert.equal(body.source, SOURCE);
  const [first, second] = body.events ?? [];
  assert.deepEqual(first, { id: first?.id, name: "shop.order.failed", ts: first?.ts, props: { order_id: "o-17", reason: "payment" } },
    "an optional property that is not given is left out");
  assert.deepEqual(second, { id: second?.id, name: "shop.order.failed", ts: second?.ts, actor: "user", trace_id: "req-81",
    props: { order_id: "o-18", reason: "out-of-stock", detail: { sku: "mug-blue", wanted: 2 } } });
  const [sent, depth] = body.metrics ?? [];
  assert.deepEqual(sent, { name: "shop.emails.sent", value: 41, ts: sent?.ts, labels: {}, kind: "counter" });
  assert.deepEqual(depth, { name: "shop.queue.depth", value: 12, ts: 1794000000000, labels: { queue: "email" }, unit: "jobs", kind: "gauge" });
  assert.deepEqual(w.problems, []);
});
