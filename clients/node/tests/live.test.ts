// The client and the generated emitters against a real Trail.
//
// Skipped unless TRAIL_LIVE_URL points at a running server that has the example `shop`
// declarations published. `scripts/e2e` starts one and runs this.

import assert from "node:assert/strict";
import { existsSync, readdirSync } from "node:fs";
import { test } from "node:test";

import { TrailClient } from "../src/index.ts";
import type { TrailProblem } from "../src/index.ts";
import { events, metrics } from "./shop_events.ts";

const URL = process.env["TRAIL_LIVE_URL"] ?? "";
// `down` while the server is stopped, then `up` once it is back; SPOOL is the folder both runs share.
const OUTAGE = process.env["TRAIL_LIVE_OUTAGE"] ?? "";
const SPOOL = process.env["TRAIL_LIVE_SPOOL"] ?? "";
const skip = URL === "" || OUTAGE !== "" ? "set TRAIL_LIVE_URL to a running Trail with the shop example published" : false;
const skipOutage = URL === "" || OUTAGE === "" || SPOOL === "" ? "run by scripts/e2e, which stops and restarts the server around it" : false;

type Row = Record<string, unknown> & { props: Record<string, unknown>; name: string };

async function get(path: string): Promise<Record<string, Row[]>> {
  const response = await fetch(URL + path);
  assert.equal(response.status, 200, path);
  return (await response.json()) as Record<string, Row[]>;
}

test("everything a service sends arrives clean", { skip }, async () => {
  const problems: TrailProblem[] = [];
  const client = new TrailClient({ source: "shop", url: URL, version: "live-node", onProblem: (p) => problems.push(p) });
  const emit = events(client);
  const measure = metrics(client);
  const who = { actor: "live-node", client: "node", traceId: "live-node-1" };

  emit.shopOrderPlaced({ order_id: "node-1", total_cents: 1200, method: "card", items: ["tea"], latency_ms: 80 }, who);
  emit.shopOrderPlaced({ order_id: "node-2", total_cents: 300, method: "cash", items: [] }, who);
  emit.shopOrderFailed({ order_id: "node-3", reason: "out-of-stock", detail: { sku: "mug" } }, who);
  emit.shopPageViewed({ path: "/", signed_in: true, load_ms: 12.5 }, who);
  measure.shopQueueDepth(7, { queue: "live-node" });
  measure.shopEmailsSent(3);
  client.error(new RangeError("live test"), { context: { route: "/live" } });
  client.log("warn", "live test line", { stream: "live-node", fields: { n: 1 } });
  assert.equal(await client.flush(10_000), true);
  await client.close();
  assert.deepEqual(problems, []);

  const found = (await get("/v1/events?actor=live-node"))["events"] ?? [];
  assert.deepEqual(found.map((e) => e.name).sort(), ["shop.order.failed", "shop.order.placed", "shop.order.placed", "shop.page.viewed"]);
  for (const event of found) {
    // Stored exactly as declared: nothing marked.
    assert.equal("problems" in event, false);
    assert.deepEqual([event["version"], event["client"], event["trace_id"]], ["live-node", "node", "live-node-1"]);
  }
  const placed = found.find((e) => e.props["order_id"] === "node-1");
  assert.deepEqual(placed?.props, { order_id: "node-1", total_cents: 1200, method: "card", items: ["tea"], latency_ms: 80 });

  const depth = (await get("/v1/metrics/latest?name=shop.queue.depth&label.queue=live-node"))["series"] ?? [];
  assert.deepEqual([depth[0]?.["value"], depth[0]?.["unit"]], [7, "jobs"]);
  const errors = ((await get("/v1/errors?source=shop"))["errors"] ?? []).filter((e) => e["version"] === "live-node");
  assert.deepEqual([errors[0]?.["kind"], errors[0]?.["message"], errors[0]?.["context"]], ["RangeError", "live test", { route: "/live" }]);
  assert.match(String(errors[0]?.["stack"]), /live\.test\.ts/);
  const logs = (await get("/v1/logs?source=shop&stream=live-node"))["logs"] ?? [];
  assert.deepEqual([logs[0]?.["level"], logs[0]?.["message"], logs[0]?.["fields"]], ["warn", "live test line", { n: 1 }]);
});

test("a wrong record comes back as a problem", { skip }, async () => {
  const problems: TrailProblem[] = [];
  const client = new TrailClient({ source: "shop", url: URL, onProblem: (p) => problems.push(p) });
  // Around the generated code on purpose: this is what it exists to prevent.
  client.recordEvent("shop.order.placed", { order_id: "node-bad", total: 1 });
  client.recordEvent("Not A Name", {});
  assert.equal(await client.flush(10_000), true);
  await client.close();
  const kinds = problems.map((p) => p.kind).sort();
  assert.equal(kinds[0], "mismatched");
  assert.equal(kinds.at(-1), "rejected");
  assert.ok(problems.some((p) => p.message.includes("total")));
});

test("what is recorded while Trail is down arrives once it is back", { skip: skipOutage }, async () => {
  const client = new TrailClient({ source: "shop", url: URL, version: "outage-node", spoolDir: SPOOL });
  const emit = events(client);
  const spooled = (): string[] =>
    existsSync(SPOOL) ? readdirSync(SPOOL, { recursive: true, encoding: "utf8" }).filter((name) => name.endsWith(".json")) : [];
  if (OUTAGE === "down") {
    for (const path of ["/a", "/b", "/c"]) emit.shopPageViewed({ path, signed_in: false }, { actor: "outage-node" });
    assert.equal(await client.flush(2000), false);
    await client.close(1000);
    assert.notDeepEqual(spooled(), [], "nothing was written to the spool");
  } else {
    // A new process: nothing in memory, only what the last one left on disk.
    assert.equal(await client.flush(15_000), true);
    await client.close();
    assert.deepEqual(spooled(), []);
    const found = (await get("/v1/events?actor=outage-node"))["events"] ?? [];
    assert.deepEqual(found.map((e) => e.props["path"]).sort(), ["/a", "/b", "/c"]);
  }
});
