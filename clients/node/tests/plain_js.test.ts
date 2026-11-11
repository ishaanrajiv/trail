// The two things a page with no build step uses: the emitters `trail gen --lang javascript`
// writes (plain JavaScript, typed with JSDoc and checked here by `tsc`), and the client built to
// plain JavaScript in `dist/` by `npm run build`.

import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { fileURLToPath, pathToFileURL } from "node:url";
import { afterEach, beforeEach, test } from "node:test";

import { events, metrics, SOURCE } from "./shop_events_plain.js";
import { World } from "./support.ts";

let w: World;
beforeEach(async () => {
  w = await World.create();
});
afterEach(async () => {
  await w.dispose();
});

test("the plain JavaScript emitters post the same request as the TypeScript ones", async () => {
  const client = w.client({ version: "9cf5634" });
  const shop = events(client);
  const gauges = metrics(client);
  shop.shopOrderPlaced({ order_id: "o-17", total_cents: 1200, method: "card", items: ["tea"] }, { actor: "user" });
  shop.shopPageViewed({ path: "/", signed_in: false });
  gauges.shopQueueDepth(12, { queue: "email" }, 1794000000000);
  gauges.shopEmailsSent(41);
  assert.equal(await client.flush(), true);

  const body = w.fake.bodies()[0] ?? {};
  assert.equal(body.source, SOURCE);
  const [placed, viewed] = body.events ?? [];
  assert.deepEqual(placed, { id: placed?.id, name: "shop.order.placed", ts: placed?.ts, actor: "user",
    props: { order_id: "o-17", total_cents: 1200, method: "card", items: ["tea"] } });
  assert.deepEqual(viewed?.["props"], { path: "/", signed_in: false });
  const [depth, sent] = body.metrics ?? [];
  assert.deepEqual(depth, { name: "shop.queue.depth", value: 12, ts: 1794000000000, labels: { queue: "email" }, unit: "jobs", kind: "gauge" });
  assert.equal(sent?.["kind"], "counter");
});

test("the client builds to plain JavaScript that runs as it is", async () => {
  const root = fileURLToPath(new URL("..", import.meta.url));
  execFileSync(process.execPath, [fileURLToPath(new URL("../node_modules/typescript/bin/tsc", import.meta.url)), "-p", "tsconfig.build.json"], { cwd: root });
  const built = fileURLToPath(new URL("../dist/index.js", import.meta.url));
  assert.ok(existsSync(built));
  assert.ok(existsSync(fileURLToPath(new URL("../dist/index.d.ts", import.meta.url))));
  const module = (await import(pathToFileURL(built).href)) as typeof import("../src/index.ts");
  const client = new module.TrailClient({ source: "shop", url: w.fake.url, spoolDir: w.spool });
  client.recordEvent("shop.page.viewed", { path: "/", signed_in: true });
  assert.equal(await client.flush(), true);
  await client.close();
  assert.equal(w.fake.bodies()[0]?.events?.[0]?.["name"], "shop.page.viewed");
});
