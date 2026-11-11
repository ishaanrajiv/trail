// Every row of the behaviour table in `spec/client.md`, against a fake Trail.

import assert from "node:assert/strict";
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { hostname } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, test } from "node:test";

import { TrailClient, TrailError, uuid7 } from "../src/index.ts";
import { INDEX, MemoryStorage, World, parse, runChild, sleep, waitUntil } from "./support.ts";

const UUID7 = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

let w: World;
beforeEach(async () => {
  w = await World.create();
});
afterEach(async () => {
  await w.dispose();
});

function props(body: { events?: Record<string, unknown>[] }): unknown[] {
  return (body.events ?? []).map((e) => e.props);
}

// ---------- never blocks, batches, ids ----------

test("recording returns immediately while Trail is slow or down", async () => {
  w.fake.delayMs = 500;
  const slow = w.client({ maxBatch: 10 });
  let start = performance.now();
  for (let i = 0; i < 500; i++) slow.recordEvent("shop.order.placed", { order_id: `o-${i}` });
  assert.ok(performance.now() - start < 100);
  await w.fake.down();
  const down = w.client({ maxBatch: 10, spoolDir: undefined });
  start = performance.now();
  for (let i = 0; i < 500; i++) down.recordEvent("shop.order.placed", { order_id: `o-${i}` });
  assert.ok(performance.now() - start < 100);
});

test("batches by size", async () => {
  const client = w.client({ maxBatch: 3, flushIntervalMs: 30_000 });
  for (let n = 0; n < 7; n++) client.recordEvent("shop.order.placed", { n });
  assert.ok(await w.fake.waitFor(2));
  await sleep(100);
  assert.deepEqual(w.fake.bodies().map((b) => b.events?.length), [3, 3]);
  assert.equal(await client.flush(), true);
  assert.deepEqual(w.fake.bodies().map((b) => b.events?.length), [3, 3, 1]);
});

test("batches by interval", async () => {
  const client = w.client({ flushIntervalMs: 50 });
  client.recordEvent("shop.order.placed", { n: 1 });
  client.recordEvent("shop.order.placed", { n: 2 });
  assert.ok(await w.fake.waitFor(1, 1000));
  assert.equal(w.fake.bodies()[0]?.events?.length, 2);
});

test("ids are unique UUIDv7 in order, and ts is in ms", async () => {
  const before = Date.now();
  const ids = Array.from({ length: 5000 }, () => uuid7());
  const after = Date.now();
  assert.equal(new Set(ids).size, ids.length);
  assert.deepEqual([...ids].sort(), ids, "ids from one process sort in the order they were made");
  for (const id of ids.slice(0, 50)) {
    assert.match(id, UUID7);
    const stamp = parseInt(id.replaceAll("-", "").slice(0, 12), 16);
    assert.ok(stamp >= before && stamp <= after + 2);
  }
  const client = w.client();
  client.recordEvent("shop.order.placed", {});
  client.error(new Error("x"));
  client.log("info", "started");
  assert.equal(await client.flush(), true);
  const body = w.fake.bodies()[0] ?? {};
  for (const kind of ["events", "errors", "logs"] as const) {
    const record = body[kind]?.[0] ?? {};
    assert.match(String(record.id), UUID7);
    assert.ok(Number.isInteger(record.ts) && (record.ts as number) >= before && (record.ts as number) <= Date.now());
  }
});

// ---------- the body ----------

test("the body matches spec/ingest.md exactly", async () => {
  const client = w.client({ version: "9cf5634" });
  client.recordEvent("shop.order.placed", { order_id: "o-17", total_cents: 1200, items: ["a"], note: undefined },
    { actor: "user", client: "web", traceId: "req-81", ts: 1794000000000 });
  client.recordEvent("shop.order.viewed", {});
  client.recordMetric("shop.queue.depth", 12, { queue: "email" }, { unit: "jobs", kind: "gauge", ts: 1794000000000 });
  client.recordMetric("shop.emails.sent", 3.5, {}, { kind: "counter", ts: undefined });
  client.error({ name: "KeyError", message: "missing key 42" }, { context: { route: "/pay" }, traceId: "req-81" });
  client.log("info", "started", { stream: "worker", fields: { pid: 81 } });
  client.log("warn", "slow");
  assert.equal(await client.flush(), true);

  const request = w.fake.requests[0];
  assert.ok(request);
  assert.equal(request.path, "/v1/ingest");
  assert.equal(request.headers["content-type"], "application/json");
  assert.equal(request.headers.authorization, undefined);
  const body = parse(request.raw);
  assert.deepEqual(Object.keys(body), ["source", "version", "host", "events", "metrics", "errors", "logs"]);
  assert.deepEqual([body.source, body.version, body.host], ["shop", "9cf5634", "web-1"]);
  const [first, second] = body.events ?? [];
  assert.deepEqual(first, { id: first?.id, name: "shop.order.placed", ts: 1794000000000, actor: "user", client: "web",
    trace_id: "req-81", props: { order_id: "o-17", total_cents: 1200, items: ["a"] } });
  assert.deepEqual(Object.keys(second ?? {}), ["id", "name", "ts", "props"], "unset context fields are left out, not null");
  const metrics = body.metrics ?? [];
  assert.deepEqual(metrics[0], { name: "shop.queue.depth", value: 12, ts: 1794000000000, labels: { queue: "email" }, unit: "jobs", kind: "gauge" });
  assert.deepEqual(metrics[1], { name: "shop.emails.sent", value: 3.5, ts: metrics[1]?.ts, labels: {}, kind: "counter" });
  const error = body.errors?.[0];
  assert.deepEqual(error, { id: error?.id, ts: error?.ts, severity: "error", kind: "KeyError", message: "missing key 42",
    context: { route: "/pay" }, trace_id: "req-81" });
  const [info, warn] = body.logs ?? [];
  assert.deepEqual(info, { id: info?.id, ts: info?.ts, level: "info", stream: "worker", message: "started", fields: { pid: 81 } });
  assert.deepEqual(Object.keys(warn ?? {}), ["id", "ts", "level", "message"]);
  assert.ok(!request.raw.includes("null"));
});

test("batch fields are left out when unset, and empty kinds are omitted", async () => {
  const client = w.client({ host: "" });
  client.recordMetric("shop.queue.depth", 1, { queue: "email" }, { unit: "jobs", kind: "gauge" });
  assert.equal(await client.flush(), true);
  assert.deepEqual(Object.keys(w.fake.bodies()[0] ?? {}), ["source", "metrics"]);
});

test("bearer token header, and an injected fetch is used", async () => {
  let calls = 0;
  const client = w.client({
    token: "s3cret-token",
    fetch: (input, init) => {
      calls += 1;
      return fetch(input, init);
    },
  });
  client.recordEvent("shop.order.placed", {});
  assert.equal(await client.flush(), true);
  assert.equal(w.fake.requests[0]?.headers.authorization, "Bearer s3cret-token");
  assert.equal(calls, 1);
});

// ---------- errors ----------

test("error() takes anything", async () => {
  const client = w.client();
  function charge(): never {
    throw new TypeError("card is not a string");
  }
  try {
    charge();
  } catch (error) {
    client.error(error, { severity: "warning", context: { order_id: "o-1" } });
  }
  class PaymentError extends Error {
    override name = "PaymentError";
  }
  client.error(new PaymentError("declined"));
  client.error("plain text");
  client.error({ code: 42 });
  client.error(17);
  client.error(undefined);
  assert.equal(await client.flush(), true);
  const errors = w.fake.bodies()[0]?.errors ?? [];
  assert.deepEqual(errors.map((e) => [e.kind, e.message]), [
    ["TypeError", "card is not a string"], ["PaymentError", "declined"], ["Error", "plain text"],
    ["Error", '{"code":42}'], ["Error", "17"], ["Error", "undefined"],
  ]);
  assert.match(String(errors[0]?.stack), /at charge/);
  assert.equal(errors[0]?.severity, "warning");
  assert.deepEqual(errors[0]?.context, { order_id: "o-1" });
  assert.equal(errors[2]?.stack, undefined);
});

test("expressErrorHandler records and calls next(error)", async () => {
  const client = w.client();
  const handler = client.expressErrorHandler();
  const failure = new Error("boom");
  const passed: unknown[] = [];
  handler(failure, { method: "POST", originalUrl: "/pay?card=4242" }, {}, (error) => passed.push(error));
  assert.deepEqual(passed, [failure]);
  assert.equal(await client.flush(), true);
  assert.deepEqual(w.fake.bodies()[0]?.errors?.[0]?.context, { method: "POST", path: "/pay" });
});

test("installProcessHooks: an uncaught exception is spooled and Node still crashes", async () => {
  const result = await runChild(`
    import { TrailClient } from ${JSON.stringify(INDEX)};
    const client = new TrailClient({ source: "shop", flushIntervalMs: 30000 });
    client.installProcessHooks();
    client.recordEvent("shop.order.placed", { order_id: "o-1" });
    function pay() { throw new RangeError("no money"); }
    setTimeout(pay, 10);
  `, { TRAIL_URL: w.fake.url, TRAIL_SPOOL_DIR: w.spool });
  assert.equal(result.code, 1);
  assert.match(result.stderr, /RangeError: no money/, "Node's own handling is unchanged");
  const files = w.spoolFiles();
  assert.equal(files.length, 1);
  const body = parse(w.readSpool(files[0] ?? ""));
  assert.equal(body.events?.length, 1);
  assert.deepEqual([body.errors?.[0]?.kind, body.errors?.[0]?.severity], ["RangeError", "fatal"]);
  assert.deepEqual(body.errors?.[0]?.context, { origin: "uncaughtException" });
});

test("installProcessHooks: an unhandled rejection is spooled and Node still crashes", async () => {
  const result = await runChild(`
    import { TrailClient } from ${JSON.stringify(INDEX)};
    const client = new TrailClient({ source: "shop", flushIntervalMs: 30000 });
    client.installProcessHooks();
    Promise.reject(new Error("lost promise"));
  `, { TRAIL_URL: w.fake.url, TRAIL_SPOOL_DIR: w.spool });
  assert.equal(result.code, 1);
  assert.match(result.stderr, /lost promise/);
  const body = parse(w.readSpool(w.spoolFiles()[0] ?? ""));
  assert.deepEqual([body.errors?.[0]?.message, body.errors?.[0]?.context], ["lost promise", { origin: "unhandledRejection" }]);
});

// ---------- never throws ----------

test("values that are not JSON are dropped and reported", async () => {
  const client = w.client();
  const loop: Record<string, unknown> = {};
  loop.self = loop;
  client.recordEvent("shop.order.placed", { big: 10n } as never);
  client.recordEvent("shop.order.placed", loop as never);
  client.recordEvent("shop.order.placed", { total: NaN });
  client.recordMetric("shop.queue.depth", Infinity, {}, { kind: "gauge" });
  client.log("info", "fields", { fields: { x: -Infinity } });
  client.recordEvent("shop.order.placed", { order_id: "o-1" });
  assert.equal(await client.flush(), true);
  assert.deepEqual(w.kinds(), ["dropped", "dropped", "dropped", "dropped", "dropped"]);
  assert.equal(w.problems[0]?.at, "event shop.order.placed");
  assert.equal(client.stats().dropped, 5);
  assert.deepEqual(props(w.fake.bodies()[0] ?? {}), [{ order_id: "o-1" }]);
});

test("a broken onProblem callback is ignored", async () => {
  const client = w.client({ onProblem: () => { throw new Error("callback bug"); } });
  client.recordEvent("shop.order.placed", { big: 1n } as never);
  w.fake.answer(500);
  client.recordEvent("shop.order.placed", {});
  assert.equal(await client.flush(500), false);
});

test("close twice, and recording after close", async () => {
  const client = w.client();
  client.recordEvent("shop.order.placed", {});
  await client.close();
  await client.close();
  client.recordEvent("shop.order.placed", {});
  assert.equal(w.fake.requests.length, 1);
  assert.equal(client.stats().sent, 1);
});

// ---------- spool ----------

for (const outage of ["500", "down"] as const) {
  test(`${outage === "500" ? "a 5xx" : "no answer"} spools one file per batch, resent oldest first by the next run`, async () => {
    if (outage === "500") w.fake.answer(500);
    else await w.fake.down();
    const client = w.client({ maxBatch: 2, flushIntervalMs: 30_000 });
    client.recordEvent("shop.order.placed", { n: 1 });
    client.recordEvent("shop.order.placed", { n: 2 });
    assert.equal(await client.flush(1000), false);
    assert.equal(w.spoolFiles().length, 1);
    for (const n of [3, 4, 5]) client.recordEvent("shop.order.placed", { n });
    await client.close(1000); // what the queue still holds goes to the spool, one file per batch
    const files = w.spoolFiles();
    assert.equal(files.length, 3);
    const spooled = files.map((f) => w.readSpool(f));
    assert.deepEqual(spooled.map((s) => props(parse(s)).map((p) => (p as { n: number }).n)), [[1, 2], [3, 4], [5]]);
    if (outage === "500") assert.equal(w.fake.requests[0]?.raw, spooled[0], "a spool file is the request body exactly as posted");
    assert.ok(w.kinds().includes("send"));

    if (outage === "down") await w.fake.up();
    w.fake.answer(200);
    w.fake.requests = [];
    const again = w.client({ maxBatch: 2, flushIntervalMs: 30_000 }); // the next run of the service
    assert.equal(await again.flush(2000), true);
    assert.deepEqual(w.fake.requests.map((r) => r.raw), spooled, "resent oldest first, with the same ids");
    assert.deepEqual(w.spoolFiles(), []);
    assert.equal(again.stats().sent, 5);
  });
}

test("resent in the same process after Trail returns", async () => {
  await w.fake.down();
  const client = w.client();
  client.recordEvent("shop.order.placed", { n: 1 });
  assert.equal(await client.flush(500), false);
  assert.deepEqual([client.stats().spooled, client.stats().sent], [1, 0]);
  const spooled = w.readSpool(w.spoolFiles()[0] ?? "");
  await w.fake.up();
  assert.equal(await client.flush(2000), true);
  assert.equal(w.fake.requests[0]?.raw, spooled);
});

test("a spool left by a previous process is sent without new records", async () => {
  mkdirSync(w.spool, { recursive: true });
  const left = JSON.stringify({ source: "shop", events: [{ id: uuid7(), name: "shop.order.placed", ts: 1794000000000, props: {} }] });
  writeFileSync(join(w.spool, `${uuid7()}.json`), left);
  w.client();
  assert.ok(await waitUntil(() => w.spoolFiles().length === 0, 2000));
  assert.equal(w.fake.requests[0]?.raw, left);
});

test("backoff after a failure", async () => {
  w.fake.answer(503);
  const client = w.client({ flushIntervalMs: 20 });
  client.recordEvent("shop.order.placed", {});
  assert.ok(await w.fake.waitFor(1));
  await sleep(400);
  assert.equal(w.fake.requests.length, 1, "no retry before the first backoff (1 s) is over");
  assert.equal(w.spoolFiles().length, 1);
});

test("without a spool folder, failed batches wait in memory", async () => {
  await w.fake.down();
  const client = w.client({ spoolDir: undefined });
  client.recordEvent("shop.order.placed", { n: 1 });
  assert.equal(await client.flush(500), false);
  assert.deepEqual(w.spoolFiles(), []);
  await w.fake.up();
  assert.equal(await client.flush(2000), true);
  assert.deepEqual(props(w.fake.bodies()[0] ?? {}), [{ n: 1 }]);
});

test("400, 401 and 413 are dropped and reported, not retried", async () => {
  for (const status of [400, 401, 413]) {
    w.problems = [];
    w.fake.requests = [];
    w.fake.answer(status);
    const client = w.client();
    client.recordEvent("shop.order.placed", {});
    assert.equal(await client.flush(1000), true);
    assert.equal(await client.flush(1000), true);
    assert.equal(w.fake.requests.length, 1, "not retried");
    assert.deepEqual(w.spoolFiles(), []);
    assert.deepEqual(w.kinds(), ["dropped"]);
    assert.ok(w.problems[0]?.message.includes(String(status)));
    if (status === 400) assert.ok(w.problems[0]?.message.includes("`Shop` is not a source name"));
    assert.equal(client.stats().dropped, 1);
    await client.close(200);
  }
});

test("the queue bound drops the oldest and counts them", async () => {
  await w.fake.down();
  const client = w.client({ maxQueue: 5, maxBatch: 100, flushIntervalMs: 30_000, spoolDir: undefined });
  for (let n = 0; n < 8; n++) client.recordEvent("shop.order.placed", { n });
  assert.equal(client.stats().dropped, 3);
  await w.fake.up();
  assert.equal(await client.flush(2000), true);
  assert.deepEqual(props(w.fake.bodies()[0] ?? {}), [3, 4, 5, 6, 7].map((n) => ({ n })));
  const dropped = w.problems.filter((p) => p.kind === "dropped");
  assert.equal(dropped[0]?.at, "queue");
  assert.ok(dropped[0]?.message.includes("3 record(s)"));
});

test("the spool bound drops the oldest and counts them", async () => {
  await w.fake.down();
  const record = (client: TrailClient) => {
    for (let n = 0; n < 3; n++) client.recordEvent("shop.order.placed", { n, pad: "x".repeat(100) });
  };
  const probe = w.client({ maxBatch: 1, flushIntervalMs: 30_000 });
  record(probe);
  await probe.close(500);
  assert.equal(w.spoolFiles().length, 3);
  const size = w.readSpool(w.spoolFiles()[0] ?? "").length;
  for (const name of w.spoolFiles()) rmSync(join(w.spool, name));

  w.problems = [];
  const bounded = w.client({ maxBatch: 1, flushIntervalMs: 30_000, maxSpoolBytes: size * 2 + 10 });
  record(bounded);
  await bounded.close(500);
  assert.deepEqual(w.spoolFiles().map((f) => (props(parse(w.readSpool(f)))[0] as { n: number }).n), [1, 2]);
  assert.equal(bounded.stats().dropped, 1);
  assert.ok(w.problems.some((p) => p.kind === "dropped" && p.at === "spool"));
});

// ---------- reports, strict, environment ----------

test("rejected and mismatched entries go to onProblem", async () => {
  w.fake.answer(200, {
    rejected: [{ at: "events[1]", message: "`Order` is not an event name", fix: "use snake_case parts joined by dots" }],
    mismatched: [{ at: "events[0]", message: "unknown property `latncy_ms` on `shop.order.placed`" }],
  });
  const client = w.client();
  client.recordEvent("shop.order.placed", { latncy_ms: 3 });
  client.recordEvent("Order", {});
  assert.equal(await client.flush(), true);
  assert.deepEqual(w.problems.map((p) => [p.kind, p.at]), [["rejected", "events[1]"], ["mismatched", "events[0]"]]);
  assert.equal(w.problems[0]?.fix, "use snake_case parts joined by dots");
  assert.equal(w.problems[1]?.fix, "");
  const stats = client.stats();
  assert.deepEqual([stats.sent, stats.rejected, stats.mismatched], [1, 1, 1]);
});

test("strict mode rejects at flush", async () => {
  w.fake.answer(200, { mismatched: [{ at: "events[0]", message: "unknown property", fix: "declare it" }] });
  const client = w.client({ strict: true });
  client.recordEvent("shop.order.placed", { big: 1n } as never); // does not throw here
  client.recordEvent("shop.order.placed", { extra: 1 });
  await assert.rejects(client.flush(), (error: unknown) => {
    assert.ok(error instanceof TrailError);
    assert.deepEqual(error.problems.map((p) => p.kind), ["dropped", "mismatched"]);
    assert.match(error.message, /unknown property/);
    return true;
  });
  w.fake.answer(200);
  client.recordEvent("shop.order.placed", {});
  assert.equal(await client.flush(), true, "problems are cleared once raised");
});

test("strict mode from TRAIL_STRICT rejects when sending fails", async () => {
  await w.fake.down();
  process.env.TRAIL_STRICT = "1";
  const client = w.client();
  client.recordEvent("shop.order.placed", {});
  await assert.rejects(client.flush(500), (error: unknown) => error instanceof TrailError && error.problems[0]?.kind === "send");
});

test("TRAIL_DISABLED=1 makes every call a no-op", async () => {
  process.env.TRAIL_DISABLED = "1";
  const client = w.client();
  client.recordEvent("shop.order.placed", {});
  client.recordMetric("shop.queue.depth", 1, {}, { kind: "gauge" });
  client.error(new Error("x"));
  client.log("info", "x");
  client.installProcessHooks();
  assert.equal(await client.flush(), true);
  await client.close();
  await sleep(100);
  assert.equal(w.fake.requests.length, 0);
  assert.deepEqual(w.spoolFiles(), []);
  assert.equal(client.stats().sent, 0);
  assert.equal(process.listenerCount("uncaughtExceptionMonitor"), 0);
});

test("environment defaults", async () => {
  Object.assign(process.env, {
    TRAIL_URL: `${w.fake.url}/`, TRAIL_VERSION: "9cf5634", TRAIL_HOST: "worker-2",
    TRAIL_TOKEN: "env-token", TRAIL_SPOOL_DIR: `${w.spool}-env`,
  });
  const client = new TrailClient({ source: "shop", flushIntervalMs: 50 });
  try {
    await w.fake.down();
    client.recordEvent("shop.order.placed", {});
    assert.equal(await client.flush(500), false);
    assert.equal(w.spoolFiles(`${w.spool}-env`).length, 1);
    await w.fake.up();
    assert.equal(await client.flush(), true);
    const request = w.fake.requests[0];
    assert.deepEqual([parse(request?.raw ?? "{}").version, parse(request?.raw ?? "{}").host], ["9cf5634", "worker-2"]);
    assert.equal(request?.headers.authorization, "Bearer env-token");
    assert.equal(client.strict, false);
  } finally {
    await client.close(200);
  }
});

test("defaults without environment, and options win over it", async () => {
  const plain = new TrailClient({ source: "shop" });
  assert.equal(plain.endpoint, "http://127.0.0.1:8200/v1/ingest");
  assert.equal(plain.host, hostname());
  assert.equal(plain.version, undefined);
  assert.equal(plain.strict, false);
  await plain.close(100);
  Object.assign(process.env, { TRAIL_HOST: "env-host", TRAIL_VERSION: "env" });
  const client = w.client({ version: "opt" });
  assert.deepEqual([client.host, client.version], ["web-1", "opt"]);
});

// ---------- process ----------

test("a short script loses nothing", async () => {
  const result = await runChild(`
    import { TrailClient } from ${JSON.stringify(INDEX)};
    new TrailClient({ source: "shop", flushIntervalMs: 30000 }).recordEvent("shop.order.placed", { order_id: "o-1" });
  `, { TRAIL_URL: w.fake.url });
  assert.equal(result.code, 0, result.stderr);
  assert.deepEqual(props(w.fake.bodies()[0] ?? {}), [{ order_id: "o-1" }]);
});

test("at exit, what cannot be sent goes to the spool folder", async () => {
  await w.fake.down();
  const result = await runChild(`
    import { TrailClient } from ${JSON.stringify(INDEX)};
    const client = new TrailClient({ source: "shop", flushIntervalMs: 30000, maxBatch: 2 });
    for (let n = 0; n < 5; n++) client.recordEvent("shop.order.placed", { n });
  `, { TRAIL_URL: w.fake.url, TRAIL_SPOOL_DIR: w.spool });
  assert.equal(result.code, 0, result.stderr);
  const counts = w.spoolFiles().map((f) => parse(w.readSpool(f)).events?.length ?? 0);
  assert.equal(counts.reduce((a, b) => a + b, 0), 5);
  assert.ok(counts.every((c) => c <= 2));
});

test("process.exit() writes the queue to the spool folder", async () => {
  const result = await runChild(`
    import { TrailClient } from ${JSON.stringify(INDEX)};
    const client = new TrailClient({ source: "shop", flushIntervalMs: 30000 });
    setTimeout(() => { client.recordEvent("shop.order.placed", { n: 1 }); process.exit(3); }, 50);
  `, { TRAIL_URL: w.fake.url, TRAIL_SPOOL_DIR: w.spool });
  assert.equal(result.code, 3);
  assert.equal(w.spoolFiles().length, 1);
});

test("importing the package does not load node:fs until a folder spool is used", async () => {
  const result = await runChild(`
    import { registerHooks } from "node:module";
    const seen = [];
    registerHooks({ resolve(specifier, context, next) {
      if (specifier === "node:fs" || specifier === "fs") seen.push(specifier);
      return next(specifier, context);
    } });
    const { TrailClient } = await import(${JSON.stringify(INDEX)});
    const memory = new TrailClient({ source: "shop", url: "http://127.0.0.1:9" });
    memory.recordEvent("shop.order.placed", {});
    await memory.close(50);
    const before = seen.length;
    const folder = new TrailClient({ source: "shop", url: "http://127.0.0.1:9", spoolDir: process.env.DIR });
    await folder.close(50);
    console.log(JSON.stringify({ before, after: seen.length }));
  `, { DIR: w.spool });
  assert.equal(result.code, 0, result.stderr);
  assert.deepEqual(JSON.parse(result.stdout.trim()), { before: 0, after: 1 });
});

// ---------- browser ----------

interface FakePage { window: EventTarget; document: EventTarget & { visibilityState: string }; localStorage: MemoryStorage }

function asBrowser(storage: MemoryStorage): FakePage {
  const g = globalThis as unknown as Partial<FakePage>;
  const page: FakePage = { window: new EventTarget(), document: Object.assign(new EventTarget(), { visibilityState: "visible" }), localStorage: storage };
  Object.assign(g, page);
  return page;
}

function leaveBrowser(): void {
  const g = globalThis as unknown as Record<string, unknown>;
  delete g.window;
  delete g.document;
  delete g.localStorage;
}

test("browser: the localStorage spool keeps batches for the next page load", async () => {
  const storage = new MemoryStorage();
  asBrowser(storage);
  try {
    await w.fake.down();
    const page = w.client({ spoolDir: w.spool }); // ignored in a browser
    page.recordEvent("shop.order.placed", { n: 1 });
    assert.equal(await page.flush(500), false);
    await page.close(200);
    assert.deepEqual(w.spoolFiles(), []);
    const keys = storage.keys();
    assert.equal(keys.length, 1);
    assert.match(keys[0] ?? "", /^trail-spool:shop:[0-9a-f-]{36}\.json$/);
    const spooled = storage.getItem(keys[0] ?? "");

    await w.fake.up();
    w.client(); // the next page load
    assert.ok(await waitUntil(() => storage.length === 0, 2000));
    assert.equal(w.fake.requests[0]?.raw, spooled);
  } finally {
    leaveBrowser();
  }
});

test("browser: a full localStorage drops the oldest batch", async () => {
  const storage = new MemoryStorage(900);
  asBrowser(storage);
  try {
    await w.fake.down();
    const page = w.client({ maxBatch: 1, flushIntervalMs: 30_000 });
    for (let n = 0; n < 5; n++) page.recordEvent("shop.order.placed", { n, pad: "x".repeat(150) });
    await page.close(300);
    const kept = storage.keys().map((k) => (props(parse(storage.getItem(k) ?? "{}"))[0] as { n: number }).n);
    assert.ok(kept.length >= 1 && kept.length < 5);
    const newest = Array.from({ length: kept.length }, (_, i) => 5 - kept.length + i);
    assert.deepEqual(kept, newest, "the oldest batches went");
    assert.equal(page.stats().dropped, 5 - kept.length);
  } finally {
    leaveBrowser();
  }
});

test("browser: pagehide and visibilitychange send the queue with keepalive", async () => {
  const fake = asBrowser(new MemoryStorage());
  try {
    const seen: (RequestInit | undefined)[] = [];
    const page = w.client({ flushIntervalMs: 30_000, fetch: (input, init) => { seen.push(init); return fetch(input, init); } });
    page.recordEvent("shop.order.placed", { n: 1 });
    fake.window.dispatchEvent(new Event("pagehide"));
    assert.ok(await w.fake.waitFor(1));
    page.recordEvent("shop.order.placed", { n: 2 });
    fake.document.visibilityState = "hidden";
    fake.document.dispatchEvent(new Event("visibilitychange"));
    assert.ok(await w.fake.waitFor(2));
    assert.deepEqual(seen.map((init) => init?.keepalive), [true, true]);
    assert.deepEqual(w.fake.bodies().map((b) => props(b)), [[{ n: 1 }], [{ n: 2 }]]);
    assert.ok(await waitUntil(() => fake.localStorage.length === 0, 1000), "answered batches leave the spool");
  } finally {
    leaveBrowser();
  }
});

test("browser: installBrowserHooks reports error and unhandledrejection events", async () => {
  const fake = asBrowser(new MemoryStorage());
  try {
    const page = w.client();
    page.installBrowserHooks();
    fake.window.dispatchEvent(Object.assign(new Event("error"), { error: new SyntaxError("bad token"), message: "bad token" }));
    fake.window.dispatchEvent(Object.assign(new Event("unhandledrejection"), { reason: "nope" }));
    assert.equal(await page.flush(), true);
    assert.deepEqual((w.fake.bodies()[0]?.errors ?? []).map((e) => [e.kind, e.message]), [["SyntaxError", "bad token"], ["Error", "nope"]]);
  } finally {
    leaveBrowser();
  }
});
