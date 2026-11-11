# trail-client (Node and browsers)

Sends a service's events, metrics, errors and logs to a Trail server. No dependencies. Node
22.18+ runs the TypeScript source directly (type stripping, no build step); a bundler takes it
as is for the browser. The contract it implements is `spec/client.md`.

## Install

```sh
npm install ./clients/node        # from a Trail checkout
```

Node does not strip types from files whose real path is under `node_modules`
(`ERR_UNSUPPORTED_NODE_MODULES_TYPE_STRIPPING`). A local-path install or a workspace link is a
symlink, so it works in plain Node; a copy (a tarball, or a registry install) needs a bundler or
a loader that compiles TypeScript. Bundlers (esbuild, Vite, webpack) need nothing extra.

## Use

Generate the typed emitters from your declarations, then call only those:

```sh
trail gen trail/ --lang typescript --out src/shop_events.ts
```

```ts
import { TrailClient } from "trail-client";
import { events, metrics } from "./shop_events.ts";

const client = new TrailClient({ source: "shop", spoolDir: "/var/lib/shop/trail-spool" });
events(client).shopOrderFailed({ order_id: "o-17", reason: "payment" });
metrics(client).shopQueueDepth(12, { queue: "email" });
```

Errors and logs need no declarations:

```ts
client.installProcessHooks();                 // Node: uncaught exceptions and unhandled rejections
app.use(client.expressErrorHandler());        // Express: records, then next(error)
client.error(error, { context: { order_id: "o-17" } });   // anything thrown
client.log("info", "worker started", { stream: "worker", fields: { pid: 81 } });
```

Recording never blocks and never throws. Records go out in batches of up to 200, every second.
The timer is `unref`'d, so it never keeps a process alive; when the event loop empties, the queue
gets one short flush (2 s), and on `process.exit()` what is queued is written to the spool folder.
`await client.flush()` and `await client.close()` are there when you need them, e.g. in tests.

`installProcessHooks()` listens on `uncaughtExceptionMonitor`, which sees both uncaught
exceptions and unhandled rejections without changing how Node handles them: the process still
crashes as it would have. The error and the queue are written to the spool folder synchronously
first, and sent by the next run.

## Without a build step

```sh
npm run build        # writes dist/*.js and dist/*.d.ts
```

`dist/index.js` is the client as plain JavaScript modules: serve the folder and import it from a
page, or install a copy under `node_modules` (which the note under Install rules out for the
TypeScript source). `trail gen --lang javascript` writes the emitters to go with it, typed with
JSDoc, so `tsc --checkJs` or an editor still catches a misspelt event:

```js
import { TrailClient } from "./trail-client/index.js";
import { events } from "./shop_events.js";

const shop = events(new TrailClient({ source: "shop", url: "/trail" }));
shop.shopPageViewed({ path: location.pathname, signed_in: false });
```

The generated file names `"trail-client"` only inside a type comment, which a browser never
resolves.

## In a browser

The same import works. The spool is `localStorage` (keys `trail-spool:<source>:…`; `spoolDir` is
ignored), and `node:fs` is never loaded. On `pagehide`, or when the page becomes hidden, what is
queued is sent with `fetch(…, { keepalive: true })` so it outlives the page; it stays in
`localStorage` until answered, so a page that closes first resends it on its next load.
`installBrowserHooks()` reports `error` and `unhandledrejection` events. Trail sends no CORS headers, so
reach it on the page's own origin, through the reverse proxy that serves the page (`url: "/trail"`).
A token in a browser is visible to anyone using the page.

## Environment (Node)

Read when the option is not passed in code. The source is always passed in code.

| Variable | Default | Meaning |
|---|---|---|
| `TRAIL_URL` | `http://127.0.0.1:8200` | The server; the client posts to `<url>/v1/ingest` |
| `TRAIL_VERSION` | none | The deployed version, usually a commit |
| `TRAIL_HOST` | the hostname | The machine |
| `TRAIL_TOKEN` | none | Sent as `Authorization: Bearer <token>` |
| `TRAIL_SPOOL_DIR` | none (memory) | Where batches wait while Trail is down |
| `TRAIL_STRICT` | off | `1`: `flush()` rejects with `TrailError` listing every problem |
| `TRAIL_DISABLED` | off | `1`: every call is a no-op |

## When Trail is down

In Node the spool is in memory by default: batches that could not be sent wait in the process and
are lost when it ends. Pass `spoolDir` (or set `TRAIL_SPOOL_DIR`) to keep them in a folder. In a
browser the spool is always `localStorage`.

With a spool (a folder in Node, `localStorage` in a browser), every batch is written there as one
entry (the request body as it will be posted, named `<uuid7>.json`) before it is sent, and removed
once Trail takes it, or answers `400`, `401`, or `413` for a single record. A batch that gets no
answer, a `5xx` or any other status stays and is retried oldest first, with backoff from 1 s
doubling to 60 s. So a process or page that dies mid-request leaves its batch behind, and a spool
left by a previous run is sent by the next. Records keep their ids, so a resend is stored once.

Only Trail's own answer counts as taken: a `2xx` whose body is a JSON object with `accepted`. Any
other `2xx`, such as a proxy's catch-all page or a sign-on page answering `200` with HTML, is
treated like no answer: the batch is kept and retried, and `onProblem` is told the answer did not
come from Trail. A redirect is never followed (`redirect: "manual"`), so a `3xx` is kept and
retried too: following it would turn the POST into a GET, and the new page's `200` would look like
Trail taking the batch.

A `413` (too large) splits the batch in half and sends each half on its own, splitting again on
another `413`, so one oversized record ends up alone; in a spool the two halves replace the
batch's entry. A single record that still gets `413` is dropped and reported.

The queue holds 10,000 records and the spool 50 MB; past either the oldest go, and
`client.stats().dropped` counts them. A `400` or `401` is not retried: the batch is dropped and
reported. Every problem, including records Trail rejected or marked as mismatched, goes to
`onProblem`.

## Tests

```sh
cd clients/node
npm ci
node --test            # also builds dist/ and loads it
npx tsc --noEmit       # src, tests and the generated plain JavaScript
```
