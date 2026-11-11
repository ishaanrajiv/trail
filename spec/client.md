# Clients and generated emitters

A service sends through two layers:

1. **A client library** (`clients/python`, `clients/node`, `clients/swift`): queues records, sends
   them in batches, and keeps them on disk while Trail is unreachable.
2. **A generated file** (`trail gen`): one typed function per declared event and metric, which
   calls the client. Application code calls only the generated functions, so an undeclared event
   or a wrong property type fails the service's own type check.

This file is the contract between the two, and between each client and the server
(`spec/ingest.md`). `examples/shop/generated/` holds the generated files for the example project;
each client's tests type-check against them.

## What every client does

| Behaviour | Rule |
|---|---|
| Never blocks | Recording appends to an in-memory queue and returns. Sending happens in the background. |
| Never throws | Recording never raises into application code, unless `strict` is on. |
| Batches | Up to `max_batch` (200) records per request, sent every `flush_interval` (1 s) or when a batch fills. |
| Ids and times | Each event, error and log gets a UUIDv7 `id` and a `ts` (ms) when recorded, so a resend is stored once. |
| Taken | A batch counts as taken only when the answer is a `2xx` whose body is Trail's ingest answer: a JSON object with `accepted` (`spec/ingest.md`). Any other `2xx` (a proxy's catch-all page, a sign-on wall, a captive portal answering `200` with HTML) is treated like no answer: the batch is kept and retried, and `on_problem` gets a `send` problem saying the answer did not come from Trail. |
| Spools | With a spool, each batch is written to it as one file before it is sent, and deleted only after a final answer. A batch that gets no answer, a `5xx`, or any other status not listed under "Drops what can't succeed" stays there and is retried, oldest first, with backoff (1 s doubling to 60 s). Without one, failed batches wait in memory. |
| Drops what can't succeed | `400`, `401`, and a `413` for a single record, are not retried: the batch is dropped and reported through `on_problem`. Any other status (a `404` or `429` from a proxy, say) is retried like a `5xx`. |
| Splits on `413` | A batch of more than one record that gets `413` is split in half, and each half is sent on its own, splitting again on another `413`, so one oversized record ends up alone. With a spool the two halves replace the batch's file, written before it is deleted. A single record that still gets `413` is dropped and reported through `on_problem`. |
| Redirects | A client never follows a redirect: a `3xx` is retried like any other status, with the batch kept. Following it would turn the POST into a GET, and the new page's `2xx` would look like Trail taking the batch. |
| Bounded | The queue holds `max_queue` (10,000) records and the spool `max_spool_bytes` (50 MB). Past either, the oldest go and `stats().dropped` counts them. |
| Reports | `rejected` and `mismatched` entries in a response go to `on_problem`. |
| Strict mode | For tests. Recording a record the server rejects or marks, or failing to send, raises at `flush` (`TrailError` in Python, a rejected promise in TypeScript; in Swift `flush()` returns false). On by `strict=True` or `TRAIL_STRICT=1`. |
| Off switch | `TRAIL_DISABLED=1` makes every call a no-op. |

Environment defaults, read when an option is not passed: `TRAIL_URL` (default
`http://127.0.0.1:8200`), `TRAIL_VERSION`, `TRAIL_HOST` (default the hostname), `TRAIL_TOKEN`,
`TRAIL_SPOOL_DIR`, `TRAIL_STRICT`, `TRAIL_DISABLED`. The source is always passed in code.

A spool file is one request body, exactly as it will be posted, named `<uuid7>.json` so files
sort by age. It is written before the request starts, so a process killed mid-request leaves it
for its next run, and deleted after Trail's answer, a `400`, a `401`, or a `413` for a single
record. A file split after a `413` is replaced by one file per half, each a new `<uuid7>.json`
with the same `source`, `version` and `host`. The halves are written before the file is deleted,
so a process killed in between sends some records twice, which their ids make harmless, and never
loses one. A client never posts the same file twice at once. A new batch counts toward
`max_spool_bytes` like any other: the oldest files go to make room for it, but never a file being
sent. A batch bigger than the whole limit is still sent; if that send fails, it is dropped, as any
failed batch that does not fit is. If the spool cannot be written, the batch is sent from memory
and goes back to the queue if that fails.

## The API a generated file calls

Named `record_event` and `record_metric` in Python, `recordEvent` and `recordMetric` in
TypeScript and Swift. They are public so generated code can call them; application code should
not.

### Python: `trail_client` (standard library only, Python 3.10+)

```python
@dataclass(frozen=True)
class Context:
    actor: str | None = None
    client: str | None = None
    trace_id: str | None = None
    ts: int | None = None          # ms; default now

class Client:
    def __init__(self, source: str, *, url: str | None = None, version: str | None = None,
                 host: str | None = None, token: str | None = None,
                 spool_dir: str | os.PathLike[str] | None = None, flush_interval: float = 1.0,
                 max_batch: int = 200, max_queue: int = 10_000, max_spool_bytes: int = 50_000_000,
                 strict: bool | None = None,
                 on_problem: Callable[[Problem], None] | None = None) -> None: ...
    def record_event(self, name: str, props: Mapping[str, Any], context: Context | None = None) -> None: ...
    def record_metric(self, name: str, value: float, labels: Mapping[str, str], *,
                      unit: str | None, kind: Literal["gauge", "counter"], ts: int | None = None) -> None: ...
    def error(self, error: BaseException | None = None, *, kind: str | None = None,
              message: str | None = None, severity: Literal["warning", "error", "fatal"] = "error",
              context: Mapping[str, Any] | None = None, trace_id: str | None = None) -> None: ...
    def log(self, level: Literal["trace", "debug", "info", "warn", "error", "fatal"], message: str, *,
            stream: str | None = None, fields: Mapping[str, Any] | None = None) -> None: ...
    def flush(self, timeout: float = 5.0) -> bool: ...     # True when the queue and spool are empty
    def close(self, timeout: float = 5.0) -> None: ...
    def install_excepthook(self) -> None: ...             # sys.excepthook and threading.excepthook, chained
    def stats(self) -> Stats: ...                         # sent, spooled, dropped, rejected, mismatched
```

`Problem` is a frozen dataclass: `kind` (`"rejected"`, `"mismatched"`, `"dropped"`, `"send"`),
`at`, `message`, `fix`.

### TypeScript: `trail-client` (no dependencies, Node 22+ and browsers)

Shipped as TypeScript source with erasable syntax only, so Node runs it without a build.
`npm run build` writes the same modules as plain JavaScript with `.d.ts` files to `dist/`, for a
page with no build step.

```ts
export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export interface EmitContext { actor?: string; client?: string; traceId?: string; ts?: number }
export interface TrailProblem { kind: "rejected" | "mismatched" | "dropped" | "send"; at: string; message: string; fix: string }
export interface TrailOptions {
  source: string; url?: string; version?: string; host?: string; token?: string;
  spoolDir?: string;            // Node: a folder. Browser: ignored; localStorage is used.
  flushIntervalMs?: number; maxBatch?: number; maxQueue?: number; maxSpoolBytes?: number;
  strict?: boolean; onProblem?: (problem: TrailProblem) => void; fetch?: typeof fetch;
}
export class TrailClient {
  constructor(options: TrailOptions);
  recordEvent(name: string, props: Readonly<Record<string, JsonValue | undefined>>, context?: EmitContext): void;
  recordMetric(name: string, value: number, labels: Readonly<Record<string, string>>,
               options: { unit?: string | undefined; kind: "gauge" | "counter"; ts?: number | undefined }): void;
  error(error: unknown, options?: { severity?: "warning" | "error" | "fatal"; context?: Record<string, JsonValue>; traceId?: string }): void;
  log(level: "trace" | "debug" | "info" | "warn" | "error" | "fatal", message: string,
      options?: { stream?: string; fields?: Record<string, JsonValue> }): void;
  flush(timeoutMs?: number): Promise<boolean>;
  close(timeoutMs?: number): Promise<void>;
  installProcessHooks(): void;        // Node: `uncaughtExceptionMonitor`, which sees unhandled rejections too, so Node's own handling is unchanged
  installBrowserHooks(): void;        // browser: error and unhandledrejection events
  expressErrorHandler(): (error: unknown, req: unknown, res: unknown, next: (error?: unknown) => void) => void;
  stats(): { sent: number; spooled: number; dropped: number; rejected: number; mismatched: number };
}
```

### Swift: module `TrailClient` (Foundation only, iOS 16+, macOS 13+)

```swift
public indirect enum TrailValue: Sendable, Equatable {   // also ExpressibleBy… literals
    case null, bool(Bool), int(Int), double(Double), string(String), array([TrailValue]), object([String: TrailValue])
}
public struct EmitContext: Sendable {
    public var actor: String?, client: String?, traceId: String?, ts: Int64?
    public init(actor: String? = nil, client: String? = nil, traceId: String? = nil, ts: Int64? = nil)
}
public enum MetricKind: String, Sendable { case gauge, counter }
public enum Severity: String, Sendable { case warning, error, fatal }
public enum LogLevel: String, Sendable { case trace, debug, info, warn, error, fatal }
public struct TrailProblem: Sendable { public let kind: String, at: String, message: String, fix: String }

public final class TrailClient: @unchecked Sendable {
    public init(source: String, url: URL, version: String? = nil, host: String? = nil, token: String? = nil,
                spoolDirectory: URL? = nil, flushInterval: TimeInterval = 1, maxBatch: Int = 200,
                maxQueue: Int = 10_000, maxSpoolBytes: Int = 50_000_000, session: URLSession = .shared,
                onProblem: (@Sendable (TrailProblem) -> Void)? = nil)
    public func recordEvent(_ name: String, _ props: [String: TrailValue], _ context: EmitContext = EmitContext())
    public func recordMetric(_ name: String, _ value: Double, labels: [String: String], unit: String?,
                             kind: MetricKind, ts: Int64? = nil)
    public func error(kind: String, message: String, stack: String? = nil, severity: Severity = .error,
                      context: [String: TrailValue] = [:], traceId: String? = nil)
    public func error(_ error: Error, severity: Severity = .error, context: [String: TrailValue] = [:], traceId: String? = nil)
    public func log(_ level: LogLevel, _ message: String, stream: String? = nil, fields: [String: TrailValue] = [:])
    public func flush() async -> Bool      // send everything now; call it when the app goes to the background
    public func persist()                  // write the queue to the spool without sending, for an app about to be killed
    public func close()
    public func stats() -> TrailStats
}
```

`url` is the server's base address and is required: an app has no loopback Trail to default to.
Every batch is written to the spool before it is sent, so one in flight when the app is killed is
sent at the next launch. With no `spoolDirectory` the spool is `Application Support/trail-spool/<source>`.
`persist()` and `close()` return only once everything recorded before the call is in the spool,
including a batch the background thread had already taken from the queue.
On iOS, tvOS, watchOS and visionOS `host` is left out unless given: a phone's hostname is usually
its owner's name.

## What `trail gen` writes

`--lang` is `python`, `typescript`, `javascript` or `swift`. Rules shared by all of them:

- Events and metrics in name order. Within an event, required properties in name order, then
  optional ones in name order.
- An optional property that is not given is left out of `props`, never sent as null.
- The file starts with a two-line header saying it is generated and from which source.
- The same declarations always produce the same bytes, so `trail gen --check` can compare.
- Names that would collide after conversion (below) are a `trail check` error.
- Each file imports only the names it uses.
- A property or label whose name is a keyword of the language, or clashes with a parameter the
  function already has (`context`, `value`, `ts`…), gets a trailing `_` as a parameter. The name
  on the wire never changes.
- Labels keep their declared order, and so do enum values.

| | Python | TypeScript | Swift |
|---|---|---|---|
| Event function | `shop_order_placed` (every character other than a letter or digit becomes `_`) | `shopOrderPlaced` | `shopOrderPlaced` |
| Property name | as declared | as declared, inside a props type | lowerCamel argument label |
| `string` / `int` / `float` / `bool` | `str` / `int` / `float` / `bool` | `string` / `number` / `number` / `boolean` | `String` / `Int` / `Double` / `Bool` |
| `string[]` | `list[str]` | `string[]` | `[String]` |
| `json` | `Any` | `JsonValue` | `TrailValue` |
| enum | `Literal["a", "b"]` | `"a" \| "b"` | a nested `enum …: String` |
| optional | `T \| None = None` | `name?: T` | `T? = nil` |

Python, for the example's `shop.order.failed` and its metrics:

```python
# Generated by `trail gen` from the declarations of `shop`. Do not edit.
# Change the TOML files and run `trail gen` again.
from __future__ import annotations

from typing import Any, Literal

from trail_client import Client, Context

SOURCE = "shop"


class Events:
    """Typed emitters for the events `shop` declares."""

    def __init__(self, client: Client) -> None:
        self._client = client

    def shop_order_failed(
        self,
        *,
        order_id: str,
        reason: Literal["payment", "out-of-stock"],
        detail: Any | None = None,
        context: Context | None = None,
    ) -> None:
        """An order could not be completed"""
        props: dict[str, Any] = {"order_id": order_id, "reason": reason}
        if detail is not None:
            props["detail"] = detail
        self._client.record_event("shop.order.failed", props, context)


class Metrics:
    """Typed emitters for the metrics `shop` declares."""

    def __init__(self, client: Client) -> None:
        self._client = client

    def shop_emails_sent(self, value: float, *, ts: int | None = None) -> None:
        """Emails sent since the worker started"""
        self._client.record_metric("shop.emails.sent", value, {}, unit=None, kind="counter", ts=ts)

    def shop_queue_depth(self, value: float, *, queue: str, ts: int | None = None) -> None:
        """Jobs waiting in a queue (jobs)"""
        self._client.record_metric("shop.queue.depth", value, {"queue": queue}, unit="jobs", kind="gauge", ts=ts)
```

JavaScript is the TypeScript output for a page or a service with no build step: the same
names, the same calls into `trail-client`, with the types written as JSDoc. `// @ts-check` at the
top makes an editor or `tsc --checkJs` hold callers to them (TypeScript 5.5 or later, for the
`@import` tag). Instead of classes it exports two functions, `events(client)` and
`metrics(client)`, each returning an object of emitters:

```js
// Generated by `trail gen` from the declarations of `shop`. Do not edit.
// Change the TOML files and run `trail gen` again.
// @ts-check

/** @import { EmitContext, JsonValue, TrailClient } from "trail-client" */

export const SOURCE = "shop";

/**
 * An order could not be completed
 * @typedef {object} ShopOrderFailed
 * @property {string} order_id
 * @property {"payment" | "out-of-stock"} reason
 * @property {JsonValue} [detail]
 */

/**
 * Typed emitters for the events `shop` declares.
 * @param {TrailClient} client
 */
export function events(client) {
  return {
    /**
     * An order could not be completed
     * @param {ShopOrderFailed} props
     * @param {EmitContext} [context]
     */
    shopOrderFailed(props, context) {
      client.recordEvent("shop.order.failed", props, context);
    },
  };
}
```

TypeScript. Props are `type` aliases, not interfaces, so they are assignable to the record type
`recordEvent` takes:

```ts
// Generated by `trail gen` from the declarations of `shop`. Do not edit.
// Change the TOML files and run `trail gen` again.
import type { EmitContext, JsonValue, TrailClient } from "trail-client";

export const SOURCE = "shop";

/** An order could not be completed */
export type ShopOrderFailed = {
  order_id: string;
  reason: "payment" | "out-of-stock";
  detail?: JsonValue;
};

/** Typed emitters for the events `shop` declares. */
export function events(client: TrailClient) {
  return {
    /** An order could not be completed */
    shopOrderFailed(props: ShopOrderFailed, context?: EmitContext): void {
      client.recordEvent("shop.order.failed", props, context);
    },
  };
}

/** Typed emitters for the metrics `shop` declares. */
export function metrics(client: TrailClient) {
  return {
    /** Emails sent since the worker started */
    shopEmailsSent(value: number, ts?: number): void {
      client.recordMetric("shop.emails.sent", value, {}, { kind: "counter", ts });
    },
    /** Jobs waiting in a queue (jobs) */
    shopQueueDepth(value: number, labels: { queue: string }, ts?: number): void {
      client.recordMetric("shop.queue.depth", value, labels, { unit: "jobs", kind: "gauge", ts });
    },
  };
}
```

An event with no properties has the props type `Record<string, never>`, and when every property
is optional the argument may be left out.

Swift. The two structs are named after the source in UpperCamel (`mail-worker` gives
`MailWorkerEvents`). An enum is nested, named `<Event><Property>` in UpperCamel; each case is the
value in lowerCamel, with a leading `_` if it would start with a digit and backticks if it is a
Swift keyword. Characters other than ASCII letters and digits separate words; a value with none
is a `trail check` error:

```swift
// Generated by `trail gen` from the declarations of `shop`. Do not edit.
// Change the TOML files and run `trail gen` again.
import TrailClient

/// Typed emitters for the events `shop` declares.
public struct ShopEvents: Sendable {
    private let client: TrailClient

    public init(_ client: TrailClient) {
        self.client = client
    }

    public enum ShopOrderFailedReason: String, Sendable {
        case payment = "payment"
        case outOfStock = "out-of-stock"
    }

    /// An order could not be completed
    public func shopOrderFailed(
        orderId: String,
        reason: ShopOrderFailedReason,
        detail: TrailValue? = nil,
        context: EmitContext = EmitContext()
    ) {
        var props: [String: TrailValue] = [
            "order_id": .string(orderId),
            "reason": .string(reason.rawValue),
        ]
        if let detail { props["detail"] = detail }
        client.recordEvent("shop.order.failed", props, context)
    }
}

/// Typed emitters for the metrics `shop` declares.
public struct ShopMetrics: Sendable {
    private let client: TrailClient

    public init(_ client: TrailClient) {
        self.client = client
    }

    /// Emails sent since the worker started
    public func shopEmailsSent(_ value: Double, ts: Int64? = nil) {
        client.recordMetric("shop.emails.sent", value, labels: [:], unit: nil, kind: .counter, ts: ts)
    }

    /// Jobs waiting in a queue (jobs)
    public func shopQueueDepth(_ value: Double, queue: String, ts: Int64? = nil) {
        client.recordMetric("shop.queue.depth", value, labels: ["queue": queue], unit: "jobs", kind: .gauge, ts: ts)
    }
}
```

In Swift, `int` is `.int(x)`, `float` is `.double(x)`, `bool` is `.bool(x)`, `string` is
`.string(x)`, and `string[]` is `.array(x.map { .string($0) })`. A `props` dictionary with no
optional properties is declared with `let`.
