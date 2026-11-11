# TrailClient (Swift)

Sends events, metrics, errors and logs to Trail. Foundation only, no dependencies; iOS 16+,
macOS 13+ and Linux. The contract it follows is `spec/client.md`.

## Adding the package

In Xcode, File > Add Package Dependencies, and add this repository's `clients/swift` folder (or
its Git URL). In a `Package.swift`:

```swift
.package(path: "../trail/clients/swift"),
// ...
.target(name: "Shop", dependencies: [.product(name: "TrailClient", package: "TrailClient")]),
```

## Usage

Declare events and metrics in TOML, then generate a typed file and add it to the app target:

```sh
trail gen trail --lang swift --out Sources/Shop/ShopEvents.swift
```

Make one client for the app's lifetime and call only the generated functions:

```swift
import TrailClient

let trail = TrailClient(source: "shop", url: URL(string: "https://trail.example.com")!,
                        version: "9cf5634", token: "…",
                        onProblem: { problem in print("trail: \(problem.at): \(problem.message)") })
let events = ShopEvents(trail)
let metrics = ShopMetrics(trail)

events.shopOrderFailed(orderId: "o-17", reason: .outOfStock, detail: ["sku": "a-1"])
metrics.shopQueueDepth(12, queue: "email")

do { try pay() } catch { trail.error(error, context: ["route": "/pay"]) }
trail.log(.info, "checkout opened", fields: ["items": 3])
```

`url` is the server's base URL; records are posted to `<url>/v1/ingest`. Recording never blocks
and never throws: it appends to a queue that a background thread sends every `flushInterval`
(1 s) or when `maxBatch` (200) records are waiting. `stats()` gives running totals.

`TRAIL_DISABLED=1` turns every call into a no-op; `TRAIL_VERSION`, `TRAIL_HOST`, `TRAIL_TOKEN`
and `TRAIL_SPOOL_DIR` fill in arguments left nil. On iOS the host is left out unless you pass
one, since a phone's host name is usually its owner's device name.

## When to call `flush()` in an iOS app

Call it when the app goes to the background, inside a background task so iOS gives it time:

```swift
.onChange(of: scenePhase) { phase in
    guard phase == .background else { return }
    let task = UIApplication.shared.beginBackgroundTask()
    Task {
        _ = await trail.flush()
        UIApplication.shared.endBackgroundTask(task)
    }
}
```

`flush()` sends everything now, ignoring any retry backoff, and returns true when nothing is left
to send. Where there is no time to wait (`applicationWillTerminate`), call `trail.persist()`: it
writes the queue to disk synchronously and the next launch sends it. When it returns, everything
recorded before the call is on disk, including a batch the background thread was writing at that
moment. `close()` gives the same promise.

## Offline

Nothing is lost while Trail is unreachable or the app is killed:

- The spool is on disk by default, in `Application Support/trail-spool/<source>`; pass
  `spoolDirectory` to put it elsewhere.
- Every batch is written to the spool folder before it is sent, as one file `<uuid7>.json`
  holding the exact request body, and deleted only on a final answer (Trail taking it, `400`,
  `401`, or `413` for a single record). A record lives only in memory for at most `flushInterval`.
- Only Trail's own answer counts as taken: a `2xx` whose body is a JSON object with `accepted`.
  Any other `2xx`, such as a captive portal or a sign-on page answering `200` with HTML, keeps the
  file, and `onProblem` is told the answer did not come from Trail.
- No answer, a `5xx` or any other status keeps the file. Files are retried oldest first with
  backoff, 1 s doubling to 60 s. The next launch sends files a previous run left before anything new.
- A redirect is never followed, so a `3xx` is kept and retried like any other status. URLSession
  would otherwise turn the POST into a GET of the new location, and a login page's `200` would
  look like Trail taking the batch. This holds for a `session:` you pass in too: each request gets
  its own task delegate that refuses redirects, which URLSession asks before the session's
  delegate. Your session's delegate still handles everything else, such as auth challenges.
- Each event, error and log has a UUIDv7 id made when it was recorded, so a batch the server
  stored before the app died is stored once when it is sent again.
- A `413` (too large) splits the batch in half: two files, written before the batch's own is
  deleted, each sent on its own and split again on another `413`, so one oversized record ends up
  alone. A single record that still gets `413` is dropped and passed to `onProblem`.
- `400` and `401` cannot succeed by retrying: the batch is dropped and passed to `onProblem`. So
  are the server's `rejected` and `mismatched` entries.
- The queue holds `maxQueue` (10,000) records and the spool `maxSpoolBytes` (50 MB). Past either
  the oldest are dropped and counted in `stats().dropped`.

## Tests

```sh
swift test
```

The tests use a `URLProtocol` that records each request and answers with a scripted status, so
they need no server. The redirect test serves the same script over HTTP on a loopback port, since
FoundationNetworking cannot take a redirect from a `URLProtocol`. `Tests/TrailClientTests/ShopEvents.swift` is the generated example from
`spec/client.md`.
