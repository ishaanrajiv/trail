# Trail

One small server that keeps what a system does: its **events**, **metrics**, **errors** and
**logs**. It is for a single machine or a small fleet, where a full observability stack is more to
run than the thing being observed.

- **One binary, about 8 MB, about 9 MB of memory idle.** No runtime, no JVM, no cluster.
- **Two open file formats.** Recent data is in SQLite; every finished day is one Parquet file.
  Anything that reads those can query your data without going through Trail.
- **Declared, then checked.** A service says what it sends in a TOML file. Trail generates typed
  emitters from it for Python, TypeScript, plain JavaScript and Swift, so a misspelt event is a type error, not a
  silent gap found months later.
- **Nothing is lost.** Clients keep batches Trail has not yet taken in a spool (on disk by
  default in Swift; in Python and Node in memory unless `spool_dir` / `spoolDir` or
  `TRAIL_SPOOL_DIR` names a folder; a browser uses `localStorage`), every record carries an id
  so a resend is stored once, and a record that does not match its declaration is stored and
  marked rather than refused.
- **Aggregations computed as events arrive.** "Orders per minute by payment method" is four lines
  of TOML and is readable while the minute is still filling.
- **Written to be worked on by coding agents.** Every error says what to change, the API lists
  itself at `GET /v1`, and `scripts/check` fails with the fix.

Status: 0.1. The formats in `spec/` may still change.

## Try it

```sh
cargo build --release -p trail          # the binary is target/release/trail
trail serve --seed                      # a server on 127.0.0.1:8200 with three hours of made-up traffic
```

```sh
trail get /v1                                                   # every route, with its parameters
trail get '/v1/events?name=shop.order.*&limit=3'
trail get '/v1/metrics/query?name=shop.orders.count&since=-3h&step=15m&agg=sum'
trail get /v1/alerts
```

## Sending from a service

1. **Declare** what the service sends, in a folder beside its code ([`spec/declarations.md`](spec/declarations.md)):

   ```toml
   # trail/events.toml
   source = "shop"

   [events."shop.order.placed"]
   description = "A customer placed an order"
   [events."shop.order.placed".props]
   order_id = "string"
   total_cents = "int"
   method = { enum = ["card", "cash"] }
   ```

2. **Check, generate, publish:**

   ```sh
   trail check trail/
   trail gen trail/ --lang python --out shop_events.py
   trail publish trail/
   ```

3. **Emit**, through the generated file and a client library ([`clients/`](clients/)):

   ```python
   from trail_client import Client
   from shop_events import Events

   events = Events(Client("shop"))
   events.shop_order_placed(order_id="o-17", total_cents=1200, method="card")
   ```

From a shell script or a cron job there is nothing to generate:

```sh
trail event shop shop.order.placed order_id=o-17 total_cents=1200 method=card
trail metric shop shop.queue.depth 12 queue=email
trail log backup "nightly backup finished" bytes=81234567
```

Telegraf, or anything else that writes InfluxDB line protocol, can point at `/v1/write`.
`trail collect journald` follows the systemd journal: every line as a log, error lines as errors
too, and an out-of-memory kill or a failed unit as an event ([`examples/journal`](examples/journal)).
A container's level is read from its line (JSON `level`, `ERROR:`, `E!`, `[error]`…), since
Docker's journald driver marks all of stderr as errors.
`trail collect jsonl` follows a file of JSON lines.

## Rollups and alerts

```toml
# trail/rollups.toml
[rollups.orders]
description = "Orders and their value, by payment method"
from = "shop.order.placed"
by = ["method"]
measure = ["count", "sum(total_cents)", "p95(latency_ms)"]
every = "1m"
```

That writes the metric series `shop.orders.count`, `shop.orders.total_cents.sum` and
`shop.orders.latency_ms.p95`, each with a `method` label. A late event (a phone that was offline)
causes its bucket to be recomputed, and `trail rollup rebuild` recomputes one from the raw events.

```toml
# trail/alerts.toml
[alerts.queue_backlog]
metric = "shop.queue.depth"
above = 100
for = "10m"
title = "The {queue} queue is backing up"
```

Trail posts what is firing to one webhook ([`spec/alerts.md`](spec/alerts.md)). What happens next
(a push, an email) belongs to whatever receives it.

## Where the data is

```
trail-data/
  trail.db                        SQLite: today and the last two days, the metric tiers, the registry
  archive/events/2026-11-10.parquet
  archive/samples/2026-11-10.parquet
  archive/errors/…  archive/logs/…
```

[`spec/storage.md`](spec/storage.md) is the contract for both. Trail has no query language of its
own, on purpose: its API answers the common lookups from indexes, and for anything else you point
DuckDB, pandas or `sqlite3` at the files.

## What it is not

- Not a cluster. One process, one disk.
- Not a tracing system. Records carry a `trace_id` to tie them together, and that is all.
- Not a dashboard. It serves JSON.
- No TLS and no user accounts. It listens on loopback; put a reverse proxy in front to reach it
  from elsewhere. Optional bearer tokens guard writes per source and reads as a whole.

## The repository

| Path | |
|---|---|
| [`spec/`](spec/) | The contracts: [ingest](spec/ingest.md), [declarations](spec/declarations.md), [clients](spec/client.md), [the API](spec/api.md), [alerts](spec/alerts.md), [storage](spec/storage.md), [config](spec/config.md) |
| `crates/` | The server and the `trail` command, in Rust |
| `clients/` | Python, Node/browser and Swift client libraries, each with no dependencies |
| [`examples/shop`](examples/shop) | A complete example |
| [`examples/agents-template.md`](examples/agents-template.md) | A section to copy into your own project's agent instructions |
| [`AGENTS.md`](AGENTS.md) | The rules for working on Trail |

`scripts/check` runs everything CI runs, bar CI's scan of the history for secrets. `scripts/e2e` starts a real server and sends to it from
all three clients.

## License

MIT or Apache-2.0, at your option.
