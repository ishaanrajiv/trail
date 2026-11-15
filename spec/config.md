# Configuration

`trail serve --config trail.toml`. Every field is optional: with no file, Trail listens on
loopback and keeps its data in `./trail-data`. An unknown field is an error, so a typo is caught
at start. The parser is `crates/core/src/config.rs`.

```toml
listen = "127.0.0.1:8200"     # address and port. Loopback by default: put a proxy in front to reach it from elsewhere.
data_dir = "./trail-data"     # the database and the day files (spec/storage.md)
timezone = "UTC"              # IANA name. Decides where a day and an hour start.
hot_days = 2                  # finished local days kept in SQLite beside today (1 to 30)
min_step = "30s"              # the usual collection interval: metric queries never use a finer step
workers = 4                   # queries answered at once (1 to 64), beside the one writer; see "How much of the machine" below.
sync = "normal"               # "full" puts every accepted write on disk before it is answered; see below.
counters = ["net.bytes_*"]    # glob patterns of undeclared metrics that are running totals

[retention]
samples_5m = "90d"            # the 5-minute metric tier
samples_1h = "forever"        # the hourly metric tier
logs = "90d"
errors = "forever"

[caps]                        # the size each store's day files may reach; the oldest days go first
events = "20GB"
metrics = "2GB"
errors = "1GB"
logs = "5GB"                  # "none" turns a cap off

[limits]
body_bytes = 8388608          # largest request body
batch_items = 10000           # most records in one batch
rollup_groups = 1000          # most groups one rollup keeps per bucket; the rest are counted as overflow
series = 100000               # most metric series kept; a reading that would start one more is refused and reported

[alerts]
webhook = "http://127.0.0.1:9000/hooks/trail"   # where alerts are posted (spec/alerts.md). Unset: evaluated, not sent.
interval = "30s"              # how often rules are evaluated

[auth]
read_token = "…"              # when set, read endpoints need `Authorization: Bearer …`
admin_token = "…"             # when set, changing declarations, rollups and error status needs it; see below

[auth.tokens]                 # when a source is listed, its writes need its token, and so does publishing its declarations
shop = "…"
```

`timezone` is fixed once a data folder has rows: day files and hourly buckets are cut at local
midnight and the local hour, so Trail refuses to start on a folder written under another zone.

Durations are a whole number and a unit: `30s`, `5m`, `48h`, `90d`, `2w`, or `forever`. Sizes are
decimal: `500MB`, `2GB`.

How long things are kept:

| Data | Kept |
|---|---|
| An event | As its declaration's `retention` says (default forever), within `caps.events` |
| Raw metric samples | Until the metrics day files reach `caps.metrics` |
| 5-minute and hourly metric tiers | `retention.samples_5m`, `retention.samples_1h` |
| Errors, logs | `retention.errors`, `retention.logs`, within their caps |

A cap is a guard against a full disk, not a retention setting. `trail.archive.cap_used{store}` is
the share of its cap a store's day files fill (1 is full); alert on it rather than letting a cap
delete days. `trail.archive.failing` is how many days the last archive run could not finish, and
0 after a clean run: alert on `above = 0`. Any source's `alerts.toml` may watch Trail's own
metrics.

## The admin token

With no tokens set, any caller that reaches the port may replace or remove a source's
declarations (and with them its rollups and alert rules: removing a rule looks like a recovery to
the alert receiver), rebuild a rollup, and resolve or ignore an error group. `admin_token`
(16 characters or more) closes that. When it is set, these routes need
`Authorization: Bearer <admin_token>`:

- `PUT` and `DELETE /v1/registry/{source}`
- `POST /v1/rollups/{source}/{name}/rebuild`
- `POST /v1/errors/groups/{fingerprint}`

A source listed under `[auth.tokens]` may still publish, remove and rebuild its own with its write
token, as without an admin token. The read token does not open them. Ingest and reads are
unchanged. Without the right token the answer is `401`, whose `fix` names `admin_token` and
`TRAIL_ADMIN_TOKEN`. When `admin_token` is unset, these routes behave exactly as before: open, or
behind the source's write token, or behind `read_token` when that is set.

`trail publish` and `trail rollup rebuild` send the admin token from `--admin-token` or
`TRAIL_ADMIN_TOKEN`, and otherwise the usual `--token` or `TRAIL_TOKEN`. Tokens are compared in
constant time and are never logged or repeated in an answer.

## How much of the machine

`workers` is how many queries Trail answers at the same time. Connections are cheap and are
not counted: a slow or idle caller holds no worker, only a request that is being answered does.

- Writes (`/v1/ingest`, `/v1/write` and its InfluxDB aliases) have a slot of their own, one
  writer at a time whatever `workers` says: SQLite has a single writer, and rollups are computed
  in arrival order. A write never waits behind a query, and a query never waits behind a write.
- Every other route except the two below is a query and takes one of the `workers` slots:
  reads, and also publishing or removing declarations, a rollup rebuild and an error group's
  status. `workers = 1` means one query at a time beside the writer. Those three change what is
  stored, but they wait in the workers' lane, not for the writer's slot, which is ingest's alone.
- A request's lane comes from its path read as routing reads it: slashes before and after don't
  count, so `//v1/ingest` and `/v1/ingest/` are ingest and wait for the writer's slot.
- `GET /v1/health` and `GET /v1` wait for no slot at all, so a slow query never holds up the
  health check.
- A request that has waited 10 seconds for its slot is answered `503` with `Retry-After: 5`,
  rather than queueing for ever. Clients keep a batch on any `5xx` and send it again.
- With `workers = 1`, request handling uses at most two threads at once: one query and one
  write. The timers (rollups, tiers, archive, alerts) run on their own threads, a few seconds of
  work at a time.
- For a hard ceiling, set it outside Trail, where it also covers the timers: `CPUQuota=100%` in
  a systemd unit, `--cpus 1` for a container, or `taskset -c 3 trail serve`. Trail has no unsafe
  code, so it cannot pin itself.

`docs/performance.md` has measured throughput for one core and for all of them.

## How safe a write is

A `200` means the records are in SQLite's write-ahead log. With `sync = "normal"` that survives
Trail being killed or crashing, but the last moments before a power cut or a kernel crash can be
lost, although the client was told they were stored. `sync = "full"` waits for the disk on every
request, which closes that gap and costs ingest speed (`docs/performance.md`). Day files are
always on disk, and their folder with them, before the rows they hold leave SQLite.
