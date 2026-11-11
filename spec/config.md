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

[alerts]
webhook = "http://127.0.0.1:9000/hooks/trail"   # where alerts are posted (spec/alerts.md). Unset: evaluated, not sent.
interval = "30s"              # how often rules are evaluated

[auth]
read_token = "…"              # when set, read endpoints need `Authorization: Bearer …`

[auth.tokens]                 # when a source is listed, its writes need its token
shop = "…"
```

Durations are a whole number and a unit: `30s`, `5m`, `48h`, `90d`, `2w`, or `forever`. Sizes are
decimal: `500MB`, `2GB`.

How long things are kept:

| Data | Kept |
|---|---|
| An event | As its declaration's `retention` says (default forever), within `caps.events` |
| Raw metric samples | Until the metrics day files reach `caps.metrics` |
| 5-minute and hourly metric tiers | `retention.samples_5m`, `retention.samples_1h` |
| Errors, logs | `retention.errors`, `retention.logs`, within their caps |

A cap is a guard against a full disk, not a retention setting. `trail.archive.bytes{store}` reports
how close each store is; alert on it rather than letting a cap delete days.
