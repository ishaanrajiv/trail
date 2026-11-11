# Storage

Everything Trail keeps is in `data_dir`, in two open formats, so any tool that reads SQLite and
Parquet can query it without going through Trail. Trail itself has no SQL engine: it answers its
own API from indexed lookups and computes rollups as events arrive.

```
<data_dir>/
  trail.db                          SQLite (WAL). Today and the last `hot_days` finished days of
                                    every store, the metric tiers, the registry and Trail's state.
  archive/
    events/2026-11-10.parquet       events kept forever, one file per finished local day
    events-30d/2026-11-10.parquet   events declared `retention = "30d"`: expiry deletes whole files
    samples/2026-11-10.parquet      raw metric samples
    errors/2026-11-10.parquet
    logs/2026-11-10.parquet
```

A reader opens `trail.db` read-only and the Parquet files directly. A day's rows are in both
places while the day is inside the hot window (today is never in a file). To read each row once:
take a day's file when `archive_days` lists it, add any rows SQLite holds for that day whose id
(for samples, series and `ts`) is not in the file, and take SQLite alone for days with no file.
The rows SQLite adds to a filed day are late arrivals not yet merged in.

## Day files

- A day is a local day in the configured timezone. Its file is written once, after the day ends:
  to a temporary name, read back and its row count checked, then renamed into place.
- Rows leave SQLite only after their day's file is in place and the day is outside the hot window.
- Zstandard compression. Events are sorted by (`name`, `ts`, `id`), samples by (`series`, `ts`),
  errors and logs by (`source`, `ts`, `id`), so a reader can skip row groups.
- A record that arrives for a day that already has a file (a phone that was offline for a week)
  is stored in SQLite, and the day's file is rewritten with it at the next archive run. A resend
  of an id whose day has already left SQLite is dropped at that rewrite, not at ingest.
- An event's retention class is read from the registry when its day is archived. Changing a
  declaration's retention applies to days archived afterwards.
- A retention shorter than the hot window takes effect once the day has left SQLite.

Columns are the same in SQLite and Parquet. Times are milliseconds since 1970, UTC. JSON columns
are text.

| Store | Columns |
|---|---|
| events | `id`, `name`, `ts`, `received_at`, `source`, `version`, `host`, `actor`, `client`, `trace_id`, `props` (JSON object), `problems` (JSON list of strings, or null when the record matched its declaration) |
| samples | `series`, `ts`, `value`. Parquet adds `name` and `labels` (JSON object, keys sorted) so a file stands alone |
| errors | `id`, `ts`, `received_at`, `source`, `version`, `host`, `severity`, `kind`, `message`, `fingerprint`, `stack`, `context` (JSON object), `trace_id` |
| logs | `id`, `ts`, `received_at`, `source`, `host`, `stream`, `level`, `message`, `fields` (JSON object) |

## Tables that stay in SQLite

| Table | Holds |
|---|---|
| `series` | One row per metric series: `id`, `name`, `labels`, `source`, `unit`, `kind`, `first_ts`, `last_ts` |
| `samples_5m`, `samples_1h` | The metric tiers: `series`, `ts` (bucket start), `avg`, `min`, `max`, `n` |
| `error_groups` | One row per fingerprint: `source`, `kind`, `message`, `first_seen`, `last_seen`, `count`, `status` |
| `registry` | Each source's published declarations, as JSON |
| `archive_days` | Each day file: `store`, `class`, `day`, `rows`, `bytes` |
| `problems` | Records that differed from their declaration, grouped: `source`, `kind`, `name`, `message`, `first_seen`, `last_seen`, `count` |
| `tier_dirty`, `archive_late`, `marks`, `state` | Trail's own bookkeeping: tier buckets to recompute, late rows per filed day, how far each job has got, and the alert state. Not for readers. |

`series` also carries `last_raw` and `last_raw_ts`, the previous reading of a counter, which the
next rate is worked out from.

## Metrics

- A counter (a running total) is stored as the per-second rate between two readings. The first
  reading, and the first after the total goes down, only set the baseline.
- A sample is rolled into the 5-minute and hourly tiers once its bucket has closed and a 90-second
  grace has passed. A sample that arrives after that marks its bucket to be recomputed.
- A query picks the finest tier whose points cover the requested step: raw samples inside the hot
  window, then 5-minute, then hourly. It reads from the start of its first bucket, so that bucket
  is whole. The tiers keep an average, a minimum, a maximum and a count, so `last` over a tier is
  the newest tier bucket's average.
