# Storage

Everything Trail keeps is in `data_dir`, in two open formats, so any tool that reads SQLite and
Parquet can query it without going through Trail. Trail itself has no SQL engine: it answers its
own API from indexed lookups and computes rollups as events arrive.

```
<data_dir>/
  trail.db                          SQLite (WAL). Today and the last `hot_days` finished days of
                                    every store, the metric tiers, the registry and Trail's state.
  previous/v1/                      what an upgrade from schema version 1 kept (below). Trail
                                    never reads or deletes it.
  archive/
    events/2026-11-10.parquet       events kept forever, one file per finished local day
    events-30d/2026-11-10.parquet   events declared `retention = "30d"`: expiry deletes whole files
    samples/2026-11-10.parquet      raw metric samples
    errors/2026-11-10.parquet
    logs/2026-11-10.parquet
```

A reader opens `trail.db` read-only and the Parquet files directly. A day's rows are in both
places while the day is inside the hot window (today is never in a file). To read each row once:
take a day's file when `archive_days` lists it, add any rows SQLite holds for that day whose
(`source`, `id`) (for samples, `series` and `ts`) is not in the file, and take SQLite alone for days
with no file.
The rows SQLite adds to a filed day are late arrivals not yet merged in.

To follow what arrives, read by `seq` instead of by time: the rows of SQLite with a `seq` above
the last one read, plus those of the day files whose `archive_days.max_seq` is above it, taking a
`seq` found in both once. Read SQLite first: a day file is in place before its rows leave SQLite,
so a row is always in one or the other. A row in SQLite for a day before the evicted mark whose
file holds the same (`source`, `id`) under another `seq` is a resend the next archive run drops.

## Day files

- A day is a local day in the configured timezone. Its file is written once, after the day ends:
  to a temporary name, read back and its row count checked, synced to disk, renamed into place,
  and the folder synced. The timezone a folder was first written under is kept in `state`, and
  Trail refuses to open the folder under another.
- Rows leave SQLite only after their day's file is in place and the day is outside the hot window.
- Zstandard compression. Events are sorted by (`name`, `ts`, `id`, `source`), samples by
  (`series`, `ts`), errors and logs by (`source`, `ts`, `id`), so a reader can skip row groups.
- A record's identity is (`source`, `id`): the primary key of `events`, `errors` and `logs`, and
  what a day file's merge and every read keep one row per. Two sources may use the same id.
- A record that arrives for a day that already has a file (a phone that was offline for a week)
  is stored in SQLite, and the day's file is rewritten with it at the next archive run. A resend
  of a record (same source and id) whose day has already left SQLite is dropped at that rewrite,
  not at ingest.
- `archive_late` marks a filed day that has such rows. The mark is only removed in the
  transaction that records the rewritten file, and only if no row arrived for the day while it
  was being written (`claimed` says which). A run that dies half-way leaves the mark, and the
  next run writes the day again; the rows never leave SQLite before that.
- When a file is rewritten, a row of it that has no `seq` (written before seq existed) takes the
  one SQLite holds for the same record. An upgrade to version 2 marks every filed day that still
  has rows in SQLite as late, so its file is rewritten that way before those rows leave.
- Expiry deletes a file first and its `archive_days` row after. A file that can't be deleted
  keeps its row and is tried again; a row whose file is missing is skipped by readers.
- An event's retention class is read from the registry when its day is archived. Changing a
  declaration's retention applies to days archived afterwards.
- A retention shorter than the hot window takes effect once the day has left SQLite.

Columns are the same in SQLite and Parquet. Times are milliseconds since 1970, UTC. JSON columns
are text.

| Store | Columns |
|---|---|
| events | `id`, `name`, `ts`, `received_at`, `source`, `version`, `host`, `actor`, `client`, `trace_id`, `props` (JSON object), `problems` (JSON list of strings, or null when the record matched its declaration), `seq` |
| samples | `series`, `ts`, `value`. Parquet adds `name` and `labels` (JSON object, keys sorted) so a file stands alone |
| errors | `id`, `ts`, `received_at`, `source`, `version`, `host`, `severity`, `kind`, `message`, `fingerprint`, `stack`, `context` (JSON object), `trace_id`, `seq` |
| logs | `id`, `ts`, `received_at`, `source`, `host`, `stream`, `level`, `message`, `fields` (JSON object), `seq` |

`seq` (format 2 on) is each event, error and log's position in arrival order: a positive integer
from one counter for the whole server (`marks.seq` holds the last one given), assigned in the
write transaction, so it rises in commit order, is never given twice, and survives restarts and
rows leaving SQLite. A row keeps its seq in its day file. Rows in SQLite when a database was
upgraded to version 2 were numbered in (`received_at`, `id`) order; rows that were then only in
format-1 day files have none (null). Every row in SQLite has one, unique.

## File format versions

Each day file carries its format in its Parquet key-value metadata, `trail.format`; this Trail
writes `"2"`. A file without the key is format 1, and stays readable for ever.

- Columns are read by name, never by position. A column a file lacks reads as null, and a column
  Trail doesn't know is ignored. Columns are only ever added, and an added column is nullable.
- A file is never rewritten just to bring it to a newer format. It is rewritten when a late row
  is merged into its day (above), and comes out in the format the running Trail writes.
- A reader that wants to know which columns to expect checks `trail.format` or simply the
  file's schema.

## Tables that stay in SQLite

| Table | Holds |
|---|---|
| `series` | One row per metric series: `id`, `name`, `labels`, `source`, `unit`, `kind`, `first_ts`, `last_ts` |
| `samples_5m`, `samples_1h` | The metric tiers: `series`, `ts` (bucket start), `avg`, `min`, `max`, `n` |
| `error_groups` | One row per fingerprint: `source`, `kind`, `message`, `first_seen`, `last_seen`, `count`, `status` |
| `registry` | Each source's published declarations, as JSON |
| `archive_days` | Each day file: `store`, `class`, `day`, `rows`, `bytes`, `max_seq` (the largest `seq` in it; null for a file whose rows have none) |
| `problems` | Records that differed from their declaration, grouped: `source`, `kind`, `name`, `message`, `first_seen`, `last_seen`, `count` |
| `tier_dirty`, `archive_late`, `marks`, `state` | Trail's own bookkeeping: tier buckets to recompute, late rows per filed day, how far each job has got, the alert state and the timezone. Not for readers. |

`series` also carries `last_raw` and `last_raw_ts`, the previous reading of a counter, which the
next rate is worked out from.

What a sender could otherwise grow without end is bounded: `series` by `limits.series`,
`problems` at 10,000 rows and `error_groups` at 50,000, the ones seen longest ago going first.

## Metrics

- A counter (a running total) is stored as the per-second rate between two readings. The first
  reading, and the first after the total goes down, only set the baseline.
- A sample is rolled into the 5-minute and hourly tiers once its bucket has closed and a 90-second
  grace has passed. A sample that arrives after that marks its bucket to be recomputed from the
  raw samples. Where those are gone (the cap deleted the day's file), the bucket is left as it
  was built: a few late samples do not replace a whole bucket.
- A backlog of buckets (after an import, or days stopped) is built a slice at a time, a day of
  5-minute buckets or a month of hourly ones per transaction, so ingest never waits for all of it.
- `GET /v1/metrics/latest` gives a series' newest raw sample. Once its raw samples have left
  SQLite it gives the time of its last sample with the newest tier bucket's average, so a
  series that went quiet days ago is still seen as quiet rather than as never having existed.
- A query picks the finest tier whose points cover the requested step: raw samples inside the hot
  window, then 5-minute, then hourly. It reads from the start of its first bucket, so that bucket
  is whole. The tiers keep an average, a minimum, a maximum and a count, so `last` over a tier is
  the newest tier bucket's average.

## Schema versions

`trail.db` carries its schema version in `PRAGMA user_version`; this Trail writes version 2. A
Trail refuses a database newer than it knows. An older one is upgraded when Trail starts:

1. Trail keeps what the upgrade is about to change, under `previous/v<old>/`: `trail.db`, a
   consistent copy of the database (SQLite's `VACUUM INTO`, so it includes the write-ahead log),
   and `archive/…`, a hard link to every day file. A day file is replaced by rename and removed by
   unlink, so the link keeps the old bytes at no cost. The copy is built as `previous/v<old>.partial`
   and renamed into place once complete and synced; an older `previous/v<old>` is first renamed to
   `previous/v<old>-1` (or the next free number), never deleted.
2. If the copy can't be made (no space for it, a file system without hard links, a folder Trail
   can't write), Trail refuses to start, says what to free or change, and has changed nothing.
3. Every schema change then runs in one transaction. A kill at any point leaves either the
   untouched old database or a complete new one; started again, Trail makes a fresh copy and
   upgrades.

Existing day files are not rewritten by an upgrade. Trail reads every file format it has written
(see "File format versions").

## Going back a version

1. Stop Trail.
2. Put back what the upgrade kept: move `previous/v1/trail.db` to `trail.db` (delete
   `trail.db-wal` and `trail.db-shm` first), and replace `archive/` with `previous/v1/archive/`.
3. Run the older Trail.

Whatever was recorded after the upgrade is lost: events, metrics, errors and logs that arrived,
and day files written, since.
