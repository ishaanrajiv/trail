# Performance

What one Trail takes in and answers, measured with `scripts/bench` on the code as of this
commit. Run it again after any change to the ingest path, the HTTP layer or the store, and
update the table.

The machine: an 8-core, 16-thread laptop-class CPU (2022), NVMe disk, ext4. The load generator
(`crates/server/examples/load.rs`) runs on the same machine, on other CPUs where the server is
pinned to one. Each run is one minute against a fresh server, with 4 writer threads posting
events and 4 reader threads cycling three queries (the newest 100 events of a name, a rollup
over time, the latest value of a metric). Every event sent was stored in every run.

| Run | `workers` | Events/s | Write p50 / p99 | Reads/s | Read p50 / p99 | Cores used | Peak memory |
|---|---|---|---|---|---|---|---|
| One CPU, ingest only (batches of 100) | 1 | 119,700 | 2.5 / 14.7 ms | – | – | 0.87 | 15 MB |
| One CPU, ingest and reads | 1 | 71,900 | 4.7 / 18.3 ms | 1,790 | 1.6 / 12.1 ms | 0.93 | 17 MB |
| One CPU, one event per request | 1 | 5,300 | 0.4 / 6.3 ms | 2,200 | 1.3 / 7.9 ms | 0.91 | 16 MB |
| One CPU, ingest and reads, `sync = "full"` | 1 | 59,300 | 4.7 / 38.8 ms | 380 | 4.1 / 108.9 ms | 0.59 | 17 MB |
| One worker, every CPU allowed | 1 | 95,300 | 1.9 / 37.6 ms | 640 | 2.0 / 54.4 ms | 1.09 | 17 MB |
| Default, ingest and reads | 4 | 140,800 | 1.8 / 13.9 ms | 3,190 | 0.8 / 11.2 ms | 2.36 | 18 MB |
| Every CPU, ingest only | 16 | 169,400 | 1.5 / 10.4 ms | – | – | 1.40 | 15 MB |

What the numbers say:

- **Ingest is one core's work.** SQLite has one writer and rollups are computed in arrival
  order, so more workers add little to ingest: sixteen workers take in about 1.4 times what one
  does. `workers = 1` is a reasonable setting for a machine that has other things to do.
- **Extra workers are for reads.** With one worker a query waits its turn behind writes; with
  four, queries run beside them and are answered in under a millisecond at the median.
- **Batch.** A request is a transaction. A hundred events in one request cost little more than
  one, so the clients' batching (up to 200 records a second apart) is worth about 14 times the
  throughput of one event per request.
- **`sync = "full"` costs** about a fifth of ingest on this disk and makes reads wait behind each
  disk flush. It buys safety against a power cut, not against a crash of Trail, which `normal`
  already covers (`spec/config.md`).
- **Memory stays small** whatever the load: the server holds open rollup buckets and SQLite's
  page cache, nothing per connection beyond a thread stack.
- **Disk:** an event of this size takes about 270 bytes in SQLite while its day is hot, and a
  small fraction of that once the day is a Parquet file.

"One worker, every CPU allowed" uses a little over one core because the timers (rollups, tiers,
alerts) run on their own threads and the kernel's network work is not Trail's to cap. To hold
Trail to a core for certain, cap it from outside: `CPUQuota=100%`, `--cpus 1` or `taskset`.

A pinned core at full load runs hot on a small machine. `scripts/bench` waits for the CPU to
cool between runs and stops a run that stays above `MAX_TEMP` (85 °C).

## What has not been measured yet

Left for a later pass; none of it blocks using Trail on one core.

- **Every CPU, ingest and reads** and **every CPU, one event per request** at one minute. Both
  runs were stopped by the temperature guard while other work was heating the machine. A
  10-second run of the first gave about 130,000 events and 10,300 reads a second (read p50
  0.2 ms, p99 1.2 ms, 4.4 cores); treat that as indicative only.
- **Metrics ingest** (`/v1/ingest` with readings, and `/v1/write` line protocol). The generator
  posts events only.
- **Reads over archived days.** Every run starts empty, so all reads hit SQLite. A query that
  walks Parquet day files, and one with a filter that has no row-group statistics (`actor`,
  `trace_id`, `q`), will be slower.
- **A long run.** A minute never reaches the archive, tier maintenance or eviction, each of
  which holds the write lock for a moment.
- **More than 4 writers on one core.** An early run with 8 writers on the previous HTTP layer
  reported a handful of requests as failed that had in fact been stored; it did not reproduce
  with 4 writers or after the HTTP layer was replaced, and was not chased further.
- **`sync = "full"` on other disks.** Its cost is the disk's flush time, so the fifth measured
  here is this NVMe drive's.

Before the server had its own HTTP layer (it used a general-purpose HTTP library), every read
took 41 to 44 ms because the header and body were written separately with Nagle's algorithm on.
The table above is after that change.
