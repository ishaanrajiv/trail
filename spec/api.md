# The HTTP API

Everything is JSON over plain HTTP. `GET /v1` returns this list as data: every route, its summary
and its parameters, so a program can ask the server what it offers. The table is `ROUTES` in
`crates/server/src/api.rs`, and a test fails if a route or a parameter is missing from this file.

Conventions:

- Times are milliseconds since 1970, UTC. A time parameter also takes how long ago: `-15m`,
  `-1h`, `-7d`.
- An error is `{"error": {"at", "message", "fix"}}`. `fix` says what to send instead. An unknown
  parameter is an error, with the name you probably meant.
- Lists are newest first. When a response has a `next` value, pass it as `before` for the next page.
- To follow what arrives instead, pass `after_seq` (see "Following what arrives").
- With `read_token` set in `trail.toml`, every route except health and the two write routes needs
  `Authorization: Bearer <token>`.
- With `admin_token` set, the four routes that change what Trail keeps (publishing and removing
  a source's declarations, rebuilding a rollup, setting an error group's status) need
  `Authorization: Bearer <admin_token>` instead, or, for a source's own declarations and
  rollups, that source's write token. Otherwise `401` (spec/config.md, "The admin token").

## Writing

### `POST /v1/ingest`

Events, metrics, errors and logs in one body. See `spec/ingest.md`.

### `POST /v1/write`

Metrics as InfluxDB line protocol, also at `/api/v2/write` and `/write` for InfluxDB clients.
`204` when every line was read; `400` with the bad `lines` otherwise (the good ones are stored).

| Parameter | |
|---|---|
| `source` | the sending source. Default: `bucket` or `db`, else `influx` |
| `precision` | timestamp unit: `ns` (default), `us`, `ms`, `s` |
| `bucket`, `db`, `org` | what InfluxDB clients send. `bucket` and `db` stand in for `source`; `org` is ignored |

## The registry

### `GET /v1/registry`

The sources that have published declarations, with how many events, metrics, rollups and alerts
each declares. `trail` is the server's own.

### `GET /v1/registry/{source}`

One source's declarations: every event with its description and property types, every metric,
rollup and alert. This is how to find out what an event means before querying it.

### `PUT /v1/registry/{source}`

Publishes a source's declarations; `trail publish <folder>` sends this. `400` with `problems` if
they fail the check, `409` if a name belongs to another source. Publishing clears the source's
rows in `/v1/problems`. With `admin_token` set it needs that token, or the source's write token.

### `DELETE /v1/registry/{source}`

Removes a source's declarations. Its records stay and are no longer checked. With `admin_token` set it needs that token, or the source's write token.

## Events

### `GET /v1/events`

Events, newest first, from SQLite and the day files alike. Returns `{"events": […], "next"}`.
Each event carries `seq`, its position in arrival order (null for one stored before Trail kept
it). With `after_seq`, the events stored after that position, oldest first, and `next_seq`.

| Parameter | |
|---|---|
| `source` | only this source |
| `name` | an event name, or a prefix like `shop.order.*` |
| `actor`, `trace_id` | only events with this value |
| `mismatched` | `1` for only events that differ from their declaration; each lists why in `problems` |
| `since`, `until` | the time range |
| `limit` | most rows (default 100, at most 1000) |
| `before` | the previous page's `next` |
| `after_seq` | only events stored after this position, oldest first: `0`, then the previous response's `next_seq`. Refused together with `before` |

### Following what arrives

Paging by time can't follow a stream: an event that arrives late, with a week-old `ts`, lands
behind every reader's position. So every event, error and log is given a `seq` when it is stored:
a positive integer from one counter for the whole server, rising in the order writes commit,
never given twice, and kept through restarts and through rows moving to day files.

`GET /v1/events?after_seq=N` (and `/v1/errors`, `/v1/logs`) returns the records with a seq above
`N` that match the other filters, oldest seq first, at most `limit`, as
`{"events": […], "next": null, "next_seq": M}`. `next_seq` is the position to ask after next
time: the last seq the page covered, or `N` when nothing new matched. A reader that starts at `0`
and always passes back `next_seq` is given every record stored afterwards exactly once: one with
a week-old `ts`, one whose day already has a day file, and one whose day file was rewritten
between two of its reads. A page may hold fewer than `limit` rows while more are waiting; ask
again until one comes back empty.

Records stored before Trail kept a seq, and only found in day files written then, have none
(`seq` is null) and are never returned by `after_seq`. A resend of a record whose day has
already left SQLite is not returned again.

## Metrics

### `GET /v1/metrics/series`

The series that exist, each with its labels, source, unit, kind and first and last reading.

| Parameter | |
|---|---|
| `name` | a metric name, or a pattern with `*` |
| `source` | only this source |
| `label.<name>` | only series whose label equals this: `label.queue=email` |

### `GET /v1/metrics/query`

A metric over time: `{"start", "end", "step", "tier", "series": [{"name", "labels", …, "points": [[ts, value], …]}]}`.
A bucket with no readings has a null value. `tier` says what was read: `raw` samples inside the
hot window, else the `5m` or `1h` tier.

| Parameter | |
|---|---|
| `name` | a metric name, or a pattern with `*`. Required |
| `label.<name>` | only series whose label equals this |
| `since`, `until` | the time range. Default: the last hour |
| `step` | bucket length, like `5m`. Default: about 300 points over the range |
| `agg` | how a bucket's readings become one value: `avg` (default), `min`, `max`, `sum`, `count`, `last`. Use `sum` to add up a `count` rollup over longer buckets. Over a tier, `last` is the newest tier bucket's average |

### `GET /v1/metrics/latest`

The newest reading of each matching series.

| Parameter | |
|---|---|
| `name` | a metric name, or a pattern with `*`. Required |
| `label.<name>` | only series whose label equals this |

## Rollups

### `GET /v1/rollups`

The rollups being computed, and the name of the series each measure writes.

### `POST /v1/rollups/{source}/{name}/rebuild`

Recomputes a rollup from the raw events, as far back as they are kept. Use it after declaring a
new rollup over events that already exist. Returns how many events and buckets it covered.
With `admin_token` set it needs that token, or the source's write token; `trail rollup rebuild`
sends it.

| Parameter | |
|---|---|
| `since` | rebuild from here instead of from the first event |

## Errors

### `GET /v1/errors`

Errors, newest first. Returns `{"errors": […], "next"}`, each error with its `seq`.

| Parameter | |
|---|---|
| `source` | only this source |
| `fingerprint` | only this group |
| `severity` | `warning`, `error` or `fatal` |
| `since`, `until`, `limit`, `before`, `after_seq` | as for events |

### `GET /v1/errors/groups`

Errors grouped by fingerprint: the first message, first and last seen, how many, and a status.

| Parameter | |
|---|---|
| `source` | only this source |
| `status` | `open`, `resolved` or `ignored` |
| `limit` | most rows |

### `POST /v1/errors/groups/{fingerprint}`

Sets a group's status. Body: `{"status": "resolved"}`. A resolved group reopens when the error
happens again; an ignored one stays ignored. With `admin_token` set it needs that token.

## Logs

### `GET /v1/logs`

Log lines, newest first. Returns `{"logs": […], "next"}`, each line with its `seq`.

| Parameter | |
|---|---|
| `source` | only this source |
| `stream` | only this stream |
| `level` | this level and above: `trace`, `debug`, `info`, `warn`, `error`, `fatal` |
| `q` | only lines whose message contains this text |
| `since`, `until`, `limit`, `before`, `after_seq` | as for events |

## The server itself

### `GET /v1`

Every route with its summary and parameters.

### `GET /v1/health`

`{"ok": true, "version", "started"}`. Needs no token.

### `GET /v1/alerts`

What is firing, the notices waiting for the webhook, and every rule. See `spec/alerts.md`.

### `GET /v1/problems`

The drift report: records that were stored but differ from their declaration, grouped by source,
kind, name and message, with a count and when first and last seen.

| Parameter | |
|---|---|
| `source` | only this source |

### `GET /v1/stats`

Rows in SQLite and day files and their sizes per store, the database size, the series count.
