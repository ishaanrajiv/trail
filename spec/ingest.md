# Ingest

How records reach Trail. The types are in `crates/core/src/envelope.rs`; where this file and the
code disagree, the code is right and this file has a bug.

## `POST /v1/ingest`

One JSON object carries any mix of the four kinds of record.

```json
{
  "source": "shop",
  "version": "9cf5634",
  "host": "web-1",
  "events":  [{"id": "0192f0c1-7a3e-7b1a-9c2d-acde48001122", "name": "shop.order.placed",
               "ts": 1794000000000, "actor": "user", "client": "web", "trace_id": "req-81",
               "props": {"order_id": "o-17", "total_cents": 1200, "method": "card", "items": ["a"]}}],
  "metrics": [{"name": "shop.queue.depth", "value": 12, "ts": 1794000000000,
               "labels": {"queue": "email"}, "unit": "jobs", "kind": "gauge"}],
  "errors":  [{"id": "…", "ts": 1794000000000, "severity": "error", "kind": "KeyError",
               "message": "missing key 42", "stack": "…", "context": {"route": "/pay"}, "trace_id": "req-81"}],
  "logs":    [{"id": "…", "ts": 1794000000000, "level": "info", "stream": "worker",
               "message": "started", "fields": {"pid": 81}}]
}
```

Batch fields:

| Field | Required | Meaning |
|---|---|---|
| `source` | yes | The sending service: lowercase letters, digits, hyphens |
| `version` | no | Its deployed version, usually a commit |
| `host` | no | The machine it runs on |
| `events`, `metrics`, `errors`, `logs` | no | Lists of records |

Record fields. Anything not listed is refused, so a typo is caught instead of dropped.

| Kind | Required | Optional |
|---|---|---|
| event | `name` | `id`, `ts`, `actor`, `client`, `trace_id`, `props` (object) |
| metric | `name`, `value` (finite number) | `ts`, `labels` (string to string), `unit`, `kind` (`gauge` or `counter`) |
| error | `kind`, `message` | `id`, `ts`, `severity` (`warning`, `error`, `fatal`), `stack`, `context` (object), `trace_id`, `fingerprint` |
| log | `message` | `id`, `ts`, `level` (`trace`, `debug`, `info`, `warn`, `error`, `fatal`), `stream`, `fields` (object) |

- `ts` is milliseconds since 1970, UTC. Left out, it is the arrival time. Any age back to the
  year 2000 is accepted: a client that was offline for a week sends its queue and every record
  lands on its own day. A `ts` before 2000 or more than a day ahead is a unit mix-up or a clock
  that was never set; that record is refused and reported.
- `id` is 1 to 64 letters, digits or `_ - : .`. Senders use a UUIDv7. A record's identity is its
  source and id: the same id sent twice by the same source is stored once, so a retry is always
  safe, and another source's record with the same id is a record of its own. Left out, Trail
  makes one, and a retry is then a duplicate.
- A `counter` metric is a running total. Trail stores the per-second rate between readings; the
  first reading, and the first after a reset, only set the baseline. A rate needs readings in
  order, so a counter reading older than the newest one stored for its series is refused and
  reported.
- A metric has no id. Its identity is its series and `ts`: the same reading sent again replaces
  the value, so a resend is safe for metrics too, as long as the sender sets `ts`.
- `props` are at most 64 KB. Messages are cut at 16 KB and stacks at 64 KB.

## The response

`200` whenever the body could be read, even if some records were refused:

```json
{
  "accepted":   {"events": 1, "metrics": 1, "errors": 1, "logs": 1},
  "duplicates": 0,
  "rejected":   [{"at": "events[1]", "message": "`Order` is not an event name",
                  "fix": "use two or more snake_case parts joined by dots, like `shop.order.placed`"}],
  "mismatched": [{"at": "events[0]", "message": "unknown property `latncy_ms` on `shop.order.placed`; declared: …"}]
}
```

- `accepted`: records newly stored.
- `duplicates`: records whose id this source had already stored.
- `rejected`: records that were not stored; each says why and how to fix it. Besides a record
  that could not be read, these are:
  - a record under a name another source owns, when that source has a write token (or is
    `trail`, the server's own source). Without a token on the owner the record is stored and
    marked instead, and feeds none of the owner's rollups or alert rules;
  - a counter reading out of order;
  - a reading that would start a new series beyond `limits.series`.
- `mismatched`: records that were stored but differ from their declaration. They are marked.

A name belongs to the source that declares it: an event, a metric, and the series a rollup
writes. Nobody sends as `trail`.

Other statuses:

| Status | When | A client should |
|---|---|---|
| `400` | The body is not the JSON object above, or `source` is invalid. Body: `{"error": {"at", "message", "fix"}}` | Drop the batch and report it: a retry cannot succeed |
| `401` | A write token is configured for this source and the bearer token is missing or wrong | Drop and report |
| `413` | The body is over the size limit, or has too many records | Send it again as two halves; drop and report a single record that is still too large |
| `415` | The body has a `Content-Encoding` other than `identity`. Trail does not read compressed bodies | Send it uncompressed |
| `5xx`, any other status, or no answer | Trail is down or restarting, or something between you and it answered | Keep the batch and retry later |

A `2xx` that is not the JSON above did not come from Trail (a sign-in page, a captive portal):
a client keeps the batch.

What the server itself answers before a request reaches a route:

| Status | When |
|---|---|
| `400` | The request is not HTTP/1.1 Trail can read: a bad request line, two different `Content-Length`s, or both a length and `Transfer-Encoding` |
| `408` | The headers took more than 10 seconds to arrive, or the body more than 60 |
| `431` | The headers are over 16 KB |
| `501` | A `Transfer-Encoding` other than `chunked` |
| `503` | 256 connections are already open, or the request waited 10 seconds for a worker (the one writer's slot, for ingest; spec/config.md, "How much of the machine"). It carries `Retry-After: 5`; keep the batch and send it again |

A connection may carry many requests, one after another, and is closed after 60 idle seconds.

## `POST /v1/write`

InfluxDB line protocol, for Telegraf and anything else that writes to InfluxDB. The source comes
from `?source=`, and `?precision=` is `ns` (default), `us`, `ms` or `s`. `/api/v2/write?bucket=`
and `/write?db=` are accepted too, since InfluxDB clients call those. Field `f` of measurement `m`
becomes metric `m.f`; string fields are skipped. Bad lines get a `400` listing them by line
number, and the good lines in the same body are still stored. A bad line is one that does not
parse, or whose reading would be refused on `/v1/ingest` too: a name or label that is not valid,
a time before 2000 or more than a day ahead (the usual cause is a `precision` that does not match
the sender), a name another source owns, a counter reading out of order. More lines than
`limits.batch_items` is a `413`. The body must be uncompressed (Telegraf:
`content_encoding = "identity"`, the default of its `influxdb` output); a chunked body is fine.

## Auth

By default there is none: Trail listens on loopback and whatever proxy fronts it is the
perimeter. With `[auth.tokens]` in `trail.toml`, a write from a listed source needs
`Authorization: Bearer <token>`. So does changing what that source declares: publishing or
removing its declarations and rebuilding its rollups take the source's write token, not the
read token. With `admin_token` set in `[auth]`, those three, and setting an error group's status,
need the admin token; a source's own write token still does the three for that source
(spec/config.md, "The admin token"). Ingest never needs the admin token.
