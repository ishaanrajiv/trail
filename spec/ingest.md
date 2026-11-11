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

- `ts` is milliseconds since 1970, UTC. Left out, it is the arrival time. Any age is accepted: a
  client that was offline for a week sends its queue and every record lands on its own day.
- `id` is 1 to 64 letters, digits or `_ - : .`. Senders use a UUIDv7. The same id sent twice is
  stored once, so a retry is always safe. Left out, Trail makes one, and a retry is then a duplicate.
- A `counter` metric is a running total. Trail stores the per-second rate between readings; the
  first reading, and the first after a reset, only set the baseline.
- `props` are at most 64 KB. Messages are cut at 16 KB and stacks at 64 KB.

## The response

`200` whenever the body could be read, even if some records were refused:

```json
{
  "accepted":   {"events": 1, "metrics": 1, "errors": 1, "logs": 1},
  "duplicates": 0,
  "rejected":   [{"at": "events[1]", "message": "`Order` is not an event name",
                  "fix": "use two or more snake_case parts joined by dots, like `home.device.action`"}],
  "mismatched": [{"at": "events[0]", "message": "unknown property `latncy_ms` on `shop.order.placed`; declared: …"}]
}
```

- `accepted`: records newly stored.
- `duplicates`: records whose id was already stored.
- `rejected`: records that could not be read. They are not stored; each says how to fix it.
- `mismatched`: records that were stored but differ from their declaration. They are marked.

Other statuses:

| Status | When | A client should |
|---|---|---|
| `400` | The body is not the JSON object above, or `source` is invalid. Body: `{"error": {"at", "message", "fix"}}` | Drop the batch and report it: a retry cannot succeed |
| `401` | A write token is configured for this source and the bearer token is missing or wrong | Drop and report |
| `413` | The body is over the size limit, or has too many records | Send smaller batches |
| `5xx`, or no answer | Trail is down or restarting | Keep the batch and retry later |

## `POST /v1/write`

InfluxDB line protocol, for Telegraf and anything else that writes to InfluxDB. The source comes
from `?source=`, and `?precision=` is `ns` (default), `us`, `ms` or `s`. `/api/v2/write?bucket=`
and `/write?db=` are accepted too, since InfluxDB clients call those. Field `f` of measurement `m`
becomes metric `m.f`; string fields are skipped. Bad lines get a `400` listing them, and the good
lines in the same body are still stored.

## Auth

By default there is none: Trail listens on loopback and whatever proxy fronts it is the
perimeter. With `[auth.tokens]` in `trail.toml`, a write from a listed source needs
`Authorization: Bearer <token>`.
