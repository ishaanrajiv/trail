# Declarations

A service declares what it sends in a folder of TOML files beside its code. `trail check <folder>`
validates it, `trail gen` writes typed emitters from it, and `trail publish` loads it into the
server, replacing everything the source declared before; `trail publish --keep` adds to it
instead, replacing by name and keeping what the folder lacks, for a deploy that publishes before
it restarts the service (`spec/api.md`, `PUT /v1/registry/{source}`). `examples/shop/trail/` is a
complete folder. The parser and every check are in
`crates/core/src/decl.rs`.

## `events.toml` (required)

```toml
source = "shop"                       # the source's name: lowercase letters, digits, hyphens
service = "shop"                      # optional: the service it belongs to. Default: the source's name

[events."shop.order.placed"]          # two or more snake_case parts joined by dots
description = "A customer placed an order"   # required: one sentence, for whoever queries it later
retention = "forever"                 # or whole days: "30d", "90d". Default forever.
[events."shop.order.placed".props]
order_id = "string"
total_cents = "int"
method = { enum = ["card", "cash"] }
items = "string[]"
latency_ms = "int?"                   # ? marks it optional
```

`service` says which service a source belongs to, when one service sends as several sources (an
app, its worker, its container's log source): give them all the same `service`. It is lowercase
letters, digits, `-` and `_`, starting with a letter, at most 64 bytes. Readers get it from
`GET /v1/registry`, and alert posts carry it as the label `service` (`spec/alerts.md`). It
changes nothing that is stored.

Property types:

| Type | JSON it accepts |
|---|---|
| `string` | a string |
| `int` | a whole number |
| `float` | any number |
| `bool` | true or false |
| `string[]` | a list of strings |
| `{ enum = ["a", "b"] }` | one of the listed strings. Add `optional = true` for optional. |
| `json` | anything. An escape hatch: nothing can group or measure it. |

Rules the check enforces:

- An event name belongs to one source.
- Property names are snake_case and may not reuse an envelope field (`id`, `name`, `ts`, `source`,
  `version`, `host`, `actor`, `client`, `trace_id`, `props`, `received_at`).
- A property whose name contains `password`, `secret`, `token`, `otp`, `api_key`,
  `authorization`, `cookie` or `credential` is refused. Events are kept for years; record an id or
  a boolean instead.

## `metrics.toml`

```toml
[metrics."shop.queue.depth"]
description = "Jobs waiting in a queue"
unit = "jobs"                         # optional
kind = "gauge"                        # or "counter" (a running total). Default gauge.
labels = ["queue"]                    # every reading carries exactly these labels
```

Keep label values to a small set (a room, a queue), never something unique per reading: every
distinct combination is a new series.

A series is its source, name and labels. A reading another source sends under the same name and
labels is that source's own series: it never changes this source's readings, and never fires or
clears its alerts. The machine a reading came from (`host`) is not part of a series, because a
container's hostname changes every time it is recreated. So a source that sends from several
machines at once must tell their readings apart with a label (`labels = ["node"]`); otherwise
they land in one series, and two counters interleaved in one series read as resets.

## `rollups.toml`

An aggregation Trail computes as events arrive. It writes one metric series per measure, named
`<source>.<rollup>.<measure>`, with a label per `by` field.

```toml
[rollups.orders]
description = "Orders, their value and how long checkout took, by payment method"
from = "shop.order.placed"            # an event this source declares
where = { method = "card" }           # optional: only events whose properties equal these
by = ["method"]                       # optional: properties, or actor, client, host. At most 4.
measure = ["count", "sum(total_cents)", "p95(latency_ms)"]
every = "1m"                          # 1m, 5m, 1h or 1d
```

| Measure | Series | Value |
|---|---|---|
| `count` | `shop.orders.count` | events in the bucket |
| `sum(p)`, `avg(p)`, `min(p)`, `max(p)` | `shop.orders.p.sum` and so on | over a numeric property |
| `p50(p)`, `p90(p)`, `p95(p)`, `p99(p)` | `shop.orders.p.p95` | percentile, within about 2% |

- A bucket's sample is stamped with the bucket's start. Hourly and daily buckets start on the
  local hour and at local midnight, in the server's configured timezone.
- `by` takes string, int, bool and enum properties. An event without an optional `by` property
  groups under the empty string.
- An event without a measured optional property still counts, and is left out of that measure.
- A new or changed rollup is computed from its current bucket on. `trail rollup rebuild <source>
  <name>` recomputes it from the raw events, as far back as they are kept. A changed rollup still
  recomputes the older buckets late events had marked before the publish, and the buckets it had
  open; when `every` changed, as the buckets that cover the same time under the new `every`.
- To change what a rollup means (its `where` or `by`), give it a new name: series already written
  under the old name keep their old groups.

## `alerts.toml`

```toml
[alerts.queue_backlog]                # a metric rule: fires per matching series
metric = "shop.queue.depth"
match = { queue = "email" }           # optional: only series with these labels
above = 100                           # exactly one of above, below, absent
clear = 80                            # optional: once firing, clears only back past this
for = "10m"                           # optional: the average over this window must be past the threshold,
                                      # and the series must have been reporting when it began (spec/alerts.md)
severity = "warning"                  # info, warning (default) or severe
kind = "shop.queue_backlog"           # optional: the name the receiver knows this alert by
title = "The {queue} queue is backing up"    # {label} and {value} are filled in
body = "{value} jobs are waiting."

[alerts.worker_quiet]
metric = "shop.queue.depth"
absent = "2m"                         # fires when the newest reading is older than this
title = "The queue worker stopped reporting"

[alerts.payment_failed]               # an event rule: fires once per matching event
event = "shop.order.failed"
where = { reason = "payment" }
title = "Payment failed for order {order_id}"   # {property} is filled in
```

`kind` is for the receiver: it lets it look up how to notify (a push sound, a wording of its own)
by name instead of by severity. Write it like an event name: two or more parts joined by dots,
each lowercase letters, digits and `_` starting with a letter, at most 128 bytes in all
(`shop.queue_backlog`, `trail.disk_failing`). Trail does not interpret it: it is checked, sent on
the rule's posts and listed by `GET /v1/alerts`. `severity`, `title` and `body` are sent all the
same, so a receiver that does not know the kind works as before.

Trail posts alerts to the webhook in `trail.toml` (`spec/alerts.md`). Metric rules are states:
each post carries the complete list of what is firing, and the receiver works out what is new
and what recovered. Event rules are one-off notices.
