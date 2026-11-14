# Declarations

A service declares what it sends in a folder of TOML files beside its code. `trail check <folder>`
validates it, `trail gen` writes typed emitters from it, and `trail publish` loads it into the
server. `examples/shop/trail/` is a complete folder. The parser and every check are in
`crates/core/src/decl.rs`.

## `events.toml` (required)

```toml
source = "shop"                       # the service's name: lowercase letters, digits, hyphens

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
  recomputes the older buckets late events had marked before the publish.
- To change what a rollup means (its `where` or `by`), give it a new name: series already written
  under the old name keep their old groups.

## `alerts.toml`

```toml
[alerts.queue_backlog]                # a metric rule: fires per matching series
metric = "shop.queue.depth"
match = { queue = "email" }           # optional: only series with these labels
above = 100                           # exactly one of above, below, absent
clear = 80                            # optional: once firing, clears only back past this
for = "10m"                           # optional: the average over this window must be past the threshold
severity = "warning"                  # info, warning (default) or severe
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

Trail posts alerts to the webhook in `trail.toml` (`spec/alerts.md`). Metric rules are states:
each post carries the complete list of what is firing, and the receiver works out what is new
and what recovered. Event rules are one-off notices.
