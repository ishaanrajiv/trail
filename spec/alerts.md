# Alerts

Rules live in each service's `alerts.toml` (`spec/declarations.md`). Trail evaluates them every
`alerts.interval` and posts JSON to the one webhook in `trail.toml`. What the receiver does with a
post (push to a phone, open a ticket) is its own business. The code is `crates/server/src/alerts.rs`.

Trail posts over plain HTTP: point the webhook at a receiver on the same machine or network. Any
`2xx` answer counts as delivered. Anything else, or no answer within 5 seconds, is retried at the
next evaluation.

## Metric rules: `snapshot`

A metric rule is a state: it is firing or it is not, per series. Each post carries the complete
list of what is firing, so the receiver never has to remember anything: whatever is in the list
is on, and whatever was in the last list and is not in this one has recovered.

```json
{
  "type": "snapshot",
  "sent_at": 1794312001000,
  "firing": [
    {
      "key": "shop.queue_backlog{queue=email}",
      "source": "shop",
      "alert": "queue_backlog",
      "severity": "warning",
      "kind": "shop.queue_backlog",
      "title": "The email queue is backing up",
      "body": "150 jobs are waiting.",
      "labels": {"queue": "email", "service": "shop"},
      "value": 150.0,
      "since": 1794312001000
    }
  ]
}
```

- A rule watches only the series of the source that owns the metric's name (the one that
  declares it, or whose rollup writes it). Another source's readings under that name are series
  of their own and neither fire nor clear it. A name nobody declares is watched across every
  source that sends it.
- `labels` are the series' labels, and `service`: the service of the rule's source
  (`spec/declarations.md`; its own name unless events.toml says otherwise). A series that has a
  `service` label of its own keeps its value. `key` is made from the series' labels alone, so
  the added label never changes it.
- `kind` is the rule's `kind` from `alerts.toml`, left out when the rule has none. Trail does
  not interpret it; `severity`, `title` and `body` are always there for a receiver that ignores it.
- `key` is the same for as long as that series keeps firing. Use it to tell new from ongoing.
- `value` is what decided it: the newest reading, or the average over `for` (see below). Null
  for `absent`.
- A snapshot is posted when the set of keys changes, and again every 5 minutes while nothing
  changes, so a receiver that restarted or missed a post is put right.
- An empty `firing` list means everything recovered.

How a rule decides:

| Rule | Fires | Stops |
|---|---|---|
| `above = N` | the value is over N | the value is at or under `clear` (default N) |
| `below = N` | the value is under N | the value is at or over `clear` (default N) |
| `absent = "2m"` | the newest reading is older than that | a reading arrives, or the series has been silent for 7 days (it is gone, not late) |

`above` and `below` skip a series whose newest reading is more than a day old; use `absent` to
catch a sender that stopped.

With `for`, the value is the average of the series' readings in the last `for` (the window), and
the rule starts firing only once the series was already reporting when the window began: it has
a reading in the window's first fifth, or in the `for` before the window starts. Exactly, with
`for = W` evaluated at time `now`, a series that is not firing may start only when it has a
reading at a time `t` with `now - 2W <= t < now - W + W/5`. Until then it holds its previous
state: it does not fire, and `GET /v1/alerts` lists it under `waiting`. Clearing is not held back:
a firing series clears as soon as the average is back at `clear`. `absent` rules have no `for`.

The window reads the readings kept in the database (`hot_days` in `spec/config.md`: today and
the finished days before it), not the day files. A `for` longer than that is the average over
what is kept, and counts as covered once the series has a reading in the first fifth of what is
kept: `W` above is then the time since the hot window began.

For example, `for = "5m"` and `below = 1` on a new app's health check: the app's first reading is
a failure at 12:00. One reading is not five minutes of failing, so the rule waits. If the check
keeps failing every minute, the rule fires at the first evaluation after 12:04: at 12:04:01 the
window is 11:59:01 to 12:04:01, and its first minute holds the 12:00 reading. The same holds for a series back after a gap longer than `for`: its readings from
before the gap are too old to count.

## Event rules: `notice`

An event rule fires once for each matching event.

```json
{
  "type": "notice",
  "id": "shop.payment_failed:0192f0c1-7a3e-7b1a-9c2d-acde48001122",
  "source": "shop",
  "alert": "payment_failed",
  "severity": "warning",
  "title": "Payment failed for order o-9",
  "body": "",
  "ts": 1794313560000,
  "labels": {"service": "shop"},
  "event": {"id": "0192f0c1-…", "name": "shop.order.failed", "props": {"order_id": "o-9", "reason": "payment"},
            "actor": null, "client": null, "host": "web-1"}
}
```

- `kind` is as in a snapshot: the rule's `kind`, left out when it has none. `labels` holds
  `service`, the service of the rule's source.
- `id` is unique per rule and event. A notice can be posted twice if Trail stops between the
  webhook's answer and saving that it was sent, so a receiver should ignore an `id` it has seen.
- `ts` is the event's own time, unchanged however late the notice goes out, so the receiver can
  judge how old it is.
- Each event is looked at once, in the order events were stored (their `seq`). At every
  evaluation Trail reads the events stored after the last one it looked at, queues their
  notices, and saves the queue together with the `seq` it got to. A restart carries on from
  that `seq`, so an event stored just before Trail stopped, or while it was down, still gets its
  notice. The first start of a Trail that keeps this mark (and a new data folder) starts it at
  the last event already stored: events from before raise no notices.
- An event that was more than an hour old when it arrived (its `received_at` more than an hour
  after its `ts`) raises no notice: a phone sending last week's queue is not news. How long ago
  it arrived does not matter: an event stored while the webhook was down raises its notice when
  the webhook is back, however long that was.
- Notices are sent in order. While the webhook is unreachable (no answer, a `5xx`, `408` or
  `429`) they wait. Once 1,000 wait in the queue Trail stops reading further events (an event
  is read whole, so the queue can pass 1,000 by the rules the last one matched). They wait in
  the store and are read, in order, as the webhook takes the queued ones,
  so an outage of any length loses no notice (as long as the event's retention keeps it). With no webhook configured the queue keeps the
  newest 1,000 for `GET /v1/alerts`, oldest dropped first. Any answer other than those means the
  receiver will never take that post: the notice is dropped, logged and counted in
  `trail.webhook.failures`, and the ones behind it go out.
- An event rule matches with the declarations as they are when the event is looked at, at the
  first evaluation after it is stored.

## Looking

`GET /v1/alerts` returns what is firing (`firing`), the series a rule with `for` holds back
because their readings do not yet cover the window (`waiting`: `key`, `source`, `alert`,
`labels`, and `value`, the average so far), the notices still waiting, and every rule with its
`kind` (null when it has none). It works without a webhook, which is how to try rules out.
