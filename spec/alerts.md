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
      "title": "The email queue is backing up",
      "body": "150 jobs are waiting.",
      "labels": {"queue": "email"},
      "value": 150.0,
      "since": 1794312001000
    }
  ]
}
```

- `key` is the same for as long as that series keeps firing. Use it to tell new from ongoing.
- `value` is what decided it: the newest reading, or the average over `for`. Null for `absent`.
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
  "event": {"id": "0192f0c1-…", "name": "shop.order.failed", "props": {"order_id": "o-9", "reason": "payment"},
            "actor": null, "client": null, "host": "web-1"}
}
```

- `id` is unique per rule and event. A notice can be posted twice if Trail stops between the
  webhook's answer and saving that it was sent, so a receiver should ignore an `id` it has seen.
- Notices are sent in order. While the webhook is unreachable up to 1,000 wait, oldest dropped first.
- An event more than an hour old when it arrives raises no notice: a phone sending last week's
  queue is not news.

## Looking

`GET /v1/alerts` returns what is firing, the notices still waiting, and every rule. It works
without a webhook, which is how to try rules out.
