# The example: a shop

A made-up service that shows every part of Trail in one folder.

| Path | What it is |
|---|---|
| `trail/events.toml` | The three events the shop sends, with their properties |
| `trail/metrics.toml` | Its two metrics |
| `trail/rollups.toml` | Orders per minute by payment method; failures per hour by reason |
| `trail/alerts.toml` | A threshold rule, a "stopped reporting" rule and an event rule |
| `generated/` | What `trail gen` writes from those, for Python, TypeScript and Swift |
| `trail.toml` | A server config |

Try it:

```sh
trail serve --seed                        # a server with three hours of this shop's traffic
trail get '/v1/events?name=shop.order.*&limit=3'
trail get '/v1/metrics/query?name=shop.orders.count&since=-3h&step=15m&agg=sum'
trail get /v1/alerts                      # the email queue is backing up
```

Sending from code, after `trail gen examples/shop/trail --lang python --out shop_events.py`:

```python
from trail_client import Client
from shop_events import Events

events = Events(Client("shop"))
events.shop_order_placed(order_id="o-17", total_cents=1200, method="card", items=["tea"])
```

A misspelt property or a string where an int belongs fails the type check, before it runs.
