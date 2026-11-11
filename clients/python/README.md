# trail-client (Python)

Sends a service's events, metrics, errors and logs to a Trail server. Standard library only,
Python 3.10+. The contract it implements is `spec/client.md`.

## Install

```sh
pip install ./clients/python        # from a Trail checkout
```

## Use

Generate the typed emitters from your declarations, then call only those:

```sh
trail gen trail/ --lang python --out shop_events.py
```

```python
from trail_client import Client
from shop_events import Events, Metrics

client = Client("shop", spool_dir="/var/lib/shop/trail-spool")
Events(client).shop_order_failed(order_id="o-17", reason="payment")
Metrics(client).shop_queue_depth(12, queue="email")
```

Errors and logs need no declarations:

```python
client.install_excepthook()           # uncaught exceptions, in any thread
try:
    charge(order)
except PaymentError as exc:
    client.error(exc, context={"order_id": order.id})
client.log("info", "worker started", stream="worker", fields={"pid": 81})
```

Recording never blocks and never raises. Records go out in batches of up to 200, every second.
At exit the queue gets one short flush (2 s). `client.flush()` and `client.close()` are there
when you need them, e.g. in tests.

## Environment

Read when the option is not passed in code. The source is always passed in code.

| Variable | Default | Meaning |
|---|---|---|
| `TRAIL_URL` | `http://127.0.0.1:8200` | The server; the client posts to `<url>/v1/ingest` |
| `TRAIL_VERSION` | none | The deployed version, usually a commit |
| `TRAIL_HOST` | the hostname | The machine |
| `TRAIL_TOKEN` | none | Sent as `Authorization: Bearer <token>` |
| `TRAIL_SPOOL_DIR` | none (memory) | Where batches wait while Trail is down |
| `TRAIL_STRICT` | off | `1`: `flush()` raises `TrailError` listing every problem |
| `TRAIL_DISABLED` | off | `1`: every call is a no-op |

## When Trail is down

A batch that gets no answer or a `5xx` is written to the spool folder as one file (the request
body as it would be posted, named `<uuid7>.json`) and retried oldest first, with backoff from
1 s doubling to 60 s. Records keep their ids, so a resend is stored once. Files left by a
previous run are sent by the next one. Without a spool folder, failed batches wait in memory and
are lost when the process ends.

The queue holds 10,000 records and the spool 50 MB; past either the oldest go, and
`client.stats().dropped` counts them. A `400`, `401` or `413` is not retried: the batch is
dropped and reported. Every problem, including the records Trail rejected or marked as
mismatched, goes to `on_problem`:

```python
client = Client("shop", on_problem=lambda problem: print(problem))
```

## Tests

```sh
cd clients/python
python3 -m unittest discover -s tests
bunx pyright
```
