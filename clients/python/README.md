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

By default the spool is in memory: batches that could not be sent wait in the process and are
lost when it ends. Pass `spool_dir` (or set `TRAIL_SPOOL_DIR`) to keep them on disk.

With a spool folder, every batch is written there as one file (the request body as it will be
posted, named `<uuid7>.json`) before it is sent, and the file is deleted once Trail takes it, or
answers `400`, `401`, or `413` for a single record. A batch that gets no answer, a `5xx` or any
other status stays and is retried oldest first, with backoff from 1 s doubling to 60 s. So a
process killed mid-request leaves its batch on disk, and files left by a previous run are sent by
the next one. Records keep their ids, so a resend is stored once.

Only Trail's own answer counts as taken: a `2xx` whose body is a JSON object with `accepted`. Any
other `2xx`, such as a proxy's catch-all page or a sign-on page answering `200` with HTML, is
treated like no answer: the batch is kept and retried, and `on_problem` is told the answer did not
come from Trail. A redirect is never followed, so a `3xx` is kept and retried too: following it
would turn the POST into a GET, and the new page's `200` would look like Trail taking the batch.

A `413` (too large) splits the batch in half and sends each half on its own, splitting again on
another `413`, so one oversized record ends up alone; in a spool folder the two halves replace the
batch's file. A single record that still gets `413` is dropped and reported.

The queue holds 10,000 records and the spool 50 MB; past either the oldest go, and
`client.stats().dropped` counts them. A `400` or `401` is not retried: the batch is dropped and
reported. Every problem, including the records Trail rejected or marked as mismatched, goes to
`on_problem`:

```python
client = Client("shop", on_problem=lambda problem: print(problem))
```

## Tests

```sh
cd clients/python
python3 -m unittest discover -s tests
bunx pyright
```
