# Telemetry (copy this section into your project's AGENTS.md or CLAUDE.md)

This project sends its events, metrics, errors and logs to Trail. What it may send is declared
in `trail/` and the emitters in `<path to the generated file>` are generated from that. Replace
the paths and the language below with this project's.

## When to add an event

Add one when the code does something a person would later ask about:

- **A decision or an outcome**: an order placed, a job finished or failed, a login refused.
  Record the outcome and why, not each step on the way to it.
- **Anything done for someone or to something outside the process**: a message sent, a device
  switched, a file deleted, a call to another service that changes state.
- **A fallback or a give-up**: a retry that ran out, a default used because a value was missing.

Do not add an event for a function being entered, for a value that is only interesting while
debugging (log it), or for a number that is read on a schedule (that is a metric).

A count or a timing that is wanted as a graph is a **rollup** of an event (`trail/rollups.toml`),
not a second thing to emit. A level that is read, not counted (a queue's depth, a temperature),
is a **metric**.

## How to add one

1. Declare it in `trail/events.toml`, with a description and every property typed. Property
   names say what the value is and its unit (`latency_ms`, `total_cents`). Nothing secret and
   nothing that identifies a person beyond an id.
2. Run `trail check trail/` and fix what it says.
3. Run `trail gen trail/ --lang <python|typescript|javascript|swift> --out <the generated file>`
   and commit the result with the declaration.
4. Call the generated function. Never build an event by hand or call the client's
   `record_event` directly: the generated function is what makes a wrong name or type fail the
   type check.
5. Changing an event: add properties freely; to rename or retype one, add a new event name
   instead, because stored records keep the old shape.

## Rules

- Emitting never blocks and never throws, so do not wrap it in error handling or wait on it.
- Errors go through the client's `error(...)`, not through an event named `…error`.
- Tests run with `TRAIL_STRICT=1`, which turns a record Trail would reject or mark into a
  failure at `flush`.
- `trail gen --check` runs in this project's checks; a stale generated file fails them.
- After a deploy, `trail get '/v1/problems?source=<this source>'` lists anything sent that did
  not match its declaration. It should be empty.
