# The journal collector's declarations

`trail collect journald` sends every journal line as a log. Its source is the container's name
for a container's line, else the unit's name. A scope (a login session, a `systemd-run`, a
terminal's command) is a different unit every run, so its lines are named after the program that
wrote them (`sudo`, `systemd`), or the scope's name without its run id when none is given.

A line's level comes from its journal priority, except a container's: Docker's journald driver
sets the priority from the stream alone (stdout info, stderr error), and most programs log
everything to stderr. A container's level is read from the line instead:

- a JSON object's `level`, `severity` or `lvl` (its `message` or `msg` becomes the message and its
  other keys the log's fields);
- else a level the line starts with, after up to three timestamp words: logfmt's `level=error`,
  Telegraf's `E!`, nginx's `[error]`, Python's `ERROR:name:…`, uvicorn's `INFO:`, or a bare
  upper-case `ERROR`;
- else info.

It also sends, unless run with `--logs-only`:

- each line at error level or worse as an **error**, under the same source, so repeated
  failures group by fingerprint. A container's error is of the kind named after the container,
  since its identifier is its id, which changes with every new container;
- `journal.oom.killed` and `journal.unit.failed` **events**, under the source `journal`.

This folder declares those two events and an alert on each. Publish it to use them:

```sh
trail publish examples/journal/trail
```
