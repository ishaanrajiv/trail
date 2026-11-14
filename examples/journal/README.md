# The journal collector's declarations

`trail collect journald` sends every journal line as a log. Its source is the container's name
for a container's line, else the unit's name. A scope (a login session, a `systemd-run`, a
terminal's command) is a different unit every run, so its lines are named after the program that
wrote them (`sudo`, `systemd`), or the scope's name without its run id when none is given.

A machine ends up with a source per system program that way (`sudo`, `cron`, `kernel`,
`systemd-logind`…). To keep those together, name the source they share with `--group-as` and
list the names that keep a source of their own:

```sh
trail collect journald --group-as system --keep-source shop-api --keep-source mail-worker
trail collect journald --group-as system --keep-file /etc/trail/keep-sources
```

`--keep-source` takes a source name as the collector would make it (a unit `shop-api.service`
is `shop-api`), and can be given again; `--keep-file` is a file of them, one per line, `#`
starting a comment. Both can be used together. A container's lines always keep their source.
Every other line goes under the `--group-as` source with the name its source would have had in
the field `program` (its `stream` and the `unit` and `identifier` fields are as before). Without
`--group-as` nothing changes. Errors from grouped lines still group by program: their kind is
the program's name, and `<unit>/<identifier>` when a unit's program calls itself something else
(so two units that both run `python3` do not share a fingerprint). Turning grouping on gives
those programs' errors new fingerprints, since their source changes.

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
