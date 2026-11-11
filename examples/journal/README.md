# The journal collector's declarations

`trail collect journald` sends every journal line as a log, named after its unit or container.
It also sends, unless run with `--logs-only`:

- each line at error priority or worse as an **error**, under the same source, so repeated
  failures group by fingerprint;
- `journal.oom.killed` and `journal.unit.failed` **events**, under the source `journal`.

This folder declares those two events and an alert on each. Publish it to use them:

```sh
trail publish examples/journal/trail
```
