# Working on Trail

Trail is one small Rust server that keeps a system's events, metrics, errors and logs. It is
maintained by coding agents, so its rules are checks that fail with a message saying what to do.
Run `scripts/check` before every commit; CI runs the same script.

## This repository is public

Anyone can read everything here: the code, every commit in the history, commit messages, branch
names, pull requests and their comments. Write as if a stranger reads it, because one will.

- **Never commit a secret**, even for a moment: keys, tokens, passwords, certificates, `.env`
  files, Apple signing keys (`.p8`) or real team ids, bundle ids and device tokens. Removing it
  in a later commit does not help; the history keeps it. If one is pushed, say so, and treat it
  as leaked: it has to be revoked and replaced, not just deleted.
- **Nothing about the machine Trail runs on, or its owner.** No real hostnames, IP addresses,
  domains, usernames, home or data paths (`/home/…`, `/srv/…`), names of other services or
  projects, or people. Use `example.com`, loopback addresses and made-up names instead.
- **No real data.** No databases, logs, journal lines, exports or screenshots from a running
  Trail. Tests and examples use data made up for them.
- **The deployment lives elsewhere.** Config, secrets and anything specific to where Trail is run
  belong in that deployment's own (private) repository, not here. If a task needs something
  private to be written down, stop and ask rather than putting it here.
- **CI's secret scan is a backstop, not a check you can rely on.** It looks for things shaped like
  keys; it can't tell a real hostname or a personal detail from an example.

## Layout

| Path | What it is |
|---|---|
| `crates/core` | Types and pure functions: envelope, declarations, registry check, config, time. No sockets, no database. |
| `crates/codegen` | `trail gen`: typed emitters for Python, TypeScript, JavaScript (JSDoc) and Swift. |
| `crates/store` | SQLite and Parquet: writes, queries, day files, retention. |
| `crates/server` | The HTTP/1.1 server (its own, on `std::net`, in `src/httpd.rs`, shared with another server), the API routes, the rollup aggregator, alerts. |
| `crates/cli` | The `trail` binary. |
| `clients/` | The Python, Node and Swift client libraries. |
| `spec/` | The contracts: ingest, declarations, clients, config, storage, alerts, the read API. |
| `examples/shop` | A complete example: declarations, generated emitters, a `trail.toml`. |
| `examples/journal` | Declarations for the events `trail collect journald` sends. |
| `examples/agents-template.md` | What a project that sends to Trail tells its own agents. |

## Rules

- **The spec and the code change together.** A change to a wire format, a file format or a config
  field updates its file in `spec/` in the same commit.
- **No new dependencies.** `Cargo.toml` lists every crate Trail may use, and `scripts/check` fails
  on any other. Trail has no async runtime, no SQL engine and no logging framework on purpose. The
  clients use their language's standard library only.
- **Every error says how to fix it.** Return a `Problem { at, message, fix }` (`crates/core/src/problem.rs`).
  Name the field, say what is wrong with the value, and give a value that would work. Use
  `closest` to suggest the name the writer probably meant.
- **No `unwrap`, `expect`, `panic!` or `unsafe`** outside tests; the lints deny them. Return the error.
- **Ingest never loses a record it could read.** A record that differs from its declaration is
  stored and marked, never refused. A record is rejected only when it cannot be read or cannot
  be placed, and the sender is always told which and why: `spec/ingest.md` lists the cases (a
  name a token-guarded source owns, a counter reading out of order, a series over the limit).
- **Nothing is trusted to memory alone.** Work that is owed (a late day to rewrite, buckets to
  recompute, a notice to send) survives a kill at any line: it is either in SQLite in the same
  transaction as what caused it, or found again at start from what is stored.
- **Declared, not free-form.** A new aggregation is a new measure in `rollups.toml`, computed at
  ingest. Do not add a query language.
- **Behaviour has a test.** A bug fix starts with the test that fails.
- **Nothing private in the repository.** It is public: see "This repository is public" above.
  Examples use `example.com`, `shop` and made-up names.
- **The transport is shared.** `crates/server/src/httpd.rs` (reading requests under limits and
  timeouts, the connection cap, the worker and writer slots, writing answers) is kept byte for
  byte the same as a copy in another server, and a check outside this repository fails when they
  differ. Change it in both in the same piece of work. It names neither server and uses only
  `std` (and `serde_json` in its tests); what is Trail's own (the error body, which requests take
  which slot, the numbers) goes in through `httpd::Config` and `httpd::Handler`, from `lib.rs`
  and `http.rs`.
- **Heavy jobs are capped.** Build with `CARGO_BUILD_JOBS=4`; `scripts/check` does.

## Commands

```sh
scripts/check                 # format, lint, tests, generated files, dependency, size and memory budgets
scripts/check --fast          # Rust only: skips the client suites, the release build and the end-to-end run
scripts/e2e                   # a real server, sent to from all three clients, through an outage
scripts/bench                 # throughput on one core and on all of them, a minute each (docs/performance.md)
cargo run -p trail -- serve --seed          # a local server with made-up data, on 127.0.0.1:8200
cargo run -p trail -- check examples/shop/trail
cargo run -p trail -- gen examples/shop/trail --lang python --out examples/shop/generated/shop_events.py
```
