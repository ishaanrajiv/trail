# Working on Trail

Trail is one small Rust server that keeps a system's events, metrics, errors and logs. It is
maintained by coding agents, so its rules are checks that fail with a message saying what to do.
Run `scripts/check` before every commit; CI runs the same script.

## Layout

| Path | What it is |
|---|---|
| `crates/core` | Types and pure functions: envelope, declarations, registry check, config, time. No sockets, no database. |
| `crates/codegen` | `trail gen`: typed emitters for Python, TypeScript and Swift. |
| `crates/store` | SQLite and Parquet: writes, queries, day files, retention. |
| `crates/server` | HTTP API, the rollup aggregator, alerts. |
| `crates/cli` | The `trail` binary. |
| `clients/` | The Python, Node and Swift client libraries. |
| `spec/` | The contracts: ingest, declarations, clients, config, storage, alerts, the read API. |
| `examples/shop` | A complete example: declarations, generated emitters, a `trail.toml`. |

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
  stored and marked, never refused. Only a record that cannot be read is rejected.
- **Declared, not free-form.** A new aggregation is a new measure in `rollups.toml`, computed at
  ingest. Do not add a query language.
- **Behaviour has a test.** A bug fix starts with the test that fails.
- **Nothing private in the repository.** Examples use `example.com`, `shop` and made-up names. No
  real hostnames, addresses, domains or people.
- **Heavy jobs are capped.** Build with `CARGO_BUILD_JOBS=4`; `scripts/check` does.

## Commands

```sh
scripts/check                 # format, lint, tests, generated files, dependency and size budgets
scripts/check --fast          # skip the release build and the client tests
cargo run -p trail -- serve --seed          # a local server with made-up data, on 127.0.0.1:8200
cargo run -p trail -- check examples/shop/trail
cargo run -p trail -- gen examples/shop/trail --lang python --out examples/shop/generated/shop_events.py
```
