//! The `trail` command. `trail help` lists what it does; each part lives in its own module:
//!
//! - `args`: reading the command line, with the option you probably meant when one is unknown.
//! - `decls`: `check`, `gen`, `publish`.
//! - `send`: `event`, `metric`, `error`, `log`, `flush`, `import`, `get`, `rollup rebuild`.
//! - `collect`: `collect journald`, `collect jsonl`.
//! - `serve`: `serve`, and the made-up data behind `--seed`.

mod args;
mod collect;
mod decls;
mod send;
mod serve;

use std::io::Write;
use std::process::ExitCode;
use trail_core::Problem;
use trail_core::problem::closest;

const USAGE: &str = "\
trail: keeps a system's events, metrics, errors and logs.

Run the server
  trail serve [--config trail.toml]      start Trail (spec/config.md lists the settings)
  trail serve --seed                     start it on made-up data, to try things against

Declare what a service sends (spec/declarations.md)
  trail check <folder>...                validate declarations
  trail gen <folder> --lang python|typescript|javascript|swift [--out FILE] [--check]
                                         write typed emitters; --check fails if FILE is stale
  trail publish <folder>...              load declarations into the server

Send from a script
  trail event <source> <name> [key=value ...] [--actor A] [--client C] [--trace-id T]
  trail metric <source> <name> <value> [label=value ...] [--unit U] [--counter]
  trail error <source> <kind> <message> [--severity warning|error|fatal] [--stack TEXT]
  trail log <source> <message> [key=value ...] [--level L] [--stream S]
  trail flush                            resend what was spooled while Trail was down
  trail import <file.jsonl>...           post a file of ingest bodies, one per line

Collect logs
  trail collect journald [--source NAME] [--unit U]... [--user] [--logs-only]
  trail collect jsonl <file> --source NAME [--stream S]

Look
  trail get <path>                       e.g. trail get '/v1/events?name=shop.order.*&since=-1h'
  trail get /v1                          every route the server has, with its parameters
  trail rollup rebuild <source> <name> [--since T]

Everywhere: --url URL (or TRAIL_URL; default http://127.0.0.1:8200), --token T (or TRAIL_TOKEN).
`publish` and `rollup rebuild` send --admin-token T (or TRAIL_ADMIN_TOKEN) when given, else the token.
With TRAIL_SPOOL_DIR set, a record that can't be sent is kept there for `trail flush`.
Sending a record, and each file `trail flush` sends, waits at most 10 s for an answer.
A value in key=value is read as JSON when it is JSON (3, true, [1,2]) and as text otherwise.";

const COMMANDS: [&str; 14] = [
    "serve", "check", "gen", "publish", "event", "metric", "error", "log", "flush", "import", "collect",
    "get", "rollup", "help",
];

/// Prints a line to stdout. A closed pipe (`trail get … | head`) is not an error.
pub(crate) fn out(text: &str) {
    let _ = writeln!(std::io::stdout(), "{text}");
}

/// Prints a problem to stderr.
pub(crate) fn complain(problem: &Problem) {
    eprintln!("{problem}");
}

/// What a command returns: how many problems it printed.
pub(crate) type Done = Result<(), Vec<Problem>>;

fn main() -> ExitCode {
    let mut raw: Vec<String> = std::env::args().skip(1).collect();
    if raw.is_empty() {
        out(USAGE);
        return ExitCode::from(2);
    }
    let command = raw.remove(0);
    let done = match command.as_str() {
        "help" | "--help" | "-h" => {
            out(USAGE);
            Ok(())
        }
        "--version" | "version" => {
            out(env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        "serve" => serve::serve(raw),
        "check" => decls::check(raw),
        "gen" => decls::generate(raw),
        "publish" => decls::publish(raw),
        "event" => send::event(raw),
        "metric" => send::metric(raw),
        "error" => send::error(raw),
        "log" => send::log(raw),
        "flush" => send::flush(raw),
        "import" => send::import(raw),
        "get" => send::get(raw),
        "rollup" => send::rollup(raw),
        "collect" => collect::collect(raw),
        other => {
            let hint = closest(other, COMMANDS)
                .map(|c| format!("did you mean `trail {c}`? "))
                .unwrap_or_default();
            Err(vec![Problem::new(
                "trail",
                format!("`{other}` is not a command"),
                format!("{hint}`trail help` lists them"),
            )])
        }
    };
    match done {
        Ok(()) => ExitCode::SUCCESS,
        Err(problems) => {
            for problem in &problems {
                complain(problem);
            }
            ExitCode::FAILURE
        }
    }
}
