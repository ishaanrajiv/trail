//! Reading a command line: positionals, `--option value` and `--flag`, checked against what the
//! command takes. An unknown option is refused with the one probably meant.

use std::collections::{BTreeMap, BTreeSet};
use trail_core::Problem;
use trail_core::problem::closest;

/// Options every command that talks to the server takes.
pub const SERVER: [&str; 2] = ["url", "token"];
/// Options of the commands that change what Trail keeps (declarations, rollups).
pub const ADMIN: [&str; 3] = ["url", "token", "admin-token"];

#[derive(Debug, Default)]
pub struct Args {
    pub positional: Vec<String>,
    values: BTreeMap<String, Vec<String>>,
    flags: BTreeSet<String>,
}

impl Args {
    /// Parses `raw` for `command`, which takes the `options` (with a value) and `flags` named.
    pub fn parse(
        command: &str,
        raw: Vec<String>,
        options: &[&str],
        flags: &[&str],
    ) -> Result<Args, Vec<Problem>> {
        let mut args = Args::default();
        let mut parser = lexopt::Parser::from_args(raw);
        let fail =
            |message: String, fix: String| vec![Problem::new(format!("trail {command}"), message, fix)];
        loop {
            let arg = match parser.next() {
                Ok(Some(arg)) => arg,
                Ok(None) => break,
                Err(e) => return Err(fail(e.to_string(), format!("see `trail help` for `{command}`"))),
            };
            match arg {
                lexopt::Arg::Value(value) => args.positional.push(value.to_string_lossy().into_owned()),
                lexopt::Arg::Long(name) => {
                    let name = name.to_string();
                    if flags.contains(&name.as_str()) {
                        args.flags.insert(name);
                    } else if options.contains(&name.as_str()) {
                        let value = parser.value().map_err(|_| {
                            fail(
                                format!("`--{name}` needs a value"),
                                format!("write `--{name} <value>`"),
                            )
                        })?;
                        args.values
                            .entry(name)
                            .or_default()
                            .push(value.to_string_lossy().into_owned());
                    } else {
                        let known: Vec<&str> = options.iter().chain(flags).copied().collect();
                        let hint = closest(&name, known.iter().copied())
                            .map(|c| format!("did you mean `--{c}`? "))
                            .unwrap_or_default();
                        let listed = known
                            .iter()
                            .map(|k| format!("--{k}"))
                            .collect::<Vec<_>>()
                            .join(", ");
                        return Err(fail(
                            format!("`--{name}` is not an option of `trail {command}`"),
                            format!("{hint}it takes: {listed}"),
                        ));
                    }
                }
                lexopt::Arg::Short(letter) => {
                    return Err(fail(
                        format!("`-{letter}` is not an option"),
                        "options are written in full, like `--url`".to_string(),
                    ));
                }
            }
        }
        Ok(args)
    }

    pub fn value(&self, name: &str) -> Option<&str> {
        self.values.get(name).and_then(|v| v.last()).map(String::as_str)
    }

    pub fn values(&self, name: &str) -> &[String] {
        self.values.get(name).map_or(&[], Vec::as_slice)
    }

    pub fn flag(&self, name: &str) -> bool {
        self.flags.contains(name)
    }

    /// The server's address: `--url`, else `TRAIL_URL`, else loopback.
    pub fn url(&self) -> String {
        self.value("url")
            .map(str::to_string)
            .or_else(|| std::env::var("TRAIL_URL").ok().filter(|v| !v.is_empty()))
            .unwrap_or_else(|| "http://127.0.0.1:8200".to_string())
            .trim_end_matches('/')
            .to_string()
    }

    pub fn token(&self) -> Option<String> {
        self.value("token")
            .map(str::to_string)
            .or_else(|| std::env::var("TRAIL_TOKEN").ok().filter(|v| !v.is_empty()))
    }

    /// The token for a route that changes what Trail keeps: `--admin-token`, else
    /// `TRAIL_ADMIN_TOKEN`, else the usual token (`--token`, else `TRAIL_TOKEN`), which is a
    /// source's write token or what a server with no admin token accepts.
    pub fn admin_token(&self) -> Option<String> {
        self.value("admin-token")
            .map(str::to_string)
            .or_else(|| {
                std::env::var(trail_core::config::ADMIN_TOKEN_ENV)
                    .ok()
                    .filter(|v| !v.is_empty())
            })
            .or_else(|| self.token())
    }

    /// The positionals, which must number at least `names.len()`; says which is missing.
    pub fn need(&self, command: &str, names: &[&str]) -> Result<(), Vec<Problem>> {
        if self.positional.len() >= names.len() {
            return Ok(());
        }
        let missing = names[self.positional.len()];
        Err(vec![Problem::new(
            format!("trail {command}"),
            format!("missing <{missing}>"),
            format!(
                "write `trail {command} {}`",
                names
                    .iter()
                    .map(|n| format!("<{n}>"))
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
        )])
    }
}

/// Splits `key=value` arguments. A value that is JSON is read as JSON; anything else is text.
pub fn pairs(
    command: &str,
    items: &[String],
) -> Result<serde_json::Map<String, serde_json::Value>, Vec<Problem>> {
    let mut map = serde_json::Map::new();
    for item in items {
        let Some((key, value)) = item.split_once('=') else {
            return Err(vec![Problem::new(
                format!("trail {command}"),
                format!("`{item}` is not key=value"),
                "write properties as key=value, like order_id=o-17 total_cents=1200",
            )]);
        };
        let value =
            serde_json::from_str(value).unwrap_or_else(|_| serde_json::Value::String(value.to_string()));
        map.insert(key.to_string(), value);
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_admin_token_flag_wins_over_the_token() {
        let raw = |list: &[&str]| list.iter().map(|s| (*s).to_string()).collect::<Vec<_>>();
        let both = Args::parse(
            "publish",
            raw(&["--token", "write-token", "--admin-token", "admin-token", "x"]),
            &ADMIN,
            &[],
        )
        .unwrap();
        assert_eq!(both.admin_token().as_deref(), Some("admin-token"));
        assert_eq!(both.token().as_deref(), Some("write-token"));
        // `--admin-token` is only for the commands that change what Trail keeps.
        assert!(Args::parse("get", raw(&["--admin-token", "a", "/v1"]), &SERVER, &[]).is_err());
    }
}
