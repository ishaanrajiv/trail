//! Typed emitters, generated from a source's declarations.
//!
//! Each generator returns the full text of one file: a function per declared event and metric, in
//! that language's own types. A service imports the generated file and never calls a free-form
//! `emit(name, props)`, so an undeclared event or a wrong property type is a type error in its own
//! build. `spec/client.md` is the contract between the generated code and each client library.

use std::collections::BTreeMap;
use trail_core::Problem;
use trail_core::decl::{Base, Declarations};

mod names;
mod python;
mod swift;
mod typescript;

#[cfg(test)]
mod tests;

/// A language `trail gen` can write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    Python,
    TypeScript,
    Swift,
}

impl Language {
    pub const ALL: [Language; 3] = [Language::Python, Language::TypeScript, Language::Swift];

    pub fn name(self) -> &'static str {
        match self {
            Language::Python => "python",
            Language::TypeScript => "typescript",
            Language::Swift => "swift",
        }
    }

    pub fn parse(text: &str) -> Option<Language> {
        match text {
            "python" | "py" => Some(Language::Python),
            "typescript" | "ts" => Some(Language::TypeScript),
            "swift" => Some(Language::Swift),
            _ => None,
        }
    }
}

/// The generated file for `decl` in `language`. The same declarations always give the same text,
/// so a stale file can be detected by generating again and comparing.
pub fn generate(decl: &Declarations, language: Language) -> String {
    match language {
        Language::Python => python::generate(decl),
        Language::TypeScript => typescript::generate(decl),
        Language::Swift => swift::generate(decl),
    }
}

/// Problems that only show up when generating: names the TOML allows but that would give two
/// functions, parameters or enum cases the same name in some language, or no name at all. Run it
/// before [`generate`]; the generated file for declarations with a problem may not compile.
pub fn check(decl: &Declarations) -> Vec<Problem> {
    let mut problems = Vec::new();

    let methods: [(&str, Convert); 3] = [
        ("Python", python::method),
        ("TypeScript", typescript::method),
        ("Swift", swift::method),
    ];
    let events: Vec<&str> = decl.events.keys().map(String::as_str).collect();
    for clash in clashes(&events, &methods) {
        problems.push(Problem::new(
            format!("events.toml: events.\"{}\"", clash.second),
            format!(
                "`{}` and `{}` both become {}, so one generated function would hide the other",
                clash.first, clash.second, clash.becomes
            ),
            format!(
                "rename one of them so the names differ in more than `.`, `_` and `-`, like `{}`",
                suggest(clash.second, &events, &methods)
            ),
        ));
    }
    let metrics: Vec<&str> = decl.metrics.keys().map(String::as_str).collect();
    for clash in clashes(&metrics, &methods) {
        problems.push(Problem::new(
            format!("metrics.toml: metrics.\"{}\"", clash.second),
            format!(
                "`{}` and `{}` both become {}, so one generated function would hide the other",
                clash.first, clash.second, clash.becomes
            ),
            format!(
                "rename one of them so the names differ in more than punctuation and case, like `{}`",
                suggest(clash.second, &metrics, &methods)
            ),
        ));
    }

    let mut enum_types: BTreeMap<String, (&str, &str)> = BTreeMap::new();
    for (name, event) in &decl.events {
        let at = format!("events.toml: events.\"{name}\"");
        let props_type = typescript::props_type(name);
        if typescript::IMPORTED.contains(&props_type.as_str()) {
            problems.push(Problem::new(
                &at,
                format!(
                    "`{name}` gives the TypeScript props type `{props_type}`, which the generated file imports from trail-client"
                ),
                format!("rename the event, like `{name}_event`"),
            ));
        }

        let params: [(&str, Convert); 2] = [("Python", python::event_param), ("Swift", swift::event_param)];
        let props: Vec<&str> = event.props.keys().map(String::as_str).collect();
        for clash in clashes(&props, &params) {
            problems.push(Problem::new(
                format!("{at}.props.{}", clash.second),
                format!(
                    "properties `{}` and `{}` both become the parameter {}",
                    clash.first, clash.second, clash.becomes
                ),
                format!(
                    "rename one of them so they differ in more than underscores, like `{}`",
                    suggest(clash.second.trim_end_matches('_'), &props, &params)
                ),
            ));
        }

        for (prop, kind) in &event.props {
            let Base::Enum(values) = &kind.base else {
                continue;
            };
            let at = format!("{at}.props.{prop}");
            let type_name = swift::enum_type(name, prop);
            if let Some((other_event, other_prop)) = enum_types.get(&type_name) {
                problems.push(Problem::new(
                    &at,
                    format!(
                        "this enum and `{other_prop}` of `{other_event}` both become the Swift enum `{type_name}`"
                    ),
                    format!(
                        "rename the property, like `{}`",
                        suggest(prop, &[], &[])
                    ),
                ));
            } else {
                enum_types.insert(type_name, (name.as_str(), prop.as_str()));
            }

            let mut cases: BTreeMap<String, &str> = BTreeMap::new();
            for value in values {
                match swift::case_name(value) {
                    None => problems.push(Problem::new(
                        &at,
                        format!(
                            "the value `{value}` has no ASCII letter or digit, so it cannot become a Swift enum case"
                        ),
                        "use a value with at least one ASCII letter or digit, like `other`",
                    )),
                    Some(case) => match cases.get(&case) {
                        Some(first) => problems.push(Problem::new(
                            &at,
                            format!(
                                "the values `{first}` and `{value}` both become the Swift case `{case}`"
                            ),
                            format!(
                                "keep one of them, or change `{value}` so the two differ in letters or digits, not only punctuation or case"
                            ),
                        )),
                        None => {
                            cases.insert(case, value.as_str());
                        }
                    },
                }
            }
        }
    }

    for (name, metric) in &decl.metrics {
        let params: [(&str, Convert); 2] = [("Python", python::metric_param), ("Swift", swift::metric_param)];
        let labels: Vec<&str> = metric.labels.iter().map(String::as_str).collect();
        for clash in clashes(&labels, &params) {
            problems.push(Problem::new(
                format!("metrics.toml: metrics.\"{name}\".labels"),
                format!(
                    "labels `{}` and `{}` both become the parameter {}",
                    clash.first, clash.second, clash.becomes
                ),
                format!(
                    "rename one of them so they differ in more than underscores, like `{}`",
                    suggest(clash.second.trim_end_matches('_'), &labels, &params)
                ),
            ));
        }
    }
    problems
}

/// How a language turns a declared name into an identifier.
type Convert = fn(&str) -> String;

/// Two declared names that become the same identifier in one or more languages.
struct Clash<'a> {
    first: &'a str,
    second: &'a str,
    /// What they become, like "`shop_order_placed` in Python and `shopOrderPlaced` in Swift".
    becomes: String,
}

fn clashes<'a>(names: &[&'a str], conversions: &[(&str, Convert)]) -> Vec<Clash<'a>> {
    let mut found: BTreeMap<(&str, &str), Vec<(&str, String)>> = BTreeMap::new();
    for (language, convert) in conversions {
        let mut seen: BTreeMap<String, &str> = BTreeMap::new();
        for name in names {
            let converted = convert(name);
            match seen.get(&converted) {
                Some(first) => found
                    .entry((first, name))
                    .or_default()
                    .push((language, converted)),
                None => {
                    seen.insert(converted, name);
                }
            }
        }
    }
    found
        .into_iter()
        .map(|((first, second), hits)| Clash {
            first,
            second,
            becomes: describe(&hits),
        })
        .collect()
}

/// "`a` in Python and `b` in TypeScript and Swift".
fn describe(hits: &[(&str, String)]) -> String {
    let mut groups: Vec<(&str, Vec<&str>)> = Vec::new();
    for (language, converted) in hits {
        match groups.iter_mut().find(|(name, _)| name == converted) {
            Some((_, languages)) => languages.push(language),
            None => groups.push((converted, vec![language])),
        }
    }
    groups
        .iter()
        .map(|(name, languages)| format!("`{name}` in {}", languages.join(" and ")))
        .collect::<Vec<_>>()
        .join(" and ")
}

/// `name` with `_2` (or 3, …) appended, picking the first that clashes with none of `names`
/// in any of `conversions`.
fn suggest(name: &str, names: &[&str], conversions: &[(&str, Convert)]) -> String {
    let mut n = 2;
    loop {
        let candidate = format!("{name}_{n}");
        let taken = conversions.iter().any(|(_, convert)| {
            let converted = convert(&candidate);
            names.iter().any(|other| convert(other) == converted)
        });
        if !taken || n >= 99 {
            return candidate;
        }
        n += 1;
    }
}
