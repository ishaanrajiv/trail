//! `trail check`, `trail gen` and `trail publish`: a service's declarations, from TOML to the
//! server and to generated code.

use crate::args::{ADMIN, Args};
use crate::send::{Http, problems_in};
use crate::{Done, out};
use std::path::Path;
use trail_codegen::Language;
use trail_core::Problem;
use trail_core::decl::{self, Declarations};
use trail_core::registry::Registry;

/// Loads a folder and runs every check, including the ones that only matter to generated code.
fn load(folder: &str) -> Result<Declarations, Vec<Problem>> {
    let decl = decl::load_dir(Path::new(folder)).map_err(|problems| in_folder(folder, problems))?;
    let problems = trail_codegen::check(&decl);
    if problems.is_empty() {
        Ok(decl)
    } else {
        Err(in_folder(folder, problems))
    }
}

/// Puts the folder in front of each problem's place, so two folders' problems can be told apart.
fn in_folder(folder: &str, problems: Vec<Problem>) -> Vec<Problem> {
    problems
        .into_iter()
        .map(|p| {
            let at = if p.at.starts_with(folder) {
                p.at
            } else {
                format!("{}/{}", folder.trim_end_matches('/'), p.at)
            };
            Problem::new(at, p.message, p.fix)
        })
        .collect()
}

fn summary(decl: &Declarations) -> String {
    format!(
        "{}: {} events, {} metrics, {} rollups, {} alerts",
        decl.source,
        decl.events.len(),
        decl.metrics.len(),
        decl.rollups.len(),
        decl.alerts.len()
    )
}

/// `trail check <folder>...`
pub fn check(raw: Vec<String>) -> Done {
    let args = Args::parse("check", raw, &[], &[])?;
    args.need("check", &["folder"])?;
    let mut problems = Vec::new();
    let mut registry = Registry::new();
    for folder in &args.positional {
        match load(folder) {
            Err(found) => problems.extend(found),
            Ok(decl) => {
                // Several folders are checked together: a name may belong to only one of them.
                let conflicts = registry.conflicts(&decl);
                if conflicts.is_empty() {
                    out(&format!("ok  {}", summary(&decl)));
                    registry.set(decl);
                } else {
                    problems.extend(in_folder(folder, conflicts));
                }
            }
        }
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems)
    }
}

/// `trail gen <folder> --lang L [--out FILE] [--check]`
pub fn generate(raw: Vec<String>) -> Done {
    let args = Args::parse("gen", raw, &["lang", "out"], &["check"])?;
    args.need("gen", &["folder"])?;
    let folder = &args.positional[0];
    let names = Language::ALL.map(Language::name).join(", ");
    let language = args.value("lang").and_then(Language::parse).ok_or_else(|| {
        vec![Problem::new(
            "trail gen",
            match args.value("lang") {
                Some(given) => format!("`{given}` is not a language Trail generates"),
                None => "which language?".to_string(),
            },
            format!("add `--lang <language>`: {names}"),
        )]
    })?;
    let decl = load(folder)?;
    let text = trail_codegen::generate(&decl, language);
    let Some(file) = args.value("out") else {
        if args.flag("check") {
            return Err(vec![Problem::new(
                "trail gen",
                "`--check` needs the file to compare with",
                "add `--out <file>`",
            )]);
        }
        out(text.trim_end_matches('\n'));
        return Ok(());
    };
    let current = std::fs::read_to_string(file).ok();
    if current.as_deref() == Some(text.as_str()) {
        return Ok(());
    }
    if args.flag("check") {
        return Err(vec![Problem::new(
            file,
            if current.is_some() {
                "this generated file is out of date with the declarations"
            } else {
                "this generated file does not exist"
            },
            format!(
                "run `trail gen {folder} --lang {} --out {file}` and commit the result",
                language.name()
            ),
        )]);
    }
    if let Some(parent) = Path::new(file).parent().filter(|p| !p.as_os_str().is_empty()) {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::write(file, text).map_err(|e| {
        vec![Problem::new(
            file,
            format!("can't write: {e}"),
            "check the folder exists and is writable",
        )]
    })?;
    out(&format!("wrote {file}"));
    Ok(())
}

/// `trail publish <folder>...`
pub fn publish(raw: Vec<String>) -> Done {
    let args = Args::parse("publish", raw, &ADMIN, &[])?;
    args.need("publish", &["folder"])?;
    let http = Http::admin(&args);
    // Everything is checked before anything is sent.
    let mut loaded = Vec::new();
    let mut problems = Vec::new();
    for folder in &args.positional {
        match load(folder) {
            Ok(decl) => loaded.push((folder, decl)),
            Err(found) => problems.extend(found),
        }
    }
    if !problems.is_empty() {
        return Err(problems);
    }
    for (folder, decl) in loaded {
        let path = format!("/v1/registry/{}", decl.source);
        let (status, body) = http.request("PUT", &path, Some(&decl.to_json().to_string()))?;
        if status == 200 {
            out(&format!("published  {}", summary(&decl)));
        } else {
            problems.extend(in_folder(folder, problems_in(&body, status)));
        }
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems)
    }
}
