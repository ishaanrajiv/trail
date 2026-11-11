//! Typed emitters, generated from a source's declarations.
//!
//! Each generator returns the full text of one file: a function per declared event and metric, in
//! that language's own types. A service imports the generated file and never calls a free-form
//! `emit(name, props)`, so an undeclared event or a wrong property type is a type error in its own
//! build. `spec/client.md` is the contract between the generated code and each client library.

use trail_core::decl::Declarations;

mod python;
mod swift;
mod typescript;

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
