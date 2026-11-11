//! The store's error: what failed, and how to fix it when the caller can.

use rusqlite::ErrorCode;
use std::fmt;
use std::path::Path;
use trail_core::Problem;

/// What failed (`at`, `message`) and what to change (`fix`), like a [`Problem`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub at: String,
    pub message: String,
    pub fix: String,
}

/// The store's result type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// An error with its place, its cause and its fix.
    pub fn new(at: impl Into<String>, message: impl Into<String>, fix: impl Into<String>) -> Error {
        Error {
            at: at.into(),
            message: message.into(),
            fix: fix.into(),
        }
    }

    /// The same error as a [`Problem`], for an API response.
    pub fn problem(&self) -> Problem {
        Problem::new(&self.at, &self.message, &self.fix)
    }

    /// A SQLite failure while doing `doing`.
    pub(crate) fn sql(doing: &str, e: &rusqlite::Error) -> Error {
        let fix = match e.sqlite_error_code() {
            Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) => {
                "another program holds a lock on trail.db; stop it (only one Trail may use a data_dir), or retry"
            }
            Some(ErrorCode::DiskFull) => {
                "free space on the disk that holds data_dir, or lower the caps in trail.toml"
            }
            Some(ErrorCode::ReadOnly | ErrorCode::CannotOpen | ErrorCode::PermissionDenied) => {
                "make data_dir and trail.db writable by the user Trail runs as"
            }
            Some(ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase) => {
                "trail.db is damaged: stop Trail, move trail.db aside and restore it from a backup; archive/ is unaffected"
            }
            _ => "if this repeats, report it as a bug in trail-store with this message",
        };
        Error::new("trail.db", format!("{doing}: {e}"), fix)
    }

    /// A file system failure on `path` while doing `doing`.
    pub(crate) fn io(path: &Path, doing: &str, e: &std::io::Error) -> Error {
        let fix = match e.kind() {
            std::io::ErrorKind::PermissionDenied => "make data_dir writable by the user Trail runs as",
            std::io::ErrorKind::StorageFull => "free space on the disk, or lower the caps in trail.toml",
            std::io::ErrorKind::NotFound => {
                "check that data_dir in trail.toml points at an existing, mounted folder"
            }
            _ => "check the disk that holds data_dir",
        };
        Error::new(path.display().to_string(), format!("{doing}: {e}"), fix)
    }

    /// A Parquet failure on `path` while doing `doing`.
    pub(crate) fn parquet(path: &Path, doing: &str, e: &parquet::errors::ParquetError) -> Error {
        Error::new(
            path.display().to_string(),
            format!("{doing}: {e}"),
            "if the file is damaged, move it out of archive/ (its rows are lost unless restored from a backup)",
        )
    }

    /// A day file whose contents are not what Trail writes.
    pub(crate) fn bad_file(path: &Path, message: impl Into<String>) -> Error {
        Error::new(
            path.display().to_string(),
            message,
            "this file was not written by this version of Trail; move it out of archive/",
        )
    }

    /// A caller passed a value the store cannot use.
    pub(crate) fn input(at: &str, message: impl Into<String>, fix: impl Into<String>) -> Error {
        Error::new(at, message, fix)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.at, self.message)?;
        if !self.fix.is_empty() {
            write!(f, "\n  fix: {}", self.fix)?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Error {
        Error::sql("query failed", &e)
    }
}

impl From<Error> for Problem {
    fn from(e: Error) -> Problem {
        Problem::new(e.at, e.message, e.fix)
    }
}
