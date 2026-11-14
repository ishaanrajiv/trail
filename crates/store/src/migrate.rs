//! Bringing `trail.db` to the schema version this crate writes. `spec/storage.md` ("Going back a
//! version") is the contract.
//!
//! Before a step changes anything, what it is about to change is kept under
//! `<data_dir>/previous/v<old>/`: a consistent copy of the database and a hard link to every day
//! file. The copy is built under a `.partial` name and renamed into place once complete; then
//! every step runs in one transaction. A kill at any line leaves either the untouched old database
//! or a complete new one. Trail never reads or deletes `previous/` afterwards.

use crate::{Error, Result};
use rusqlite::Connection;
use std::fs::File;
use std::path::{Path, PathBuf};

/// The schema version this crate writes (`PRAGMA user_version`).
pub(crate) const SCHEMA_VERSION: i64 = 2;

/// Each step after version 1: the version it brings the database to, and its SQL. Version 1 is
/// `schema.sql`, which never changes; a new database runs it and then every step.
const STEPS: &[(i64, &str)] = &[(2, include_str!("migrate_v2.sql"))];

/// Sets the connection up and returns the database's schema version, refusing a newer one.
pub(crate) fn prepare(conn: &mut Connection) -> Result<i64> {
    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(|e| Error::sql("can't read the schema version", &e))?;
    if version > SCHEMA_VERSION {
        return Err(Error::new(
            "trail.db",
            format!("the database is schema version {version}; this Trail knows up to {SCHEMA_VERSION}"),
            "run the newer Trail that wrote it, or go back a version as spec/storage.md (\"Going back a version\") describes",
        ));
    }
    if version == 0 {
        // Must be set before the first table exists.
        conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL")
            .map_err(|e| Error::sql("can't set auto_vacuum", &e))?;
    }
    let mode: String = conn
        .query_row("PRAGMA journal_mode = WAL", [], |r| r.get(0))
        .map_err(|e| Error::sql("can't switch to WAL", &e))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(Error::new(
            "trail.db",
            format!("SQLite kept journal mode `{mode}` instead of WAL"),
            "put data_dir on a local disk: WAL needs shared memory, which network file systems lack",
        ));
    }
    conn.execute_batch("PRAGMA synchronous = NORMAL; PRAGMA temp_store = MEMORY;")
        .map_err(|e| Error::sql("can't set pragmas", &e))?;
    Ok(version)
}

/// Brings a database at `version` to [`SCHEMA_VERSION`]. A new database (version 0) gets the
/// whole schema; an existing one is first kept under `previous/v<version>/`.
pub(crate) fn upgrade(conn: &mut Connection, data_dir: &Path, version: i64) -> Result<()> {
    if version == SCHEMA_VERSION {
        return Ok(());
    }
    if version > 0 {
        keep_previous(conn, data_dir, version)?;
    }
    let tx = conn
        .transaction()
        .map_err(|e| Error::sql("can't start the schema change", &e))?;
    if version == 0 {
        tx.execute_batch(include_str!("schema.sql"))
            .map_err(|e| Error::sql("can't create the schema", &e))?;
    }
    for (to, text) in STEPS {
        if *to > version.max(1) {
            tx.execute_batch(text)
                .map_err(|e| Error::sql(&format!("can't bring the schema to version {to}"), &e))?;
        }
    }
    tx.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION}"))
        .map_err(|e| Error::sql("can't set the schema version", &e))?;
    tx.commit()
        .map_err(|e| Error::sql("can't commit the schema change", &e))
}

/// Keeps what a migration from `version` is about to change: `previous/v<version>/trail.db`, a
/// consistent copy of the database, and `previous/v<version>/archive/…`, a hard link to every day
/// file. Fails, having changed nothing, when either can't be made.
fn keep_previous(conn: &Connection, data_dir: &Path, version: i64) -> Result<()> {
    let previous = data_dir.join("previous");
    let target = previous.join(format!("v{version}"));
    let partial = previous.join(format!("v{version}.partial"));
    let refuse = |message: String, fix: String| {
        Error::new(
            partial.display().to_string(),
            format!(
                "{message}; Trail keeps a copy of version {version} before upgrading, and has changed nothing"
            ),
            fix,
        )
    };
    // A copy a run that died left half made: Trail's own scratch, never a kept copy.
    if partial.exists() {
        std::fs::remove_dir_all(&partial).map_err(|e| {
            refuse(
                format!("can't remove an unfinished copy: {e}"),
                format!("remove {} yourself, then start Trail again", partial.display()),
            )
        })?;
    }
    std::fs::create_dir_all(&partial).map_err(|e| {
        refuse(
            format!("can't create the folder for the copy: {e}"),
            format!(
                "make {} a folder Trail can write (it is created if missing), then start Trail again",
                previous.display()
            ),
        )
    })?;
    let copied = copy_database(conn, data_dir, &partial).and_then(|()| link_archive(data_dir, &partial));
    if let Err(e) = copied {
        let _ = std::fs::remove_dir_all(&partial);
        return Err(e);
    }
    // An older copy (an upgrade that died after its copy, or one made before going back a
    // version) is moved aside, never deleted.
    if target.exists() {
        let mut n = 1;
        let aside = loop {
            let candidate = previous.join(format!("v{version}-{n}"));
            if !candidate.exists() {
                break candidate;
            }
            n += 1;
        };
        std::fs::rename(&target, &aside).map_err(|e| {
            refuse(
                format!("can't move the older copy at {} aside: {e}", target.display()),
                format!(
                    "move {} out of the way yourself, then start Trail again",
                    target.display()
                ),
            )
        })?;
    }
    std::fs::rename(&partial, &target).map_err(|e| {
        refuse(
            format!("can't rename the copy into place: {e}"),
            format!(
                "check that {} is writable, then start Trail again",
                previous.display()
            ),
        )
    })?;
    sync_dir(&previous)
}

/// `VACUUM INTO` a consistent copy of the open database at `<dir>/trail.db`, synced to disk.
fn copy_database(conn: &Connection, data_dir: &Path, dir: &Path) -> Result<()> {
    let copy = dir.join("trail.db");
    let size = std::fs::metadata(data_dir.join("trail.db")).map_or(0, |m| m.len())
        + std::fs::metadata(data_dir.join("trail.db-wal")).map_or(0, |m| m.len());
    let text = copy.to_str().ok_or_else(|| {
        Error::new(
            copy.display().to_string(),
            "the path is not UTF-8, which SQLite needs for the copy",
            "move data_dir to a path of plain UTF-8 characters, then start Trail again",
        )
    })?;
    conn.execute("VACUUM INTO ?1", [text]).map_err(|e| {
        Error::new(
            copy.display().to_string(),
            format!("can't copy trail.db before upgrading it: {e}; nothing has been changed"),
            format!(
                "free at least {} MB on the disk that holds data_dir (the copy is about as large as trail.db), then start Trail again",
                size / 1_000_000 + 1
            ),
        )
    })?;
    File::open(&copy)
        .and_then(|f| f.sync_all())
        .map_err(|e| Error::io(&copy, "can't sync the copy of trail.db to disk", &e))?;
    sync_dir(dir)
}

/// Hard-links every day file under `<data_dir>/archive` to the same place under `<dir>/archive`.
/// A link keeps the old bytes at no cost: day files are replaced by rename and removed by unlink.
fn link_archive(data_dir: &Path, dir: &Path) -> Result<()> {
    let archive = data_dir.join("archive");
    if !archive.is_dir() {
        return Ok(());
    }
    let mut pending: Vec<PathBuf> = vec![PathBuf::new()];
    while let Some(rel) = pending.pop() {
        let from = archive.join(&rel);
        let to = dir.join("archive").join(&rel);
        std::fs::create_dir_all(&to)
            .map_err(|e| Error::io(&to, "can't create a folder for the kept day files", &e))?;
        let entries =
            std::fs::read_dir(&from).map_err(|e| Error::io(&from, "can't list the day files", &e))?;
        for entry in entries {
            let entry = entry.map_err(|e| Error::io(&from, "can't list the day files", &e))?;
            let kind = entry
                .file_type()
                .map_err(|e| Error::io(&entry.path(), "can't read a day file's type", &e))?;
            let name = entry.file_name();
            if kind.is_dir() {
                pending.push(rel.join(&name));
            } else if Path::new(&name).extension().is_some_and(|ext| ext == "parquet") {
                let target = to.join(&name);
                std::fs::hard_link(entry.path(), &target).map_err(|e| {
                    Error::new(
                        entry.path().display().to_string(),
                        format!("can't hard-link a day file into {}: {e}; nothing has been changed", target.display()),
                        "keep data_dir, archive/ included, on one file system that allows hard links (ext4, xfs, btrfs and APFS do), then start Trail again",
                    )
                })?;
            }
        }
        sync_dir(&to)?;
    }
    Ok(())
}

fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| Error::io(dir, "can't sync a folder to disk", &e))
}
