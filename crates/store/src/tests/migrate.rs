//! Upgrading a version 1 database, and what is kept to go back.

use super::*;
use rusqlite::Connection;
use std::path::Path;

/// Makes `<data>/trail.db` the way Trail version 1 left it: `schema.sql` at user_version 1,
/// with a few rows in every store, and one day file under `archive/`.
pub(crate) fn v1_folder(config: &Config) {
    let data = &config.data_dir;
    std::fs::create_dir_all(data.join("archive/events")).unwrap();
    let conn = Connection::open(data.join("trail.db")).unwrap();
    conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL").unwrap();
    let _: String = conn
        .query_row("PRAGMA journal_mode = WAL", [], |r| r.get(0))
        .unwrap();
    conn.execute_batch(include_str!("../schema.sql")).unwrap();
    conn.execute_batch("PRAGMA user_version = 1").unwrap();
    conn.execute(
        "INSERT INTO state (key, value) VALUES ('timezone', ?1)",
        [config.zone.name()],
    )
    .unwrap();
    for (i, id) in ["e-1", "e-2", "e-3"].iter().enumerate() {
        let ts = T0 + 10 * D + i as i64 * M;
        conn.execute(
            "INSERT INTO events (id, name, ts, received_at, source, props) VALUES (?1, 'shop.order.placed', ?2, ?3, 'shop', '{}')",
            (id, ts, ts + 5),
        )
        .unwrap();
    }
    conn.execute(
        "INSERT INTO errors (id, ts, received_at, source, severity, kind, message, fingerprint, context)
         VALUES ('x-1', ?1, ?1, 'shop', 'error', 'KeyError', 'missing', 'f1', '{}')",
        [T0 + 10 * D],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO logs (id, ts, received_at, source, level, message, fields)
         VALUES ('l-1', ?1, ?1, 'shop', 'info', 'started', '{}')",
        [T0 + 10 * D],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO series (id, name, labels, source, kind, first_ts, last_ts, last_raw, last_raw_ts)
         VALUES (7, 'shop.bytes', '{}', 'shop', 'counter', ?1, ?2, 300.0, ?2)",
        (T0 + 10 * D, T0 + 10 * D + M),
    )
    .unwrap();
    conn.execute(
        "INSERT INTO samples (series, ts, value) VALUES (7, ?1, 2.5)",
        [T0 + 10 * D + M],
    )
    .unwrap();
    drop(conn);
    std::fs::write(data.join("archive/events/2026-11-01.parquet"), b"kept as it was").unwrap();
}

fn version_of(path: &Path) -> i64 {
    let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    conn.query_row("PRAGMA user_version", [], |r| r.get(0)).unwrap()
}

fn rows_in(path: &Path, table: &str) -> i64 {
    let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}

#[test]
fn a_new_database_is_the_current_version() {
    let (dir, store) = store("migrate-new");
    drop(store);
    let data = dir.0.join("data");
    assert_eq!(version_of(&data.join("trail.db")), crate::migrate::SCHEMA_VERSION);
    assert!(
        !data.join("previous").exists(),
        "nothing to keep for a new database"
    );
}

#[test]
fn a_v1_database_is_kept_then_upgraded() {
    let dir = TempDir::new("migrate-v1");
    let config = dir.config();
    v1_folder(&config);
    let data = config.data_dir.clone();
    let store = Store::open(&config).unwrap();
    drop(store);

    assert_eq!(version_of(&data.join("trail.db")), 2);
    let kept = data.join("previous/v1");
    assert_eq!(version_of(&kept.join("trail.db")), 1);
    for table in ["events", "errors", "logs", "series", "samples"] {
        assert_eq!(
            rows_in(&kept.join("trail.db"), table),
            rows_in(&data.join("trail.db"), table),
            "{table}"
        );
    }
    assert_eq!(rows_in(&kept.join("trail.db"), "events"), 3);
    let linked = kept.join("archive/events/2026-11-01.parquet");
    assert_eq!(std::fs::read(&linked).unwrap(), b"kept as it was");
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let a = std::fs::metadata(&linked).unwrap();
        let b = std::fs::metadata(data.join("archive/events/2026-11-01.parquet")).unwrap();
        assert_eq!(a.ino(), b.ino(), "a hard link, not a copy");
    }
    assert!(!data.join("previous/v1.partial").exists());

    // Opening again changes nothing under previous/.
    let store = Store::open(&config).unwrap();
    drop(store);
    assert!(!data.join("previous/v1-1").exists());
    assert_eq!(version_of(&kept.join("trail.db")), 1);
}

#[test]
fn no_copy_no_upgrade() {
    let dir = TempDir::new("migrate-refuse");
    let config = dir.config();
    v1_folder(&config);
    let data = config.data_dir.clone();
    // `previous` can't be a folder: the copy can't be made.
    std::fs::write(data.join("previous"), b"in the way").unwrap();
    let err = Store::open(&config).err().unwrap();
    assert!(err.message.contains("has changed nothing"), "{err}");
    assert!(err.fix.contains("previous"), "{err}");
    assert_eq!(version_of(&data.join("trail.db")), 1);
    assert_eq!(rows_in(&data.join("trail.db"), "events"), 3);

    // Once it is out of the way, the upgrade goes ahead.
    std::fs::remove_file(data.join("previous")).unwrap();
    drop(Store::open(&config).unwrap());
    assert_eq!(version_of(&data.join("trail.db")), 2);
}

#[test]
fn an_unfinished_copy_is_redone_and_an_older_one_moved_aside() {
    let dir = TempDir::new("migrate-again");
    let config = dir.config();
    v1_folder(&config);
    let data = config.data_dir.clone();
    // A run that died while copying, and a copy from an upgrade that was gone back from.
    std::fs::create_dir_all(data.join("previous/v1.partial")).unwrap();
    std::fs::write(data.join("previous/v1.partial/trail.db"), b"half").unwrap();
    std::fs::create_dir_all(data.join("previous/v1")).unwrap();
    std::fs::write(data.join("previous/v1/note"), b"older").unwrap();
    drop(Store::open(&config).unwrap());
    assert_eq!(std::fs::read(data.join("previous/v1-1/note")).unwrap(), b"older");
    assert_eq!(version_of(&data.join("previous/v1/trail.db")), 1);
    assert!(!data.join("previous/v1.partial").exists());
}

#[test]
fn a_v1_folder_under_another_zone_is_refused_before_the_copy() {
    let dir = TempDir::new("migrate-zone");
    let mut config = dir.config();
    v1_folder(&config);
    config.zone = trail_core::time::Zone::new("America/New_York").unwrap();
    let err = Store::open(&config).err().unwrap();
    assert!(err.at.contains("timezone"), "{err}");
    assert!(!config.data_dir.join("previous").exists());
    assert_eq!(version_of(&config.data_dir.join("trail.db")), 1);
}

#[test]
fn an_upgraded_database_keys_records_by_source_and_id() {
    let dir = TempDir::new("migrate-identity");
    let config = dir.config();
    v1_folder(&config);
    let store = Store::open(&config).unwrap();
    let mut other = event("e-1", "shop.order.placed", T0 + 10 * D);
    other.source = "till".to_string();
    let same = event("e-1", "shop.order.placed", T0 + 10 * D);
    assert_eq!(write_events(&store, vec![other, same]).events, [true, false]);
    let ids: Vec<(String, String)> = all_events(&store).into_iter().map(|e| (e.source, e.id)).collect();
    assert_eq!(ids.len(), 4);
    assert!(ids.contains(&("till".to_string(), "e-1".to_string())));
}

#[test]
fn an_upgraded_database_keeps_its_series_ids_and_counter_baselines() {
    let dir = TempDir::new("migrate-series");
    let config = dir.config();
    v1_folder(&config);
    let store = Store::open(&config).unwrap();
    let series = store.series(&SeriesFilter::default()).unwrap();
    assert_eq!(series.len(), 1);
    assert_eq!((series[0].id, series[0].source.as_str()), (7, "shop"));
    // The next reading continues from the baseline version 1 kept (300 at T0 + 10d + 1m).
    let mut p = point("shop.bytes", &[], 900.0, T0 + 10 * D + 2 * M, true);
    p.source = "shop".to_string();
    assert_eq!(write_points(&store, vec![p]).points, [PointOutcome::Stored]);
    let latest = store
        .latest("shop.bytes", &BTreeMap::new(), Some("shop"))
        .unwrap();
    assert_eq!((latest[0].series.id, latest[0].value), (7, 10.0));
    // Another source's reading under the same name is a series of its own.
    let mut other = point("shop.bytes", &[], 1.0, T0 + 10 * D + 2 * M, false);
    other.source = "till".to_string();
    write_points(&store, vec![other]);
    assert_eq!(store.series(&SeriesFilter::default()).unwrap().len(), 2);
}
