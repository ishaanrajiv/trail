//! The store's tests. Each test gets its own folder under the system temp dir, removed after.

use crate::*;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use trail_core::Ts;
use trail_core::config::Config;
use trail_core::envelope::{ErrorRecord, Event, Level, LogRecord, Object, Point, Severity};
use trail_core::time::{DAY, HOUR, MINUTE};

mod archive;
mod formats;
mod metrics;
mod migrate;
mod records;
mod scale;

/// A temp folder removed when dropped.
pub(crate) struct TempDir(pub PathBuf);

impl TempDir {
    pub(crate) fn new(name: &str) -> TempDir {
        static N: AtomicU32 = AtomicU32::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("trail-store-{name}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }

    pub(crate) fn config(&self) -> Config {
        Config::default_in(&self.0.join("data"))
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A store in a fresh temp folder, with `edit` applied to the default config.
pub(crate) fn store_with(name: &str, edit: impl FnOnce(&mut Config)) -> (TempDir, Store) {
    let dir = TempDir::new(name);
    let mut config = dir.config();
    edit(&mut config);
    let store = Store::open(&config).unwrap();
    (dir, store)
}

pub(crate) fn store(name: &str) -> (TempDir, Store) {
    store_with(name, |_| {})
}

/// 2026-11-10 00:00 UTC.
pub(crate) const T0: i64 = 1_794_268_800_000;
pub(crate) const M: i64 = MINUTE;
pub(crate) const H: i64 = HOUR;
pub(crate) const D: i64 = DAY;

pub(crate) fn event(id: &str, name: &str, ts: i64) -> Event {
    Event {
        id: id.to_string(),
        name: name.to_string(),
        ts: Ts(ts),
        received_at: Ts(ts),
        source: "shop".to_string(),
        version: None,
        host: None,
        actor: None,
        client: None,
        trace_id: None,
        props: Object::new(),
        problems: Vec::new(),
    }
}

pub(crate) fn error(id: &str, fingerprint: &str, message: &str, ts: i64) -> ErrorRecord {
    ErrorRecord {
        id: id.to_string(),
        ts: Ts(ts),
        received_at: Ts(ts),
        source: "shop".to_string(),
        version: None,
        host: None,
        severity: Severity::Error,
        kind: "KeyError".to_string(),
        message: message.to_string(),
        fingerprint: fingerprint.to_string(),
        stack: None,
        context: Object::new(),
        trace_id: None,
    }
}

pub(crate) fn log(id: &str, level: Level, message: &str, ts: i64) -> LogRecord {
    LogRecord {
        id: id.to_string(),
        ts: Ts(ts),
        received_at: Ts(ts),
        source: "shop".to_string(),
        host: None,
        stream: Some("worker".to_string()),
        level,
        message: message.to_string(),
        fields: Object::new(),
    }
}

pub(crate) fn point(name: &str, labels: &[(&str, &str)], value: f64, ts: i64, counter: bool) -> MetricPoint {
    MetricPoint {
        source: "shop".to_string(),
        point: Point {
            name: name.to_string(),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<_, _>>(),
            value,
            ts: Ts(ts),
            unit: None,
            kind: None,
        },
        counter,
    }
}

pub(crate) fn write_events(store: &Store, events: Vec<Event>) -> Written {
    store
        .write(&Batch {
            events,
            ..Batch::default()
        })
        .unwrap()
}

pub(crate) fn write_points(store: &Store, points: Vec<MetricPoint>) -> Written {
    store
        .write(&Batch {
            points,
            ..Batch::default()
        })
        .unwrap()
}

/// Every event, newest first.
pub(crate) fn all_events(store: &Store) -> Vec<Event> {
    store
        .events(&EventFilter {
            limit: 10_000,
            ..EventFilter::default()
        })
        .unwrap()
}

/// Runs one SQL count on the writer connection.
pub(crate) fn count(store: &Store, text: &str) -> i64 {
    store
        .writer()
        .unwrap()
        .conn
        .query_row(text, [], |r| r.get(0))
        .unwrap()
}

#[test]
fn every_statement_prepares() {
    let (_dir, store) = store("prepare");
    let w = store.writer().unwrap();
    for (name, text) in sql::ALL {
        if let Err(e) = w.conn.prepare(text) {
            panic!("{name} does not prepare: {e}\n{text}");
        }
    }
}

#[test]
fn reopening_keeps_the_schema_and_refuses_a_newer_one() {
    let dir = TempDir::new("reopen");
    let config = dir.config();
    {
        let store = Store::open(&config).unwrap();
        store.set_state("k", "v").unwrap();
    }
    let store = Store::open(&config).unwrap();
    assert_eq!(store.state("k").unwrap().as_deref(), Some("v"));
    assert_eq!(count(&store, "PRAGMA auto_vacuum"), 2, "incremental auto-vacuum");
    let mode: String = store
        .writer()
        .unwrap()
        .conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
    store
        .writer()
        .unwrap()
        .conn
        .execute_batch("PRAGMA user_version = 99")
        .unwrap();
    drop(store);
    let err = Store::open(&config).err().unwrap();
    assert!(err.to_string().contains("schema version 99"), "{err}");
    assert!(err.to_string().contains("fix:"), "{err}");
}

#[test]
fn an_unwritable_data_dir_says_how_to_fix_it() {
    let dir = TempDir::new("unwritable");
    let file = dir.0.join("not-a-folder");
    std::fs::write(&file, "x").unwrap();
    let config = Config::default_in(&file.join("data"));
    let err = Store::open(&config).err().unwrap();
    assert!(err.message.contains("can't create data_dir"), "{err}");
    assert!(!err.fix.is_empty());
}

#[test]
fn the_store_is_send_and_sync() {
    fn check<T: Send + Sync>() {}
    check::<Store>();
}

#[test]
fn state_and_registry() {
    let (_dir, store) = store("state");
    assert_eq!(store.state("alerts").unwrap(), None);
    store.set_state("alerts", "{\"firing\":[]}").unwrap();
    store.set_state("alerts", "{\"firing\":[1]}").unwrap();
    assert_eq!(
        store.state("alerts").unwrap().as_deref(),
        Some("{\"firing\":[1]}")
    );

    let decl = trail_core::decl::Declarations::from_json(serde_json::json!({
        "source": "shop",
        "events": {"shop.order.placed": {"description": "A customer placed an order", "retention": "30d",
                                          "props": {"total_cents": "int"}}},
    }))
    .unwrap();
    store.put_declarations(&decl).unwrap();
    let mut other = decl.clone();
    other.source = "till".to_string();
    other.events.clear();
    store.put_declarations(&other).unwrap();
    store.put_declarations(&decl).unwrap();
    let (loaded, bad) = store.registry().unwrap();
    assert_eq!(loaded, vec![decl.clone(), other.clone()]);
    assert!(bad.is_empty());
    assert!(store.delete_declarations("till").unwrap());
    assert!(!store.delete_declarations("till").unwrap());
    assert_eq!(store.registry().unwrap().0, vec![decl.clone()]);

    // A stored row that no longer parses names the source and how to fix it, and the other
    // sources still load.
    store.put_declarations(&other).unwrap();
    store
        .writer()
        .unwrap()
        .conn
        .execute(
            "UPDATE registry SET declarations = 'nope' WHERE source = 'shop'",
            [],
        )
        .unwrap();
    let (good, bad) = store.registry().unwrap();
    assert_eq!(good, vec![other]);
    assert_eq!(bad.len(), 1);
    assert!(
        bad[0].at.contains("shop") && bad[0].fix.contains("trail publish"),
        "{}",
        bad[0]
    );
}
