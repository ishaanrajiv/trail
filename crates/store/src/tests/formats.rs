//! Day files of every format Trail has written stay readable.

use super::*;
use crate::parquet_io::{FileRows, SampleRow, file_format};
use parquet::data_type::{ByteArray, ByteArrayType, DoubleType, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::path::Path;
use std::sync::Arc;
use trail_core::decl::Retention;

/// One column's values, for [`write_raw`].
pub(crate) enum Col<'a> {
    Str(Vec<&'a str>),
    OptStr(Vec<Option<&'a str>>),
    I64(Vec<i64>),
    OptI64(Vec<Option<i64>>),
    F64(Vec<f64>),
}

/// Writes a Parquet file with `schema` and these columns, in order, as one row group, with no
/// key-value metadata: what Trail version 1 wrote.
pub(crate) fn write_raw(path: &Path, schema: &str, columns: Vec<Col<'_>>) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let schema = Arc::new(parse_message_type(schema).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let file = std::fs::File::create(path).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    let mut rg = writer.next_row_group().unwrap();
    for col in columns {
        let mut c = rg.next_column().unwrap().unwrap();
        match col {
            Col::Str(v) => {
                let vals: Vec<ByteArray> = v.into_iter().map(ByteArray::from).collect();
                c.typed::<ByteArrayType>().write_batch(&vals, None, None).unwrap();
            }
            Col::OptStr(v) => {
                let defs: Vec<i16> = v.iter().map(|x| i16::from(x.is_some())).collect();
                let vals: Vec<ByteArray> = v.into_iter().flatten().map(ByteArray::from).collect();
                c.typed::<ByteArrayType>()
                    .write_batch(&vals, Some(&defs), None)
                    .unwrap();
            }
            Col::I64(v) => {
                c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
            }
            Col::OptI64(v) => {
                let defs: Vec<i16> = v.iter().map(|x| i16::from(x.is_some())).collect();
                let vals: Vec<i64> = v.into_iter().flatten().collect();
                c.typed::<Int64Type>()
                    .write_batch(&vals, Some(&defs), None)
                    .unwrap();
            }
            Col::F64(v) => {
                c.typed::<DoubleType>().write_batch(&v, None, None).unwrap();
            }
        }
        c.close().unwrap();
    }
    rg.close().unwrap();
    writer.close().unwrap();
}

/// Trail version 1's events layout.
pub(crate) const V1_EVENTS: &str = "message event {
    required binary id (STRING);
    required binary name (STRING);
    required int64 ts (TIMESTAMP(MILLIS,true));
    required int64 received_at (TIMESTAMP(MILLIS,true));
    required binary source (STRING);
    optional binary version (STRING);
    optional binary host (STRING);
    optional binary actor (STRING);
    optional binary client (STRING);
    optional binary trace_id (STRING);
    required binary props (JSON);
    optional binary problems (JSON);
}";

/// A format-1 events file for 2026-11-10 (UTC) with two events, sorted as version 1 sorted them.
pub(crate) fn v1_events_file(path: &Path) {
    write_raw(
        path,
        V1_EVENTS,
        vec![
            Col::Str(vec!["old-1", "old-2"]),
            Col::Str(vec!["shop.order.placed", "shop.order.placed"]),
            Col::I64(vec![T0 + H, T0 + 2 * H]),
            Col::I64(vec![T0 + H, T0 + 2 * H]),
            Col::Str(vec!["shop", "shop"]),
            Col::OptStr(vec![Some("abc123"), None]),
            Col::OptStr(vec![None, None]),
            Col::OptStr(vec![Some("user"), None]),
            Col::OptStr(vec![None, None]),
            Col::OptStr(vec![None, None]),
            Col::Str(vec![r#"{"total_cents":1200}"#, "{}"]),
            Col::OptStr(vec![None, Some(r#"["unknown property `x`"]"#)]),
        ],
    );
}

/// Puts a file Trail did not write itself into `archive_days` and moves the evicted mark past
/// its day, as if version 1 had archived and evicted it.
pub(crate) fn adopt(store: &Store, store_name: &str, class: &str, day: &str, rows: i64, evicted_before: i64) {
    let mut w = store.writer().unwrap();
    w.conn
        .execute(
            "INSERT INTO archive_days (store, class, day, rows, bytes) VALUES (?1, ?2, ?3, ?4, 1)",
            (store_name, class, day, rows),
        )
        .unwrap();
    w.conn
        .execute(
            "INSERT INTO marks (key, value) VALUES (?1, ?2)",
            (format!("evicted_before:{store_name}"), evicted_before),
        )
        .unwrap();
    w.reload().unwrap();
}

#[test]
fn a_format_1_events_file_reads_and_comes_out_format_2_when_merged() {
    let (_dir, store) = store("format-1");
    let path = store.day_path("events", "2026-11-10".parse().unwrap());
    v1_events_file(&path);
    assert_eq!(file_format(&path).unwrap(), 1);
    adopt(&store, "events", "events", "2026-11-10", 2, T0 + D);

    let events = all_events(&store);
    assert_eq!(ids(&events), ["old-2", "old-1"]);
    assert_eq!(events[1].version.as_deref(), Some("abc123"));
    assert_eq!(events[1].actor.as_deref(), Some("user"));
    assert_eq!(events[1].props["total_cents"], 1200);
    assert_eq!(events[0].problems, ["unknown property `x`"]);

    // A late event for that day: the file is rewritten with it, in this Trail's format.
    write_events(&store, vec![event("late-1", "shop.order.placed", T0 + 3 * H)]);
    let report = store.archive(Ts(T0 + 5 * D), &|_| Retention::Forever).unwrap();
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert_eq!(file_format(&path).unwrap(), 2);
    let mut rows = FileRows::<Event>::open(&path, None).unwrap();
    let mut kept = Vec::new();
    while let Some(e) = rows.next_row().unwrap() {
        kept.push(e.id);
    }
    assert_eq!(kept, ["old-1", "old-2", "late-1"]);
    assert_eq!(ids(&all_events(&store)), ["late-1", "old-2", "old-1"]);
}

#[test]
fn columns_are_read_by_name() {
    let dir = TempDir::new("by-name");
    let path = dir.0.join("samples.parquet");
    // Out of order, with a column Trail doesn't know: it is ignored.
    write_raw(
        &path,
        "message sample {
            required binary labels (JSON);
            required double value;
            required binary extra (STRING);
            required int64 ts (TIMESTAMP(MILLIS,true));
            required binary name (STRING);
            required int64 series;
        }",
        vec![
            Col::Str(vec!["{}"]),
            Col::F64(vec![2.5]),
            Col::Str(vec!["ignored"]),
            Col::I64(vec![T0]),
            Col::Str(vec!["shop.queue.depth"]),
            Col::I64(vec![7]),
        ],
    );
    let mut rows = FileRows::<SampleRow>::open(&path, None).unwrap();
    let row = rows.next_row().unwrap().unwrap();
    assert_eq!((row.series, row.ts, row.value), (7, T0, 2.5));
    assert_eq!(&*row.name, "shop.queue.depth");

    // A required column a file lacks is a file Trail did not write, and says so.
    let broken = dir.0.join("broken.parquet");
    write_raw(
        &broken,
        "message sample { required int64 series; required int64 ts; }",
        vec![Col::I64(vec![7]), Col::I64(vec![T0])],
    );
    let mut rows = FileRows::<SampleRow>::open(&broken, None).unwrap();
    let err = rows.next_row().err().unwrap();
    assert!(err.message.contains("`value` is missing"), "{err}");
    assert!(!err.fix.is_empty());
}

fn ids(events: &[Event]) -> Vec<String> {
    events.iter().map(|e| e.id.clone()).collect()
}
