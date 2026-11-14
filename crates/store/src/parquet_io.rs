//! The day files: one Parquet file per store, class and local day, written with parquet's
//! low-level column writers and read back a row group at a time with its record API.

use crate::{Error, Result};
use parquet::basic::{Compression, ZstdLevel};
use parquet::data_type::{ByteArray, ByteArrayType, DoubleType, Int64Type};
use parquet::file::metadata::{KeyValue, RowGroupMetaData};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use parquet::file::writer::{SerializedColumnWriter, SerializedFileWriter, SerializedRowGroupWriter};
use parquet::record::Field;
use parquet::schema::parser::parse_message_type;
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use trail_core::Ts;
use trail_core::envelope::{ErrorRecord, Event, Level, LogRecord, Object, Severity};

/// Rows per row group.
pub(crate) const ROW_GROUP: usize = 65_536;

/// The key-value metadata key a day file's format is written under, and the format this Trail
/// writes. A file without the key is format 1. Every format stays readable: columns are read by
/// name, a column a file lacks reads as null, and a column Trail doesn't know is ignored.
pub(crate) const FORMAT_KEY: &str = "trail.format";
pub(crate) const FORMAT: &str = "2";

/// A row with an identity, (source, id), and a time, for newest-first reads.
pub(crate) trait Kept {
    fn id(&self) -> &str;
    fn source(&self) -> &str;
    fn ts(&self) -> i64;
}

/// A set of record identities, (source, id), each with the seq it was filed under if known.
#[derive(Debug, Default)]
pub(crate) struct Keys(HashMap<String, HashMap<String, Option<i64>>>);

impl Keys {
    pub(crate) fn insert(&mut self, source: String, id: String, seq: Option<i64>) {
        self.0.entry(source).or_default().insert(id, seq);
    }

    pub(crate) fn contains(&self, source: &str, id: &str) -> bool {
        self.0.get(source).is_some_and(|ids| ids.contains_key(id))
    }

    /// The seq a record was filed under: None when it isn't in the set, Some(None) when it is
    /// but has no seq.
    pub(crate) fn seq(&self, source: &str, id: &str) -> Option<Option<i64>> {
        self.0.get(source).and_then(|ids| ids.get(id)).copied()
    }

    pub(crate) fn extend(&mut self, other: Keys) {
        for (source, ids) in other.0 {
            self.0.entry(source).or_default().extend(ids);
        }
    }
}

/// A row type with a day file layout.
pub(crate) trait ArchRow: Sized {
    /// The Parquet schema, in column order.
    const SCHEMA: &'static str;
    /// Writes one row group's columns, in schema order.
    fn write_group(rows: &[Self], g: &mut Group<'_, '_>) -> Result<()>;
    /// Reads one row's fields, by column name.
    fn read(f: &mut Fields) -> Result<Self>;
    /// The file's sort order.
    fn order(&self, other: &Self) -> Ordering;
    /// Its position in arrival order, for a store that keeps one.
    fn seq(&self) -> Option<i64> {
        None
    }
    /// Gives a row without a seq the one SQLite holds for the same record.
    fn fill_seq(&mut self, _fill: &Seqs) {}
}

/// The seq SQLite holds for each record identity (source, id) of a day.
#[derive(Debug, Default)]
pub(crate) struct Seqs(HashMap<String, HashMap<String, i64>>);

impl Seqs {
    pub(crate) fn insert(&mut self, source: String, id: String, seq: i64) {
        self.0.entry(source).or_default().insert(id, seq);
    }

    fn get(&self, source: &str, id: &str) -> Option<i64> {
        self.0.get(source).and_then(|ids| ids.get(id)).copied()
    }
}

/// One raw metric sample as a day file holds it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SampleRow {
    pub series: i64,
    pub ts: i64,
    pub value: f64,
    pub name: Arc<str>,
    pub labels: Arc<str>,
    /// A counter reading's raw total; None for a gauge, or a reading from before Trail kept it.
    pub total: Option<f64>,
    /// A counter reading's increase since the reading before it.
    pub increase: Option<f64>,
}

// ---------- writing ----------

/// The columns of one row group being written.
pub(crate) struct Group<'a, 'b> {
    rg: &'a mut SerializedRowGroupWriter<'b, File>,
    path: &'a Path,
}

impl Group<'_, '_> {
    fn column(
        &mut self,
        write: impl FnOnce(&mut SerializedColumnWriter<'_>) -> parquet::errors::Result<()>,
    ) -> Result<()> {
        let path = self.path;
        let fail = |e: parquet::errors::ParquetError| Error::parquet(path, "can't write a column", &e);
        let mut col = self
            .rg
            .next_column()
            .map_err(fail)?
            .ok_or_else(|| Error::bad_file(path, "the writer has more columns than the schema"))?;
        write(&mut col).map_err(fail)?;
        col.close().map_err(fail)
    }

    fn strs<'r>(&mut self, values: impl Iterator<Item = &'r str>) -> Result<()> {
        let vals: Vec<ByteArray> = values.map(ByteArray::from).collect();
        self.column(|c| {
            c.typed::<ByteArrayType>()
                .write_batch(&vals, None, None)
                .map(|_| ())
        })
    }

    fn opt_strs<'r>(&mut self, values: impl Iterator<Item = Option<&'r str>>) -> Result<()> {
        let mut vals = Vec::new();
        let mut defs = Vec::new();
        for v in values {
            match v {
                Some(s) => {
                    vals.push(ByteArray::from(s));
                    defs.push(1);
                }
                None => defs.push(0),
            }
        }
        self.column(|c| {
            c.typed::<ByteArrayType>()
                .write_batch(&vals, Some(&defs), None)
                .map(|_| ())
        })
    }

    fn i64s(&mut self, values: impl Iterator<Item = i64>) -> Result<()> {
        let vals: Vec<i64> = values.collect();
        self.column(|c| c.typed::<Int64Type>().write_batch(&vals, None, None).map(|_| ()))
    }

    fn opt_i64s(&mut self, values: impl Iterator<Item = Option<i64>>) -> Result<()> {
        let mut vals = Vec::new();
        let mut defs = Vec::new();
        for v in values {
            match v {
                Some(n) => {
                    vals.push(n);
                    defs.push(1);
                }
                None => defs.push(0),
            }
        }
        self.column(|c| {
            c.typed::<Int64Type>()
                .write_batch(&vals, Some(&defs), None)
                .map(|_| ())
        })
    }

    fn opt_f64s(&mut self, values: impl Iterator<Item = Option<f64>>) -> Result<()> {
        let mut vals = Vec::new();
        let mut defs = Vec::new();
        for v in values {
            match v {
                Some(n) => {
                    vals.push(n);
                    defs.push(1);
                }
                None => defs.push(0),
            }
        }
        self.column(|c| {
            c.typed::<DoubleType>()
                .write_batch(&vals, Some(&defs), None)
                .map(|_| ())
        })
    }

    fn f64s(&mut self, values: impl Iterator<Item = f64>) -> Result<()> {
        let vals: Vec<f64> = values.collect();
        self.column(|c| c.typed::<DoubleType>().write_batch(&vals, None, None).map(|_| ()))
    }
}

/// Writes a day file to a temporary name; [`DayWriter::finish`] checks it and renames it into place.
pub(crate) struct DayWriter<T: ArchRow> {
    tmp: PathBuf,
    writer: SerializedFileWriter<File>,
    buf: Vec<T>,
    rows: u64,
    max_seq: Option<i64>,
}

/// A day file in place: its rows, its size and the largest seq it holds.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Finished {
    pub rows: u64,
    pub bytes: u64,
    pub max_seq: Option<i64>,
}

impl<T: ArchRow> DayWriter<T> {
    /// Starts `<path>.tmp`, creating the folder.
    pub(crate) fn create(path: &Path) -> Result<DayWriter<T>> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)
                .map_err(|e| Error::io(dir, "can't create the archive folder", &e))?;
        }
        let tmp = path.with_extension("parquet.tmp");
        let schema = parse_message_type(T::SCHEMA)
            .map_err(|e| Error::parquet(&tmp, "the day file schema doesn't parse", &e))?;
        let level = ZstdLevel::try_new(3).map_err(|e| Error::parquet(&tmp, "bad zstd level", &e))?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(level))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_created_by("trail".to_string())
            .set_key_value_metadata(Some(vec![KeyValue::new(
                FORMAT_KEY.to_string(),
                FORMAT.to_string(),
            )]))
            .build();
        let file = File::create(&tmp).map_err(|e| Error::io(&tmp, "can't create a day file", &e))?;
        let writer = SerializedFileWriter::new(file, Arc::new(schema), Arc::new(props))
            .map_err(|e| Error::parquet(&tmp, "can't start a day file", &e))?;
        Ok(DayWriter {
            tmp,
            writer,
            buf: Vec::new(),
            rows: 0,
            max_seq: None,
        })
    }

    pub(crate) fn push(&mut self, row: T) -> Result<()> {
        self.max_seq = self.max_seq.max(row.seq());
        self.buf.push(row);
        if self.buf.len() >= ROW_GROUP {
            self.flush()?;
        }
        Ok(())
    }

    pub(crate) fn rows(&self) -> u64 {
        self.rows + self.buf.len() as u64
    }

    fn flush(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let tmp = self.tmp.clone();
        let fail = |e: parquet::errors::ParquetError| Error::parquet(&tmp, "can't write a row group", &e);
        let mut rg = self.writer.next_row_group().map_err(fail)?;
        T::write_group(
            &self.buf,
            &mut Group {
                rg: &mut rg,
                path: &tmp,
            },
        )?;
        rg.close().map_err(fail)?;
        self.rows += self.buf.len() as u64;
        self.buf.clear();
        Ok(())
    }

    /// Closes the file, reads it back, checks its row count and renames it to `path`.
    pub(crate) fn finish(mut self, path: &Path) -> Result<Finished> {
        self.flush()?;
        let tmp = self.tmp.clone();
        let rows = self.rows;
        self.writer
            .close()
            .map_err(|e| Error::parquet(&tmp, "can't close a day file", &e))?;
        let check = (|| {
            let file = File::open(&tmp).map_err(|e| Error::io(&tmp, "can't reopen a day file", &e))?;
            let reader = SerializedFileReader::new(file)
                .map_err(|e| Error::parquet(&tmp, "can't read back a day file", &e))?;
            let counted: i64 = reader
                .metadata()
                .row_groups()
                .iter()
                .map(RowGroupMetaData::num_rows)
                .sum();
            if u64::try_from(counted).ok() != Some(rows) {
                return Err(Error::new(
                    tmp.display().to_string(),
                    format!("wrote {rows} rows but the file holds {counted}"),
                    "check the disk; the rows stay in trail.db and the next archive run tries again",
                ));
            }
            Ok(())
        })();
        if let Err(e) = check {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        // On disk before it has its name, and its name on disk before the caller deletes the rows
        // it holds from SQLite: a power cut must not leave the name pointing at half a file.
        let synced = File::open(&tmp)
            .and_then(|f| f.sync_all())
            .map_err(|e| Error::io(&tmp, "can't sync a day file to disk", &e));
        if let Err(e) = synced {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        std::fs::rename(&tmp, path).map_err(|e| Error::io(path, "can't rename a day file into place", &e))?;
        if let Some(dir) = path.parent() {
            File::open(dir)
                .and_then(|d| d.sync_all())
                .map_err(|e| Error::io(dir, "can't sync the archive folder to disk", &e))?;
        }
        let bytes = std::fs::metadata(path)
            .map_err(|e| Error::io(path, "can't stat a day file", &e))?
            .len();
        Ok(Finished {
            rows,
            bytes,
            max_seq: self.max_seq,
        })
    }

    /// Drops the temporary file.
    pub(crate) fn abandon(self) {
        let tmp = self.tmp.clone();
        drop(self.writer);
        let _ = std::fs::remove_file(tmp);
    }
}

// ---------- reading ----------

/// Skips row groups whose statistics show no row can match. Columns are named, not numbered,
/// so a file of any format is skipped by the same rule; a file without the column skips nothing.
#[derive(Debug, Clone)]
pub(crate) enum Skip {
    /// The text column's values may fall in [lo, hi] (bytes, inclusive).
    Bytes {
        column: &'static str,
        lo: Vec<u8>,
        hi: Vec<u8>,
    },
    /// The int64 column's values may fall in [lo, hi].
    Int { column: &'static str, lo: i64, hi: i64 },
}

impl Skip {
    fn may_match(&self, group: &RowGroupMetaData) -> bool {
        let stats = |name: &str| {
            group
                .columns()
                .iter()
                .find(|c| c.column_descr().name() == name)
                .and_then(|c| c.statistics())
        };
        match self {
            Skip::Bytes { column, lo, hi } => {
                let Some(stats) = stats(column) else {
                    return true;
                };
                match (stats.min_bytes_opt(), stats.max_bytes_opt()) {
                    (Some(min), Some(max)) => !(max < lo.as_slice() || min > hi.as_slice()),
                    _ => true,
                }
            }
            Skip::Int { column, lo, hi } => match stats(column) {
                Some(Statistics::Int64(s)) => match (s.min_opt(), s.max_opt()) {
                    (Some(min), Some(max)) => !(max < lo || min > hi),
                    _ => true,
                },
                _ => true,
            },
        }
    }
}

/// A day file's rows in file order, decoded one row group at a time.
pub(crate) struct FileRows<T> {
    reader: SerializedFileReader<File>,
    path: PathBuf,
    /// Each column's position in a row of this file, by name.
    index: HashMap<String, usize>,
    next_group: usize,
    filters: Vec<Skip>,
    buf: VecDeque<T>,
}

impl<T: ArchRow> FileRows<T> {
    pub(crate) fn open(path: &Path, filter: Option<Skip>) -> Result<FileRows<T>> {
        let file = File::open(path).map_err(|e| Error::io(path, "can't open a day file", &e))?;
        let reader =
            SerializedFileReader::new(file).map_err(|e| Error::parquet(path, "can't read a day file", &e))?;
        let index = reader
            .metadata()
            .file_metadata()
            .schema()
            .get_fields()
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().to_string(), i))
            .collect();
        Ok(FileRows {
            reader,
            path: path.to_path_buf(),
            index,
            next_group: 0,
            filters: filter.into_iter().collect(),
            buf: VecDeque::new(),
        })
    }

    /// Also skips the row groups `skip` rules out.
    pub(crate) fn also(mut self, skip: Skip) -> FileRows<T> {
        self.filters.push(skip);
        self
    }

    /// Like `open`, but None when the file is missing (deleted by expiry since it was listed).
    pub(crate) fn open_if_exists(path: &Path, filter: Option<Skip>) -> Result<Option<FileRows<T>>> {
        if !path.exists() {
            return Ok(None);
        }
        FileRows::open(path, filter).map(Some)
    }

    fn fill(&mut self) -> Result<()> {
        while self.buf.is_empty() && self.next_group < self.reader.num_row_groups() {
            let i = self.next_group;
            self.next_group += 1;
            let meta = self.reader.metadata().row_group(i);
            if self.filters.iter().any(|f| !f.may_match(meta)) {
                continue;
            }
            let fail =
                |e: parquet::errors::ParquetError| Error::parquet(&self.path, "can't read a row group", &e);
            let group = self.reader.get_row_group(i).map_err(fail)?;
            let rows = group.get_row_iter(None).map_err(fail)?;
            for row in rows {
                let row = row.map_err(fail)?;
                let mut fields = Fields {
                    path: &self.path,
                    index: &self.index,
                    row: row.into_columns(),
                };
                self.buf.push_back(T::read(&mut fields)?);
            }
        }
        Ok(())
    }

    pub(crate) fn peek(&mut self) -> Result<Option<&T>> {
        self.fill()?;
        Ok(self.buf.front())
    }

    pub(crate) fn next_row(&mut self) -> Result<Option<T>> {
        self.fill()?;
        Ok(self.buf.pop_front())
    }
}

/// Every record identity in a file, with its seq (its `source`, `id` and `seq` columns only).
pub(crate) fn file_keys(path: &Path) -> Result<Keys> {
    let fail = |e: parquet::errors::ParquetError| Error::parquet(path, "can't read a day file's ids", &e);
    let file = File::open(path).map_err(|e| Error::io(path, "can't open a day file", &e))?;
    let reader = SerializedFileReader::new(file).map_err(fail)?;
    let root = reader.metadata().file_metadata().schema();
    let wanted: Vec<_> = root
        .get_fields()
        .iter()
        .filter(|f| matches!(f.name(), "id" | "source" | "seq"))
        .cloned()
        .collect();
    if wanted.iter().filter(|f| f.name() != "seq").count() != 2 {
        return Err(Error::bad_file(
            path,
            "the file lacks its `source` or `id` column",
        ));
    }
    let projection = parquet::schema::types::Type::group_type_builder(root.name())
        .with_fields(wanted)
        .build()
        .map_err(fail)?;
    let mut keys = Keys::default();
    for row in reader.get_row_iter(Some(projection)).map_err(fail)? {
        let (mut source, mut id, mut seq) = (None, None, None);
        for (name, field) in row.map_err(fail)?.into_columns() {
            match (name.as_str(), field) {
                ("source", Field::Str(text)) => source = Some(text),
                ("id", Field::Str(text)) => id = Some(text),
                ("seq", Field::Long(n)) => seq = Some(n),
                _ => {}
            }
        }
        if let (Some(source), Some(id)) = (source, id) {
            keys.insert(source, id, seq);
        }
    }
    Ok(keys)
}

/// A day file's format: the `trail.format` key of its metadata, or 1 when it has none.
#[cfg(test)]
pub(crate) fn file_format(path: &Path) -> Result<u32> {
    let file = File::open(path).map_err(|e| Error::io(path, "can't open a day file", &e))?;
    let reader =
        SerializedFileReader::new(file).map_err(|e| Error::parquet(path, "can't read a day file", &e))?;
    let found = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .and_then(|kv| kv.iter().find(|k| k.key == FORMAT_KEY))
        .and_then(|k| k.value.as_deref())
        .map(str::to_string);
    match found {
        None => Ok(1),
        Some(text) => text
            .parse()
            .map_err(|_| Error::bad_file(path, format!("`{FORMAT_KEY}` is `{text}`, not a number"))),
    }
}

/// One row's fields, taken by column name. A column the file lacks reads as null.
pub(crate) struct Fields<'a> {
    path: &'a Path,
    index: &'a HashMap<String, usize>,
    row: Vec<(String, Field)>,
}

impl Fields<'_> {
    fn take(&mut self, name: &str) -> Field {
        match self.index.get(name).and_then(|i| self.row.get_mut(*i)) {
            Some((_, field)) => std::mem::replace(field, Field::Null),
            None => Field::Null,
        }
    }

    fn wrong(&self, name: &str, field: &Field, wanted: &str) -> Error {
        let message = if matches!(field, Field::Null) {
            format!("column `{name}` is missing or null, expected {wanted}")
        } else {
            format!("column `{name}` holds {field:?}, expected {wanted}")
        };
        Error::bad_file(self.path, message)
    }

    fn str(&mut self, name: &str) -> Result<String> {
        match self.take(name) {
            Field::Str(s) => Ok(s),
            other => Err(self.wrong(name, &other, "text")),
        }
    }

    fn opt_str(&mut self, name: &str) -> Result<Option<String>> {
        match self.take(name) {
            Field::Str(s) => Ok(Some(s)),
            Field::Null => Ok(None),
            other => Err(self.wrong(name, &other, "text or null")),
        }
    }

    fn opt_i64(&mut self, name: &str) -> Result<Option<i64>> {
        match self.take(name) {
            Field::Long(v) | Field::TimestampMillis(v) => Ok(Some(v)),
            Field::Null => Ok(None),
            other => Err(self.wrong(name, &other, "an integer or null")),
        }
    }

    fn i64(&mut self, name: &str) -> Result<i64> {
        match self.take(name) {
            Field::Long(v) | Field::TimestampMillis(v) => Ok(v),
            other => Err(self.wrong(name, &other, "an integer")),
        }
    }

    fn f64(&mut self, name: &str) -> Result<f64> {
        match self.take(name) {
            Field::Double(v) => Ok(v),
            other => Err(self.wrong(name, &other, "a number")),
        }
    }

    fn opt_f64(&mut self, name: &str) -> Result<Option<f64>> {
        match self.take(name) {
            Field::Double(v) => Ok(Some(v)),
            Field::Null => Ok(None),
            other => Err(self.wrong(name, &other, "a number or null")),
        }
    }

    fn object(&mut self, name: &str) -> Result<Object> {
        let text = self.str(name)?;
        serde_json::from_str(&text)
            .map_err(|e| Error::bad_file(self.path, format!("`{name}` doesn't parse as JSON: {e}")))
    }
}

// ---------- the four layouts ----------

impl Kept for Event {
    fn id(&self) -> &str {
        &self.id
    }
    fn source(&self) -> &str {
        &self.source
    }
    fn ts(&self) -> i64 {
        self.ts.ms()
    }
}

impl Kept for ErrorRecord {
    fn id(&self) -> &str {
        &self.id
    }
    fn source(&self) -> &str {
        &self.source
    }
    fn ts(&self) -> i64 {
        self.ts.ms()
    }
}

impl Kept for LogRecord {
    fn id(&self) -> &str {
        &self.id
    }
    fn source(&self) -> &str {
        &self.source
    }
    fn ts(&self) -> i64 {
        self.ts.ms()
    }
}

impl ArchRow for Event {
    const SCHEMA: &'static str = "message event {
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
        optional int64 seq;
    }";

    fn write_group(rows: &[Event], g: &mut Group<'_, '_>) -> Result<()> {
        g.strs(rows.iter().map(|r| r.id.as_str()))?;
        g.strs(rows.iter().map(|r| r.name.as_str()))?;
        g.i64s(rows.iter().map(|r| r.ts.ms()))?;
        g.i64s(rows.iter().map(|r| r.received_at.ms()))?;
        g.strs(rows.iter().map(|r| r.source.as_str()))?;
        g.opt_strs(rows.iter().map(|r| r.version.as_deref()))?;
        g.opt_strs(rows.iter().map(|r| r.host.as_deref()))?;
        g.opt_strs(rows.iter().map(|r| r.actor.as_deref()))?;
        g.opt_strs(rows.iter().map(|r| r.client.as_deref()))?;
        g.opt_strs(rows.iter().map(|r| r.trace_id.as_deref()))?;
        let props: Vec<String> = rows
            .iter()
            .map(|r| crate::records::object_text(&r.props))
            .collect();
        g.strs(props.iter().map(String::as_str))?;
        let problems: Vec<Option<String>> = rows
            .iter()
            .map(|r| crate::records::problems_text(&r.problems))
            .collect();
        g.opt_strs(problems.iter().map(Option::as_deref))?;
        g.opt_i64s(rows.iter().map(|r| r.seq))
    }

    fn read(f: &mut Fields) -> Result<Event> {
        let problems = match f.opt_str("problems")? {
            Some(text) => serde_json::from_str(&text)
                .map_err(|e| Error::bad_file(f.path, format!("`problems` doesn't parse: {e}")))?,
            None => Vec::new(),
        };
        Ok(Event {
            id: f.str("id")?,
            name: f.str("name")?,
            ts: Ts(f.i64("ts")?),
            received_at: Ts(f.i64("received_at")?),
            source: f.str("source")?,
            version: f.opt_str("version")?,
            host: f.opt_str("host")?,
            actor: f.opt_str("actor")?,
            client: f.opt_str("client")?,
            trace_id: f.opt_str("trace_id")?,
            props: f.object("props")?,
            problems,
            seq: f.opt_i64("seq")?,
        })
    }

    fn order(&self, other: &Event) -> Ordering {
        (&self.name, self.ts, &self.id, &self.source).cmp(&(&other.name, other.ts, &other.id, &other.source))
    }

    fn seq(&self) -> Option<i64> {
        self.seq
    }

    fn fill_seq(&mut self, fill: &Seqs) {
        if self.seq.is_none() {
            self.seq = fill.get(&self.source, &self.id);
        }
    }
}

impl ArchRow for ErrorRecord {
    const SCHEMA: &'static str = "message error {
        required binary id (STRING);
        required int64 ts (TIMESTAMP(MILLIS,true));
        required int64 received_at (TIMESTAMP(MILLIS,true));
        required binary source (STRING);
        optional binary version (STRING);
        optional binary host (STRING);
        required binary severity (STRING);
        required binary kind (STRING);
        required binary message (STRING);
        required binary fingerprint (STRING);
        optional binary stack (STRING);
        required binary context (JSON);
        optional binary trace_id (STRING);
        optional int64 seq;
    }";

    fn write_group(rows: &[ErrorRecord], g: &mut Group<'_, '_>) -> Result<()> {
        g.strs(rows.iter().map(|r| r.id.as_str()))?;
        g.i64s(rows.iter().map(|r| r.ts.ms()))?;
        g.i64s(rows.iter().map(|r| r.received_at.ms()))?;
        g.strs(rows.iter().map(|r| r.source.as_str()))?;
        g.opt_strs(rows.iter().map(|r| r.version.as_deref()))?;
        g.opt_strs(rows.iter().map(|r| r.host.as_deref()))?;
        g.strs(rows.iter().map(|r| r.severity.as_str()))?;
        g.strs(rows.iter().map(|r| r.kind.as_str()))?;
        g.strs(rows.iter().map(|r| r.message.as_str()))?;
        g.strs(rows.iter().map(|r| r.fingerprint.as_str()))?;
        g.opt_strs(rows.iter().map(|r| r.stack.as_deref()))?;
        let context: Vec<String> = rows
            .iter()
            .map(|r| crate::records::object_text(&r.context))
            .collect();
        g.strs(context.iter().map(String::as_str))?;
        g.opt_strs(rows.iter().map(|r| r.trace_id.as_deref()))?;
        g.opt_i64s(rows.iter().map(|r| r.seq))
    }

    fn read(f: &mut Fields) -> Result<ErrorRecord> {
        Ok(ErrorRecord {
            id: f.str("id")?,
            ts: Ts(f.i64("ts")?),
            received_at: Ts(f.i64("received_at")?),
            source: f.str("source")?,
            version: f.opt_str("version")?,
            host: f.opt_str("host")?,
            severity: Severity::parse(&f.str("severity")?).unwrap_or_default(),
            kind: f.str("kind")?,
            message: f.str("message")?,
            fingerprint: f.str("fingerprint")?,
            stack: f.opt_str("stack")?,
            context: f.object("context")?,
            trace_id: f.opt_str("trace_id")?,
            seq: f.opt_i64("seq")?,
        })
    }

    fn order(&self, other: &ErrorRecord) -> Ordering {
        (&self.source, self.ts, &self.id).cmp(&(&other.source, other.ts, &other.id))
    }

    fn seq(&self) -> Option<i64> {
        self.seq
    }

    fn fill_seq(&mut self, fill: &Seqs) {
        if self.seq.is_none() {
            self.seq = fill.get(&self.source, &self.id);
        }
    }
}

impl ArchRow for LogRecord {
    const SCHEMA: &'static str = "message log {
        required binary id (STRING);
        required int64 ts (TIMESTAMP(MILLIS,true));
        required int64 received_at (TIMESTAMP(MILLIS,true));
        required binary source (STRING);
        optional binary host (STRING);
        optional binary stream (STRING);
        required binary level (STRING);
        required binary message (STRING);
        required binary fields (JSON);
        optional int64 seq;
    }";

    fn write_group(rows: &[LogRecord], g: &mut Group<'_, '_>) -> Result<()> {
        g.strs(rows.iter().map(|r| r.id.as_str()))?;
        g.i64s(rows.iter().map(|r| r.ts.ms()))?;
        g.i64s(rows.iter().map(|r| r.received_at.ms()))?;
        g.strs(rows.iter().map(|r| r.source.as_str()))?;
        g.opt_strs(rows.iter().map(|r| r.host.as_deref()))?;
        g.opt_strs(rows.iter().map(|r| r.stream.as_deref()))?;
        g.strs(rows.iter().map(|r| r.level.as_str()))?;
        g.strs(rows.iter().map(|r| r.message.as_str()))?;
        let fields: Vec<String> = rows
            .iter()
            .map(|r| crate::records::object_text(&r.fields))
            .collect();
        g.strs(fields.iter().map(String::as_str))?;
        g.opt_i64s(rows.iter().map(|r| r.seq))
    }

    fn read(f: &mut Fields) -> Result<LogRecord> {
        Ok(LogRecord {
            id: f.str("id")?,
            ts: Ts(f.i64("ts")?),
            received_at: Ts(f.i64("received_at")?),
            source: f.str("source")?,
            host: f.opt_str("host")?,
            stream: f.opt_str("stream")?,
            level: Level::parse(&f.str("level")?).unwrap_or_default(),
            message: f.str("message")?,
            fields: f.object("fields")?,
            seq: f.opt_i64("seq")?,
        })
    }

    fn order(&self, other: &LogRecord) -> Ordering {
        (&self.source, self.ts, &self.id).cmp(&(&other.source, other.ts, &other.id))
    }

    fn seq(&self) -> Option<i64> {
        self.seq
    }

    fn fill_seq(&mut self, fill: &Seqs) {
        if self.seq.is_none() {
            self.seq = fill.get(&self.source, &self.id);
        }
    }
}

impl ArchRow for SampleRow {
    const SCHEMA: &'static str = "message sample {
        required int64 series;
        required int64 ts (TIMESTAMP(MILLIS,true));
        required double value;
        required binary name (STRING);
        required binary labels (JSON);
        optional double total;
        optional double increase;
    }";

    fn write_group(rows: &[SampleRow], g: &mut Group<'_, '_>) -> Result<()> {
        g.i64s(rows.iter().map(|r| r.series))?;
        g.i64s(rows.iter().map(|r| r.ts))?;
        g.f64s(rows.iter().map(|r| r.value))?;
        g.strs(rows.iter().map(|r| &*r.name))?;
        g.strs(rows.iter().map(|r| &*r.labels))?;
        g.opt_f64s(rows.iter().map(|r| r.total))?;
        g.opt_f64s(rows.iter().map(|r| r.increase))
    }

    fn read(f: &mut Fields) -> Result<SampleRow> {
        Ok(SampleRow {
            series: f.i64("series")?,
            ts: f.i64("ts")?,
            value: f.f64("value")?,
            name: Arc::from(f.str("name")?),
            labels: Arc::from(f.str("labels")?),
            total: f.opt_f64("total")?,
            increase: f.opt_f64("increase")?,
        })
    }

    fn order(&self, other: &SampleRow) -> Ordering {
        (self.series, self.ts).cmp(&(other.series, other.ts))
    }
}

/// Merges sorted new rows into a sorted old file while writing. On equal sort keys the new row
/// wins when `new_wins` (samples), the old one otherwise.
pub(crate) struct Merge<'a, T: ArchRow> {
    pub old: Option<FileRows<T>>,
    pub out: DayWriter<T>,
    pub new_wins: bool,
    /// Seqs for old rows that lack one (written before seq existed).
    pub fill: &'a Seqs,
}

impl<T: ArchRow> Merge<'_, T> {
    pub(crate) fn push(&mut self, new: T) -> Result<()> {
        if let Some(old) = self.old.as_mut() {
            while let Some(o) = old.peek()? {
                match o.order(&new) {
                    Ordering::Less => {
                        if let Some(mut o) = old.next_row()? {
                            o.fill_seq(self.fill);
                            self.out.push(o)?;
                        }
                    }
                    Ordering::Equal => {
                        if self.new_wins {
                            old.next_row()?;
                            break;
                        }
                        if let Some(mut o) = old.next_row()? {
                            o.fill_seq(self.fill);
                            self.out.push(o)?;
                        }
                        return Ok(());
                    }
                    Ordering::Greater => break,
                }
            }
        }
        self.out.push(new)
    }

    /// Copies the rest of the old file and returns the writer.
    pub(crate) fn finish(mut self) -> Result<DayWriter<T>> {
        if let Some(old) = self.old.as_mut() {
            while let Some(mut o) = old.next_row()? {
                o.fill_seq(self.fill);
                self.out.push(o)?;
            }
        }
        Ok(self.out)
    }
}
