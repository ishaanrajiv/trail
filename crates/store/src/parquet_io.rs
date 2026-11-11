//! The day files: one Parquet file per store, class and local day, written with parquet's
//! low-level column writers and read back a row group at a time with its record API.

use crate::{Error, Result};
use parquet::basic::{Compression, ZstdLevel};
use parquet::data_type::{ByteArray, ByteArrayType, DoubleType, Int64Type};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use parquet::file::writer::{SerializedColumnWriter, SerializedFileWriter, SerializedRowGroupWriter};
use parquet::record::Field;
use parquet::schema::parser::parse_message_type;
use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use trail_core::Ts;
use trail_core::envelope::{ErrorRecord, Event, Level, LogRecord, Object, Severity};

/// Rows per row group.
pub(crate) const ROW_GROUP: usize = 65_536;

/// A row with an id and a time, for newest-first reads.
pub(crate) trait Kept {
    fn id(&self) -> &str;
    fn ts(&self) -> i64;
}

/// A row type with a day file layout.
pub(crate) trait ArchRow: Sized {
    /// The Parquet schema, in column order.
    const SCHEMA: &'static str;
    /// Writes one row group's columns, in schema order.
    fn write_group(rows: &[Self], g: &mut Group<'_, '_>) -> Result<()>;
    /// Reads one row's fields, in schema order.
    fn read(f: &mut Fields) -> Result<Self>;
    /// The file's sort order.
    fn order(&self, other: &Self) -> Ordering;
}

/// One raw metric sample as a day file holds it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SampleRow {
    pub series: i64,
    pub ts: i64,
    pub value: f64,
    pub name: Arc<str>,
    pub labels: Arc<str>,
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
            .build();
        let file = File::create(&tmp).map_err(|e| Error::io(&tmp, "can't create a day file", &e))?;
        let writer = SerializedFileWriter::new(file, Arc::new(schema), Arc::new(props))
            .map_err(|e| Error::parquet(&tmp, "can't start a day file", &e))?;
        Ok(DayWriter {
            tmp,
            writer,
            buf: Vec::new(),
            rows: 0,
        })
    }

    pub(crate) fn push(&mut self, row: T) -> Result<()> {
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
    /// Returns (rows, bytes).
    pub(crate) fn finish(mut self, path: &Path) -> Result<(u64, u64)> {
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
        std::fs::rename(&tmp, path).map_err(|e| Error::io(path, "can't rename a day file into place", &e))?;
        let bytes = std::fs::metadata(path)
            .map_err(|e| Error::io(path, "can't stat a day file", &e))?
            .len();
        Ok((rows, bytes))
    }

    /// Drops the temporary file.
    pub(crate) fn abandon(self) {
        let tmp = self.tmp.clone();
        drop(self.writer);
        let _ = std::fs::remove_file(tmp);
    }
}

// ---------- reading ----------

/// Skips row groups whose statistics show no row can match.
#[derive(Debug, Clone)]
pub(crate) enum Skip {
    /// The text column's values may fall in [lo, hi] (bytes, inclusive).
    Bytes { column: usize, lo: Vec<u8>, hi: Vec<u8> },
    /// The int64 column's values may fall in [lo, hi].
    Int { column: usize, lo: i64, hi: i64 },
}

impl Skip {
    fn may_match(&self, group: &RowGroupMetaData) -> bool {
        match self {
            Skip::Bytes { column, lo, hi } => {
                let Some(stats) = group.columns().get(*column).and_then(|c| c.statistics()) else {
                    return true;
                };
                match (stats.min_bytes_opt(), stats.max_bytes_opt()) {
                    (Some(min), Some(max)) => !(max < lo.as_slice() || min > hi.as_slice()),
                    _ => true,
                }
            }
            Skip::Int { column, lo, hi } => match group.columns().get(*column).and_then(|c| c.statistics()) {
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
    next_group: usize,
    filter: Option<Skip>,
    buf: VecDeque<T>,
}

impl<T: ArchRow> FileRows<T> {
    pub(crate) fn open(path: &Path, filter: Option<Skip>) -> Result<FileRows<T>> {
        let file = File::open(path).map_err(|e| Error::io(path, "can't open a day file", &e))?;
        let reader =
            SerializedFileReader::new(file).map_err(|e| Error::parquet(path, "can't read a day file", &e))?;
        Ok(FileRows {
            reader,
            path: path.to_path_buf(),
            next_group: 0,
            filter,
            buf: VecDeque::new(),
        })
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
            if self.filter.as_ref().is_some_and(|f| !f.may_match(meta)) {
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
                    fields: row.into_columns().into_iter(),
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

/// Every id in a file (its `id` column only).
pub(crate) fn file_ids(path: &Path) -> Result<HashSet<String>> {
    let fail = |e: parquet::errors::ParquetError| Error::parquet(path, "can't read a day file's ids", &e);
    let file = File::open(path).map_err(|e| Error::io(path, "can't open a day file", &e))?;
    let reader = SerializedFileReader::new(file).map_err(fail)?;
    let root = reader.metadata().file_metadata().schema();
    let id: Vec<_> = root
        .get_fields()
        .iter()
        .filter(|f| f.name() == "id")
        .cloned()
        .collect();
    if id.is_empty() {
        return Err(Error::bad_file(path, "the file has no `id` column"));
    }
    let projection = parquet::schema::types::Type::group_type_builder(root.name())
        .with_fields(id)
        .build()
        .map_err(fail)?;
    let mut ids = HashSet::new();
    for row in reader.get_row_iter(Some(projection)).map_err(fail)? {
        let row = row.map_err(fail)?;
        if let Some((_, Field::Str(id))) = row.into_columns().into_iter().next() {
            ids.insert(id);
        }
    }
    Ok(ids)
}

/// One row's fields, taken in schema order.
pub(crate) struct Fields<'a> {
    path: &'a Path,
    fields: std::vec::IntoIter<(String, Field)>,
}

impl Fields<'_> {
    fn take(&mut self) -> Result<(String, Field)> {
        self.fields
            .next()
            .ok_or_else(|| Error::bad_file(self.path, "a row has fewer columns than Trail writes"))
    }

    fn wrong(&self, name: &str, field: &Field, wanted: &str) -> Error {
        Error::bad_file(
            self.path,
            format!("column `{name}` holds {field:?}, expected {wanted}"),
        )
    }

    fn str(&mut self) -> Result<String> {
        match self.take()? {
            (_, Field::Str(s)) => Ok(s),
            (name, other) => Err(self.wrong(&name, &other, "text")),
        }
    }

    fn opt_str(&mut self) -> Result<Option<String>> {
        match self.take()? {
            (_, Field::Str(s)) => Ok(Some(s)),
            (_, Field::Null) => Ok(None),
            (name, other) => Err(self.wrong(&name, &other, "text or null")),
        }
    }

    fn i64(&mut self) -> Result<i64> {
        match self.take()? {
            (_, Field::Long(v) | Field::TimestampMillis(v)) => Ok(v),
            (name, other) => Err(self.wrong(&name, &other, "an integer")),
        }
    }

    fn f64(&mut self) -> Result<f64> {
        match self.take()? {
            (_, Field::Double(v)) => Ok(v),
            (name, other) => Err(self.wrong(&name, &other, "a number")),
        }
    }

    fn object(&mut self) -> Result<Object> {
        let text = self.str()?;
        serde_json::from_str(&text)
            .map_err(|e| Error::bad_file(self.path, format!("a JSON column doesn't parse: {e}")))
    }
}

// ---------- the four layouts ----------

impl Kept for Event {
    fn id(&self) -> &str {
        &self.id
    }
    fn ts(&self) -> i64 {
        self.ts.ms()
    }
}

impl Kept for ErrorRecord {
    fn id(&self) -> &str {
        &self.id
    }
    fn ts(&self) -> i64 {
        self.ts.ms()
    }
}

impl Kept for LogRecord {
    fn id(&self) -> &str {
        &self.id
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
        g.opt_strs(problems.iter().map(Option::as_deref))
    }

    fn read(f: &mut Fields) -> Result<Event> {
        let id = f.str()?;
        let name = f.str()?;
        let ts = Ts(f.i64()?);
        let received_at = Ts(f.i64()?);
        let source = f.str()?;
        let version = f.opt_str()?;
        let host = f.opt_str()?;
        let actor = f.opt_str()?;
        let client = f.opt_str()?;
        let trace_id = f.opt_str()?;
        let props = f.object()?;
        let problems = match f.opt_str()? {
            Some(text) => serde_json::from_str(&text)
                .map_err(|e| Error::bad_file(f.path, format!("`problems` doesn't parse: {e}")))?,
            None => Vec::new(),
        };
        Ok(Event {
            id,
            name,
            ts,
            received_at,
            source,
            version,
            host,
            actor,
            client,
            trace_id,
            props,
            problems,
        })
    }

    fn order(&self, other: &Event) -> Ordering {
        (&self.name, self.ts, &self.id).cmp(&(&other.name, other.ts, &other.id))
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
        g.opt_strs(rows.iter().map(|r| r.trace_id.as_deref()))
    }

    fn read(f: &mut Fields) -> Result<ErrorRecord> {
        let id = f.str()?;
        let ts = Ts(f.i64()?);
        let received_at = Ts(f.i64()?);
        let source = f.str()?;
        let version = f.opt_str()?;
        let host = f.opt_str()?;
        let severity = Severity::parse(&f.str()?).unwrap_or_default();
        Ok(ErrorRecord {
            id,
            ts,
            received_at,
            source,
            version,
            host,
            severity,
            kind: f.str()?,
            message: f.str()?,
            fingerprint: f.str()?,
            stack: f.opt_str()?,
            context: f.object()?,
            trace_id: f.opt_str()?,
        })
    }

    fn order(&self, other: &ErrorRecord) -> Ordering {
        (&self.source, self.ts, &self.id).cmp(&(&other.source, other.ts, &other.id))
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
        g.strs(fields.iter().map(String::as_str))
    }

    fn read(f: &mut Fields) -> Result<LogRecord> {
        let id = f.str()?;
        let ts = Ts(f.i64()?);
        let received_at = Ts(f.i64()?);
        let source = f.str()?;
        let host = f.opt_str()?;
        let stream = f.opt_str()?;
        let level = Level::parse(&f.str()?).unwrap_or_default();
        Ok(LogRecord {
            id,
            ts,
            received_at,
            source,
            host,
            stream,
            level,
            message: f.str()?,
            fields: f.object()?,
        })
    }

    fn order(&self, other: &LogRecord) -> Ordering {
        (&self.source, self.ts, &self.id).cmp(&(&other.source, other.ts, &other.id))
    }
}

impl ArchRow for SampleRow {
    const SCHEMA: &'static str = "message sample {
        required int64 series;
        required int64 ts (TIMESTAMP(MILLIS,true));
        required double value;
        required binary name (STRING);
        required binary labels (JSON);
    }";

    fn write_group(rows: &[SampleRow], g: &mut Group<'_, '_>) -> Result<()> {
        g.i64s(rows.iter().map(|r| r.series))?;
        g.i64s(rows.iter().map(|r| r.ts))?;
        g.f64s(rows.iter().map(|r| r.value))?;
        g.strs(rows.iter().map(|r| &*r.name))?;
        g.strs(rows.iter().map(|r| &*r.labels))
    }

    fn read(f: &mut Fields) -> Result<SampleRow> {
        Ok(SampleRow {
            series: f.i64()?,
            ts: f.i64()?,
            value: f.f64()?,
            name: Arc::from(f.str()?),
            labels: Arc::from(f.str()?),
        })
    }

    fn order(&self, other: &SampleRow) -> Ordering {
        (self.series, self.ts).cmp(&(other.series, other.ts))
    }
}

/// Merges sorted new rows into a sorted old file while writing. On equal sort keys the new row
/// wins when `new_wins` (samples), the old one otherwise.
pub(crate) struct Merge<T: ArchRow> {
    pub old: Option<FileRows<T>>,
    pub out: DayWriter<T>,
    pub new_wins: bool,
}

impl<T: ArchRow> Merge<T> {
    pub(crate) fn push(&mut self, new: T) -> Result<()> {
        if let Some(old) = self.old.as_mut() {
            while let Some(o) = old.peek()? {
                match o.order(&new) {
                    Ordering::Less => {
                        if let Some(o) = old.next_row()? {
                            self.out.push(o)?;
                        }
                    }
                    Ordering::Equal => {
                        if self.new_wins {
                            old.next_row()?;
                            break;
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
            while let Some(o) = old.next_row()? {
                self.out.push(o)?;
            }
        }
        Ok(self.out)
    }
}
