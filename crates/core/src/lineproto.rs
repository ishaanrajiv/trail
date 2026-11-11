//! InfluxDB line protocol, the format Telegraf and other InfluxDB clients write:
//!
//! ```text
//! measurement,tag=value,tag2=value2 field=1.5,count=3i,ok=true 1730000000000000000
//! ```
//!
//! Each numeric or boolean field becomes a point named `<measurement>.<field>` with the tags as
//! labels. String fields are skipped: they aren't metrics. A bad line is reported and the good
//! lines in the same body are still returned, as InfluxDB does.

use crate::envelope::Point;
use crate::time::Ts;
use std::collections::BTreeMap;

/// A line that could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineError {
    /// 1-based line number in the body.
    pub line: usize,
    pub message: String,
}

/// The timestamp unit a client writes in. InfluxDB 1 says `n u ms s m h`, InfluxDB 2 `ns us ms s`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Precision {
    Nanos,
    Micros,
    Millis,
    Seconds,
    Minutes,
    Hours,
}

impl Precision {
    /// Unknown values read as nanoseconds, InfluxDB's default.
    pub fn parse(text: &str) -> Precision {
        match text {
            "us" | "u" => Precision::Micros,
            "ms" => Precision::Millis,
            "s" => Precision::Seconds,
            "m" => Precision::Minutes,
            "h" => Precision::Hours,
            _ => Precision::Nanos,
        }
    }

    fn to_ms(self, stamp: i64) -> i64 {
        match self {
            Precision::Nanos => stamp.div_euclid(1_000_000),
            Precision::Micros => stamp.div_euclid(1000),
            Precision::Millis => stamp,
            Precision::Seconds => stamp.saturating_mul(1000),
            Precision::Minutes => stamp.saturating_mul(60_000),
            Precision::Hours => stamp.saturating_mul(3_600_000),
        }
    }
}

/// Parses every line of a body. Lines without a timestamp get `now`.
pub fn parse(body: &str, precision: Precision, now: Ts) -> (Vec<Point>, Vec<LineError>) {
    let (points, errors) = parse_numbered(body, precision, now);
    (points.into_iter().map(|(_, point)| point).collect(), errors)
}

/// As [`parse`], with the 1-based line each reading came from, so a later check can name it.
pub fn parse_numbered(body: &str, precision: Precision, now: Ts) -> (Vec<(usize, Point)>, Vec<LineError>) {
    let mut points = Vec::new();
    let mut errors = Vec::new();
    for (index, line) in body.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match parse_line(line, precision, now) {
            Ok(line_points) => points.extend(line_points.into_iter().map(|point| (index + 1, point))),
            Err(message) => errors.push(LineError {
                line: index + 1,
                message,
            }),
        }
    }
    (points, errors)
}

fn parse_line(line: &str, precision: Precision, now: Ts) -> Result<Vec<Point>, String> {
    let sections: Vec<&str> = split(line, ' ', true, None)?
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect();
    if sections.len() != 2 && sections.len() != 3 {
        return Err("expected measurement, fields and an optional timestamp".to_string());
    }
    let head = split(sections[0], ',', false, None)?;
    let measurement = unescape(head[0]);
    if measurement.is_empty() {
        return Err("no measurement".to_string());
    }
    let mut labels = BTreeMap::new();
    for tag in &head[1..] {
        let pair = split(tag, '=', false, Some(1))?;
        if pair.len() != 2 || pair[0].is_empty() || pair[1].is_empty() {
            return Err(format!("bad tag `{tag}`"));
        }
        labels.insert(unescape(pair[0]), unescape(pair[1]));
    }
    let ts = match sections.get(2) {
        Some(stamp) => Ts(precision.to_ms(
            stamp
                .parse::<i64>()
                .map_err(|_| format!("bad timestamp `{stamp}`"))?,
        )),
        None => now,
    };
    let mut points = Vec::new();
    let mut fields = 0;
    for item in split(sections[1], ',', true, None)? {
        let pair = split(item, '=', true, Some(1))?;
        if pair.len() != 2 || pair[0].is_empty() || pair[1].is_empty() {
            return Err(format!("bad field `{item}`"));
        }
        fields += 1;
        if let Some(value) = number(pair[1])? {
            points.push(Point {
                name: format!("{measurement}.{}", unescape(pair[0])),
                labels: labels.clone(),
                value,
                ts,
                unit: None,
                kind: None,
            });
        }
    }
    if fields == 0 {
        return Err("no fields".to_string());
    }
    Ok(points)
}

/// Splits on `sep`, skipping backslash-escaped characters and, for fields, double-quoted strings.
/// `limit` caps the number of splits.
fn split(text: &str, sep: char, quotes: bool, limit: Option<usize>) -> Result<Vec<&str>, String> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut remaining = limit;
    let mut chars = text.char_indices();
    while let Some((i, c)) = chars.next() {
        if c == '\\' {
            chars.next();
        } else if quotes && c == '"' {
            quoted = !quoted;
        } else if c == sep && !quoted && remaining != Some(0) {
            parts.push(&text[start..i]);
            start = i + c.len_utf8();
            remaining = remaining.map(|n| n - 1);
        }
    }
    if quoted {
        return Err("unterminated string".to_string());
    }
    parts.push(&text[start..]);
    Ok(parts)
}

fn unescape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match chars.peek() {
            Some(next) if c == '\\' && matches!(next, ',' | ' ' | '=' | '"' | '\\') => {
                out.push(*next);
                chars.next();
            }
            _ => out.push(c),
        }
    }
    out
}

/// A field's value as a number. None for a string field, which is skipped.
fn number(raw: &str) -> Result<Option<f64>, String> {
    if raw.starts_with('"') {
        return Ok(None);
    }
    if matches!(raw, "t" | "T" | "true" | "True" | "TRUE") {
        return Ok(Some(1.0));
    }
    if matches!(raw, "f" | "F" | "false" | "False" | "FALSE") {
        return Ok(Some(0.0));
    }
    let bad = || format!("bad value `{raw}`");
    if let Some(integer) = raw.strip_suffix('i').or_else(|| raw.strip_suffix('u')) {
        #[allow(clippy::cast_precision_loss)]
        return integer.parse::<i128>().map(|n| Some(n as f64)).map_err(|_| bad());
    }
    raw.parse::<f64>().map(Some).map_err(|_| bad())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn telegraf_line() {
        let body = "cpu,cpu=cpu-total,host=box usage_active=12.5,usage_idle=87.5 1794000000000000000\n";
        let (points, errors) = parse(body, Precision::Nanos, Ts(0));
        assert!(errors.is_empty());
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].name, "cpu.usage_active");
        assert_eq!(points[0].value, 12.5);
        assert_eq!(points[0].ts, Ts(1_794_000_000_000));
        assert_eq!(points[0].labels.get("cpu").map(String::as_str), Some("cpu-total"));
    }

    #[test]
    fn types_escapes_and_missing_timestamp() {
        let body = r#"disk\ io,path=/var\,lib reads=3i,ok=true,name="sda 1",ratio=-0.5"#;
        let (points, errors) = parse(body, Precision::Nanos, Ts(77));
        assert!(errors.is_empty(), "{errors:?}");
        let names: Vec<&str> = points.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, ["disk io.reads", "disk io.ok", "disk io.ratio"]);
        assert_eq!(points[0].labels.get("path").map(String::as_str), Some("/var,lib"));
        assert_eq!(points[1].value, 1.0);
        assert_eq!(points[0].ts, Ts(77));
    }

    #[test]
    fn bad_lines_are_reported_and_good_ones_kept() {
        let body = "good value=1 1000\n\n# comment\nbad\nalso,bad value=x\nfine v=2i 2000\n";
        let (points, errors) = parse(body, Precision::Seconds, Ts(0));
        assert_eq!(points.len(), 2);
        assert_eq!(points[1].ts, Ts(2_000_000));
        assert_eq!(errors.iter().map(|e| e.line).collect::<Vec<_>>(), [4, 5]);
    }

    #[test]
    fn precisions() {
        assert_eq!(Precision::parse("ms").to_ms(5), 5);
        assert_eq!(Precision::parse("u").to_ms(5_000), 5);
        assert_eq!(Precision::parse("s").to_ms(5), 5_000);
        assert_eq!(Precision::parse("whatever"), Precision::Nanos);
    }
}
