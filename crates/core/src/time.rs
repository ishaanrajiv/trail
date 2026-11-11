//! Timestamps, durations and local days.
//!
//! Every time in Trail is [`Ts`]: integer milliseconds since the Unix epoch, UTC. Days and hourly
//! buckets follow a configured [`Zone`], so "one file per day" means a local day.

use jiff::civil::Date;
use jiff::tz::TimeZone;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

pub const SECOND: i64 = 1000;
pub const MINUTE: i64 = 60 * SECOND;
pub const HOUR: i64 = 60 * MINUTE;
pub const DAY: i64 = 24 * HOUR;

/// Milliseconds since the Unix epoch, UTC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct Ts(pub i64);

impl Ts {
    pub fn now() -> Ts {
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        Ts(i64::try_from(ms).unwrap_or(i64::MAX))
    }

    pub fn ms(self) -> i64 {
        self.0
    }

    pub fn plus(self, ms: i64) -> Ts {
        Ts(self.0.saturating_add(ms))
    }

    pub fn minus(self, ms: i64) -> Ts {
        Ts(self.0.saturating_sub(ms))
    }
}

impl fmt::Display for Ts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// An RFC 3339 time (`2026-11-10T12:00:00Z`, `2026-11-10T17:30:00+05:30`) as a timestamp.
pub fn parse_rfc3339(text: &str) -> Option<Ts> {
    text.trim()
        .parse::<jiff::Timestamp>()
        .ok()
        .map(|t| Ts(t.as_millisecond()))
}

/// `30s`, `5m`, `48h`, `90d`, `2w` to milliseconds. `forever`, `never` and `0` mean no limit (None).
pub fn parse_duration(text: &str) -> Result<Option<i64>, String> {
    let text = text.trim().to_ascii_lowercase();
    if matches!(text.as_str(), "forever" | "never" | "0") {
        return Ok(None);
    }
    let (number, unit) = text.split_at(text.len().saturating_sub(1));
    let factor = match unit {
        "s" => SECOND,
        "m" => MINUTE,
        "h" => HOUR,
        "d" => DAY,
        "w" => 7 * DAY,
        _ => {
            return Err(format!(
                "`{text}` is not a duration; write a number and a unit, like 30s, 5m, 48h, 90d or forever"
            ));
        }
    };
    let number: i64 = number.parse().map_err(|_| {
        format!("`{text}` is not a duration; write a whole number and a unit, like 30s, 5m, 48h or 90d")
    })?;
    if number <= 0 {
        return Err(format!(
            "`{text}` must be more than zero; write forever to keep without limit"
        ));
    }
    number
        .checked_mul(factor)
        .map(Some)
        .ok_or_else(|| format!("`{text}` is too long a duration"))
}

/// `500MB`, `2GB`, `1TB` (decimal units) or a plain number of bytes.
pub fn parse_bytes(text: &str) -> Result<u64, String> {
    let upper = text.trim().to_ascii_uppercase();
    let (number, factor) = if let Some(n) = upper.strip_suffix("TB") {
        (n, 1_000_000_000_000u64)
    } else if let Some(n) = upper.strip_suffix("GB") {
        (n, 1_000_000_000)
    } else if let Some(n) = upper.strip_suffix("MB") {
        (n, 1_000_000)
    } else if let Some(n) = upper.strip_suffix("KB") {
        (n, 1_000)
    } else {
        (upper.as_str(), 1)
    };
    let number: f64 = number
        .trim()
        .parse()
        .map_err(|_| format!("`{text}` is not a size; write a number and a unit, like 500MB or 2GB"))?;
    if !number.is_finite() || number < 0.0 {
        return Err(format!(
            "`{text}` is not a size; write a number and a unit, like 500MB or 2GB"
        ));
    }
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    Ok((number * factor as f64) as u64)
}

/// How often an aggregation closes a bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Every {
    #[serde(rename = "1m")]
    Minute,
    #[serde(rename = "5m")]
    FiveMinutes,
    #[serde(rename = "1h")]
    Hour,
    #[serde(rename = "1d")]
    Day,
}

impl Every {
    pub const ALL: [Every; 4] = [Every::Minute, Every::FiveMinutes, Every::Hour, Every::Day];

    pub fn as_str(self) -> &'static str {
        match self {
            Every::Minute => "1m",
            Every::FiveMinutes => "5m",
            Every::Hour => "1h",
            Every::Day => "1d",
        }
    }

    pub fn parse(text: &str) -> Option<Every> {
        Every::ALL.into_iter().find(|e| e.as_str() == text)
    }

    /// The bucket's nominal length. A local day can be an hour longer or shorter than this.
    pub fn nominal_ms(self) -> i64 {
        match self {
            Every::Minute => MINUTE,
            Every::FiveMinutes => 5 * MINUTE,
            Every::Hour => HOUR,
            Every::Day => DAY,
        }
    }
}

/// A calendar day in the configured zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Day {
    pub year: i16,
    pub month: i8,
    pub day: i8,
}

impl fmt::Display for Day {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

impl FromStr for Day {
    type Err = String;

    fn from_str(text: &str) -> Result<Day, String> {
        let bad = || format!("`{text}` is not a day; write YYYY-MM-DD");
        let mut parts = text.split('-');
        let year: i16 = parts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
        let month: i8 = parts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
        let day: i8 = parts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
        if parts.next().is_some() || text.len() != 10 {
            return Err(bad());
        }
        Date::new(year, month, day).map_err(|_| bad())?;
        Ok(Day { year, month, day })
    }
}

/// The timezone that decides where a day and an hour start.
#[derive(Debug, Clone)]
pub struct Zone {
    name: String,
    tz: TimeZone,
}

impl Zone {
    pub fn new(name: &str) -> Result<Zone, String> {
        let tz = TimeZone::get(name).map_err(|e| {
            format!("`{name}` is not a timezone this system knows ({e}); write an IANA name like UTC or Asia/Colombo")
        })?;
        Ok(Zone {
            name: name.to_string(),
            tz,
        })
    }

    pub fn utc() -> Zone {
        Zone {
            name: "UTC".to_string(),
            tz: TimeZone::UTC,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn zoned(&self, ts: Ts) -> jiff::Zoned {
        // Timestamps far outside jiff's range (year 9999) are clamped: they only come from bad input.
        let clamped = ts.0.clamp(-62_135_596_800_000, 253_402_207_200_000);
        jiff::Timestamp::from_millisecond(clamped)
            .unwrap_or(jiff::Timestamp::UNIX_EPOCH)
            .to_zoned(self.tz.clone())
    }

    /// The local day that holds `ts`.
    pub fn day_of(&self, ts: Ts) -> Day {
        let date = self.zoned(ts).date();
        Day {
            year: date.year(),
            month: date.month(),
            day: date.day(),
        }
    }

    /// The first instant of a local day.
    pub fn day_start(&self, day: Day) -> Ts {
        Date::new(day.year, day.month, day.day)
            .and_then(|date| date.to_zoned(self.tz.clone()))
            .map(|z| Ts(z.timestamp().as_millisecond()))
            .unwrap_or(Ts(0))
    }

    /// The day after `day`.
    pub fn next_day(&self, day: Day) -> Day {
        Date::new(day.year, day.month, day.day)
            .and_then(Date::tomorrow)
            .map(|d| Day {
                year: d.year(),
                month: d.month(),
                day: d.day(),
            })
            .unwrap_or(day)
    }

    /// The first instant of the day after `day`: where that day ends.
    pub fn day_end(&self, day: Day) -> Ts {
        self.day_start(self.next_day(day))
    }

    /// The part of this zone's UTC offset below an hour, in ms (30 minutes for Asia/Colombo).
    /// Hourly buckets shift by this much so they start on the local hour.
    pub fn hour_offset_ms(&self, at: Ts) -> i64 {
        (i64::from(self.zoned(at).offset().seconds()) * SECOND).rem_euclid(HOUR)
    }

    /// The start of the bucket that holds `ts`.
    pub fn floor(&self, ts: Ts, every: Every) -> Ts {
        match every {
            Every::Minute => Ts(ts.0 - ts.0.rem_euclid(MINUTE)),
            Every::FiveMinutes => Ts(ts.0 - ts.0.rem_euclid(5 * MINUTE)),
            Every::Hour => {
                let offset = self.hour_offset_ms(ts);
                Ts((ts.0 + offset).div_euclid(HOUR) * HOUR - offset)
            }
            Every::Day => self.day_start(self.day_of(ts)),
        }
    }

    /// The start of the bucket after the one that holds `ts`.
    pub fn next(&self, ts: Ts, every: Every) -> Ts {
        match every {
            Every::Day => self.day_end(self.day_of(ts)),
            _ => self.floor(ts, every).plus(every.nominal_ms()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("30s"), Ok(Some(30_000)));
        assert_eq!(parse_duration("90d"), Ok(Some(90 * DAY)));
        assert_eq!(parse_duration("forever"), Ok(None));
        assert!(parse_duration("soon").is_err());
        assert!(parse_duration("-5m").is_err());
    }

    #[test]
    fn rfc3339() {
        assert_eq!(parse_rfc3339("2026-11-10T12:00:00Z"), Some(Ts(1_794_312_000_000)));
        assert_eq!(
            parse_rfc3339("2026-11-10T17:30:00.250+05:30"),
            Some(Ts(1_794_312_000_250))
        );
        assert_eq!(parse_rfc3339("yesterday"), None);
    }

    #[test]
    fn sizes() {
        assert_eq!(parse_bytes("2GB"), Ok(2_000_000_000));
        assert_eq!(parse_bytes("1.5MB"), Ok(1_500_000));
        assert_eq!(parse_bytes("42"), Ok(42));
        assert!(parse_bytes("lots").is_err());
    }

    #[test]
    fn days_follow_the_zone() {
        let colombo = Zone::new("Asia/Colombo").unwrap();
        // 2026-11-10 20:00 UTC is 01:30 on the 11th in Colombo.
        let ts = Ts(1_794_340_800_000);
        let day = colombo.day_of(ts);
        assert_eq!(day.to_string(), "2026-11-11");
        assert_eq!(Zone::utc().day_of(ts).to_string(), "2026-11-10");
        let start = colombo.day_start(day);
        assert_eq!(colombo.day_end(day).ms() - start.ms(), DAY);
        assert_eq!(colombo.floor(ts, Every::Day), start);
        assert_eq!("2026-11-11".parse::<Day>().unwrap(), day);
        assert!("2026-13-01".parse::<Day>().is_err());
    }

    #[test]
    fn hours_start_on_the_local_hour() {
        let colombo = Zone::new("Asia/Colombo").unwrap();
        let ts = Ts(1_794_340_800_000 + 10 * MINUTE); // 01:40 local
        let floor = colombo.floor(ts, Every::Hour);
        assert_eq!(ts.ms() - floor.ms(), 40 * MINUTE);
        assert_eq!(colombo.next(ts, Every::Hour).ms() - floor.ms(), HOUR);
        assert_eq!(Zone::utc().floor(ts, Every::FiveMinutes).ms() % (5 * MINUTE), 0);
    }
}
