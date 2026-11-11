//! A problem a check found, written for whoever has to fix it: where it is, what is wrong, and
//! what to change. Every user-facing error in Trail has this shape.

use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Problem {
    /// Where: a file and key (`events.toml: events."shop.order.placed".props.total`), or a record
    /// (`event shop.order.placed`).
    pub at: String,
    /// What is wrong, in one sentence.
    pub message: String,
    /// What to change. Empty only when the message already says it.
    pub fix: String,
}

impl Problem {
    pub fn new(at: impl Into<String>, message: impl Into<String>, fix: impl Into<String>) -> Problem {
        Problem {
            at: at.into(),
            message: message.into(),
            fix: fix.into(),
        }
    }
}

impl fmt::Display for Problem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.at, self.message)?;
        if !self.fix.is_empty() {
            write!(f, "\n  fix: {}", self.fix)?;
        }
        Ok(())
    }
}

/// Names the closest candidate to `word`, for "did you mean" hints. None when nothing is close.
pub fn closest<'a>(word: &str, candidates: impl IntoIterator<Item = &'a str>) -> Option<&'a str> {
    let mut best: Option<(usize, &str)> = None;
    for candidate in candidates {
        let distance = edit_distance(word, candidate);
        if best.is_none_or(|(d, _)| distance < d) {
            best = Some((distance, candidate));
        }
    }
    best.filter(|(d, c)| *d <= 2.max(c.len() / 4)).map(|(_, c)| c)
}

fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.iter().enumerate() {
        let mut previous = row[0];
        row[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cost = if ca == cb { previous } else { previous + 1 };
            previous = row[j + 1];
            row[j + 1] = cost.min(row[j] + 1).min(previous + 1);
        }
    }
    row[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closest_finds_a_typo() {
        assert_eq!(
            closest("latncy_ms", ["device", "latency_ms", "ok"]),
            Some("latency_ms")
        );
        assert_eq!(closest("zzz", ["device", "latency_ms"]), None);
    }

    #[test]
    fn display_states_the_fix() {
        let p = Problem::new("events.toml", "no source", "add `source = \"shop\"` at the top");
        assert_eq!(
            p.to_string(),
            "events.toml: no source\n  fix: add `source = \"shop\"` at the top"
        );
    }
}
