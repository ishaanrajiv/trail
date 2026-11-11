//! A small quantile sketch for rollup percentiles.
//!
//! Values go into buckets whose edges grow by a fixed ratio, so a percentile read back is within
//! about 2% of the true value whatever the range, and a bucket of a million readings costs a few
//! hundred counters. (This is the idea behind DDSketch, without the merging machinery.)

use std::collections::BTreeMap;

/// Each bucket's upper edge is this many times the one below. Relative error: (γ-1)/(γ+1) ≈ 2%.
const GAMMA: f64 = 1.04;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Sketch {
    zeros: u64,
    positive: BTreeMap<i32, u64>,
    negative: BTreeMap<i32, u64>,
    count: u64,
}

impl Sketch {
    pub fn add(&mut self, value: f64) {
        if !value.is_finite() {
            return;
        }
        self.count += 1;
        if value == 0.0 {
            self.zeros += 1;
            return;
        }
        #[allow(clippy::cast_possible_truncation)]
        let key = (value.abs().ln() / GAMMA.ln()).ceil().clamp(-1.0e9, 1.0e9) as i32;
        let side = if value > 0.0 {
            &mut self.positive
        } else {
            &mut self.negative
        };
        *side.entry(key).or_insert(0) += 1;
    }

    /// The value at quantile `q` (0 to 1), or None when nothing was added.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let rank = ((q.clamp(0.0, 1.0) * self.count as f64).ceil() as u64).clamp(1, self.count);
        let mut seen = 0u64;
        // Most negative first: the largest keys of the negative side.
        for (key, n) in self.negative.iter().rev() {
            seen += n;
            if seen >= rank {
                return Some(-middle(*key));
            }
        }
        seen += self.zeros;
        if seen >= rank {
            return Some(0.0);
        }
        for (key, n) in &self.positive {
            seen += n;
            if seen >= rank {
                return Some(middle(*key));
            }
        }
        None
    }
}

/// The value that stands for bucket `key`, which covers (γ^(key-1), γ^key].
fn middle(key: i32) -> f64 {
    2.0 * GAMMA.powi(key) / (GAMMA + 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_are_within_two_percent() {
        let mut sketch = Sketch::default();
        for n in 1..=10_000 {
            sketch.add(f64::from(n));
        }
        for (q, truth) in [(0.5, 5000.0), (0.9, 9000.0), (0.95, 9500.0), (0.99, 9900.0)] {
            let got = sketch.quantile(q).unwrap();
            assert!((got - truth).abs() / truth < 0.021, "p{q}: {got} vs {truth}");
        }
    }

    #[test]
    fn handles_zero_negative_and_empty() {
        let mut sketch = Sketch::default();
        assert_eq!(sketch.quantile(0.5), None);
        for value in [-10.0, -1.0, 0.0, 0.0, 5.0] {
            sketch.add(value);
        }
        sketch.add(f64::NAN);
        assert!((sketch.quantile(0.0).unwrap() + 10.0).abs() < 0.3);
        assert_eq!(sketch.quantile(0.5), Some(0.0));
        assert!((sketch.quantile(1.0).unwrap() - 5.0).abs() < 0.15);
    }

    #[test]
    fn one_value_reads_back_close() {
        let mut sketch = Sketch::default();
        sketch.add(0.0042);
        let got = sketch.quantile(0.95).unwrap();
        assert!((got - 0.0042).abs() / 0.0042 < 0.021);
    }
}
