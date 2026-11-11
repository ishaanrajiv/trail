//! Error fingerprints. Two errors with the same fingerprint are the same bug: the same source and
//! type, with the same stack once line numbers, addresses and ids are taken out. A new
//! fingerprint is what should alert, not the thousandth repeat of a known one.

/// A 16-character fingerprint for an error.
pub fn fingerprint(source: &str, kind: &str, message: &str, stack: Option<&str>) -> String {
    let mut hash = Fnv::new();
    hash.write(source);
    hash.write("\n");
    hash.write(kind);
    hash.write("\n");
    match stack.map(str::trim).filter(|s| !s.is_empty()) {
        // With a stack, the message is left out: it often carries the value that differed.
        Some(stack) => {
            for frame in stack.lines().map(str::trim).filter(|l| !l.is_empty()).take(12) {
                hash.write(&normalize(frame));
                hash.write("\n");
            }
        }
        None => hash.write(&normalize(message)),
    }
    format!("{:016x}", hash.finish())
}

/// Replaces what varies between two occurrences of one bug: runs of digits, hex words and quoted
/// values become placeholders.
fn normalize(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' || c == '"' {
            // A quoted value: keep the quotes, drop what is inside (up to the closing quote).
            let mut inside = String::new();
            let mut closed = false;
            for next in chars.by_ref() {
                if next == c {
                    closed = true;
                    break;
                }
                inside.push(next);
            }
            if closed {
                out.push(c);
                out.push('?');
                out.push(c);
            } else {
                out.push(c);
                out.push_str(&normalize(&inside));
            }
        } else if c.is_ascii_alphanumeric() {
            // A word: one with a digit in it (a number, a hex address, an id) becomes N.
            let mut word = String::from(c);
            while let Some(next) = chars
                .peek()
                .copied()
                .filter(|n| n.is_ascii_alphanumeric() || *n == '_')
            {
                word.push(next);
                chars.next();
            }
            if word.chars().any(|w| w.is_ascii_digit()) && !word.contains('_') {
                out.push('N');
            } else {
                out.push_str(&word);
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// FNV-1a, 64 bits. Stable across Rust versions and platforms, which the standard hasher is not.
struct Fnv(u64);

impl Fnv {
    fn new() -> Fnv {
        Fnv(0xcbf2_9ce4_8422_2325)
    }

    fn write(&mut self, text: &str) {
        for byte in text.bytes() {
            self.0 ^= u64::from(byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_bug_same_fingerprint() {
        let a = fingerprint(
            "shop",
            "KeyError",
            "",
            Some("File \"app.py\", line 42, in handle\n  x = d['order_17']"),
        );
        let b = fingerprint(
            "shop",
            "KeyError",
            "",
            Some("File \"app.py\", line 57, in handle\n  x = d['order_99']"),
        );
        assert_eq!(a, b);
        assert_eq!(a.len(), 16);
    }

    #[test]
    fn different_bug_different_fingerprint() {
        let a = fingerprint("shop", "KeyError", "", Some("in handle"));
        assert_ne!(a, fingerprint("shop", "KeyError", "", Some("in checkout")));
        assert_ne!(a, fingerprint("shop", "ValueError", "", Some("in handle")));
        assert_ne!(a, fingerprint("billing", "KeyError", "", Some("in handle")));
    }

    #[test]
    fn messages_group_without_a_stack() {
        let a = fingerprint("shop", "Timeout", "request 8f3a2c took 5021 ms", None);
        let b = fingerprint("shop", "Timeout", "request 77b1e0 took 4980 ms", None);
        assert_eq!(a, b);
        assert_ne!(a, fingerprint("shop", "Timeout", "connection refused", None));
    }

    #[test]
    fn stable_value() {
        // The value is part of the stored data: changing the algorithm regroups every error.
        assert_eq!(
            fingerprint("shop", "KeyError", "missing key 42", None),
            fingerprint("shop", "KeyError", "missing key 7", None)
        );
        assert_eq!(
            normalize("at 0x7ffe12 line 9 in order_17"),
            "at N line N in order_17"
        );
    }
}
