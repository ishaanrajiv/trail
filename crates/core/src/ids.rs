//! Record ids. Senders make a UUIDv7 for each record, so sending it twice stores it once and ids
//! sort by time.

use crate::time::Ts;

/// A UUIDv7 for a record made at `now`: 48 bits of time, then random bits.
pub fn uuid7(now: Ts) -> String {
    let mut bytes = [0u8; 16];
    if getrandom::fill(&mut bytes).is_err() {
        // No system randomness: fall back to the clock's low bits. Ids stay unique enough to tell
        // records apart within one sender, which is all dedupe needs.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
            .to_be_bytes();
        bytes.copy_from_slice(&nanos);
    }
    let ms = u64::try_from(now.ms().max(0)).unwrap_or(0).to_be_bytes();
    bytes[..6].copy_from_slice(&ms[2..]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::names::is_id;

    #[test]
    fn uuid7_is_ordered_by_time_and_unique() {
        let early = uuid7(Ts(1_794_000_000_000));
        let late = uuid7(Ts(1_794_000_000_001));
        assert!(early < late);
        assert_eq!(early.len(), 36);
        assert_eq!(&early[14..15], "7");
        assert!(is_id(&early));
        assert_ne!(uuid7(Ts(5)), uuid7(Ts(5)));
    }
}
