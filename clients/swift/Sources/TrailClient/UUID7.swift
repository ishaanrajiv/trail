import Foundation

/// UUIDv7 ids (RFC 9562): a 48-bit ms timestamp, version 7, variant `10`, and random bits.
///
/// Ids made in the same millisecond use the 12 bits after the version as a counter (RFC 9562
/// method 1), so ids from one process are strictly increasing and spool files sort by age.
final class UUID7: @unchecked Sendable {
    static let shared = UUID7()

    private let lock = NSLock()
    private var lastMs: UInt64 = 0
    private var counter: UInt16 = 0

    func next(nowMs: UInt64 = UInt64(max(0, Date().timeIntervalSince1970 * 1000))) -> String {
        var random = SystemRandomNumberGenerator()
        lock.lock()
        var ms = max(nowMs, lastMs)
        if ms == lastMs {
            counter += 1
            if counter > 0xFFF {
                ms += 1
                counter = UInt16.random(in: 0..<0x800, using: &random)
            }
        } else {
            // Start low so the counter has room within the millisecond.
            counter = UInt16.random(in: 0..<0x800, using: &random)
        }
        lastMs = ms
        let count = counter
        lock.unlock()

        var bytes = [UInt8](repeating: 0, count: 16)
        for index in 0..<6 {
            bytes[index] = UInt8(truncatingIfNeeded: ms >> (8 * (5 - UInt64(index))))
        }
        bytes[6] = 0x70 | (UInt8(truncatingIfNeeded: count >> 8) & 0x0F)
        bytes[7] = UInt8(truncatingIfNeeded: count)
        let tail = random.next() as UInt64
        for index in 8..<16 {
            bytes[index] = UInt8(truncatingIfNeeded: tail >> (8 * UInt64(index - 8)))
        }
        bytes[8] = 0x80 | (bytes[8] & 0x3F)

        let hex = "0123456789abcdef".map { $0 }
        var text = ""
        text.reserveCapacity(36)
        for (index, byte) in bytes.enumerated() {
            if index == 4 || index == 6 || index == 8 || index == 10 { text.append("-") }
            text.append(hex[Int(byte >> 4)])
            text.append(hex[Int(byte & 0x0F)])
        }
        return text
    }
}

func nowMs() -> Int64 {
    Int64(Date().timeIntervalSince1970 * 1000)
}
