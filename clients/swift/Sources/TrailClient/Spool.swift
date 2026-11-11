import Foundation

/// The batches not yet answered, one file per batch: `<uuid7>.json`, the exact request body.
///
/// Every batch is written here before it is sent and deleted on a final answer, so a batch in
/// flight when the app is killed is sent again at the next launch. If the folder cannot be
/// written, the batch is kept in memory instead and sent the same way.
final class Spool: @unchecked Sendable {
    struct Entry {
        let name: String
        let size: Int
        let records: Int
        /// The body, when it could not be written to disk.
        let memory: Data?
        var countedAsSpooled = false
    }

    struct Eviction {
        var files = 0
        var records = 0
    }

    let directory: URL?
    let maxBytes: Int
    private let lock = NSLock()
    private var entries: [Entry] = []   // sorted by name, which is by age
    private var inFlight: String?

    /// The problem met creating the folder, if any.
    private(set) var setupProblem: String?

    init(directory: URL?, maxBytes: Int) {
        self.directory = directory
        self.maxBytes = maxBytes
        guard let directory else { return }
        let files = FileManager.default
        do {
            try files.createDirectory(at: directory, withIntermediateDirectories: true)
            let names = try files.contentsOfDirectory(atPath: directory.path)
            for name in names.sorted() where Spool.isSpoolName(name) {
                let url = directory.appendingPathComponent(name)
                guard let data = try? Data(contentsOf: url) else { continue }
                entries.append(Entry(name: name, size: data.count, records: Spool.countRecords(data), memory: nil))
            }
        } catch {
            setupProblem = "cannot use spool folder \(directory.path): \(error)"
        }
    }

    static func isSpoolName(_ name: String) -> Bool {
        guard name.hasSuffix(".json"), name.count == 41 else { return false }
        return name.dropLast(5).allSatisfy { $0 == "-" || $0.isHexDigit }
    }

    static func countRecords(_ data: Data) -> Int {
        guard let body = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any] else { return 0 }
        return ["events", "metrics", "errors", "logs"].reduce(0) { total, key in
            total + ((body[key] as? [Any])?.count ?? 0)
        }
    }

    /// A request body of two or more records cut into two halves, each with the batch fields
    /// (`source`, `version`, `host`) of the whole, with the records in each; nil when it cannot be
    /// read or holds fewer than two records. For a `413`: sent on their own, one oversized record
    /// ends up alone. Records are copied byte for byte, so a double written `2.0` stays a double.
    static func split(_ data: Data) -> [(data: Data, records: Int)]? {
        var scanner = JSONScanner(bytes: [UInt8](data))
        let kinds = ["\"events\"", "\"metrics\"", "\"errors\"", "\"logs\""]
        var fields: [Range<Int>] = []                       // `"key":value`, copied whole
        var records: [(kind: String, range: Range<Int>)] = []
        guard scanner.take(UInt8(ascii: "{")) else { return nil }
        if !scanner.take(UInt8(ascii: "}")) {
            repeat {
                guard let key = scanner.value(), scanner.take(UInt8(ascii: ":")) else { return nil }
                let name = String(decoding: scanner.bytes[key], as: UTF8.self)
                if kinds.contains(name) {
                    guard scanner.take(UInt8(ascii: "[")) else { return nil }
                    if !scanner.take(UInt8(ascii: "]")) {
                        repeat {
                            guard let record = scanner.value() else { return nil }
                            records.append((name, record))
                        } while scanner.take(UInt8(ascii: ","))
                        guard scanner.take(UInt8(ascii: "]")) else { return nil }
                    }
                } else {
                    guard let value = scanner.value() else { return nil }
                    fields.append(key.lowerBound..<value.upperBound)
                }
            } while scanner.take(UInt8(ascii: ","))
            guard scanner.take(UInt8(ascii: "}")) else { return nil }
        }
        guard records.count >= 2 else { return nil }
        let middle = records.count / 2
        return [Array(records[..<middle]), Array(records[middle...])].map { part in
            var members = fields.map { Data(scanner.bytes[$0]) }
            for kind in kinds {
                let items = part.filter { $0.kind == kind }.map { Data(scanner.bytes[$0.range]) }
                if !items.isEmpty {
                    members.append(Data("\(kind):[".utf8) + Data(items.joined(separator: Data(",".utf8))) + Data("]".utf8))
                }
            }
            return (Data("{".utf8) + Data(members.joined(separator: Data(",".utf8))) + Data("}".utf8), part.count)
        }
    }

    var isEmpty: Bool {
        lock.lock()
        defer { lock.unlock() }
        return entries.isEmpty
    }

    var totalBytes: Int {
        lock.lock()
        defer { lock.unlock() }
        return entries.reduce(0) { $0 + $1.size }
    }

    /// Adds a batch, then drops the oldest batches while the spool is over `maxBytes`.
    /// Returns what was dropped, and the error if the file could not be written.
    func add(name: String, data: Data, records: Int) -> (Eviction, String?) {
        var memory: Data?
        var writeProblem: String?
        if let directory {
            do {
                try data.write(to: directory.appendingPathComponent(name), options: .atomic)
            } catch {
                memory = data
                writeProblem = "cannot write \(directory.appendingPathComponent(name).path): \(error)"
            }
        } else {
            memory = data
        }
        lock.lock()
        defer { lock.unlock() }
        let entry = Entry(name: name, size: data.count, records: records, memory: memory)
        let index = entries.firstIndex { $0.name > name } ?? entries.endIndex
        entries.insert(entry, at: index)

        var eviction = Eviction()
        var total = entries.reduce(0) { $0 + $1.size }
        while total > maxBytes, let victim = entries.firstIndex(where: { $0.name != inFlight }) {
            let gone = entries.remove(at: victim)
            deleteFile(gone)
            total -= gone.size
            eviction.files += 1
            eviction.records += gone.records
        }
        return (eviction, writeProblem)
    }

    /// The oldest batch not in `tried`, marked as in flight so a bound cannot delete it.
    func next(excluding tried: Set<String>) -> (Entry, Data)? {
        lock.lock()
        defer { lock.unlock() }
        for entry in entries where !tried.contains(entry.name) {
            let data: Data?
            if let memory = entry.memory {
                data = memory
            } else if let directory {
                data = try? Data(contentsOf: directory.appendingPathComponent(entry.name))
            } else {
                data = nil
            }
            guard let data else { continue }
            inFlight = entry.name
            return (entry, data)
        }
        return nil
    }

    /// Puts the batches a batch was split into in its place. Their files are written before its
    /// own is deleted, so a crash in between leaves records twice (harmless: they have ids), never
    /// lost. A part that cannot be written is kept in memory, as `add` does. Returns that problem.
    func replace(_ name: String, with parts: [(name: String, data: Data, records: Int)]) -> String? {
        var written: [Entry] = []
        var writeProblem: String?
        for part in parts {
            var memory: Data?
            if let directory {
                let url = directory.appendingPathComponent(part.name)
                do {
                    try part.data.write(to: url, options: .atomic)
                } catch {
                    memory = part.data
                    writeProblem = writeProblem ?? "cannot write \(url.path): \(error)"
                }
            } else {
                memory = part.data
            }
            written.append(Entry(name: part.name, size: part.data.count, records: part.records, memory: memory))
        }
        lock.lock()
        defer { lock.unlock() }
        guard let index = entries.firstIndex(where: { $0.name == name }) else {
            for entry in written { deleteFile(entry) }   // already gone: nothing to put in its place
            return writeProblem
        }
        let original = entries.remove(at: index)
        for var entry in written {
            entry.countedAsSpooled = original.countedAsSpooled
            entries.insert(entry, at: entries.firstIndex { $0.name > entry.name } ?? entries.endIndex)
        }
        deleteFile(original)
        return writeProblem
    }

    func finishedSending() {
        lock.lock()
        inFlight = nil
        lock.unlock()
    }

    /// Deletes a batch after a final answer. Returns false if a bound already removed it.
    @discardableResult
    func remove(_ name: String) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard let index = entries.firstIndex(where: { $0.name == name }) else { return false }
        deleteFile(entries.remove(at: index))
        return true
    }

    /// Marks every batch as counted in `stats().spooled`; returns the records not counted before.
    func markAllSpooled() -> Int {
        lock.lock()
        defer { lock.unlock() }
        var records = 0
        for index in entries.indices where !entries[index].countedAsSpooled {
            entries[index].countedAsSpooled = true
            records += entries[index].records
        }
        return records
    }

    private func deleteFile(_ entry: Entry) {
        guard entry.memory == nil, let directory else { return }
        try? FileManager.default.removeItem(at: directory.appendingPathComponent(entry.name))
    }
}

/// Finds where JSON values start and end without building them. Brackets are counted rather than
/// recursed into, so a deeply nested value cannot exhaust the stack.
struct JSONScanner {
    let bytes: [UInt8]
    var index = 0

    mutating func skipSpace() {
        while index < bytes.count, [0x20, 0x09, 0x0A, 0x0D].contains(bytes[index]) { index += 1 }
    }

    /// Steps over `byte` after any whitespace; false, without moving, when something else is next.
    mutating func take(_ byte: UInt8) -> Bool {
        skipSpace()
        guard index < bytes.count, bytes[index] == byte else { return false }
        index += 1
        return true
    }

    /// The range of the value that starts after any whitespace, which it steps over; nil when
    /// there is none.
    mutating func value() -> Range<Int>? {
        skipSpace()
        let start = index
        var depth = 0
        var inString = false
        while index < bytes.count {
            let byte = bytes[index]
            if inString {
                if byte == UInt8(ascii: "\\") {
                    index += 2
                    continue
                }
                index += 1
                if byte == UInt8(ascii: "\"") {
                    inString = false
                    if depth == 0 { return start..<index }
                }
                continue
            }
            switch byte {
            case UInt8(ascii: "\""):
                inString = true
            case UInt8(ascii: "{"), UInt8(ascii: "["):
                depth += 1
            case UInt8(ascii: "}"), UInt8(ascii: "]"):
                if depth == 0 { return index > start ? start..<index : nil }   // the end of what holds a number or literal
                depth -= 1
                if depth == 0 {
                    index += 1
                    return start..<index
                }
            case UInt8(ascii: ","), 0x20, 0x09, 0x0A, 0x0D:
                if depth == 0 { return index > start ? start..<index : nil }
            default:
                break
            }
            index += 1
        }
        return depth == 0 && !inString && index > start ? start..<index : nil
    }
}
