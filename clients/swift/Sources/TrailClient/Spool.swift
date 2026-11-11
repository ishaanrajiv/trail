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
