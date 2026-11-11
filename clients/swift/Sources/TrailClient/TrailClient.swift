import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

/// Sends events, metrics, errors and logs to Trail.
///
/// Recording appends to an in-memory queue and returns. A background thread writes the queue to
/// the spool folder every `flushInterval` (or when `maxBatch` records are waiting), one file per
/// batch, and posts the files oldest first to `POST <url>/v1/ingest`. A file is deleted when the
/// server gives a final answer, so nothing is lost while Trail is unreachable or the app is killed.
///
/// `TRAIL_DISABLED=1` makes every call a no-op. `TRAIL_VERSION`, `TRAIL_HOST`, `TRAIL_TOKEN` and
/// `TRAIL_SPOOL_DIR` are read when the matching argument is nil. With `TRAIL_STRICT=1`, `flush()`
/// also returns false when any problem was reported since the last flush.
public final class TrailClient: @unchecked Sendable {
    let core: Core?

    public convenience init(source: String, url: URL, version: String? = nil, host: String? = nil, token: String? = nil,
                            spoolDirectory: URL? = nil, flushInterval: TimeInterval = 1, maxBatch: Int = 200,
                            maxQueue: Int = 10_000, maxSpoolBytes: Int = 50_000_000, session: URLSession = .shared,
                            onProblem: (@Sendable (TrailProblem) -> Void)? = nil) {
        self.init(source: source, url: url, version: version, host: host, token: token,
                  spoolDirectory: spoolDirectory, flushInterval: flushInterval, maxBatch: maxBatch,
                  maxQueue: maxQueue, maxSpoolBytes: maxSpoolBytes, session: session, onProblem: onProblem,
                  timing: Core.Timing())
    }

    /// The designated initializer; `timing` lets tests use short backoffs.
    init(source: String, url: URL, version: String?, host: String?, token: String?,
         spoolDirectory: URL?, flushInterval: TimeInterval, maxBatch: Int,
         maxQueue: Int, maxSpoolBytes: Int, session: URLSession,
         onProblem: (@Sendable (TrailProblem) -> Void)?, timing: Core.Timing) {
        let env = ProcessInfo.processInfo.environment
        if env["TRAIL_DISABLED"] == "1" {
            core = nil
            return
        }
        let directory = spoolDirectory
            ?? env["TRAIL_SPOOL_DIR"].flatMap { $0.isEmpty ? nil : URL(fileURLWithPath: $0, isDirectory: true) }
            ?? TrailClient.defaultSpoolDirectory(source: source)
        core = Core(
            source: source,
            endpoint: TrailClient.endpoint(url),
            version: version ?? env["TRAIL_VERSION"],
            host: host ?? env["TRAIL_HOST"] ?? TrailClient.defaultHost(),
            token: token ?? env["TRAIL_TOKEN"],
            spool: Spool(directory: directory, maxBytes: max(0, maxSpoolBytes)),
            flushInterval: max(0.01, flushInterval),
            maxBatch: max(1, maxBatch),
            maxQueue: max(1, maxQueue),
            strict: env["TRAIL_STRICT"] == "1",
            session: session,
            onProblem: onProblem,
            timing: timing
        )
        core?.start()
    }

    deinit {
        core?.close()
    }

    // MARK: Recording (called by generated code)

    public func recordEvent(_ name: String, _ props: [String: TrailValue], _ context: EmitContext = EmitContext()) {
        guard let core else { return }
        var record: [String: TrailValue] = [
            "id": .string(UUID7.shared.next()),
            "name": .string(name),
            "ts": .int(Int(context.ts ?? nowMs())),
        ]
        if !props.isEmpty { record["props"] = .object(props) }
        if let actor = context.actor { record["actor"] = .string(actor) }
        if let client = context.client { record["client"] = .string(client) }
        if let traceId = context.traceId { record["trace_id"] = .string(traceId) }
        core.record(.events, .object(record))
    }

    public func recordMetric(_ name: String, _ value: Double, labels: [String: String], unit: String?,
                             kind: MetricKind, ts: Int64? = nil) {
        guard let core else { return }
        guard value.isFinite else {
            core.dropUnsendable(TrailProblem(
                kind: "dropped", at: "metric \(name)", message: "value \(value) is not a finite number",
                fix: "record a finite value; JSON has no NaN or infinity"))
            return
        }
        var record: [String: TrailValue] = [
            "name": .string(name),
            "value": .double(value),
            "ts": .int(Int(ts ?? nowMs())),
            "kind": .string(kind.rawValue),
        ]
        if !labels.isEmpty { record["labels"] = .object(labels.mapValues { .string($0) }) }
        if let unit { record["unit"] = .string(unit) }
        core.record(.metrics, .object(record))
    }

    // MARK: Errors and logs

    public func error(kind: String, message: String, stack: String? = nil, severity: Severity = .error,
                      context: [String: TrailValue] = [:], traceId: String? = nil) {
        guard let core else { return }
        var record: [String: TrailValue] = [
            "id": .string(UUID7.shared.next()),
            "ts": .int(Int(nowMs())),
            "severity": .string(severity.rawValue),
            "kind": .string(kind),
            "message": .string(message),
        ]
        if let stack { record["stack"] = .string(stack) }
        if !context.isEmpty { record["context"] = .object(context) }
        if let traceId { record["trace_id"] = .string(traceId) }
        core.record(.errors, .object(record))
    }

    /// Reports a Swift error: `kind` is its type name, `message` its description, `stack` the
    /// current call stack.
    public func error(_ error: Error, severity: Severity = .error, context: [String: TrailValue] = [:], traceId: String? = nil) {
        guard core != nil else { return }
        self.error(kind: TrailClient.kind(of: error), message: TrailClient.message(of: error),
                   stack: Thread.callStackSymbols.joined(separator: "\n"),
                   severity: severity, context: context, traceId: traceId)
    }

    public func log(_ level: LogLevel, _ message: String, stream: String? = nil, fields: [String: TrailValue] = [:]) {
        guard let core else { return }
        var record: [String: TrailValue] = [
            "id": .string(UUID7.shared.next()),
            "ts": .int(Int(nowMs())),
            "level": .string(level.rawValue),
            "message": .string(message),
        ]
        if let stream { record["stream"] = .string(stream) }
        if !fields.isEmpty { record["fields"] = .object(fields) }
        core.record(.logs, .object(record))
    }

    // MARK: Control

    /// Sends everything now, ignoring any retry backoff. True when the queue and spool are empty.
    /// Call it when an app goes to the background.
    public func flush() async -> Bool {
        guard let core else { return true }
        return await core.flush()
    }

    /// Writes the queue to the spool without sending, synchronously. For callers that cannot
    /// wait, like `applicationWillTerminate`. The next start sends it.
    public func persist() {
        core?.persist()
    }

    /// Stops the background thread after writing the queue to the spool. Recording afterwards
    /// does nothing. Called on deinit.
    public func close() {
        core?.close()
    }

    public func stats() -> TrailStats {
        core?.currentStats() ?? TrailStats()
    }

    // MARK: Defaults

    static func endpoint(_ url: URL) -> URL {
        if url.path.hasSuffix("/v1/ingest") { return url }
        return url.appendingPathComponent("v1").appendingPathComponent("ingest")
    }

    static func defaultSpoolDirectory(source: String) -> URL {
        let base = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
            ?? FileManager.default.temporaryDirectory
        return base.appendingPathComponent("trail-spool", isDirectory: true)
            .appendingPathComponent(source, isDirectory: true)
    }

    static func defaultHost() -> String? {
        #if os(iOS) || os(tvOS) || os(watchOS) || os(visionOS)
        // A phone's host name is usually its owner's device name; leave it out.
        return nil
        #else
        let name = ProcessInfo.processInfo.hostName
        return name.isEmpty ? nil : name
        #endif
    }

    static func kind(of error: Error) -> String {
        let name = String(describing: type(of: error))
        if name == "NSError" || name.hasPrefix("__") {
            let ns = error as NSError
            return "\(ns.domain) \(ns.code)"
        }
        return name
    }

    static func message(of error: Error) -> String {
        if let localized = error as? LocalizedError, let description = localized.errorDescription {
            return description
        }
        let name = String(describing: type(of: error))
        if name == "NSError" || name.hasPrefix("__") {
            return error.localizedDescription
        }
        // For a plain Swift error, `localizedDescription` is "The operation couldn't be
        // completed. (… error 1.)"; its description names the case and its values.
        return String(describing: error)
    }
}

// MARK: - The queue and the sender

enum RecordKind: String, CaseIterable {
    case events, metrics, errors, logs
}

final class Core: @unchecked Sendable {
    struct Batch {
        let name: String
        let records: [(RecordKind, TrailValue)]
    }

    enum Outcome {
        case accepted(Data)
        case final(Int, Data)
        case retry(String, fix: String = "check that Trail is running and reachable at this URL")
    }

    static let notTrailFix = "check that `url` points at Trail itself, and that nothing between the app and Trail "
        + "(a proxy, a sign-on page, a captive portal) answers in its place"

    let source: String
    let endpoint: URL
    let version: String?
    let host: String?
    let token: String?
    let spool: Spool
    let flushInterval: TimeInterval
    let maxBatch: Int
    let maxQueue: Int
    let strict: Bool
    let session: URLSession
    let onProblem: (@Sendable (TrailProblem) -> Void)?
    struct Timing {
        var requestTimeout: TimeInterval = 15
        var initialBackoff: TimeInterval = 1
        var maxBackoff: TimeInterval = 60
    }

    let requestTimeout: TimeInterval
    let initialBackoff: TimeInterval
    let maxBackoff: TimeInterval

    // Guarded by `condition`.
    private let condition = NSCondition()
    private var queue: [(RecordKind, TrailValue)] = []
    private var waiters: [CheckedContinuation<Bool, Never>] = []
    private var stopped = false
    private var stats = TrailStats()
    private var queueDropsToReport = 0
    private var problemSinceFlush = false
    private var backoff: TimeInterval = 0
    private var nextAttempt = Date.distantPast
    private var failing = false
    // A write takes batches from the queue and puts them in the spool. It holds a ticket from the
    // moment it drains the queue until its files are written, so `persist()` and `close()` can
    // wait for every write that began before them: records are never only in a drained batch
    // when they return. The lock is never held across disk I/O, so recording never waits on it.
    private var writesBegun = 0
    private var writesInProgress = Set<Int>()
    private var beforeBackgroundWrite: (@Sendable () -> Void)?

    init(source: String, endpoint: URL, version: String?, host: String?, token: String?, spool: Spool,
         flushInterval: TimeInterval, maxBatch: Int, maxQueue: Int, strict: Bool, session: URLSession,
         onProblem: (@Sendable (TrailProblem) -> Void)?, timing: Timing) {
        self.requestTimeout = timing.requestTimeout
        self.initialBackoff = timing.initialBackoff
        self.maxBackoff = timing.maxBackoff
        self.source = source
        self.endpoint = endpoint
        self.version = version
        self.host = host
        self.token = token
        self.spool = spool
        self.flushInterval = flushInterval
        self.maxBatch = maxBatch
        self.maxQueue = maxQueue
        self.strict = strict
        self.session = session
        self.onProblem = onProblem
    }

    func start() {
        if let problem = spool.setupProblem {
            report([TrailProblem(kind: "send", at: "spool", message: problem + "; batches are kept in memory only",
                                 fix: "pass a writable spoolDirectory")])
        }
        let thread = Thread { [self] in run() }
        thread.name = "trail-client"
        thread.start()
    }

    // MARK: Called from any thread

    func record(_ kind: RecordKind, _ value: TrailValue) {
        condition.lock()
        defer { condition.unlock() }
        guard !stopped else { return }
        if queue.count >= maxQueue {
            queue.removeFirst()
            stats.dropped += 1
            queueDropsToReport += 1
        }
        queue.append((kind, value))
        if queue.count >= maxBatch { condition.broadcast() }
    }

    func dropUnsendable(_ problem: TrailProblem) {
        condition.lock()
        stats.dropped += 1
        condition.unlock()
        report([problem])
    }

    func flush() async -> Bool {
        await withCheckedContinuation { (continuation: CheckedContinuation<Bool, Never>) in
            condition.lock()
            if stopped {
                condition.unlock()
                continuation.resume(returning: false)
                return
            }
            waiters.append(continuation)
            condition.broadcast()
            condition.unlock()
        }
    }

    /// Returns once everything recorded before the call is in the spool, including batches the
    /// background thread has drained but not yet written.
    func persist() {
        condition.lock()
        let batches = drainLocked()
        let ticket = beginWriteLocked()
        condition.unlock()
        let problems = write(batches)
        endWrite(ticket, waitForEarlier: true)
        report(problems)
    }

    func close() {
        condition.lock()
        let pending = waiters
        waiters = []
        stopped = true
        let batches = drainLocked()
        let ticket = beginWriteLocked()
        condition.broadcast()
        condition.unlock()
        let problems = write(batches)
        endWrite(ticket, waitForEarlier: true)
        report(problems)
        for waiter in pending { waiter.resume(returning: false) }
    }

    /// Test hook: `hook` runs on the background thread each time it has drained records from the
    /// queue and not yet written them.
    func onBackgroundWrite(_ hook: (@Sendable () -> Void)?) {
        condition.lock()
        beforeBackgroundWrite = hook
        condition.unlock()
    }

    func currentStats() -> TrailStats {
        condition.lock()
        defer { condition.unlock() }
        return stats
    }

    // MARK: The background thread

    private func run() {
        var first = true   // the first cycle runs at once, to send what a previous run left
        while true {
            condition.lock()
            if !first {
                let deadline = Date().addingTimeInterval(flushInterval)
                while !stopped && waiters.isEmpty && queue.count < maxBatch && Date() < deadline {
                    _ = condition.wait(until: deadline)
                }
            }
            if stopped {
                condition.unlock()
                return
            }
            let force = !waiters.isEmpty
            let flushing = waiters
            waiters = []
            // The first cycle only sends what a previous run left; new records wait for a full
            // batch, the interval or a flush, as they always do.
            let batches = first && !force ? [] : drainLocked()
            let ticket = beginWriteLocked()
            let hook = batches.isEmpty ? nil : beforeBackgroundWrite
            first = false
            let queueDrops = queueDropsToReport
            queueDropsToReport = 0
            let due = force || Date() >= nextAttempt
            condition.unlock()

            hook?()
            var problems: [TrailProblem] = []
            if queueDrops > 0 {
                problems.append(TrailProblem(
                    kind: "dropped", at: "queue",
                    message: "\(queueDrops) records dropped, oldest first: the queue holds \(maxQueue)",
                    fix: "raise maxQueue, record less, or flush more often"))
            }
            problems += write(batches)
            // The ticket ends before `onProblem` runs, so a handler that calls `persist()` cannot
            // wait on its own write.
            endWrite(ticket, waitForEarlier: false)
            report(problems)
            if due { sendSpool() }

            condition.lock()
            var done = queue.isEmpty && spool.isEmpty
            if strict && problemSinceFlush && !flushing.isEmpty {
                done = false
            }
            if !flushing.isEmpty { problemSinceFlush = false }
            condition.unlock()
            for waiter in flushing { waiter.resume(returning: done) }
        }
    }

    /// Takes the whole queue as batches of at most `maxBatch`. Names are made here, in queue order,
    /// so spool files sort the way the records were recorded.
    private func drainLocked() -> [Batch] {
        guard !queue.isEmpty else { return [] }
        var batches: [Batch] = []
        var start = 0
        while start < queue.count {
            let end = min(start + maxBatch, queue.count)
            batches.append(Batch(name: UUID7.shared.next() + ".json", records: Array(queue[start..<end])))
            start = end
        }
        queue.removeAll(keepingCapacity: true)
        return batches
    }

    /// Starts a write; call with `condition` held, in the same hold as the drain it covers.
    private func beginWriteLocked() -> Int {
        writesBegun += 1
        writesInProgress.insert(writesBegun)
        return writesBegun
    }

    /// Ends a write. With `waitForEarlier`, also waits until every write begun before it has ended.
    private func endWrite(_ ticket: Int, waitForEarlier: Bool) {
        condition.lock()
        writesInProgress.remove(ticket)
        condition.broadcast()
        if waitForEarlier {
            while writesInProgress.contains(where: { $0 < ticket }) { condition.wait() }
        }
        condition.unlock()
    }

    /// Writes batches to the spool and returns the problems met, for the caller to report once
    /// its write has ended.
    private func write(_ batches: [Batch]) -> [TrailProblem] {
        var problems: [TrailProblem] = []
        for batch in batches {
            let (eviction, problem) = spool.add(name: batch.name, data: body(batch), records: batch.records.count)
            if let problem {
                problems.append(TrailProblem(kind: "send", at: "spool", message: problem + "; the batch is kept in memory",
                                             fix: "check that the spool folder is writable and the disk has room"))
            }
            if eviction.files > 0 {
                condition.lock()
                stats.dropped += eviction.records
                condition.unlock()
                problems.append(TrailProblem(
                    kind: "dropped", at: "spool",
                    message: "\(eviction.records) records in \(eviction.files) unsent batches dropped, oldest first: the spool holds \(spool.maxBytes) bytes",
                    fix: "raise maxSpoolBytes, or make Trail reachable"))
            }
        }
        return problems
    }

    func body(_ batch: Batch) -> Data {
        var object: [String: TrailValue] = ["source": .string(source)]
        if let version { object["version"] = .string(version) }
        if let host { object["host"] = .string(host) }
        for kind in RecordKind.allCases {
            let records = batch.records.filter { $0.0 == kind }.map { $0.1 }
            if !records.isEmpty { object[kind.rawValue] = .array(records) }
        }
        return TrailValue.object(object).jsonData
    }

    /// Posts spool files oldest first until one gets no final answer.
    private func sendSpool() {
        var tried = Set<String>()
        while !isStopped, let next = spool.next(excluding: tried) {
            let (entry, data) = next
            tried.insert(entry.name)
            let outcome = post(data)
            // Too large for Trail, and more than one record: send it as two halves, which take its
            // place in the spool (done before `finishedSending`, so no bound can take it meanwhile).
            // They are newer entries, not in `tried`, so this loop sends them; each split leaves
            // smaller batches, so it ends.
            if case .final(413, _) = outcome, entry.records > 1, let halves = Spool.split(data) {
                let problem = spool.replace(entry.name, with: halves.map { (UUID7.shared.next() + ".json", $0.data, $0.records) })
                spool.finishedSending()
                condition.lock()
                reachedLocked()
                condition.unlock()
                if let problem {
                    report([TrailProblem(kind: "send", at: "spool", message: problem + "; the batch is kept in memory",
                                         fix: "check that the spool folder is writable and the disk has room")])
                }
                continue
            }
            spool.finishedSending()
            switch outcome {
            case .accepted(let response):
                spool.remove(entry.name)
                let problems = Core.problems(in: response, request: data)
                condition.lock()
                stats.sent += entry.records
                stats.rejected += problems.filter { $0.kind == "rejected" }.count
                stats.mismatched += problems.filter { $0.kind == "mismatched" }.count
                reachedLocked()
                condition.unlock()
                report(problems)
            case .final(let status, let response):
                spool.remove(entry.name)
                condition.lock()
                stats.dropped += entry.records
                reachedLocked()
                condition.unlock()
                report([Core.finalProblem(status: status, response: response, records: entry.records)])
            case .retry(let reason, let fix):
                let waiting = spool.markAllSpooled()
                condition.lock()
                stats.spooled += waiting
                backoff = backoff == 0 ? initialBackoff : min(backoff * 2, maxBackoff)
                nextAttempt = Date().addingTimeInterval(backoff)
                let wasFailing = failing
                failing = true
                problemSinceFlush = true
                let wait = backoff
                condition.unlock()
                if !wasFailing {
                    report([TrailProblem(
                        kind: "send", at: endpoint.absoluteString,
                        message: "Trail did not take the batch (\(reason)); it is kept in the spool and retried in \(Int(wait.rounded())) s, then with backoff",
                        fix: fix)])
                }
                return
            }
        }
    }

    private var isStopped: Bool {
        condition.lock()
        defer { condition.unlock() }
        return stopped
    }

    private func reachedLocked() {
        backoff = 0
        nextAttempt = .distantPast
        failing = false
    }

    private func post(_ data: Data) -> Outcome {
        var request = URLRequest(url: endpoint, timeoutInterval: requestTimeout)
        request.httpMethod = "POST"
        request.httpBody = data
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        if let token, !token.isEmpty {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        let result = ResultBox()
        let done = DispatchSemaphore(value: 0)
        let task = session.dataTask(with: request) { body, response, error in
            result.set(body: body, response: response, error: error)
            done.signal()
        }
        // A task's own delegate is asked before the session's, so this holds for a session the
        // caller passed in, whatever its delegate does, while that delegate still handles the rest.
        task.delegate = RefuseRedirects()
        task.resume()
        if done.wait(timeout: .now() + requestTimeout + 5) == .timedOut {
            task.cancel()
            return .retry("no answer in \(Int(requestTimeout)) s")
        }
        let (body, response, error) = result.get()
        if let error { return .retry(error.localizedDescription) }
        guard let http = response as? HTTPURLResponse else { return .retry("not an HTTP response") }
        // Should a redirect be followed anyway (by a URLProtocol, say), the answer is from some
        // other page, not from Trail: keep the batch.
        if let answered = http.url, Core.isRedirect(from: endpoint, to: answered) {
            return .retry("redirected to \(answered.absoluteString)", fix: Core.notTrailFix)
        }
        switch http.statusCode {
        case 200..<300:
            // Only Trail's ingest answer means Trail took the batch: a proxy's catch-all page, a
            // sign-on wall or a captive portal can answer 200 with HTML.
            let answer = body ?? Data()
            guard Core.isIngestAnswer(answer) else {
                return .retry("the answer did not come from Trail: status \(http.statusCode) with a body that is not "
                              + "Trail's ingest answer (a JSON object with `accepted`); \(Core.preview(answer))",
                              fix: Core.notTrailFix)
            }
            return .accepted(answer)
        case 300..<400: return .retry("status \(http.statusCode)", fix: Core.notTrailFix)
        case 400, 401, 413: return .final(http.statusCode, body ?? Data())
        default: return .retry("status \(http.statusCode)")
        }
    }

    /// True when a 2xx body is Trail's answer to `POST /v1/ingest`: a JSON object with `accepted`
    /// (`spec/ingest.md`).
    static func isIngestAnswer(_ body: Data) -> Bool {
        guard let object = (try? JSONSerialization.jsonObject(with: body)) as? [String: Any] else { return false }
        return object["accepted"] != nil
    }

    static func preview(_ body: Data) -> String {
        let text = String(decoding: body.prefix(80), as: UTF8.self)
            .split(whereSeparator: { $0.isWhitespace }).joined(separator: " ")
        return text.isEmpty ? "it is empty" : "it begins `\(text)`"
    }

    /// True when the answer came from another page than the one posted to: another path, host or
    /// scheme. Case in the host and scheme is not a difference.
    static func isRedirect(from endpoint: URL, to answered: URL) -> Bool {
        answered.path != endpoint.path
            || answered.host?.lowercased() != endpoint.host?.lowercased()
            || answered.scheme?.lowercased() != endpoint.scheme?.lowercased()
    }

    static func finalProblem(status: Int, response: Data, records: Int) -> TrailProblem {
        var at = "batch"
        var detail = String(decoding: response.prefix(500), as: UTF8.self)
        var fix: String
        switch status {
        case 401: fix = "pass the write token this source needs as `token:`"
        case 413: fix = "the record is over Trail's size limit on its own; send smaller props, messages or fields"
        default: fix = "this is a client bug or a bad `source`; source must be lowercase letters, digits and hyphens"
        }
        if let object = (try? JSONSerialization.jsonObject(with: response)) as? [String: Any],
           let error = object["error"] as? [String: Any] {
            at = error["at"] as? String ?? at
            detail = error["message"] as? String ?? detail
            if let serverFix = error["fix"] as? String, !serverFix.isEmpty { fix = serverFix }
        }
        let message = "status \(status), \(records) records dropped" + (detail.isEmpty ? "" : ": \(detail)")
        return TrailProblem(kind: "dropped", at: at, message: message, fix: fix)
    }

    /// The response's `rejected` and `mismatched` entries. `at` gets the record's name added
    /// when the request has one at that place (`events[0] shop.order.placed`).
    static func problems(in response: Data, request: Data) -> [TrailProblem] {
        guard let object = (try? JSONSerialization.jsonObject(with: response)) as? [String: Any] else { return [] }
        let rejected = object["rejected"] as? [[String: Any]] ?? []
        let mismatched = object["mismatched"] as? [[String: Any]] ?? []
        if rejected.isEmpty && mismatched.isEmpty { return [] }
        let sent = (try? JSONSerialization.jsonObject(with: request)) as? [String: Any] ?? [:]
        func name(at place: String) -> String? {
            guard let open = place.firstIndex(of: "["), let close = place.firstIndex(of: "]"), open < close,
                  let index = Int(place[place.index(after: open)..<close]),
                  let list = sent[String(place[..<open])] as? [[String: Any]],
                  list.indices.contains(index) else { return nil }
            return list[index]["name"] as? String
        }
        func convert(_ entry: [String: Any], kind: String) -> TrailProblem {
            var at = entry["at"] as? String ?? ""
            if let name = name(at: at) { at += " \(name)" }
            return TrailProblem(kind: kind, at: at, message: entry["message"] as? String ?? "",
                                fix: entry["fix"] as? String ?? "")
        }
        return rejected.map { convert($0, kind: "rejected") } + mismatched.map { convert($0, kind: "mismatched") }
    }

    private func report(_ problems: [TrailProblem]) {
        guard !problems.isEmpty else { return }
        condition.lock()
        problemSinceFlush = true
        condition.unlock()
        guard let onProblem else { return }
        for problem in problems { onProblem(problem) }
    }
}

/// Never follows a redirect. URLSession turns a redirected POST into a GET of the new location,
/// so a login page's `200` would look like Trail taking the batch; refused, the `3xx` itself is the
/// answer, which is retried like any other unexpected status.
private final class RefuseRedirects: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    func urlSession(_ session: URLSession, task: URLSessionTask, willPerformHTTPRedirection response: HTTPURLResponse,
                    newRequest request: URLRequest, completionHandler: @escaping @Sendable (URLRequest?) -> Void) {
        completionHandler(nil)
    }
}

private final class ResultBox: @unchecked Sendable {
    private let lock = NSLock()
    private var body: Data?
    private var response: URLResponse?
    private var error: Error?

    func set(body: Data?, response: URLResponse?, error: Error?) {
        lock.lock()
        self.body = body
        self.response = response
        self.error = error
        lock.unlock()
    }

    func get() -> (Data?, URLResponse?, Error?) {
        lock.lock()
        defer { lock.unlock() }
        return (body, response, error)
    }
}
