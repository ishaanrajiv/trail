import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import XCTest
@testable import TrailClient

/// Collects what `onProblem` is given.
final class ProblemLog: @unchecked Sendable {
    private let lock = NSLock()
    private var items: [TrailProblem] = []

    var problems: [TrailProblem] {
        lock.lock()
        defer { lock.unlock() }
        return items
    }

    func of(_ kind: String) -> [TrailProblem] { problems.filter { $0.kind == kind } }

    var handler: @Sendable (TrailProblem) -> Void {
        { [self] problem in
            lock.lock()
            items.append(problem)
            lock.unlock()
        }
    }
}

final class TrailClientTests: XCTestCase {
    private var directories: [URL] = []
    private var clients: [TrailClient] = []

    override func tearDown() {
        for client in clients { client.close() }
        clients = []
        for directory in directories { try? FileManager.default.removeItem(at: directory) }
        directories = []
        super.tearDown()
    }

    private func temporaryDirectory() -> URL {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("trail-test-\(UUID().uuidString)", isDirectory: true)
        directories.append(directory)
        return directory
    }

    private func spoolFiles(_ directory: URL) -> [String] {
        ((try? FileManager.default.contentsOfDirectory(atPath: directory.path)) ?? [])
            .filter { $0.hasSuffix(".json") }.sorted()
    }

    private func makeClient(_ server: MockServer, directory: URL? = nil, version: String? = "9cf5634",
                            host: String? = "web-1", token: String? = nil, flushInterval: TimeInterval = 60,
                            maxBatch: Int = 200, maxQueue: Int = 10_000, maxSpoolBytes: Int = 50_000_000,
                            problems: ProblemLog? = nil, backoff: TimeInterval = 1) -> TrailClient {
        let client = TrailClient(
            source: "shop", url: server.url, version: version, host: host, token: token,
            spoolDirectory: directory ?? temporaryDirectory(), flushInterval: flushInterval,
            maxBatch: maxBatch, maxQueue: maxQueue, maxSpoolBytes: maxSpoolBytes, session: server.session,
            onProblem: problems?.handler,
            timing: Core.Timing(requestTimeout: 5, initialBackoff: backoff, maxBackoff: backoff * 4))
        clients.append(client)
        return client
    }

    private func ids(_ requests: [MockServer.Request], _ kind: String = "events") -> [String] {
        requests.flatMap { $0.records(kind) }.compactMap { $0["id"] as? String }
    }

    // MARK: Recording and batching

    func testRecordingReturnsAtOnce() {
        let server = MockServer()
        let client = makeClient(server, maxBatch: 5_000)
        let start = Date()
        for index in 0..<2_000 {
            client.recordEvent("shop.order.placed", ["order_id": .string("o-\(index)")])
        }
        XCTAssertLessThan(Date().timeIntervalSince(start), 1.0)
        XCTAssertTrue(server.requests.isEmpty, "nothing is sent before a batch fills or the interval passes")
    }

    func testBatchesBySize() async {
        let server = MockServer()
        let client = makeClient(server, maxBatch: 3)
        let two = server.expectRequests(2)
        for index in 0..<7 {
            client.recordEvent("shop.order.placed", ["n": .int(index)])
        }
        await fulfillment(of: [two], timeout: 5)
        let flushed = await client.flush()
        XCTAssertTrue(flushed)
        let requests = server.requests
        XCTAssertTrue(requests.allSatisfy { $0.recordCount <= 3 }, "\(requests.map(\.recordCount))")
        let numbers = requests.flatMap { $0.records("events") }.compactMap { ($0["props"] as? [String: Any])?["n"] as? Int }
        XCTAssertEqual(numbers, Array(0..<7))
    }

    func testBatchesByInterval() async {
        let server = MockServer()
        let client = makeClient(server, flushInterval: 0.05)
        let one = server.expectRequests(1)
        client.recordEvent("shop.order.placed", [:])
        client.recordEvent("shop.order.placed", [:])
        await fulfillment(of: [one], timeout: 5)
        XCTAssertEqual(server.requests.first?.recordCount, 2)
    }

    // MARK: The request body

    func testBodyMatchesIngestSpec() async {
        let server = MockServer()
        let client = makeClient(server)
        client.recordEvent("shop.order.placed",
                           ["order_id": "o-17", "total_cents": .int(1200), "ratio": .double(0.5), "whole": .double(2),
                            "items": ["a"]],
                           EmitContext(actor: "user", client: "web", traceId: "req-81", ts: 1_794_000_000_000))
        client.recordEvent("shop.cart.viewed", [:])
        client.recordMetric("shop.queue.depth", 12, labels: ["queue": "email"], unit: "jobs", kind: .gauge,
                            ts: 1_794_000_000_000)
        client.recordMetric("shop.emails.sent", 3, labels: [:], unit: nil, kind: .counter)
        client.error(kind: "KeyError", message: "missing key 42", stack: "at pay()", severity: .fatal,
                     context: ["route": "/pay"], traceId: "req-81")
        client.error(kind: "Timeout", message: "slow")
        client.log(.info, "started", stream: "worker", fields: ["pid": 81])
        client.log(.warn, "plain")
        let flushed = await client.flush()
        XCTAssertTrue(flushed)

        XCTAssertEqual(server.requests.count, 1)
        guard let request = server.requests.first else { return }
        XCTAssertEqual(request.url.path, "/v1/ingest")
        XCTAssertEqual(request.headers["Content-Type"], "application/json")
        XCTAssertNil(request.headers["Authorization"])
        XCTAssertFalse(request.text.contains("null"), request.text)

        let body = request.json
        XCTAssertEqual(Set(body.keys), ["source", "version", "host", "events", "metrics", "errors", "logs"])
        XCTAssertEqual(body["source"] as? String, "shop")
        XCTAssertEqual(body["version"] as? String, "9cf5634")
        XCTAssertEqual(body["host"] as? String, "web-1")

        let events = request.records("events")
        XCTAssertEqual(events.count, 2)
        XCTAssertEqual(Set(events[0].keys), ["id", "name", "ts", "actor", "client", "trace_id", "props"])
        XCTAssertEqual(events[0]["ts"] as? Int, 1_794_000_000_000)
        XCTAssertEqual(events[0]["actor"] as? String, "user")
        XCTAssertEqual(events[0]["client"] as? String, "web")
        XCTAssertEqual(events[0]["trace_id"] as? String, "req-81")
        XCTAssertEqual(Set(events[1].keys), ["id", "name", "ts"], "empty props and missing context are left out")
        let ts = events[1]["ts"] as? Int ?? 0
        XCTAssertLessThan(abs(Int64(ts) - nowMs()), 5_000)

        // Integers stay integers and doubles stay floating, keys sorted.
        XCTAssertTrue(request.text.contains(
            #""props":{"items":["a"],"order_id":"o-17","ratio":0.5,"total_cents":1200,"whole":2.0}"#), request.text)

        let metrics = request.records("metrics")
        XCTAssertEqual(Set(metrics[0].keys), ["name", "value", "ts", "labels", "unit", "kind"])
        XCTAssertEqual(metrics[0]["labels"] as? [String: String], ["queue": "email"])
        XCTAssertEqual(metrics[0]["kind"] as? String, "gauge")
        XCTAssertEqual(Set(metrics[1].keys), ["name", "value", "ts", "kind"])
        XCTAssertEqual(metrics[1]["kind"] as? String, "counter")
        XCTAssertTrue(request.text.contains(#""value":12.0"#), request.text)

        let errors = request.records("errors")
        XCTAssertEqual(Set(errors[0].keys), ["id", "ts", "severity", "kind", "message", "stack", "context", "trace_id"])
        XCTAssertEqual(errors[0]["severity"] as? String, "fatal")
        XCTAssertEqual(errors[0]["context"] as? [String: String], ["route": "/pay"])
        XCTAssertEqual(Set(errors[1].keys), ["id", "ts", "severity", "kind", "message"])
        XCTAssertEqual(errors[1]["severity"] as? String, "error")

        let logs = request.records("logs")
        XCTAssertEqual(Set(logs[0].keys), ["id", "ts", "level", "message", "stream", "fields"])
        XCTAssertTrue(request.text.contains(#""fields":{"pid":81}"#), request.text)
        XCTAssertEqual(Set(logs[1].keys), ["id", "ts", "level", "message"])
        XCTAssertEqual(logs[1]["level"] as? String, "warn")
    }

    func testBatchFieldsLeftOutWhenNotGiven() async {
        let server = MockServer()
        let client = makeClient(server, version: nil, host: nil)
        client.log(.info, "only a log")
        _ = await client.flush()
        let body = server.requests.first?.json ?? [:]
        XCTAssertEqual(Set(body.keys).subtracting(["host"]), ["source", "logs"])
    }

    func testBearerToken() async {
        let server = MockServer()
        let client = makeClient(server, token: "s3cret-example")
        client.log(.info, "hello")
        _ = await client.flush()
        XCTAssertEqual(server.requests.first?.headers["Authorization"], "Bearer s3cret-example")
    }

    func testSwiftError() async {
        enum PaymentError: Error { case declined(code: Int) }
        struct Described: LocalizedError { var errorDescription: String? { "card expired" } }
        let server = MockServer()
        let client = makeClient(server)
        client.error(PaymentError.declined(code: 51), context: ["order_id": "o-1"])
        client.error(Described(), severity: .warning)
        _ = await client.flush()
        let errors = server.requests.first?.records("errors") ?? []
        XCTAssertEqual(errors.count, 2)
        XCTAssertEqual(errors.first?["kind"] as? String, "PaymentError")
        XCTAssertEqual(errors.first?["message"] as? String, "declined(code: 51)")
        XCTAssertFalse((errors.first?["stack"] as? String ?? "").isEmpty)
        XCTAssertEqual(errors.last?["kind"] as? String, "Described")
        XCTAssertEqual(errors.last?["message"] as? String, "card expired")
        XCTAssertEqual(errors.last?["severity"] as? String, "warning")
    }

    // MARK: Ids

    func testIdsAreTimeOrderedUUIDv7() async throws {
        let pattern = try NSRegularExpression(pattern: "^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
        let generated = (0..<5_000).map { _ in UUID7.shared.next() }
        for id in generated {
            XCTAssertEqual(pattern.numberOfMatches(in: id, range: NSRange(id.startIndex..., in: id)), 1, id)
        }
        XCTAssertEqual(Set(generated).count, generated.count)
        XCTAssertEqual(generated, generated.sorted())
        let first = generated[0]
        let ms = Int64(first.replacingOccurrences(of: "-", with: "").prefix(12), radix: 16) ?? 0
        XCTAssertLessThan(abs(ms - nowMs()), 5_000, "the first 48 bits are the time in ms")

        let server = MockServer()
        let client = makeClient(server)
        for _ in 0..<50 { client.recordEvent("shop.order.placed", [:]) }
        for _ in 0..<5 { client.log(.info, "x") }
        _ = await client.flush()
        let sent = ids(server.requests) + ids(server.requests, "logs")
        XCTAssertEqual(sent.count, 55)
        XCTAssertEqual(Set(sent).count, 55)
        XCTAssertEqual(ids(server.requests), ids(server.requests).sorted())
    }

    // MARK: Spool and retry

    func testServerErrorSpoolsAndResendsOldestFirst() async throws {
        try await checkRetry(.status(503, "unavailable"))
    }

    func testNetworkErrorSpoolsAndResendsOldestFirst() async throws {
        try await checkRetry(.networkError)
    }

    private func checkRetry(_ failure: MockServer.Reply) async throws {
        let server = MockServer()
        server.always(failure)
        let directory = temporaryDirectory()
        let problems = ProblemLog()
        let client = makeClient(server, directory: directory, problems: problems)

        client.recordEvent("shop.order.placed", ["n": 1])
        var flushed = await client.flush()
        XCTAssertFalse(flushed)
        XCTAssertEqual(spoolFiles(directory).count, 1)
        client.recordEvent("shop.order.placed", ["n": 2])
        flushed = await client.flush()
        XCTAssertFalse(flushed)
        let files = spoolFiles(directory)
        XCTAssertEqual(files.count, 2)
        let spooledIds = try files.flatMap { name -> [String] in
            let data = try Data(contentsOf: directory.appendingPathComponent(name))
            let body = try JSONSerialization.jsonObject(with: data) as? [String: Any] ?? [:]
            return (body["events"] as? [[String: Any]] ?? []).compactMap { $0["id"] as? String }
        }
        XCTAssertEqual(spooledIds.count, 2)
        XCTAssertEqual(client.stats().spooled, 2)
        XCTAssertEqual(problems.of("send").count, 1, "a failure is reported once until Trail answers")

        // Each file is the exact body that was posted.
        let firstFile = try Data(contentsOf: directory.appendingPathComponent(files[0]))
        XCTAssertEqual(firstFile, server.requests[0].body)

        server.always(.status(200, MockServer.ok))
        let before = server.requests.count
        flushed = await client.flush()
        XCTAssertTrue(flushed)
        XCTAssertEqual(spoolFiles(directory), [])
        let resent = Array(server.requests[before...])
        XCTAssertEqual(resent.count, 2)
        XCTAssertEqual(resent.compactMap { ($0.records("events").first?["props"] as? [String: Any])?["n"] as? Int }, [1, 2])
        XCTAssertEqual(ids(resent), spooledIds)
        XCTAssertEqual(client.stats().sent, 2)
    }

    func testBackgroundLoopRetriesWithBackoff() async {
        let server = MockServer()
        server.reply(.status(500, ""), .networkError)
        let directory = temporaryDirectory()
        let client = makeClient(server, directory: directory, flushInterval: 0.02, backoff: 0.05)
        let delivered = server.expectRequests(3)
        client.recordEvent("shop.order.placed", [:])
        await fulfillment(of: [delivered], timeout: 5)
        let requests = server.requests
        XCTAssertEqual(Set(ids(requests)).count, 1, "the same id every time")
        let done = await client.flush()
        XCTAssertTrue(done)
        XCTAssertEqual(server.requests.count, 3, "nothing is sent again after a 200")
    }

    func testFinalStatusesAreDroppedNotRetried() async {
        for status in [400, 401, 413] {
            let server = MockServer()
            server.always(.status(status, #"{"error":{"at":"source","message":"bad","fix":"use `shop`"}}"#))
            let directory = temporaryDirectory()
            let problems = ProblemLog()
            let client = makeClient(server, directory: directory, problems: problems)
            client.recordEvent("shop.order.placed", [:])
            client.log(.info, "x")
            let flushed = await client.flush()
            XCTAssertTrue(flushed, "\(status)")
            XCTAssertEqual(spoolFiles(directory), [], "\(status)")
            XCTAssertEqual(client.stats().dropped, 2, "\(status)")
            let dropped = problems.of("dropped")
            XCTAssertEqual(dropped.count, 1, "\(status)")
            XCTAssertEqual(dropped.first?.at, "source")
            XCTAssertTrue(dropped.first?.message.contains("status \(status)") ?? false)
            XCTAssertEqual(dropped.first?.fix, "use `shop`")
            _ = await client.flush()
            XCTAssertEqual(server.requests.count, 1, "\(status) is not retried")
            XCTAssertEqual(spoolFiles(directory), [])
        }
    }

    func testQueueBoundDropsOldest() async {
        let server = MockServer()
        let problems = ProblemLog()
        let client = makeClient(server, maxQueue: 5, problems: problems)
        for index in 0..<8 { client.recordEvent("shop.order.placed", ["n": .int(index)]) }
        XCTAssertEqual(client.stats().dropped, 3)
        _ = await client.flush()
        let numbers = server.requests.flatMap { $0.records("events") }.compactMap { ($0["props"] as? [String: Any])?["n"] as? Int }
        XCTAssertEqual(numbers, [3, 4, 5, 6, 7])
        XCTAssertEqual(problems.of("dropped").count, 1)
    }

    func testSpoolBoundDropsOldest() async {
        let server = MockServer()
        server.always(.status(503, ""))
        let directory = temporaryDirectory()
        let problems = ProblemLog()
        // One event batch is about 150 bytes, so 400 bytes holds two.
        let client = makeClient(server, directory: directory, maxSpoolBytes: 400, problems: problems)
        for index in 0..<4 {
            client.recordEvent("shop.order.placed", ["n": .int(index)])
            _ = await client.flush()
        }
        XCTAssertEqual(spoolFiles(directory).count, 2)
        XCTAssertEqual(client.stats().dropped, 2)
        XCTAssertEqual(problems.of("dropped").count, 2)

        server.always(.status(200, MockServer.ok))
        let before = server.requests.count
        let flushed = await client.flush()
        XCTAssertTrue(flushed)
        let numbers = server.requests[before...].flatMap { $0.records("events") }
            .compactMap { ($0["props"] as? [String: Any])?["n"] as? Int }
        XCTAssertEqual(numbers, [2, 3])
    }

    func testRejectedAndMismatchedGoToOnProblem() async {
        let server = MockServer()
        server.reply(.status(200, """
        {"accepted":{"events":1,"metrics":0,"errors":0,"logs":0},"duplicates":0,
         "rejected":[{"at":"events[1]","message":"`Order` is not an event name","fix":"use two or more snake_case parts"}],
         "mismatched":[{"at":"events[0]","message":"unknown property `latncy_ms`"}]}
        """))
        let problems = ProblemLog()
        let client = makeClient(server, problems: problems)
        client.recordEvent("shop.order.placed", ["latncy_ms": 3])
        client.recordEvent("Order", [:])
        let flushed = await client.flush()
        XCTAssertTrue(flushed)
        XCTAssertEqual(problems.problems, [
            TrailProblem(kind: "rejected", at: "events[1] Order", message: "`Order` is not an event name",
                         fix: "use two or more snake_case parts"),
            TrailProblem(kind: "mismatched", at: "events[0] shop.order.placed", message: "unknown property `latncy_ms`", fix: ""),
        ])
        let stats = client.stats()
        XCTAssertEqual(stats.rejected, 1)
        XCTAssertEqual(stats.mismatched, 1)
        XCTAssertEqual(stats.sent, 2)
    }

    func testNewInstanceSendsSpoolLeftByPreviousOne() async {
        let directory = temporaryDirectory()
        let offline = MockServer()
        offline.always(.networkError)
        let first = makeClient(offline, directory: directory)
        first.recordEvent("shop.order.placed", ["n": 1])
        _ = await first.flush()
        first.close()
        let leftIds = ids(offline.requests)
        XCTAssertEqual(spoolFiles(directory).count, 1)

        let online = MockServer()
        let arrived = online.expectRequests(1)
        _ = makeClient(online, directory: directory)   // no flush: it sends at start
        await fulfillment(of: [arrived], timeout: 5)
        XCTAssertEqual(ids(online.requests), leftIds)
    }

    func testPersistThenNewInstanceLosesNothing() async {
        let directory = temporaryDirectory()
        let offline = MockServer()
        offline.always(.networkError)
        let first = makeClient(offline, directory: directory, maxBatch: 2)
        first.recordEvent("shop.order.placed", ["n": 1])
        first.recordEvent("shop.order.placed", ["n": 2])
        first.recordEvent("shop.order.placed", ["n": 3])
        first.persist()
        XCTAssertGreaterThanOrEqual(spoolFiles(directory).count, 2, "persist writes before it returns")
        first.close()

        let online = MockServer()
        let second = makeClient(online, directory: directory)
        let flushed = await second.flush()
        XCTAssertTrue(flushed)
        let numbers = online.requests.flatMap { $0.records("events") }.compactMap { ($0["props"] as? [String: Any])?["n"] as? Int }
        XCTAssertEqual(numbers.sorted(), [1, 2, 3])
        XCTAssertEqual(spoolFiles(directory), [])
    }

    func testDisabledByEnvironment() async {
        setenv("TRAIL_DISABLED", "1", 1)
        defer { unsetenv("TRAIL_DISABLED") }
        let server = MockServer()
        let directory = temporaryDirectory()
        let client = makeClient(server, directory: directory)
        client.recordEvent("shop.order.placed", [:])
        let flushed = await client.flush()
        XCTAssertTrue(flushed)
        XCTAssertTrue(server.requests.isEmpty)
        XCTAssertEqual(spoolFiles(directory), [])
    }

    func testNonFiniteMetricIsDroppedAndReported() async {
        let server = MockServer()
        let problems = ProblemLog()
        let client = makeClient(server, problems: problems)
        client.recordMetric("shop.queue.depth", .nan, labels: [:], unit: nil, kind: .gauge)
        let flushed = await client.flush()
        XCTAssertTrue(flushed)
        XCTAssertTrue(server.requests.isEmpty)
        XCTAssertEqual(client.stats().dropped, 1)
        XCTAssertEqual(problems.of("dropped").count, 1)
    }

    // MARK: The generated file

    func testGeneratedEmitters() async {
        let server = MockServer()
        let client = makeClient(server)
        let events = ShopEvents(client)
        let metrics = ShopMetrics(client)
        events.shopOrderFailed(orderId: "o-17", reason: .outOfStock, detail: ["sku": "a-1", "left": 0],
                               context: EmitContext(actor: "user", ts: 1_794_000_000_000))
        events.shopOrderFailed(orderId: "o-18", reason: .payment)
        metrics.shopEmailsSent(41, ts: 1_794_000_000_000)
        metrics.shopQueueDepth(12, queue: "email", ts: 1_794_000_000_000)
        let flushed = await client.flush()
        XCTAssertTrue(flushed)
        guard let request = server.requests.first else { return XCTFail("no request") }
        let events_ = request.records("events")
        XCTAssertEqual(events_.count, 2)
        guard let id = events_.first?["id"] as? String else { return XCTFail("no id") }
        XCTAssertTrue(request.text.contains(
            #"{"actor":"user","id":"\#(id)","name":"shop.order.failed","#
            + #""props":{"detail":{"left":0,"sku":"a-1"},"order_id":"o-17","reason":"out-of-stock"},"ts":1794000000000}"#),
            request.text)
        XCTAssertTrue(request.text.contains(#""props":{"order_id":"o-18","reason":"payment"}"#), request.text)
        XCTAssertTrue(request.text.contains(
            #""metrics":[{"kind":"counter","name":"shop.emails.sent","ts":1794000000000,"value":41.0},"#
            + #"{"kind":"gauge","labels":{"queue":"email"},"name":"shop.queue.depth","ts":1794000000000,"unit":"jobs","value":12.0}]"#),
            request.text)
    }
}

final class TrailValueTests: XCTestCase {
    func testLiterals() {
        let value: TrailValue = ["n": 1, "x": 1.5, "ok": true, "none": nil, "s": "hi", "list": [1, "a"], "o": ["k": "v"]]
        XCTAssertEqual(value, .object([
            "n": .int(1), "x": .double(1.5), "ok": .bool(true), "none": .null, "s": .string("hi"),
            "list": .array([.int(1), .string("a")]), "o": .object(["k": .string("v")]),
        ]))
    }

    func testEncoding() {
        let value: TrailValue = ["b": [1, 2.0, -0.25, 1e-7, 3e20], "a": ["z": nil, "y": false], "c": .int(-9_007_199_254_740_993)]
        XCTAssertEqual(value.jsonString, #"{"a":{"y":false,"z":null},"b":[1,2.0,-0.25,1e-07,3e+20],"c":-9007199254740993}"#)
        XCTAssertEqual(TrailValue.double(.infinity).jsonString, "null")
        XCTAssertEqual(TrailValue.array([]).jsonString, "[]")
        XCTAssertEqual(TrailValue.object([:]).jsonString, "{}")
    }

    func testStringEscaping() throws {
        let text = "quote \" slash \\ / newline \n tab \t cr \r bell \u{07} nul \u{00} é 日本 🚀"
        let encoded = TrailValue.string(text).jsonString
        XCTAssertEqual(encoded, #""quote \" slash \\ / newline \n tab \t cr \r bell \u0007 nul \u0000 é 日本 🚀""#)
        let decoded = try JSONSerialization.jsonObject(with: Data("[\(encoded)]".utf8)) as? [String]
        XCTAssertEqual(decoded, [text])
        XCTAssertEqual(TrailValue.object(["ключ \"k\"": 1]).jsonString, #"{"ключ \"k\"":1}"#)
    }
}
