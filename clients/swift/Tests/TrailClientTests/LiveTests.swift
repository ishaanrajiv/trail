// The client and the generated emitters against a real Trail.
//
// Skipped unless TRAIL_LIVE_URL points at a running server that has the example `shop`
// declarations published. `scripts/e2e` starts one and runs this.

import Foundation
import XCTest
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
@testable import TrailClient

final class LiveTests: XCTestCase {
    private var base: URL!

    override func setUpWithError() throws {
        let text = ProcessInfo.processInfo.environment["TRAIL_LIVE_URL"] ?? ""
        try XCTSkipIf(text.isEmpty, "set TRAIL_LIVE_URL to a running Trail with the shop example published")
        base = URL(string: text)
    }

    private func get(_ path: String) async throws -> [String: Any] {
        let url = try XCTUnwrap(URL(string: base.absoluteString + path))
        let (data, response) = try await URLSession.shared.data(from: url)
        XCTAssertEqual((response as? HTTPURLResponse)?.statusCode, 200, path)
        return try XCTUnwrap(JSONSerialization.jsonObject(with: data) as? [String: Any])
    }

    private func spool() -> URL {
        FileManager.default.temporaryDirectory.appendingPathComponent("trail-live-\(UUID().uuidString)")
    }

    func testEverythingAnAppSendsArrivesClean() async throws {
        let problems = Problems()
        let client = TrailClient(source: "shop", url: base, version: "live-swift", spoolDirectory: spool(),
                                 onProblem: { problems.add($0) })
        let events = ShopEvents(client)
        let metrics = ShopMetrics(client)
        let who = EmitContext(actor: "live-swift", client: "ios", traceId: "live-swift-1")

        events.shopOrderPlaced(items: ["tea"], method: .card, orderId: "swift-1", totalCents: 1200, latencyMs: 80, context: who)
        events.shopOrderPlaced(items: [], method: .cash, orderId: "swift-2", totalCents: 300, context: who)
        events.shopOrderFailed(orderId: "swift-3", reason: .outOfStock, detail: ["sku": "mug"], context: who)
        events.shopPageViewed(path: "/", signedIn: true, loadMs: 12.5, context: who)
        metrics.shopQueueDepth(7, queue: "live-swift")
        metrics.shopEmailsSent(3)
        client.error(kind: "DecodeError", message: "live test", context: ["route": "/live"])
        client.log(.warn, "live test line", stream: "live-swift", fields: ["n": 1])
        let sent = await client.flush()
        XCTAssertTrue(sent)
        XCTAssertEqual(problems.all().map { "\($0.kind): \($0.at) \($0.message)" }, [])

        let foundBody = try await get("/v1/events?actor=live-swift")
        let found = try XCTUnwrap(foundBody["events"] as? [[String: Any]])
        XCTAssertEqual(found.compactMap { $0["name"] as? String }.sorted(),
                       ["shop.order.failed", "shop.order.placed", "shop.order.placed", "shop.page.viewed"])
        for event in found {
            // Stored exactly as declared: nothing marked.
            XCTAssertNil(event["problems"])
            XCTAssertEqual(event["version"] as? String, "live-swift")
            XCTAssertEqual(event["client"] as? String, "ios")
            XCTAssertEqual(event["trace_id"] as? String, "live-swift-1")
        }
        let placed = try XCTUnwrap(found.first { ($0["props"] as? [String: Any])?["order_id"] as? String == "swift-1" })
        let props = try XCTUnwrap(placed["props"] as? [String: Any])
        XCTAssertEqual(props["total_cents"] as? Int, 1200)
        XCTAssertEqual(props["method"] as? String, "card")
        XCTAssertEqual(props["items"] as? [String], ["tea"])
        XCTAssertEqual(props["latency_ms"] as? Int, 80)

        let depthBody = try await get("/v1/metrics/latest?name=shop.queue.depth&label.queue=live-swift")
        let depth = try XCTUnwrap(depthBody["series"] as? [[String: Any]])
        XCTAssertEqual(depth.first?["value"] as? Double, 7)
        let errorsBody = try await get("/v1/errors?source=shop")
        let errors = try XCTUnwrap(errorsBody["errors"] as? [[String: Any]])
        let mine = try XCTUnwrap(errors.first { $0["version"] as? String == "live-swift" })
        XCTAssertEqual(mine["kind"] as? String, "DecodeError")
        let logsBody = try await get("/v1/logs?source=shop&stream=live-swift")
        let logs = try XCTUnwrap(logsBody["logs"] as? [[String: Any]])
        XCTAssertEqual(logs.first?["message"] as? String, "live test line")
        XCTAssertEqual(logs.first?["level"] as? String, "warn")
    }

    func testAWrongRecordComesBackAsAProblem() async throws {
        let problems = Problems()
        let client = TrailClient(source: "shop", url: base, spoolDirectory: spool(), onProblem: { problems.add($0) })
        // Around the generated code on purpose: this is what it exists to prevent.
        client.recordEvent("shop.order.placed", ["order_id": "swift-bad", "total": 1])
        client.recordEvent("Not A Name", [:])
        _ = await client.flush()
        let kinds = problems.all().map(\.kind).sorted()
        XCTAssertEqual(kinds.first, "mismatched")
        XCTAssertEqual(kinds.last, "rejected")
        XCTAssertTrue(problems.all().contains { $0.message.contains("total") })
    }
}

/// Collects the problems a client reports, from whatever thread it reports them on.
private final class Problems: @unchecked Sendable {
    private let lock = NSLock()
    private var list: [TrailProblem] = []

    func add(_ problem: TrailProblem) {
        lock.lock()
        list.append(problem)
        lock.unlock()
    }

    func all() -> [TrailProblem] {
        lock.lock()
        defer { lock.unlock() }
        return list
    }
}
