import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import XCTest

/// A scripted Trail server behind a `URLProtocol`. Each server answers one host
/// (`mock-<n>.example.com`), so a client left over from another test cannot reach it.
final class MockServer: @unchecked Sendable {
    enum Reply {
        case status(Int, String)
        case networkError
    }

    struct Request {
        let url: URL
        let headers: [String: String]
        let body: Data
        var text: String { String(decoding: body, as: UTF8.self) }
        var json: [String: Any] { ((try? JSONSerialization.jsonObject(with: body)) as? [String: Any]) ?? [:] }
        func records(_ kind: String) -> [[String: Any]] { json[kind] as? [[String: Any]] ?? [] }
        var recordCount: Int {
            ["events", "metrics", "errors", "logs"].reduce(0) { $0 + records($1).count }
        }
    }

    static let ok = #"{"accepted":{"events":0,"metrics":0,"errors":0,"logs":0},"duplicates":0,"rejected":[],"mismatched":[]}"#

    private final class Registry: @unchecked Sendable {
        let lock = NSLock()
        var servers: [String: MockServer] = [:]
        var counter = 0
    }

    private static let registry = Registry()

    let host: String
    let url: URL
    let session: URLSession
    private let lock = NSLock()
    private var script: [Reply] = []
    private var fallback: Reply = .status(200, MockServer.ok)
    private var received: [Request] = []
    private var waits: [(Int, XCTestExpectation)] = []

    init() {
        let registry = MockServer.registry
        registry.lock.lock()
        registry.counter += 1
        host = "mock-\(registry.counter).example.com"
        registry.lock.unlock()
        url = URL(string: "http://\(host)")!
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [MockProtocol.self]
        session = URLSession(configuration: configuration)
        registry.lock.lock()
        registry.servers[host] = self
        registry.lock.unlock()
    }

    static func server(for host: String?) -> MockServer? {
        guard let host else { return nil }
        registry.lock.lock()
        defer { registry.lock.unlock() }
        return registry.servers[host]
    }

    /// Replies used in order; after they run out, `always` (200 by default).
    func reply(_ replies: Reply...) {
        lock.lock()
        script.append(contentsOf: replies)
        lock.unlock()
    }

    func always(_ reply: Reply) {
        lock.lock()
        script = []
        fallback = reply
        lock.unlock()
    }

    var requests: [Request] {
        lock.lock()
        defer { lock.unlock() }
        return received
    }

    /// Fulfilled once `count` requests in all have arrived.
    func expectRequests(_ count: Int) -> XCTestExpectation {
        let expectation = XCTestExpectation(description: "\(count) requests to \(host)")
        lock.lock()
        if received.count >= count {
            expectation.fulfill()
        } else {
            waits.append((count, expectation))
        }
        lock.unlock()
        return expectation
    }

    func handle(_ request: Request) -> Reply {
        lock.lock()
        received.append(request)
        let reply = script.isEmpty ? fallback : script.removeFirst()
        let ready = waits.filter { $0.0 <= received.count }
        waits.removeAll { $0.0 <= received.count }
        lock.unlock()
        for (_, expectation) in ready { expectation.fulfill() }
        return reply
    }
}

final class MockProtocol: URLProtocol {
    override class func canInit(with request: URLRequest) -> Bool {
        MockServer.server(for: request.url?.host) != nil
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let url = request.url, let server = MockServer.server(for: url.host) else {
            client?.urlProtocol(self, didFailWithError: URLError(.unsupportedURL))
            return
        }
        let recorded = MockServer.Request(url: url, headers: request.allHTTPHeaderFields ?? [:], body: Self.body(of: request))
        switch server.handle(recorded) {
        case .networkError:
            client?.urlProtocol(self, didFailWithError: URLError(.notConnectedToInternet))
        case .status(let status, let body):
            let response = HTTPURLResponse(url: url, statusCode: status, httpVersion: "HTTP/1.1",
                                           headerFields: ["Content-Type": "application/json"])!
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: Data(body.utf8))
            client?.urlProtocolDidFinishLoading(self)
        }
    }

    override func stopLoading() {}

    static func body(of request: URLRequest) -> Data {
        if let body = request.httpBody { return body }
        guard let stream = request.httpBodyStream else { return Data() }
        stream.open()
        defer { stream.close() }
        var data = Data()
        var buffer = [UInt8](repeating: 0, count: 4096)
        while stream.hasBytesAvailable {
            let count = stream.read(&buffer, maxLength: buffer.count)
            if count <= 0 { break }
            data.append(buffer, count: count)
        }
        return data
    }
}
