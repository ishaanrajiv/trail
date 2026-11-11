import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import XCTest
#if canImport(Glibc)
import Glibc
#elseif canImport(Darwin)
import Darwin
#endif

/// A scripted Trail server behind a `URLProtocol`. Each server answers one host
/// (`mock-<n>.example.com`), so a client left over from another test cannot reach it.
///
/// `MockServer(loopback: true)` serves the same script over real HTTP on a loopback port instead,
/// for what a `URLProtocol` cannot show: FoundationNetworking traps when one reports a redirect,
/// so only a real response lets URLSession's own redirect handling run.
final class MockServer: @unchecked Sendable {
    enum Reply {
        case status(Int, String)
        case networkError
        /// A redirect status with `Location: <location>`. Loopback only: through the `URLProtocol`
        /// it is a plain response with that status.
        case redirect(Int, String)
    }

    struct Request {
        var method = "POST"
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
    private var decide: (@Sendable (Request) -> Reply)?
    private var received: [Request] = []
    private var waits: [(Int, XCTestExpectation)] = []

    private let listener: LoopbackListener?

    init(loopback: Bool = false) {
        if loopback {
            let listener = LoopbackListener()
            self.listener = listener
            host = "127.0.0.1:\(listener.port)"
            url = URL(string: "http://\(host)")!
            session = URLSession(configuration: .ephemeral)
            listener.start(self)
            return
        }
        listener = nil
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
        decide = nil
        lock.unlock()
    }

    /// After the scripted replies, answers each request with what `decide` returns for it.
    func answer(_ decide: @escaping @Sendable (Request) -> Reply) {
        lock.lock()
        script = []
        self.decide = decide
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
        let reply = script.isEmpty ? (decide?(request) ?? fallback) : script.removeFirst()
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
        let recorded = MockServer.Request(method: request.httpMethod ?? "GET", url: url,
                                          headers: request.allHTTPHeaderFields ?? [:], body: Self.body(of: request))
        switch server.handle(recorded) {
        case .networkError:
            client?.urlProtocol(self, didFailWithError: URLError(.notConnectedToInternet))
        case .redirect(let status, let location):
            let response = HTTPURLResponse(url: url, statusCode: status, httpVersion: "HTTP/1.1",
                                           headerFields: ["Location": location])!
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocolDidFinishLoading(self)
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

/// A minimal HTTP/1.1 server on 127.0.0.1: one request per connection, answered from a
/// `MockServer`'s script. Its thread runs until the test process exits.
final class LoopbackListener: @unchecked Sendable {
    let port: UInt16
    private let socketFD: Int32

    init() {
        #if os(Linux)
        let fd = socket(AF_INET, Int32(SOCK_STREAM.rawValue), 0)
        #else
        let fd = socket(AF_INET, SOCK_STREAM, 0)
        #endif
        precondition(fd >= 0, "socket failed")
        socketFD = fd
        var address = sockaddr_in()
        #if !os(Linux)
        address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        #endif
        address.sin_family = sa_family_t(AF_INET)
        address.sin_port = 0
        address.sin_addr.s_addr = inet_addr("127.0.0.1")
        var length = socklen_t(MemoryLayout<sockaddr_in>.size)
        let bound = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { bind(fd, $0, length) }
        }
        precondition(bound == 0 && listen(fd, 16) == 0, "cannot listen on 127.0.0.1")
        _ = withUnsafeMutablePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { getsockname(fd, $0, &length) }
        }
        port = UInt16(bigEndian: address.sin_port)
    }

    func start(_ server: MockServer) {
        let thread = Thread { [self] in
            while true {
                let connection = accept(socketFD, nil, nil)
                if connection < 0 { continue }
                serve(connection, server)
                close(connection)
            }
        }
        thread.name = "mock-trail-\(port)"
        thread.start()
    }

    private func serve(_ connection: Int32, _ server: MockServer) {
        #if !os(Linux)
        var on: Int32 = 1
        setsockopt(connection, SOL_SOCKET, SO_NOSIGPIPE, &on, socklen_t(MemoryLayout<Int32>.size))
        #endif
        var data = [UInt8]()
        var buffer = [UInt8](repeating: 0, count: 16_384)
        var head: (end: Int, lines: [String], length: Int)?
        var continued = false
        while true {
            if head == nil, let end = Self.headerEnd(data) {
                let lines = String(decoding: data[..<end], as: UTF8.self).components(separatedBy: "\r\n")
                let length = lines.dropFirst().compactMap { line -> Int? in
                    let parts = line.split(separator: ":", maxSplits: 1)
                    guard parts.count == 2, parts[0].lowercased() == "content-length" else { return nil }
                    return Int(parts[1].trimmingCharacters(in: .whitespaces))
                }.first ?? 0
                head = (end + 4, lines, length)
            }
            if let head {
                if data.count >= head.end + head.length { break }
                if !continued, head.lines.contains(where: { $0.lowercased().hasPrefix("expect: 100-continue") }) {
                    continued = true
                    send(connection, "HTTP/1.1 100 Continue\r\n\r\n")
                }
            }
            let count = recv(connection, &buffer, buffer.count, 0)
            if count <= 0 { return }
            data.append(contentsOf: buffer[0..<count])
        }
        guard let head else { return }
        let requestLine = head.lines[0].split(separator: " ")
        guard requestLine.count >= 2 else { return }
        var headers: [String: String] = [:]
        for line in head.lines.dropFirst() {
            let parts = line.split(separator: ":", maxSplits: 1)
            if parts.count == 2 { headers[String(parts[0])] = parts[1].trimmingCharacters(in: .whitespaces) }
        }
        let request = MockServer.Request(
            method: String(requestLine[0]), url: URL(string: "http://127.0.0.1:\(port)\(requestLine[1])")!,
            headers: headers, body: Data(data[head.end..<(head.end + head.length)]))
        switch server.handle(request) {
        case .networkError:
            return   // closed without an answer
        case .status(let status, let body):
            send(connection, "HTTP/1.1 \(status) Mock\r\nContent-Type: application/json\r\n"
                 + "Content-Length: \(body.utf8.count)\r\nConnection: close\r\n\r\n\(body)")
        case .redirect(let status, let location):
            send(connection, "HTTP/1.1 \(status) Mock\r\nLocation: \(location)\r\n"
                 + "Content-Length: 0\r\nConnection: close\r\n\r\n")
        }
    }

    private static func headerEnd(_ data: [UInt8]) -> Int? {
        guard data.count >= 4 else { return nil }
        for index in 0...(data.count - 4)
        where data[index] == 13 && data[index + 1] == 10 && data[index + 2] == 13 && data[index + 3] == 10 {
            return index
        }
        return nil
    }

    private func send(_ connection: Int32, _ text: String) {
        let bytes = Array(text.utf8)
        var sent = 0
        while sent < bytes.count {
            #if os(Linux)
            let count = bytes[sent...].withUnsafeBytes { Glibc.send(connection, $0.baseAddress, $0.count, Int32(MSG_NOSIGNAL)) }
            #else
            let count = bytes[sent...].withUnsafeBytes { Darwin.send(connection, $0.baseAddress, $0.count, 0) }
            #endif
            if count <= 0 { return }
            sent += count
        }
    }
}
