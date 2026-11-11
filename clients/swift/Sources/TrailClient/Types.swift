import Foundation

/// Who and what an event is about. Every field is optional; `ts` (ms) defaults to now.
public struct EmitContext: Sendable {
    public var actor: String?, client: String?, traceId: String?, ts: Int64?

    public init(actor: String? = nil, client: String? = nil, traceId: String? = nil, ts: Int64? = nil) {
        self.actor = actor
        self.client = client
        self.traceId = traceId
        self.ts = ts
    }
}

public enum MetricKind: String, Sendable { case gauge, counter }

public enum Severity: String, Sendable { case warning, error, fatal }

public enum LogLevel: String, Sendable { case trace, debug, info, warn, error, fatal }

/// Something the client could not do or the server refused, passed to `onProblem`.
///
/// `kind` is `"rejected"` or `"mismatched"` (an entry of the server's response), `"dropped"`
/// (records thrown away: a full queue or spool, a `400`, `401` or `413`) or `"send"` (Trail
/// could not be reached, or the spool could not be written; the records are kept).
public struct TrailProblem: Sendable, Equatable {
    public let kind: String, at: String, message: String, fix: String

    public init(kind: String, at: String, message: String, fix: String) {
        self.kind = kind
        self.at = at
        self.message = message
        self.fix = fix
    }
}

/// Running totals, counted in records.
public struct TrailStats: Sendable, Equatable {
    /// Records in batches the server answered with a `2xx`.
    public var sent: Int
    /// Records in batches that got no answer or a `5xx` and were kept for a retry (each counted once).
    public var spooled: Int
    /// Records thrown away: a full queue or spool, or a `400`, `401` or `413`.
    public var dropped: Int
    /// Entries in the `rejected` lists of the server's responses.
    public var rejected: Int
    /// Entries in the `mismatched` lists of the server's responses.
    public var mismatched: Int

    public init(sent: Int = 0, spooled: Int = 0, dropped: Int = 0, rejected: Int = 0, mismatched: Int = 0) {
        self.sent = sent
        self.spooled = spooled
        self.dropped = dropped
        self.rejected = rejected
        self.mismatched = mismatched
    }
}
