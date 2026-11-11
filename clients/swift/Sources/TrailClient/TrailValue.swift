import Foundation

/// A JSON value: a property of an event, a field of a log, the context of an error.
///
/// `int` and `double` stay apart on the wire: `.int(2)` is written `2` and `.double(2)` is
/// written `2.0`, so the server sees the type the declaration names.
public indirect enum TrailValue: Sendable, Equatable {
    case null, bool(Bool), int(Int), double(Double), string(String), array([TrailValue]), object([String: TrailValue])
}

extension TrailValue: ExpressibleByNilLiteral {
    public init(nilLiteral: ()) { self = .null }
}

extension TrailValue: ExpressibleByBooleanLiteral {
    public init(booleanLiteral value: Bool) { self = .bool(value) }
}

extension TrailValue: ExpressibleByIntegerLiteral {
    public init(integerLiteral value: Int) { self = .int(value) }
}

extension TrailValue: ExpressibleByFloatLiteral {
    public init(floatLiteral value: Double) { self = .double(value) }
}

extension TrailValue: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) { self = .string(value) }
}

extension TrailValue: ExpressibleByArrayLiteral {
    public init(arrayLiteral elements: TrailValue...) { self = .array(elements) }
}

extension TrailValue: ExpressibleByDictionaryLiteral {
    public init(dictionaryLiteral elements: (String, TrailValue)...) {
        var object: [String: TrailValue] = [:]
        for (key, value) in elements { object[key] = value }
        self = .object(object)
    }
}

extension TrailValue {
    /// The value as compact JSON, object keys sorted. A `double` that is NaN or infinite has no
    /// JSON form and is written `null`.
    public var jsonString: String {
        var out = ""
        write(to: &out)
        return out
    }

    var jsonData: Data { Data(jsonString.utf8) }

    func write(to out: inout String) {
        switch self {
        case .null:
            out += "null"
        case .bool(let value):
            out += value ? "true" : "false"
        case .int(let value):
            out += String(value)
        case .double(let value):
            out += TrailValue.format(value)
        case .string(let value):
            TrailValue.writeString(value, to: &out)
        case .array(let values):
            out += "["
            for (index, value) in values.enumerated() {
                if index > 0 { out += "," }
                value.write(to: &out)
            }
            out += "]"
        case .object(let object):
            out += "{"
            for (index, key) in object.keys.sorted().enumerated() {
                if index > 0 { out += "," }
                TrailValue.writeString(key, to: &out)
                out += ":"
                object[key, default: .null].write(to: &out)
            }
            out += "}"
        }
    }

    static func format(_ value: Double) -> String {
        guard value.isFinite else { return "null" }
        // Swift's description is the shortest form that reads back exactly, and always has a
        // `.` or an exponent (`2.0`, `1e-05`, `1e+16`), so it stays a float in JSON.
        let text = value.description
        if text.contains(".") || text.contains("e") || text.contains("E") { return text }
        return text + ".0"
    }

    static func writeString(_ value: String, to out: inout String) {
        out += "\""
        for scalar in value.unicodeScalars {
            switch scalar {
            case "\"": out += "\\\""
            case "\\": out += "\\\\"
            case "\n": out += "\\n"
            case "\r": out += "\\r"
            case "\t": out += "\\t"
            case "\u{08}": out += "\\b"
            case "\u{0C}": out += "\\f"
            default:
                if scalar.value < 0x20 {
                    let hex = String(scalar.value, radix: 16)
                    out += "\\u" + String(repeating: "0", count: 4 - hex.count) + hex
                } else {
                    out.unicodeScalars.append(scalar)
                }
            }
        }
        out += "\""
    }
}
