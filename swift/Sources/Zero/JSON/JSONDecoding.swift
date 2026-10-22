/// Errors from decoding a `JSONValue` into a typed protocol value. The path
/// points at the offending value, like valita's error paths.
package struct DecodingError: Error, CustomStringConvertible, Equatable {
  package var path: [String]
  package var message: String

  package init(_ message: String, path: [String] = []) {
    self.message = message
    self.path = path
  }

  package var description: String {
    path.isEmpty ? message : "\(message) at \(path.joined(separator: "."))"
  }

  func at(_ key: String) -> DecodingError {
    DecodingError(message, path: [key] + path)
  }
}

/// Helpers for hand-written decoders that follow valita's rules: optional
/// fields may be absent but not `null` (unless declared nullable), unknown
/// keys are ignored (`passthrough` mode).
package struct JSONObjectReader {
  package let object: JSONObject

  package init(_ value: JSONValue, _ what: String = "object") throws(DecodingError) {
    guard case .object(let o) = value else {
      throw DecodingError("Expected \(what), got \(value.typeName)")
    }
    object = o
  }

  package func required<T>(
    _ key: String, _ decode: (JSONValue) throws(DecodingError) -> T
  ) throws(DecodingError) -> T {
    guard let v = object[key] else { throw DecodingError("Missing property", path: [key]) }
    do {
      return try decode(v)
    } catch {
      throw error.at(key)
    }
  }

  package func optional<T>(
    _ key: String, _ decode: (JSONValue) throws(DecodingError) -> T
  ) throws(DecodingError) -> T? {
    guard let v = object[key] else { return nil }
    do {
      return try decode(v)
    } catch {
      throw error.at(key)
    }
  }

  package func string(_ key: String) throws(DecodingError) -> String {
    try required(key, JSONDecode.string)
  }

  package func optionalString(_ key: String) throws(DecodingError) -> String? {
    try optional(key, JSONDecode.string)
  }

  package func number(_ key: String) throws(DecodingError) -> Double {
    try required(key, JSONDecode.number)
  }

  package func optionalNumber(_ key: String) throws(DecodingError) -> Double? {
    try optional(key, JSONDecode.number)
  }

  package func bool(_ key: String) throws(DecodingError) -> Bool {
    try required(key, JSONDecode.bool)
  }

  package func optionalBool(_ key: String) throws(DecodingError) -> Bool? {
    try optional(key, JSONDecode.bool)
  }

  /// `v.literal(expected)`.
  package func literal(_ key: String, _ expected: String) throws(DecodingError) {
    let s = try string(key)
    if s != expected {
      throw DecodingError("Expected literal \"\(expected)\", got \"\(s)\"", path: [key])
    }
  }
}

package enum JSONDecode {
  package static func string(_ v: JSONValue) throws(DecodingError) -> String {
    guard case .string(let s) = v else { throw DecodingError("Expected string, got \(v.typeName)") }
    return s
  }

  package static func number(_ v: JSONValue) throws(DecodingError) -> Double {
    guard case .number(let n) = v else { throw DecodingError("Expected number, got \(v.typeName)") }
    return n
  }

  package static func bool(_ v: JSONValue) throws(DecodingError) -> Bool {
    guard case .bool(let b) = v else { throw DecodingError("Expected boolean, got \(v.typeName)") }
    return b
  }

  package static func object(_ v: JSONValue) throws(DecodingError) -> JSONObject {
    guard case .object(let o) = v else { throw DecodingError("Expected object, got \(v.typeName)") }
    return o
  }

  package static func array(_ v: JSONValue) throws(DecodingError) -> [JSONValue] {
    guard case .array(let a) = v else { throw DecodingError("Expected array, got \(v.typeName)") }
    return a
  }

  /// Any JSON value (`jsonSchema`).
  package static func json(_ v: JSONValue) throws(DecodingError) -> JSONValue { v }

  package static func array<T>(
    _ v: JSONValue, _ element: (JSONValue) throws(DecodingError) -> T
  ) throws(DecodingError) -> [T] {
    let a = try array(v)
    var out: [T] = []
    out.reserveCapacity(a.count)
    for (i, e) in a.enumerated() {
      do {
        out.append(try element(e))
      } catch {
        throw error.at(String(i))
      }
    }
    return out
  }

  /// `v.record(element)`. Keys that are canonically equivalent (`"é"` and
  /// `"e\u{301}"`) collide; `keyedRecord` keeps them apart.
  package static func record<T>(
    _ v: JSONValue, _ element: (JSONValue) throws(DecodingError) -> T
  ) throws(DecodingError) -> [String: T] {
    let o = try object(v)
    var out: [String: T] = [:]
    out.reserveCapacity(o.count)
    for (k, e) in o {
      do {
        out[k] = try element(e)
      } catch {
        throw error.at(k)
      }
    }
    return out
  }

  /// `v.record(element)`, keyed by code unit as JS keys are: for records
  /// of data, such as by client ID.
  package static func keyedRecord<T>(
    _ v: JSONValue, _ element: (JSONValue) throws(DecodingError) -> T
  ) throws(DecodingError) -> [JSKey: T] {
    let o = try object(v)
    var out: [JSKey: T] = [:]
    out.reserveCapacity(o.count)
    for (k, e) in o {
      do {
        out[JSKey(k)] = try element(e)
      } catch {
        throw error.at(k)
      }
    }
    return out
  }

  /// `v.record(element)` of JSON values, keeping the object's order.
  package static func objectRecord(
    _ v: JSONValue, _ element: (JSONValue) throws(DecodingError) -> JSONValue
  ) throws(DecodingError) -> JSONObject {
    var o = try object(v)
    for (k, e) in o {
      do {
        o[k] = try element(e)
      } catch {
        throw error.at(k)
      }
    }
    return o
  }

  /// A non-empty array of strings (`CompoundKey`, `PrimaryKey`).
  package static func compoundKey(_ v: JSONValue) throws(DecodingError) -> [String] {
    let k = try array(v, string)
    if k.isEmpty { throw DecodingError("Expected non-empty array") }
    return k
  }

  /// A string, number, or boolean (`primaryKeyValueSchema`).
  package static func primaryKeyValue(_ v: JSONValue) throws(DecodingError) -> JSONValue {
    switch v {
    case .string, .number, .bool: return v
    default: throw DecodingError("Expected string, number, or boolean, got \(v.typeName)")
    }
  }

  /// A tuple message `[tag, body]`.
  package static func tagged(_ v: JSONValue) throws(DecodingError) -> (String, JSONValue) {
    guard case .array(let a) = v, a.count == 2, case .string(let tag) = a[0] else {
      throw DecodingError("Expected [tag, body] tuple")
    }
    return (tag, a[1])
  }
}

/// Builds a JSON object, leaving out `nil` fields.
package struct JSONObjectBuilder {
  package var object = JSONObject()

  package init() {}

  package mutating func set(_ key: String, _ value: JSONValue) {
    object[key] = value
  }

  package mutating func set(_ key: String, _ value: JSONValue?) {
    if let value { object[key] = value }
  }

  package mutating func set(_ key: String, _ value: String?) {
    if let value { object[key] = .string(value) }
  }

  package mutating func set(_ key: String, _ value: Double?) {
    if let value { object[key] = .number(value) }
  }

  package mutating func set(_ key: String, _ value: Bool?) {
    if let value { object[key] = .bool(value) }
  }

  package var value: JSONValue { .object(object) }
}

/// A value with a JSON representation following the `zero-protocol` schema.
package protocol JSONCodable: Sendable {
  init(json: JSONValue) throws(DecodingError)
  var json: JSONValue { get }
}

extension Array where Element == String {
  var json: JSONValue { .array(map { .string($0) }) }
}
