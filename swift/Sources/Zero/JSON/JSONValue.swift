/// A JSON value, the data model Zero syncs and stores.
///
/// Numbers are `Double`, as they are in JS: integers are exact up to 2^53.
/// Objects are `JSONObject`s, which keep their keys in JS property order and
/// tell keys apart by UTF-16 code unit, as JS does.
///
/// Equality compares strings by code units, as JS does, not by Swift's
/// canonical equivalence: `"é"` and `"e\u{301}"` are different values.
public enum JSONValue: Sendable {
  case null
  case bool(Bool)
  case number(Double)
  case string(String)
  case array([JSONValue])
  case object(JSONObject)
}

/// A value in a row or a query literal.
package typealias Value = JSONValue

extension JSONValue: Hashable {
  public static func == (lhs: JSONValue, rhs: JSONValue) -> Bool {
    switch (lhs, rhs) {
    case (.null, .null): true
    case (.bool(let a), .bool(let b)): a == b
    case (.number(let a), .number(let b)): a == b
    case (.string(let a), .string(let b)): StringCompare.equal(a, b)
    case (.array(let a), .array(let b)): a == b
    case (.object(let a), .object(let b)): a == b
    default: false
    }
  }

  public func hash(into hasher: inout Hasher) {
    switch self {
    case .null:
      hasher.combine(0)
    case .bool(let b):
      hasher.combine(1)
      hasher.combine(b)
    case .number(let n):
      hasher.combine(2)
      hasher.combine(n)
    case .string(let s):
      hasher.combine(3)
      hasher.combine(s.utf8.count)
      for byte in s.utf8 { hasher.combine(byte) }
    case .array(let a):
      hasher.combine(4)
      hasher.combine(a)
    case .object(let o):
      hasher.combine(5)
      hasher.combine(o)
    }
  }
}

extension JSONValue {
  public var isNull: Bool {
    if case .null = self { return true }
    return false
  }

  public var stringValue: String? {
    if case .string(let s) = self { return s }
    return nil
  }

  public var numberValue: Double? {
    if case .number(let n) = self { return n }
    return nil
  }

  public var boolValue: Bool? {
    if case .bool(let b) = self { return b }
    return nil
  }

  public var arrayValue: [JSONValue]? {
    if case .array(let a) = self { return a }
    return nil
  }

  public var objectValue: JSONObject? {
    if case .object(let o) = self { return o }
    return nil
  }

  public subscript(key: String) -> JSONValue? {
    if case .object(let o) = self { return o[key] }
    return nil
  }

  public subscript(index: Int) -> JSONValue? {
    if case .array(let a) = self, a.indices.contains(index) { return a[index] }
    return nil
  }

  /// The JS `typeof`-style name of the value's type, for error messages.
  public var typeName: String {
    switch self {
    case .null: "null"
    case .bool: "boolean"
    case .number: "number"
    case .string: "string"
    case .array: "array"
    case .object: "object"
    }
  }
}

extension JSONValue: ExpressibleByNilLiteral {
  public init(nilLiteral: ()) { self = .null }
}

extension JSONValue: ExpressibleByBooleanLiteral {
  public init(booleanLiteral value: Bool) { self = .bool(value) }
}

extension JSONValue: ExpressibleByIntegerLiteral {
  public init(integerLiteral value: Int) { self = .number(Double(value)) }
}

extension JSONValue: ExpressibleByFloatLiteral {
  public init(floatLiteral value: Double) { self = .number(value) }
}

extension JSONValue: ExpressibleByStringLiteral {
  public init(stringLiteral value: String) { self = .string(value) }
}

extension JSONValue: ExpressibleByArrayLiteral {
  public init(arrayLiteral elements: JSONValue...) { self = .array(elements) }
}

extension JSONValue: ExpressibleByDictionaryLiteral {
  public init(dictionaryLiteral elements: (String, JSONValue)...) {
    var o = JSONObject(minimumCapacity: elements.count)
    for (k, v) in elements { o[k] = v }
    self = .object(o)
  }
}

extension JSONValue: CustomStringConvertible {
  public var description: String { JSONSerializer.stringify(self) }
}

extension JSONValue: Codable {
  public init(from decoder: any Decoder) throws {
    let container = try decoder.singleValueContainer()
    if container.decodeNil() {
      self = .null
    } else if let b = try? container.decode(Bool.self) {
      self = .bool(b)
    } else if let n = try? container.decode(Double.self) {
      self = .number(n)
    } else if let s = try? container.decode(String.self) {
      self = .string(s)
    } else if let a = try? container.decode([JSONValue].self) {
      self = .array(a)
    } else {
      self = .object(try container.decode(JSONObject.self))
    }
  }

  public func encode(to encoder: any Encoder) throws {
    var container = encoder.singleValueContainer()
    switch self {
    case .null: try container.encodeNil()
    case .bool(let b): try container.encode(b)
    case .number(let n): try container.encode(n)
    case .string(let s): try container.encode(s)
    case .array(let a): try container.encode(a)
    case .object(let o): try container.encode(o)
    }
  }
}
