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
    equal(lhs, rhs, depth: 0)
  }

  /// How deep `==` recurses before it goes on with a stack of its own.
  /// Recursion is faster, and this deep takes little of a thread's stack.
  static let maxRecursion = 64

  private static func equal(_ lhs: JSONValue, _ rhs: JSONValue, depth: Int) -> Bool {
    switch (lhs, rhs) {
    case (.null, .null): return true
    case (.bool(let a), .bool(let b)): return a == b
    case (.number(let a), .number(let b)): return a == b
    case (.string(let a), .string(let b)): return StringCompare.equal(a, b)
    case (.array(let a), .array(let b)):
      guard a.count == b.count else { return false }
      if depth >= maxRecursion { return deepEqual(lhs, rhs) }
      return a.elementsEqual(b) { equal($0, $1, depth: depth + 1) }
    case (.object(let a), .object(let b)):
      if depth >= maxRecursion { return deepEqual(lhs, rhs) }
      if a.hasSameStorage(as: b) { return true }
      guard a.count == b.count else { return false }
      for i in 0..<a.count {
        let (key, value) = a[i]
        // Usually the keys are in the same order.
        if StringCompare.equal(key, b.key(at: i)) {
          if !equal(value, b.value(at: i), depth: depth + 1) { return false }
        } else {
          guard let j = b.index(forKey: key), equal(value, b.value(at: j), depth: depth + 1)
          else { return false }
        }
      }
      return true
    default:
      return false
    }
  }

  /// `==` without recursion, for values nested past `maxRecursion`: the
  /// arrays and objects being compared are on a stack, so nesting takes
  /// heap, not stack.
  private static func deepEqual(_ lhs: JSONValue, _ rhs: JSONValue) -> Bool {
    var pending: [(JSONValue, JSONValue)] = []
    var pair = (lhs, rhs)
    while true {
      switch pair {
      case (.array(let a), .array(let b)):
        guard a.count == b.count else { return false }
        for i in a.indices.reversed() { pending.append((a[i], b[i])) }
      case (.object(let a), .object(let b)):
        guard a.count == b.count else { return false }
        for i in 0..<a.count {
          let key = a.key(at: i)
          let j = StringCompare.equal(key, b.key(at: i)) ? i : b.index(forKey: key)
          guard let j else { return false }
          pending.append((a.value(at: i), b.value(at: j)))
        }
      default:
        if !scalarEqual(pair.0, pair.1) { return false }
      }
      guard let next = pending.popLast() else { return true }
      pair = next
    }
  }

  /// Whether two values are equal scalars.
  private static func scalarEqual(_ lhs: JSONValue, _ rhs: JSONValue) -> Bool {
    switch (lhs, rhs) {
    case (.null, .null): true
    case (.bool(let a), .bool(let b)): a == b
    case (.number(let a), .number(let b)): a == b
    case (.string(let a), .string(let b)): StringCompare.equal(a, b)
    default: false
    }
  }

  /// Hashes without recursion, as `==` compares. An object's hash doesn't
  /// depend on the order of its keys, as its equality doesn't.
  public func hash(into hasher: inout Hasher) {
    if Self.hashScalar(self, into: &hasher) { return }
    // The hasher values go into: the caller's, then each object's hasher
    // of the entry being hashed.
    var hashers = [hasher]
    defer { hasher = hashers[0] }
    var frames: [HashFrame] = []
    var value = self
    nested: while true {
      switch value {
      case .array(let a):
        hashers[hashers.count - 1].combine(4)
        hashers[hashers.count - 1].combine(a.count)
        frames.append(.array(a, next: 0))
      case .object(let o):
        frames.append(.object(o, next: 0, combined: 0, inEntry: false))
      default:
        preconditionFailure("not an array or object")
      }
      // Hashes values until one is an array or object, finishing the
      // containers that are done.
      while let frame = frames.popLast() {
        switch frame {
        case .array(let a, var i):
          while i < a.count {
            let e = a[i]
            i += 1
            if Self.hashScalar(e, into: &hashers[hashers.count - 1]) { continue }
            frames.append(.array(a, next: i))
            value = e
            continue nested
          }
        case .object(let o, var i, var combined, let inEntry):
          // Each entry is hashed alone, and the entries combined in any order.
          if inEntry { combined ^= hashers.removeLast().finalize() }
          while i < o.count {
            let (key, v) = o[i]
            i += 1
            var h = Hasher()
            h.combine(JSKey(key))
            if Self.hashScalar(v, into: &h) {
              combined ^= h.finalize()
              continue
            }
            hashers.append(h)
            frames.append(.object(o, next: i, combined: combined, inEntry: true))
            value = v
            continue nested
          }
          hashers[hashers.count - 1].combine(5)
          hashers[hashers.count - 1].combine(o.count)
          hashers[hashers.count - 1].combine(combined)
        }
      }
      return
    }
  }

  /// Hashes `value` unless it is an array or object; returns whether it did.
  private static func hashScalar(_ value: JSONValue, into hasher: inout Hasher) -> Bool {
    switch value {
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
    case .array, .object:
      return false
    }
    return true
  }
}

/// An array or object being hashed, and the index of its next value.
private enum HashFrame {
  case array([JSONValue], next: Int)
  /// `combined` is the entries hashed so far; `inEntry`, whether the
  /// hasher of an entry whose value is an array or object is on `hashers`.
  case object(JSONObject, next: Int, combined: Int, inEntry: Bool)
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
