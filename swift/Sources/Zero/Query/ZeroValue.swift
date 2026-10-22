/// A value that doesn't fit the Swift type a row declares for it.
public struct ZeroValueError: Error, Sendable, Hashable, CustomStringConvertible {
  public var description: String

  public init(_ description: String) {
    self.description = description
  }

  @usableFromInline
  static func mismatch(_ expected: String, _ value: JSONValue) -> ZeroValueError {
    ZeroValueError("expected \(expected), got \(value.typeName) \(value)")
  }
}

/// A Swift type a column holds. Generated row structs use these for their
/// columns; the query builder encodes compared values with them.
public protocol ZeroValue: Sendable {
  /// Reads a value from a row; a missing column reads as `.null`.
  init(zeroValue: JSONValue) throws(ZeroValueError)
  var zeroValue: JSONValue { get }
  /// What a row's null reads as when the schema says the column can't be
  /// null; nil to fail. Databases drift from schemas, and JS shows such a
  /// row with the null rather than failing, so strings, numbers, and
  /// booleans read as `""`, `0`, and `false`. Declare the column
  /// `optional()` to read the null.
  static var valueForNull: Self? { get }
}

extension ZeroValue {
  @inlinable
  public static var valueForNull: Self? { nil }
}

extension String: ZeroValue {
  @inlinable
  public init(zeroValue: JSONValue) throws(ZeroValueError) {
    guard case .string(let s) = zeroValue else { throw .mismatch("a string", zeroValue) }
    self = s
  }

  public var zeroValue: JSONValue { .string(self) }

  @inlinable
  public static var valueForNull: String? { "" }
}

extension Double: ZeroValue {
  @inlinable
  public init(zeroValue: JSONValue) throws(ZeroValueError) {
    guard case .number(let n) = zeroValue else { throw .mismatch("a number", zeroValue) }
    self = n
  }

  public var zeroValue: JSONValue { .number(self) }

  @inlinable
  public static var valueForNull: Double? { 0 }
}

/// For a `number()` column the codegen config declares an integer: the
/// value must be exact, which every integer up to 2^53 is.
extension Int: ZeroValue {
  @inlinable
  public init(zeroValue: JSONValue) throws(ZeroValueError) {
    guard case .number(let n) = zeroValue, let i = Int(exactly: n),
      abs(i) <= 1 << 53
    else { throw .mismatch("an integer", zeroValue) }
    self = i
  }

  public var zeroValue: JSONValue { .number(Double(self)) }

  @inlinable
  public static var valueForNull: Int? { 0 }
}

extension Bool: ZeroValue {
  @inlinable
  public init(zeroValue: JSONValue) throws(ZeroValueError) {
    guard case .bool(let b) = zeroValue else { throw .mismatch("a boolean", zeroValue) }
    self = b
  }

  public var zeroValue: JSONValue { .bool(self) }

  @inlinable
  public static var valueForNull: Bool? { false }
}

extension JSONValue: ZeroValue {
  @inlinable
  public init(zeroValue: JSONValue) { self = zeroValue }
  public var zeroValue: JSONValue { self }
}

/// An optional column: `.null` reads as nil.
extension Optional: ZeroValue where Wrapped: ZeroValue {
  @inlinable
  public init(zeroValue: JSONValue) throws(ZeroValueError) {
    if case .null = zeroValue {
      self = nil
    } else {
      self = try Wrapped(zeroValue: zeroValue)
    }
  }

  public var zeroValue: JSONValue { self?.zeroValue ?? .null }
}

/// The JSON arrays a `json()` column can hold.
extension Array: ZeroValue where Element: ZeroValue {
  @inlinable
  public init(zeroValue: JSONValue) throws(ZeroValueError) {
    guard case .array(let a) = zeroValue else { throw .mismatch("an array", zeroValue) }
    var out: [Element] = []
    out.reserveCapacity(a.count)
    for e in a { out.append(try Element(zeroValue: e)) }
    self = out
  }

  public var zeroValue: JSONValue { .array(map(\.zeroValue)) }
}

/// A string enumeration (`enumeration<…>()`), generated as a `String` enum.
public protocol ZeroEnum: ZeroValue, RawRepresentable where RawValue == String {}

extension ZeroEnum {
  @inlinable
  public init(zeroValue: JSONValue) throws(ZeroValueError) {
    guard case .string(let s) = zeroValue, let v = Self(rawValue: s) else {
      throw .mismatch("one of the cases of \(Self.self)", zeroValue)
    }
    self = v
  }

  public var zeroValue: JSONValue { .string(rawValue) }
}

/// Reads the fields of a JSON object, for generated `json<T>()` types.
public struct ZeroObjectReader: Sendable {
  public let object: JSONObject

  public init(_ value: JSONValue, _ type: String) throws(ZeroValueError) {
    guard case .object(let o) = value else { throw .mismatch("a \(type) object", value) }
    object = o
  }

  /// A field; a missing one reads as `.null`, so only an optional takes it.
  public func field<V: ZeroValue>(_ name: String) throws(ZeroValueError) -> V {
    do {
      return try V(zeroValue: object[name] ?? .null)
    } catch {
      throw ZeroValueError("\(name): \(error.description)")
    }
  }
}

/// Writes the fields of a JSON object.
public struct ZeroObjectWriter: Sendable {
  public private(set) var object: JSONObject = [:]

  public init() {}

  public mutating func set(_ name: String, _ value: some ZeroValue) {
    object[name] = value.zeroValue
  }

  /// An optional field (`name?: T`): nil leaves it out.
  public mutating func setIfPresent<V: ZeroValue>(_ name: String, _ value: V?) {
    if let value { object[name] = value.zeroValue }
  }

  public var value: JSONValue { .object(object) }
}

/// Stores a one relationship of a row struct out of line, so two tables can
/// relate to each other (`Issue.oneComment` and `Comment.issue`) without
/// their structs containing each other.
@propertyWrapper
public struct Indirect<Value: Sendable>: Sendable {
  private var storage: [Value]

  public init(wrappedValue: Value?) {
    storage = wrappedValue.map { [$0] } ?? []
  }

  public var wrappedValue: Value? {
    get { storage.first }
    set { storage = newValue.map { [$0] } ?? [] }
  }
}

extension Indirect: Equatable where Value: Equatable {}
extension Indirect: Hashable where Value: Hashable {}

extension Indirect: Codable where Value: Codable {
  public init(from decoder: any Decoder) throws {
    let c = try decoder.singleValueContainer()
    self.init(wrappedValue: c.decodeNil() ? nil : try c.decode(Value.self))
  }

  public func encode(to encoder: any Encoder) throws {
    var c = encoder.singleValueContainer()
    if let v = wrappedValue { try c.encode(v) } else { try c.encodeNil() }
  }
}

extension KeyedDecodingContainer {
  /// Lets a row decode without the relationships it wasn't queried with.
  public func decode<V: Codable>(_ type: Indirect<V>.Type, forKey key: Key) throws -> Indirect<V> {
    try decodeIfPresent(type, forKey: key) ?? Indirect(wrappedValue: nil)
  }
}

extension KeyedEncodingContainer {
  public mutating func encode<V: Codable>(_ value: Indirect<V>, forKey key: Key) throws {
    try encodeIfPresent(value.wrappedValue, forKey: key)
  }
}

/// An argument the TS validator declares `nullable` (`z.nullable(…)`):
/// `nil` encodes as `null`. A plain optional is omitted when `nil`, as an
/// `optional` field is.
@propertyWrapper
public struct Nullable<Value> {
  public var wrappedValue: Value?

  public init(wrappedValue: Value?) { self.wrappedValue = wrappedValue }
}

extension Nullable: Sendable where Value: Sendable {}
extension Nullable: Equatable where Value: Equatable {}
extension Nullable: Hashable where Value: Hashable {}

extension Nullable: Codable where Value: Codable {
  public init(from decoder: any Decoder) throws {
    let c = try decoder.singleValueContainer()
    self.init(wrappedValue: c.decodeNil() ? nil : try c.decode(Value.self))
  }

  public func encode(to encoder: any Encoder) throws {
    var c = encoder.singleValueContainer()
    if let v = wrappedValue { try c.encode(v) } else { try c.encodeNil() }
  }
}

extension KeyedDecodingContainer {
  public func decode<V: Codable>(_ type: Nullable<V>.Type, forKey key: Key) throws -> Nullable<V> {
    try decodeIfPresent(type, forKey: key) ?? Nullable(wrappedValue: nil)
  }
}
