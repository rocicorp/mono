/// Encodes `Encodable` values to `JSONValue`, as `JSON.stringify` would see
/// them: optional properties that are nil are left out (JS `undefined`).
public enum JSONValueEncoder {
  public static func encode<T: Encodable>(_ value: T) throws -> JSONValue {
    if let v = value as? JSONValue { return v }
    let encoder = ValueEncoder(codingPath: [])
    try value.encode(to: encoder)
    return encoder.storage.value ?? .null
  }
}

/// Decodes `Decodable` values from `JSONValue`.
public enum JSONValueDecoder {
  public static func decode<T: Decodable>(_ type: T.Type, from value: JSONValue) throws -> T {
    if let v = value as? T { return v }
    return try T(from: ValueDecoder(value, codingPath: []))
  }
}

/// A value being built: containers write into it after they are handed out.
private final class Slot {
  var value: JSONValue?
  /// The entries of an object, in the order they were first encoded.
  var object: [(key: String, slot: Slot)]?
  var array: [Slot]?

  /// Sets an entry of the object, keeping a key's first place.
  func set(_ key: String, _ slot: Slot) {
    if let i = object!.firstIndex(where: { StringCompare.equal($0.key, key) }) {
      object![i].slot = slot
    } else {
      object!.append((key, slot))
    }
  }

  var resolved: JSONValue? {
    if let object {
      var o = JSONObject(minimumCapacity: object.count)
      for (key, slot) in object {
        if let v = slot.resolved { o[key] = v }
      }
      return .object(o)
    }
    if let array { return .array(array.map { $0.resolved ?? .null }) }
    return value
  }
}

private struct AnyKey: CodingKey {
  var stringValue: String
  var intValue: Int?
  init(stringValue: String) { self.stringValue = stringValue }
  init(intValue: Int) {
    self.stringValue = String(intValue)
    self.intValue = intValue
  }
}

private final class ValueEncoder: Encoder {
  let codingPath: [any CodingKey]
  var userInfo: [CodingUserInfoKey: Any] { [:] }
  let slot = Slot()

  init(codingPath: [any CodingKey]) { self.codingPath = codingPath }

  var storage: (value: JSONValue?, Void) { (slot.resolved, ()) }

  func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> {
    if slot.object == nil { slot.object = [] }
    return KeyedEncodingContainer(KeyedEncoder<Key>(slot: slot, codingPath: codingPath))
  }

  func unkeyedContainer() -> any UnkeyedEncodingContainer {
    if slot.array == nil { slot.array = [] }
    return UnkeyedEncoder(slot: slot, codingPath: codingPath)
  }

  func singleValueContainer() -> any SingleValueEncodingContainer {
    SingleEncoder(slot: slot, codingPath: codingPath)
  }
}

private func encodeValue<T: Encodable>(_ value: T, into slot: Slot, path: [any CodingKey]) throws {
  if let v = value as? JSONValue {
    slot.value = v
    return
  }
  let encoder = ValueEncoder(codingPath: path)
  try value.encode(to: encoder)
  slot.value = encoder.slot.resolved
  slot.object = nil
  slot.array = nil
}

private struct KeyedEncoder<Key: CodingKey>: KeyedEncodingContainerProtocol {
  let slot: Slot
  let codingPath: [any CodingKey]

  private func set(_ key: Key, _ v: JSONValue) {
    let s = Slot()
    s.value = v
    slot.set(key.stringValue, s)
  }

  mutating func encodeNil(forKey key: Key) throws { set(key, .null) }
  mutating func encode(_ value: Bool, forKey key: Key) throws { set(key, .bool(value)) }
  mutating func encode(_ value: String, forKey key: Key) throws { set(key, .string(value)) }
  mutating func encode(_ value: Double, forKey key: Key) throws { set(key, .number(value)) }
  mutating func encode(_ value: Float, forKey key: Key) throws { set(key, .number(Double(value))) }
  mutating func encode(_ value: Int, forKey key: Key) throws { set(key, .number(Double(value))) }
  mutating func encode(_ value: Int8, forKey key: Key) throws { set(key, .number(Double(value))) }
  mutating func encode(_ value: Int16, forKey key: Key) throws { set(key, .number(Double(value))) }
  mutating func encode(_ value: Int32, forKey key: Key) throws { set(key, .number(Double(value))) }
  mutating func encode(_ value: Int64, forKey key: Key) throws { set(key, .number(Double(value))) }
  mutating func encode(_ value: UInt, forKey key: Key) throws { set(key, .number(Double(value))) }
  mutating func encode(_ value: UInt8, forKey key: Key) throws { set(key, .number(Double(value))) }
  mutating func encode(_ value: UInt16, forKey key: Key) throws { set(key, .number(Double(value))) }
  mutating func encode(_ value: UInt32, forKey key: Key) throws { set(key, .number(Double(value))) }
  mutating func encode(_ value: UInt64, forKey key: Key) throws { set(key, .number(Double(value))) }

  mutating func encode<T: Encodable>(_ value: T, forKey key: Key) throws {
    let s = Slot()
    try encodeValue(value, into: s, path: codingPath + [key])
    slot.set(key.stringValue, s)
  }

  mutating func nestedContainer<NestedKey: CodingKey>(
    keyedBy keyType: NestedKey.Type, forKey key: Key
  ) -> KeyedEncodingContainer<NestedKey> {
    let s = Slot()
    s.object = []
    slot.set(key.stringValue, s)
    return KeyedEncodingContainer(KeyedEncoder<NestedKey>(slot: s, codingPath: codingPath + [key]))
  }

  mutating func nestedUnkeyedContainer(forKey key: Key) -> any UnkeyedEncodingContainer {
    let s = Slot()
    s.array = []
    slot.set(key.stringValue, s)
    return UnkeyedEncoder(slot: s, codingPath: codingPath + [key])
  }

  mutating func superEncoder() -> any Encoder { superEncoder(forKey: Key(stringValue: "super")!) }

  mutating func superEncoder(forKey key: Key) -> any Encoder {
    let encoder = ValueEncoder(codingPath: codingPath + [key])
    slot.set(key.stringValue, encoder.slot)
    return encoder
  }
}

private struct UnkeyedEncoder: UnkeyedEncodingContainer {
  let slot: Slot
  let codingPath: [any CodingKey]
  var count: Int { slot.array!.count }

  private func append(_ v: JSONValue) {
    let s = Slot()
    s.value = v
    slot.array!.append(s)
  }

  private var key: AnyKey { AnyKey(intValue: count) }

  mutating func encodeNil() throws { append(.null) }
  mutating func encode(_ value: Bool) throws { append(.bool(value)) }
  mutating func encode(_ value: String) throws { append(.string(value)) }
  mutating func encode(_ value: Double) throws { append(.number(value)) }
  mutating func encode(_ value: Float) throws { append(.number(Double(value))) }
  mutating func encode(_ value: Int) throws { append(.number(Double(value))) }
  mutating func encode(_ value: Int8) throws { append(.number(Double(value))) }
  mutating func encode(_ value: Int16) throws { append(.number(Double(value))) }
  mutating func encode(_ value: Int32) throws { append(.number(Double(value))) }
  mutating func encode(_ value: Int64) throws { append(.number(Double(value))) }
  mutating func encode(_ value: UInt) throws { append(.number(Double(value))) }
  mutating func encode(_ value: UInt8) throws { append(.number(Double(value))) }
  mutating func encode(_ value: UInt16) throws { append(.number(Double(value))) }
  mutating func encode(_ value: UInt32) throws { append(.number(Double(value))) }
  mutating func encode(_ value: UInt64) throws { append(.number(Double(value))) }

  mutating func encode<T: Encodable>(_ value: T) throws {
    let s = Slot()
    try encodeValue(value, into: s, path: codingPath + [key])
    slot.array!.append(s)
  }

  mutating func nestedContainer<NestedKey: CodingKey>(
    keyedBy keyType: NestedKey.Type
  )
    -> KeyedEncodingContainer<NestedKey>
  {
    let s = Slot()
    s.object = []
    let path = codingPath + [key]
    slot.array!.append(s)
    return KeyedEncodingContainer(KeyedEncoder<NestedKey>(slot: s, codingPath: path))
  }

  mutating func nestedUnkeyedContainer() -> any UnkeyedEncodingContainer {
    let s = Slot()
    s.array = []
    let path = codingPath + [key]
    slot.array!.append(s)
    return UnkeyedEncoder(slot: s, codingPath: path)
  }

  mutating func superEncoder() -> any Encoder {
    let encoder = ValueEncoder(codingPath: codingPath + [key])
    slot.array!.append(encoder.slot)
    return encoder
  }
}

private struct SingleEncoder: SingleValueEncodingContainer {
  let slot: Slot
  let codingPath: [any CodingKey]

  mutating func encodeNil() throws { slot.value = .null }
  mutating func encode(_ value: Bool) throws { slot.value = .bool(value) }
  mutating func encode(_ value: String) throws { slot.value = .string(value) }
  mutating func encode(_ value: Double) throws { slot.value = .number(value) }
  mutating func encode(_ value: Float) throws { slot.value = .number(Double(value)) }
  mutating func encode(_ value: Int) throws { slot.value = .number(Double(value)) }
  mutating func encode(_ value: Int8) throws { slot.value = .number(Double(value)) }
  mutating func encode(_ value: Int16) throws { slot.value = .number(Double(value)) }
  mutating func encode(_ value: Int32) throws { slot.value = .number(Double(value)) }
  mutating func encode(_ value: Int64) throws { slot.value = .number(Double(value)) }
  mutating func encode(_ value: UInt) throws { slot.value = .number(Double(value)) }
  mutating func encode(_ value: UInt8) throws { slot.value = .number(Double(value)) }
  mutating func encode(_ value: UInt16) throws { slot.value = .number(Double(value)) }
  mutating func encode(_ value: UInt32) throws { slot.value = .number(Double(value)) }
  mutating func encode(_ value: UInt64) throws { slot.value = .number(Double(value)) }
  mutating func encode<T: Encodable>(_ value: T) throws {
    try encodeValue(value, into: slot, path: codingPath)
  }
}

private struct ValueDecoder: Decoder {
  let value: JSONValue
  let codingPath: [any CodingKey]
  var userInfo: [CodingUserInfoKey: Any] { [:] }

  init(_ value: JSONValue, codingPath: [any CodingKey]) {
    self.value = value
    self.codingPath = codingPath
  }

  func container<Key: CodingKey>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
    guard case .object(let o) = value else {
      throw typeMismatch(JSONObject.self, value, codingPath)
    }
    return KeyedDecodingContainer(KeyedDecoder<Key>(object: o, codingPath: codingPath))
  }

  func unkeyedContainer() throws -> any UnkeyedDecodingContainer {
    guard case .array(let a) = value else {
      throw typeMismatch([JSONValue].self, value, codingPath)
    }
    return UnkeyedDecoder(array: a, codingPath: codingPath)
  }

  func singleValueContainer() throws -> any SingleValueDecodingContainer {
    SingleDecoder(value: value, codingPath: codingPath)
  }
}

private func typeMismatch(
  _ type: Any.Type, _ value: JSONValue, _ path: [any CodingKey]
)
  -> Swift.DecodingError
{
  Swift.DecodingError.typeMismatch(
    type, .init(codingPath: path, debugDescription: "Expected \(type), found \(value.typeName)"))
}

private func decodeValue<T: Decodable>(
  _ type: T.Type, _ value: JSONValue, _ path: [any CodingKey]
)
  throws -> T
{
  if let v = value as? T { return v }
  return try T(from: ValueDecoder(value, codingPath: path))
}

private func number<T>(
  _ value: JSONValue, _ path: [any CodingKey], _ make: (Double) -> T?
) throws
  -> T
{
  guard case .number(let n) = value, let v = make(n) else {
    throw typeMismatch(T.self, value, path)
  }
  return v
}

private func exactInteger<T: BinaryInteger>(_ n: Double) -> T? {
  guard n.rounded() == n, let v = T(exactly: n) else { return nil }
  return v
}

private struct KeyedDecoder<Key: CodingKey>: KeyedDecodingContainerProtocol {
  let object: JSONObject
  let codingPath: [any CodingKey]

  var allKeys: [Key] { object.keys.compactMap { Key(stringValue: $0) } }
  func contains(_ key: Key) -> Bool { object[key.stringValue] != nil }

  private func get(_ key: Key) throws -> JSONValue {
    guard let v = object[key.stringValue] else {
      throw Swift.DecodingError.keyNotFound(
        key, .init(codingPath: codingPath, debugDescription: "No value for \(key.stringValue)"))
    }
    return v
  }

  func decodeNil(forKey key: Key) throws -> Bool { (object[key.stringValue] ?? .null).isNull }

  func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool {
    let v = try get(key)
    guard case .bool(let b) = v else { throw typeMismatch(type, v, codingPath + [key]) }
    return b
  }

  func decode(_ type: String.Type, forKey key: Key) throws -> String {
    let v = try get(key)
    guard case .string(let s) = v else { throw typeMismatch(type, v, codingPath + [key]) }
    return s
  }

  func decode(_ type: Double.Type, forKey key: Key) throws -> Double {
    try number(get(key), codingPath + [key]) { $0 }
  }
  func decode(_ type: Float.Type, forKey key: Key) throws -> Float {
    try number(get(key), codingPath + [key]) { Float($0) }
  }
  func decode(_ type: Int.Type, forKey key: Key) throws -> Int {
    try number(get(key), codingPath + [key], exactInteger)
  }
  func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 {
    try number(get(key), codingPath + [key], exactInteger)
  }
  func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 {
    try number(get(key), codingPath + [key], exactInteger)
  }
  func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 {
    try number(get(key), codingPath + [key], exactInteger)
  }
  func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 {
    try number(get(key), codingPath + [key], exactInteger)
  }
  func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt {
    try number(get(key), codingPath + [key], exactInteger)
  }
  func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 {
    try number(get(key), codingPath + [key], exactInteger)
  }
  func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 {
    try number(get(key), codingPath + [key], exactInteger)
  }
  func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 {
    try number(get(key), codingPath + [key], exactInteger)
  }
  func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 {
    try number(get(key), codingPath + [key], exactInteger)
  }

  func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
    try decodeValue(type, get(key), codingPath + [key])
  }

  func nestedContainer<NestedKey: CodingKey>(
    keyedBy type: NestedKey.Type, forKey key: Key
  ) throws
    -> KeyedDecodingContainer<NestedKey>
  {
    try ValueDecoder(get(key), codingPath: codingPath + [key]).container(keyedBy: type)
  }

  func nestedUnkeyedContainer(forKey key: Key) throws -> any UnkeyedDecodingContainer {
    try ValueDecoder(get(key), codingPath: codingPath + [key]).unkeyedContainer()
  }

  func superDecoder() throws -> any Decoder {
    ValueDecoder(object["super"] ?? .null, codingPath: codingPath)
  }

  func superDecoder(forKey key: Key) throws -> any Decoder {
    ValueDecoder(object[key.stringValue] ?? .null, codingPath: codingPath + [key])
  }
}

private struct UnkeyedDecoder: UnkeyedDecodingContainer {
  let array: [JSONValue]
  let codingPath: [any CodingKey]
  var currentIndex = 0

  init(array: [JSONValue], codingPath: [any CodingKey]) {
    self.array = array
    self.codingPath = codingPath
  }

  var count: Int? { array.count }
  var isAtEnd: Bool { currentIndex >= array.count }

  private var path: [any CodingKey] { codingPath + [AnyKey(intValue: currentIndex)] }

  private mutating func next() throws -> JSONValue {
    guard !isAtEnd else {
      throw Swift.DecodingError.valueNotFound(
        JSONValue.self, .init(codingPath: path, debugDescription: "Unkeyed container is at end"))
    }
    defer { currentIndex += 1 }
    return array[currentIndex]
  }

  mutating func decodeNil() throws -> Bool {
    if !isAtEnd, array[currentIndex].isNull {
      currentIndex += 1
      return true
    }
    return false
  }

  mutating func decode<T: Decodable>(_ type: T.Type) throws -> T {
    let p = path
    return try decodeValue(type, next(), p)
  }

  mutating func nestedContainer<NestedKey: CodingKey>(
    keyedBy type: NestedKey.Type
  ) throws
    -> KeyedDecodingContainer<NestedKey>
  {
    let p = path
    return try ValueDecoder(next(), codingPath: p).container(keyedBy: type)
  }

  mutating func nestedUnkeyedContainer() throws -> any UnkeyedDecodingContainer {
    let p = path
    return try ValueDecoder(next(), codingPath: p).unkeyedContainer()
  }

  mutating func superDecoder() throws -> any Decoder {
    let p = path
    return ValueDecoder(try next(), codingPath: p)
  }
}

private struct SingleDecoder: SingleValueDecodingContainer {
  let value: JSONValue
  let codingPath: [any CodingKey]

  func decodeNil() -> Bool { value.isNull }

  func decode(_ type: Bool.Type) throws -> Bool {
    guard case .bool(let b) = value else { throw typeMismatch(type, value, codingPath) }
    return b
  }

  func decode(_ type: String.Type) throws -> String {
    guard case .string(let s) = value else { throw typeMismatch(type, value, codingPath) }
    return s
  }

  func decode(_ type: Double.Type) throws -> Double { try number(value, codingPath) { $0 } }
  func decode(_ type: Float.Type) throws -> Float { try number(value, codingPath) { Float($0) } }
  func decode(_ type: Int.Type) throws -> Int { try number(value, codingPath, exactInteger) }
  func decode(_ type: Int8.Type) throws -> Int8 { try number(value, codingPath, exactInteger) }
  func decode(_ type: Int16.Type) throws -> Int16 { try number(value, codingPath, exactInteger) }
  func decode(_ type: Int32.Type) throws -> Int32 { try number(value, codingPath, exactInteger) }
  func decode(_ type: Int64.Type) throws -> Int64 { try number(value, codingPath, exactInteger) }
  func decode(_ type: UInt.Type) throws -> UInt { try number(value, codingPath, exactInteger) }
  func decode(_ type: UInt8.Type) throws -> UInt8 { try number(value, codingPath, exactInteger) }
  func decode(_ type: UInt16.Type) throws -> UInt16 { try number(value, codingPath, exactInteger) }
  func decode(_ type: UInt32.Type) throws -> UInt32 { try number(value, codingPath, exactInteger) }
  func decode(_ type: UInt64.Type) throws -> UInt64 { try number(value, codingPath, exactInteger) }

  func decode<T: Decodable>(_ type: T.Type) throws -> T {
    try decodeValue(type, value, codingPath)
  }
}
