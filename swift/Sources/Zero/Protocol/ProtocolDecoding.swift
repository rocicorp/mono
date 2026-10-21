/// Decoding helpers for the `zero-protocol` messages. They follow valita's
/// `passthrough` mode (`packages/shared/src/valita.ts`): unknown keys are
/// accepted and dropped, optional fields may be absent but not `null`.
extension JSONDecode {
  /// `v.literal(expected)` for a string.
  static func literal(_ v: JSONValue, _ expected: String) throws(DecodingError) {
    guard case .string(let s) = v, s == expected else {
      throw DecodingError("Expected literal \"\(expected)\", got \(v)")
    }
  }

  /// `v.literalUnion(...)` of the raw values of `R`.
  static func literalUnion<R: RawRepresentable>(
    _ v: JSONValue, _: R.Type = R.self
  ) throws(DecodingError) -> R where R.RawValue == String {
    guard case .string(let s) = v, let r = R(rawValue: s) else {
      throw DecodingError("Expected a \(R.self) literal, got \(v)")
    }
    return r
  }

  /// `schema.nullable()`.
  static func nullable<T>(
    _ v: JSONValue, _ decode: (JSONValue) throws(DecodingError) -> T
  ) throws(DecodingError) -> T? {
    if case .null = v { return nil }
    return try decode(v)
  }

  /// `v.array(v.string())`.
  static func strings(_ v: JSONValue) throws(DecodingError) -> [String] {
    try array(v, string)
  }

  /// `v.record(v.string())`.
  static func stringRecord(_ v: JSONValue) throws(DecodingError) -> [String: String] {
    try record(v, string)
  }

  /// `v.record(v.number())`.
  static func numberRecord(_ v: JSONValue) throws(DecodingError) -> [String: Double] {
    try record(v, number)
  }

  /// `primaryKeyValueRecordSchema` (`packages/zero-protocol/src/primary-key.ts`).
  static func primaryKeyValueRecord(_ v: JSONValue) throws(DecodingError) -> [String: JSONValue] {
    try record(v, primaryKeyValue)
  }

  /// `jsonObjectSchema` (`packages/shared/src/json-schema.ts`). It checks
  /// `typeof v === 'object'`, so it accepts arrays as well as objects.
  static func jsonObject(_ v: JSONValue) throws(DecodingError) -> JSONValue {
    switch v {
    case .object, .array: return v
    default: throw DecodingError("Not a JSON object")
    }
  }

  /// The body of a `[tag, body]` tuple: exactly two elements, the first the
  /// literal `tag`.
  static func body(of v: JSONValue, tag: String) throws(DecodingError) -> JSONValue {
    let a = try array(v)
    guard a.count == 2 else {
      throw DecodingError("Expected array of length 2, got \(a.count)")
    }
    do {
      try literal(a[0], tag)
    } catch {
      throw error.at("0")
    }
    return a[1]
  }
}

extension JSONObjectReader {
  /// A required `v.literalUnion(...)` field.
  func literalUnion<R: RawRepresentable>(
    _ key: String, _ type: R.Type = R.self
  ) throws(DecodingError) -> R where R.RawValue == String {
    try required(key) { (v) throws(DecodingError) in try JSONDecode.literalUnion(v, type) }
  }

  /// An optional `v.literalUnion(...)` field.
  func optionalLiteralUnion<R: RawRepresentable>(
    _ key: String, _ type: R.Type = R.self
  ) throws(DecodingError) -> R? where R.RawValue == String {
    try optional(key) { (v) throws(DecodingError) in try JSONDecode.literalUnion(v, type) }
  }

  /// A required field whose value is one of `allowed`.
  func oneOf<R: RawRepresentable & Hashable>(
    _ key: String, _ allowed: Set<R>
  ) throws(DecodingError) -> R where R.RawValue == String {
    try required(key) { (v) throws(DecodingError) in
      let r = try JSONDecode.literalUnion(v, R.self)
      guard allowed.contains(r) else { throw DecodingError("Unexpected value \(v)") }
      return r
    }
  }

  /// An optional `jsonSchema` field: any value.
  func optionalJSON(_ key: String) -> JSONValue? { object[key] }
}

extension JSONValue {
  static func strings(_ a: [String]) -> JSONValue { .array(a.map { .string($0) }) }

  static func stringRecord(_ r: [String: String]) -> JSONValue {
    .object(r.mapValues { .string($0) })
  }

  static func numberRecord(_ r: [String: Double]) -> JSONValue {
    .object(r.mapValues { .number($0) })
  }

  /// A `[tag, body]` message.
  static func message(_ tag: String, _ body: JSONValue) -> JSONValue {
    .array([.string(tag), body])
  }

  /// Parses the text of a WebSocket message.
  static func parseMessage(_ text: String) throws(DecodingError) -> JSONValue {
    do {
      return try JSONParser.parse(text)
    } catch {
      throw DecodingError(error.description)
    }
  }
}
