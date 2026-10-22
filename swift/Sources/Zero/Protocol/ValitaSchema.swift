/// A small interpreter of valita schemas (`packages/shared/src/valita.ts`),
/// for messages kept as JSON rather than decoded into Swift types.
///
/// ``parse(_:)`` accepts what valita accepts in `passthrough` mode and
/// returns what it returns in `strip` mode: objects lose the keys their
/// schema doesn't name. A union picks its first option that accepts.
indirect enum ValitaSchema: Sendable {
  case string
  case number
  case boolean
  case null
  /// `v.unknown()` and `jsonSchema`.
  case any
  case literal(String)
  case array(ValitaSchema)
  /// `v.tuple([s]).concat(v.array(s))`.
  case nonEmptyArray(ValitaSchema)
  case tuple([ValitaSchema])
  case record(ValitaSchema)
  case object([String: Field])
  case union([ValitaSchema])
  case nullable(ValitaSchema)
  /// A schema with a Swift decoder; returns the decoded value's JSON.
  case custom(@Sendable (JSONValue) throws(DecodingError) -> JSONValue)

  enum Field: Sendable {
    case required(ValitaSchema)
    case optional(ValitaSchema)
  }

  func parse(_ v: JSONValue) throws(DecodingError) -> JSONValue {
    switch self {
    case .string:
      _ = try JSONDecode.string(v)
      return v
    case .number:
      _ = try JSONDecode.number(v)
      return v
    case .boolean:
      _ = try JSONDecode.bool(v)
      return v
    case .null:
      guard case .null = v else { throw DecodingError("Expected null, got \(v.typeName)") }
      return v
    case .any:
      return v
    case .literal(let expected):
      try JSONDecode.literal(v, expected)
      return v
    case .array(let element):
      return .array(try JSONDecode.array(v, element.parse))
    case .nonEmptyArray(let element):
      let a = try JSONDecode.array(v, element.parse)
      if a.isEmpty { throw DecodingError("Expected non-empty array") }
      return .array(a)
    case .tuple(let elements):
      let a = try JSONDecode.array(v)
      guard a.count == elements.count else {
        throw DecodingError("Expected array of length \(elements.count), got \(a.count)")
      }
      var out: [JSONValue] = []
      for (i, (e, schema)) in zip(a, elements).enumerated() {
        do {
          out.append(try schema.parse(e))
        } catch {
          throw error.at(String(i))
        }
      }
      return .array(out)
    case .record(let element):
      return .object(try JSONDecode.objectRecord(v, element.parse))
    case .object(let fields):
      let r = try JSONObjectReader(v)
      var parsed: [JSKey: JSONValue] = [:]
      for (key, field) in fields {
        switch field {
        case .required(let schema): parsed[JSKey(key)] = try r.required(key, schema.parse)
        case .optional(let schema): parsed[JSKey(key)] = try r.optional(key, schema.parse)
        }
      }
      // In the input's order, as valita keeps it.
      var out = JSONObject(minimumCapacity: parsed.count)
      for key in r.object.keys {
        if let value = parsed[JSKey(key)] { out[key] = value }
      }
      return .object(out)
    case .union(let options):
      for option in options {
        if let out = try? option.parse(v) { return out }
      }
      throw DecodingError("No union option matched the \(v.typeName)")
    case .nullable(let schema):
      if case .null = v { return v }
      return try schema.parse(v)
    case .custom(let decode):
      return try decode(v)
    }
  }
}
