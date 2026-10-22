/// A cookie: the version of the client view a client has
/// (`packages/zero-protocol/src/version.ts`). Where the protocol allows a
/// `NullableVersion`, `nil` is the state before the first pull.
package typealias Version = String

/// The values of a row's primary key columns, by column
/// (`primaryKeyValueRecordSchema` in `packages/zero-protocol/src/primary-key.ts`).
/// Each value is a string, number, or boolean.
package typealias PrimaryKeyValueRecord = JSONObject

/// A row as the protocol carries it (`rowSchema` in
/// `packages/zero-protocol/src/data.ts`): a record of JSON values.
package typealias RowRecord = JSONObject

/// `packages/zero-protocol/src/mutation-id.ts`.
package struct MutationID: Sendable, Hashable, JSONCodable {
  package var id: Double
  package var clientID: String

  package init(id: Double, clientID: String) {
    self.id = id
    self.clientID = clientID
  }

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "mutation ID")
    id = try r.number("id")
    clientID = try r.string("clientID")
  }

  package var json: JSONValue {
    ["id": .number(id), "clientID": .string(clientID)]
  }
}

/// `nullableVersionSchema`: a string or `null`.
enum NullableVersion {
  static func decode(_ v: JSONValue) throws(DecodingError) -> Version? {
    switch v {
    case .null: return nil
    case .string(let s): return s
    default: throw DecodingError("Expected string or null, got \(v.typeName)")
    }
  }

  static func json(_ v: Version?) -> JSONValue {
    v.map { .string($0) } ?? .null
  }
}
