/// The schema a client was built with, sent so zero-cache can check it
/// against the replica (`clientSchemaSchema` in
/// `packages/zero-protocol/src/client-schema.ts`).
package struct ClientSchema: Sendable, Hashable, JSONCodable {
  /// `valueTypeSchema`.
  package enum ValueType: String, Sendable, Hashable, CaseIterable {
    case string, number, boolean, null, json
  }

  /// `tableSchemaSchema`. Each column maps to its `{type}`.
  package struct Table: Sendable, Hashable {
    package var columns: [String: ValueType]
    package var primaryKey: [String]

    package init(columns: [String: ValueType], primaryKey: [String]) {
      self.columns = columns
      self.primaryKey = primaryKey
    }
  }

  package var tables: [String: Table]

  package init(tables: [String: Table]) {
    self.tables = tables
  }

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "client schema")
    tables = try r.required("tables") { (v) throws(DecodingError) in
      try JSONDecode.record(v) { (t) throws(DecodingError) -> Table in
        let table = try JSONObjectReader(t, "table schema")
        return Table(
          columns: try table.required("columns") { (v) throws(DecodingError) in
            try JSONDecode.record(v) { (c) throws(DecodingError) in
              try JSONObjectReader(c, "column schema").literalUnion("type", ValueType.self)
            }
          },
          primaryKey: try table.required("primaryKey", JSONDecode.strings))
      }
    }
  }

  package var json: JSONValue {
    .object([
      "tables": .object(
        JSONObject(
          tables.mapValues { t in
            [
              "columns": .object(
                JSONObject(t.columns.mapValues { ["type": .string($0.rawValue)] })),
              "primaryKey": t.primaryKey.json,
            ]
          }))
    ])
  }

  /// The hash of the normalized schema that names a client's local
  /// database (`clientSchemaFrom`): a schema change starts a new one.
  package var hash: String {
    String(XXHash32.h64(JSONSerializer.stringify(normalized().json)), radix: 36)
  }

  /// `normalizeClientSchema`: sorts each primary key, as JS `sort()` does,
  /// by UTF-16 code units. JS also orders the table and column entries by
  /// `<`, which is the order ``json`` puts them in.
  package func normalized() -> ClientSchema {
    ClientSchema(
      tables: tables.mapValues { t in
        Table(columns: t.columns, primaryKey: t.primaryKey.sorted(by: StringCompare.utf16Less))
      })
  }
}
