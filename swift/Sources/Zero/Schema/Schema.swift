/// The runtime description of a Zero schema (`packages/zero-types/src/schema.ts`).
///
/// `zero-swift codegen` emits one from the app's `schema.ts`; tests decode
/// them from the conformance fixtures.
public struct ZeroSchema: Sendable, Hashable {
  public var tables: [String: TableSchema]
  /// Relationships by table, then by name.
  public var relationships: [String: [String: Relationship]]
  /// Table names in declaration order.
  public var tableNames: [String]

  public init(tables: [TableSchema], relationships: [String: [String: Relationship]] = [:]) {
    var byName: [String: TableSchema] = [:]
    for table in tables { byName[table.name] = table }
    self.tables = byName
    self.tableNames = tables.map(\.name)
    self.relationships = relationships
  }

  public func table(_ name: String) -> TableSchema? { tables[name] }

  public func relationship(_ table: String, _ name: String) -> Relationship? {
    relationships[table]?[name]
  }
}

/// The type of a column value (`ValueType` in `schema-value.ts`).
public enum ValueType: String, Sendable, Hashable, CaseIterable {
  case string, number, boolean, null, json
}

/// A column (`SchemaValue`).
public struct ColumnSchema: Sendable, Hashable {
  public var type: ValueType
  public var optional: Bool
  public var serverName: String?

  public init(_ type: ValueType, optional: Bool = false, serverName: String? = nil) {
    self.type = type
    self.optional = optional
    self.serverName = serverName
  }
}

/// A table (`TableSchema` in `schema.ts`).
public struct TableSchema: Sendable, Hashable {
  public var name: String
  public var serverName: String?
  /// Columns by name.
  public var columns: [String: ColumnSchema]
  /// Column names in declaration order.
  public var columnNames: [String]
  public var primaryKey: [String]
  public var uniqueKeys: [[String]]?

  public init(
    name: String,
    serverName: String? = nil,
    columns: [(String, ColumnSchema)],
    primaryKey: [String],
    uniqueKeys: [[String]]? = nil
  ) {
    self.name = name
    self.serverName = serverName
    var byName: [String: ColumnSchema] = [:]
    for (column, schema) in columns { byName[column] = schema }
    self.columns = byName
    self.columnNames = columns.map(\.0)
    self.primaryKey = primaryKey
    self.uniqueKeys = uniqueKeys
  }
}

public enum Cardinality: String, Sendable, Hashable {
  case one, many
}

/// One hop of a relationship (`Connection` in `schema.ts`).
public struct RelationshipHop: Sendable, Hashable {
  public var sourceField: [String]
  public var destField: [String]
  public var destSchema: String
  public var cardinality: Cardinality

  public init(
    sourceField: [String], destField: [String], destSchema: String, cardinality: Cardinality
  ) {
    self.sourceField = sourceField
    self.destField = destField
    self.destSchema = destSchema
    self.cardinality = cardinality
  }
}

/// A relationship: one hop, or two through a junction table.
public typealias Relationship = [RelationshipHop]

extension ZeroSchema: JSONCodable {
  package init(json: JSONValue) throws(DecodingError) {
    let o = try JSONObjectReader(json)
    let tablesJSON = try o.required("tables", JSONDecode.object)
    var tables: [TableSchema] = []
    for (key, value) in tablesJSON {
      tables.append(try TableSchema(json: value).named(key))
    }
    var relationships: [String: [String: Relationship]] = [:]
    if let rels = try o.optional("relationships", JSONDecode.object) {
      for (table, value) in rels {
        var byName: [String: Relationship] = [:]
        for (name, hops) in try JSONDecode.object(value) {
          byName[name] = try JSONDecode.array(hops) { (h) throws(DecodingError) in
            try RelationshipHop(json: h)
          }
        }
        relationships[table] = byName
      }
    }
    self.init(tables: tables, relationships: relationships)
  }

  package var json: JSONValue {
    // Dictionaries have no order: tables and relationships by name.
    let tables = JSONObject(self.tables.mapValues(\.json))
    let rels = JSONObject(
      relationships.mapValues { byName in
        .object(JSONObject(byName.mapValues { .array($0.map(\.json)) }))
      })
    return ["tables": .object(tables), "relationships": .object(rels)]
  }
}

extension TableSchema: JSONCodable {
  fileprivate func named(_ key: String) -> TableSchema {
    var copy = self
    if copy.name.isEmpty { copy.name = key }
    return copy
  }

  /// Decodes a table. Columns are in the order of an optional `columnOrder`
  /// array, else in the order of `columns`' keys, as `Object.keys` has them.
  package init(json: JSONValue) throws(DecodingError) {
    let o = try JSONObjectReader(json)
    let name = try o.optionalString("name") ?? ""
    let serverName = try o.optionalString("serverName")
    let columnsJSON = try o.required("columns", JSONDecode.object)
    let order =
      try o.optional("columnOrder") { (v) throws(DecodingError) in
        try JSONDecode.array(v, JSONDecode.string)
      } ?? columnsJSON.keys
    var columns: [(String, ColumnSchema)] = []
    for column in order {
      guard let value = columnsJSON[column] else { continue }
      let c = try JSONObjectReader(value)
      let typeName = try c.string("type")
      guard let type = ValueType(rawValue: typeName) else {
        throw DecodingError("Unknown type \(typeName)", path: ["columns", column, "type"])
      }
      columns.append(
        (
          column,
          ColumnSchema(
            type, optional: try c.optional("optional", JSONDecode.bool) ?? false,
            serverName: try c.optionalString("serverName"))
        ))
    }
    let primaryKey = try o.required("primaryKey") { (v) throws(DecodingError) in
      try JSONDecode.array(v, JSONDecode.string)
    }
    let uniqueKeys = try o.optional("uniqueKeys") { (v) throws(DecodingError) in
      try JSONDecode.array(v) { (k) throws(DecodingError) in
        try JSONDecode.array(k, JSONDecode.string)
      }
    }
    self.init(
      name: name, serverName: serverName, columns: columns, primaryKey: primaryKey,
      uniqueKeys: uniqueKeys)
  }

  package var json: JSONValue {
    var columns: JSONObject = [:]
    for (name, column) in self.columns {
      var c: JSONObject = ["type": .string(column.type.rawValue)]
      if column.optional { c["optional"] = true }
      if let serverName = column.serverName { c["serverName"] = .string(serverName) }
      columns[name] = .object(c)
    }
    var o: JSONObject = [
      "name": .string(name),
      "columns": .object(columns),
      "columnOrder": columnNames.json,
      "primaryKey": primaryKey.json,
    ]
    if let serverName { o["serverName"] = .string(serverName) }
    if let uniqueKeys { o["uniqueKeys"] = .array(uniqueKeys.map(\.json)) }
    return .object(o)
  }
}

extension RelationshipHop: JSONCodable {
  package init(json: JSONValue) throws(DecodingError) {
    let o = try JSONObjectReader(json)
    let cardinality = try o.string("cardinality")
    self.init(
      sourceField: try o.required("sourceField") { (v) throws(DecodingError) in
        try JSONDecode.array(v, JSONDecode.string)
      },
      destField: try o.required("destField") { (v) throws(DecodingError) in
        try JSONDecode.array(v, JSONDecode.string)
      },
      destSchema: try o.string("destSchema"),
      cardinality: cardinality == "one" ? .one : .many)
  }

  package var json: JSONValue {
    [
      "sourceField": sourceField.json,
      "destField": destField.json,
      "destSchema": .string(destSchema),
      "cardinality": .string(cardinality.rawValue),
    ]
  }
}
