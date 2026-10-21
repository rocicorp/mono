/// An unknown table or column met while mapping names.
package struct NameMappingError: Error, Sendable, Hashable, CustomStringConvertible {
  package var description: String
}

/// Maps table and column names between the client and the server
/// (`NameMapper` in `packages/zero-types/src/name-mapper.ts`).
package struct NameMapper: Sendable {
  struct Destination: Sendable {
    var tableName: String
    var columns: [String: String]
    var allColumnsSame: Bool
  }

  let tables: [String: Destination]

  /// `clientToServer`.
  package static func clientToServer(_ schema: ZeroSchema) -> NameMapper {
    NameMapper(schema, fromClient: true)
  }

  /// `serverToClient`.
  package static func serverToClient(_ schema: ZeroSchema) -> NameMapper {
    NameMapper(schema, fromClient: false)
  }

  init(_ schema: ZeroSchema, fromClient: Bool) {
    var tables: [String: Destination] = [:]
    for (name, table) in schema.tables {
      var allColumnsSame = true
      var columns: [String: String] = [:]
      for (column, c) in table.columns {
        if let serverName = c.serverName, serverName != column { allColumnsSame = false }
        if fromClient {
          columns[column] = c.serverName ?? column
        } else {
          columns[c.serverName ?? column] = column
        }
      }
      let serverName = table.serverName ?? name
      tables[fromClient ? name : serverName] = Destination(
        tableName: fromClient ? serverName : name, columns: columns,
        allColumnsSame: allColumnsSame)
    }
    self.tables = tables
  }

  private func table(_ src: String) throws(NameMappingError) -> Destination {
    guard let table = tables[src] else {
      throw NameMappingError(description: "unknown table \"\(src)\"")
    }
    return table
  }

  package func tableName(_ src: String) throws(NameMappingError) -> String {
    try table(src).tableName
  }

  package func tableNameIfKnown(_ src: String) -> String? { tables[src]?.tableName }

  package func columnName(_ table: String, _ src: String) throws(NameMappingError) -> String {
    guard let dst = try self.table(table).columns[src] else {
      throw NameMappingError(description: "unknown column \"\(src)\" of \"\(table)\" table")
    }
    return dst
  }

  /// Renames the known columns of `row`; others keep their names.
  package func row(_ table: String, _ row: RowRecord) throws(NameMappingError) -> RowRecord {
    let dest = try self.table(table)
    if dest.allColumnsSame { return row }
    var out: RowRecord = [:]
    for (column, value) in row { out[dest.columns[column] ?? column] = value }
    return out
  }

  package func columns(_ table: String, _ columns: [String]) throws(NameMappingError) -> [String] {
    let dest = try self.table(table)
    if dest.allColumnsSame { return columns }
    return columns.map { dest.columns[$0] ?? $0 }
  }

  /// `mapAST`: the AST with its table and column names mapped.
  package func map(_ ast: AST) throws(NameMappingError) -> AST {
    var out = ast
    out.table = try tableName(ast.table)
    out.where = try ast.where.map { (w) throws(NameMappingError) in try map(w, table: ast.table) }
    if let related = ast.related {
      var mapped: [AST.CorrelatedSubquery] = []
      for r in related {
        var r2 = r
        r2.correlation = try map(r.correlation, parent: ast.table, child: r.subquery.table)
        r2.subquery = try map(r.subquery)
        mapped.append(r2)
      }
      out.related = mapped
    }
    if let start = ast.start {
      var row: [String: JSONValue] = [:]
      for (column, value) in start.row { row[try columnName(ast.table, column)] = value }
      out.start = AST.Bound(row: row, exclusive: start.exclusive)
    }
    if let orderBy = ast.orderBy {
      var mapped: AST.Ordering = []
      for part in orderBy {
        mapped.append(AST.OrderPart(try columnName(ast.table, part.field), part.direction))
      }
      out.orderBy = mapped
    }
    return out
  }

  /// `mapCondition`. Only the left side of a comparison names a column.
  package func map(
    _ condition: AST.Condition, table: String
  ) throws(NameMappingError)
    -> AST.Condition
  {
    switch condition {
    case .simple(let op, let left, let right):
      if case .column(let name) = left {
        return .simple(op: op, left: .column(try columnName(table, name)), right: right)
      }
      return condition
    case .correlatedSubquery(var related, let op, let flip, let scalar):
      related.correlation = try map(
        related.correlation, parent: table, child: related.subquery.table)
      related.subquery = try map(related.subquery)
      return .correlatedSubquery(related: related, op: op, flip: flip, scalar: scalar)
    case .and(let conditions):
      var mapped: [AST.Condition] = []
      for c in conditions { mapped.append(try map(c, table: table)) }
      return .and(mapped)
    case .or(let conditions):
      var mapped: [AST.Condition] = []
      for c in conditions { mapped.append(try map(c, table: table)) }
      return .or(mapped)
    }
  }

  private func map(
    _ correlation: AST.Correlation, parent: String, child: String
  )
    throws(NameMappingError) -> AST.Correlation
  {
    var parentField: [String] = []
    for c in correlation.parentField { parentField.append(try columnName(parent, c)) }
    var childField: [String] = []
    for c in correlation.childField { childField.append(try columnName(child, c)) }
    return AST.Correlation(parentField: parentField, childField: childField)
  }
}

extension ZeroSchema {
  /// The schema zero-cache checks the replica against, under server names
  /// (`clientSchemaFrom` in `packages/zero-schema/src/builder/schema-builder.ts`).
  package var clientSchema: ClientSchema {
    var tables: [String: ClientSchema.Table] = [:]
    for (name, table) in self.tables {
      var columns: [String: ClientSchema.ValueType] = [:]
      for (column, c) in table.columns {
        columns[c.serverName ?? column] = ClientSchema.ValueType(rawValue: c.type.rawValue)!
      }
      tables[table.serverName ?? name] = ClientSchema.Table(
        columns: columns,
        primaryKey: table.primaryKey.map { table.columns[$0]?.serverName ?? $0 })
    }
    return ClientSchema(tables: tables).normalized()
  }
}
