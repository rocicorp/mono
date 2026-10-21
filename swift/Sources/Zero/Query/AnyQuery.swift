/// The prefix of the aliases the builder gives `exists` subqueries
/// (`SUBQ_PREFIX` in `packages/zero-protocol/src/ast.ts`).
package let subqueryAliasPrefix = "zsubq_"

/// The name and arguments of the named query a query was built by.
public struct CustomQueryID: Sendable, Hashable {
  public var name: String
  public var args: [JSONValue]

  public init(name: String, args: [JSONValue]) {
    self.name = name
    self.args = args
  }
}

/// A value taken from the context a query runs in, such as the auth data
/// (`staticParam` in `packages/zql/src/query/static-param.ts`).
public struct StaticParameter: Sendable, Hashable {
  public var anchor: AST.Anchor
  public var field: AST.StaticField

  public init(_ anchor: AST.Anchor, _ field: String) {
    self.anchor = anchor
    self.field = .single(field)
  }

  public init(_ anchor: AST.Anchor, path: [String]) {
    self.anchor = anchor
    self.field = .path(path)
  }

  var position: AST.ValuePosition { .static(anchor: anchor, field: field) }
}

/// The untyped query builder (`QueryImpl` in
/// `packages/zql/src/query/query-impl.ts`). Fields and relationships are
/// named by string; the typed builder wraps this one.
///
/// The AST is always normalized: each step builds its part of the AST in
/// normalized form. Misuse that JS reports by throwing, such as an unknown
/// relationship or a `limit` inside a junction relationship, is a
/// programming error here and stops the program.
public struct AnyQuery: Sendable {
  public let schema: ZeroSchema
  public let table: String
  public let ast: AST
  package let format: Format
  public let system: AST.System
  public let customQueryID: CustomQueryID?
  /// The two-hop relationship this query is the destination of, if any.
  let currentJunction: String?

  public init(_ schema: ZeroSchema, _ table: String, system: AST.System = .client) {
    self.init(
      schema: schema, table: table, ast: AST(table: table), format: Format(), system: system,
      customQueryID: nil, currentJunction: nil)
  }

  init(
    schema: ZeroSchema, table: String, ast: AST, format: Format, system: AST.System,
    customQueryID: CustomQueryID?, currentJunction: String?
  ) {
    self.schema = schema
    self.table = table
    self.ast = ast
    self.format = format
    self.system = system
    self.customQueryID = customQueryID
    self.currentJunction = currentJunction
  }

  private func with(ast: AST? = nil, format: Format? = nil) -> AnyQuery {
    AnyQuery(
      schema: schema, table: table, ast: ast ?? self.ast, format: format ?? self.format,
      system: system, customQueryID: customQueryID, currentJunction: currentJunction)
  }

  private func base(_ table: String, alias: String, format: Format, junction: String?) -> AnyQuery {
    AnyQuery(
      schema: schema, table: table, ast: AST(table: table, alias: alias), format: format,
      system: system, customQueryID: customQueryID, currentJunction: junction)
  }

  /// The query's hash as the client and server know it.
  public var hash: String {
    QueryHash.hashOfQueryInternals(
      ast: ast, format: format.json, system: system, currentJunction: currentJunction,
      customQueryName: customQueryID?.name, customQueryArgs: customQueryID?.args)
  }

  public func nameAndArgs(_ name: String, _ args: [JSONValue]) -> AnyQuery {
    AnyQuery(
      schema: schema, table: table, ast: ast, format: format, system: system,
      customQueryID: CustomQueryID(name: name, args: args), currentJunction: currentJunction)
  }

  /// At most one row; the view holds the row or nothing rather than a list.
  public func one() -> AnyQuery {
    var ast = self.ast
    ast.limit = 1
    var format = self.format
    format.singular = true
    return with(ast: ast, format: format)
  }

  public func `where`(_ field: String, _ op: AST.SimpleOperator, _ value: JSONValue) -> AnyQuery {
    `where`(cmpCondition(field, op, .literal(value)))
  }

  public func `where`(_ field: String, _ value: JSONValue) -> AnyQuery {
    `where`(field, .eq, value)
  }

  public func `where`(
    _ field: String, _ op: AST.SimpleOperator, _ param: StaticParameter
  )
    -> AnyQuery
  {
    `where`(cmpCondition(field, op, param.position))
  }

  public func `where`(_ factory: (ExpressionBuilder) -> AST.Condition) -> AnyQuery {
    `where`(factory(ExpressionBuilder(self)))
  }

  public func `where`(_ condition: AST.Condition) -> AnyQuery {
    let merged = ast.where.map { andConditions([$0, condition]) } ?? condition
    var ast = self.ast
    ast.where = AST.normalizeCondition(simplifyCondition(merged))
    return with(ast: ast)
  }

  public func whereExists(
    _ relationship: String, flip: Bool? = nil, scalar: Bool? = nil,
    _ sub: ((AnyQuery) -> AnyQuery)? = nil
  ) -> AnyQuery {
    `where` { $0.exists(relationship, flip: flip, scalar: scalar, sub) }
  }

  public func start(_ row: Row, inclusive: Bool = false) -> AnyQuery {
    start(row.dictionary, inclusive: inclusive)
  }

  /// Starts after the row with these columns, or at it when `inclusive`.
  public func start(_ row: [String: JSONValue], inclusive: Bool = false) -> AnyQuery {
    var ast = self.ast
    ast.start = AST.Bound(row: row, exclusive: !inclusive)
    return with(ast: ast)
  }

  public func limit(_ limit: Int) -> AnyQuery {
    precondition(limit >= 0, "Limit must be non-negative")
    precondition(Int32(exactly: limit) != nil, "Limit must be an integer")
    if let currentJunction {
      preconditionFailure(
        "Limit is not supported in junction relationships yet. Junction relationship being limited: \(currentJunction)"
      )
    }
    var ast = self.ast
    ast.limit = Double(limit)
    return with(ast: ast)
  }

  public func orderBy(_ field: String, _ direction: AST.Direction) -> AnyQuery {
    if let currentJunction {
      preconditionFailure(
        "Order by is not supported in junction relationships yet. Junction relationship being ordered: \(currentJunction)"
      )
    }
    var ast = self.ast
    ast.orderBy = (ast.orderBy ?? []) + [AST.OrderPart(field, direction)]
    return with(ast: ast)
  }

  public func related(_ relationship: String, _ sub: ((AnyQuery) -> AnyQuery)? = nil) -> AnyQuery {
    precondition(
      !relationship.hasPrefix(subqueryAliasPrefix),
      "Relationship names may not start with \"\(subqueryAliasPrefix)\". That is a reserved prefix."
    )
    let sub = sub ?? { $0 }
    let hops = relationshipHops(relationship)
    var ast = self.ast
    var format = self.format
    switch hops.count {
    case 1:
      let hop = hops[0]
      let q = sub(
        base(
          hop.destSchema, alias: relationship,
          format: Format(singular: hop.cardinality == .one), junction: nil))
      ast.related = insertRelated(
        ast.related,
        AST.CorrelatedSubquery(
          correlation: correlation(hop), subquery: q.ast, system: system))
      format.relationships[relationship] = q.format
    case 2:
      let (first, second) = (hops[0], hops[1])
      let q = sub(
        base(
          second.destSchema, alias: relationship,
          format: Format(singular: second.cardinality == .one), junction: relationship))
      let junction = AST(
        table: first.destSchema, alias: relationship,
        related: [
          AST.CorrelatedSubquery(
            correlation: correlation(second), subquery: q.ast, system: system)
        ])
      ast.related = insertRelated(
        ast.related,
        AST.CorrelatedSubquery(
          correlation: correlation(first), subquery: junction, system: system, hidden: true))
      format.relationships[relationship] = q.format
    default:
      preconditionFailure("Invalid relationship \(relationship)")
    }
    return with(ast: ast, format: format)
  }

  /// `#exists`: the condition `exists(relationship)` adds.
  func existsCondition(
    _ relationship: String, flip: Bool?, scalar: Bool?, _ sub: ((AnyQuery) -> AnyQuery)?
  ) -> AST.Condition {
    let sub = sub ?? { $0 }
    let hops = relationshipHops(relationship)
    switch hops.count {
    case 1:
      let hop = hops[0]
      let q = sub(
        base(
          hop.destSchema, alias: subqueryAliasPrefix + relationship, format: Format(),
          junction: nil))
      return .correlatedSubquery(
        related: AST.CorrelatedSubquery(
          correlation: correlation(hop), subquery: q.ast, system: system),
        op: .exists, flip: flip, scalar: scalar)
    case 2:
      let (first, second) = (hops[0], hops[1])
      let q = sub(
        base(
          second.destSchema, alias: "\(subqueryAliasPrefix)zhidden_\(relationship)",
          format: Format(), junction: relationship))
      let inner = AST.Condition.correlatedSubquery(
        related: AST.CorrelatedSubquery(
          correlation: correlation(second), subquery: q.ast, system: system),
        op: .exists, flip: flip, scalar: scalar)
      let junction = AST(
        table: first.destSchema, alias: subqueryAliasPrefix + relationship,
        where: AST.normalizeCondition(inner))
      // The junction hop carries `flip` but not `scalar`, as in JS.
      return .correlatedSubquery(
        related: AST.CorrelatedSubquery(
          correlation: correlation(first), subquery: junction, system: system),
        op: .exists, flip: flip, scalar: nil)
    default:
      preconditionFailure("Invalid relationship \(relationship)")
    }
  }

  private func relationshipHops(_ relationship: String) -> Relationship {
    guard let hops = schema.relationship(table, relationship) else {
      preconditionFailure("Invalid relationship \(table).\(relationship)")
    }
    for hop in hops {
      precondition(
        !hop.sourceField.isEmpty && hop.sourceField.count == hop.destField.count,
        "Invalid relationship \(table).\(relationship)")
    }
    return hops
  }

  private func correlation(_ hop: RelationshipHop) -> AST.Correlation {
    AST.Correlation(parentField: hop.sourceField, childField: hop.destField)
  }
}

/// `insertRelated`: adds `subquery` to a normalized `related`, after the
/// entries it compares equal to.
func insertRelated(
  _ related: [AST.CorrelatedSubquery]?, _ subquery: AST.CorrelatedSubquery
)
  -> [AST.CorrelatedSubquery]
{
  guard var related else { return [subquery] }
  var i = related.count
  while i > 0 && AST.cmpRelated(related[i - 1], subquery) > 0 { i -= 1 }
  related.insert(subquery, at: i)
  return related
}

/// Builds the conditions of a `where` (`ExpressionBuilder` in
/// `packages/zql/src/query/expression.ts`).
public struct ExpressionBuilder: Sendable {
  let query: AnyQuery

  init(_ query: AnyQuery) { self.query = query }

  public func cmp(_ field: String, _ op: AST.SimpleOperator, _ value: JSONValue) -> AST.Condition {
    cmpCondition(field, op, .literal(value))
  }

  public func cmp(_ field: String, _ value: JSONValue) -> AST.Condition {
    cmpCondition(field, .eq, .literal(value))
  }

  public func cmp(
    _ field: String, _ op: AST.SimpleOperator, _ param: StaticParameter
  )
    -> AST.Condition
  {
    cmpCondition(field, op, param.position)
  }

  /// Compares two literals or parameters, neither of them a column.
  public func cmpLit(
    _ left: JSONValue, _ op: AST.SimpleOperator, _ right: JSONValue
  )
    -> AST.Condition
  {
    .simple(op: op, left: .literal(left), right: .literal(right))
  }

  public func cmpLit(
    _ left: StaticParameter, _ op: AST.SimpleOperator, _ right: JSONValue
  )
    -> AST.Condition
  {
    .simple(op: op, left: left.position, right: .literal(right))
  }

  public func and(_ conditions: AST.Condition?...) -> AST.Condition {
    andConditions(conditions.compactMap { $0 })
  }

  public func and(_ conditions: [AST.Condition]) -> AST.Condition { andConditions(conditions) }

  public func or(_ conditions: AST.Condition?...) -> AST.Condition {
    orConditions(conditions.compactMap { $0 })
  }

  public func or(_ conditions: [AST.Condition]) -> AST.Condition { orConditions(conditions) }

  public func not(_ condition: AST.Condition) -> AST.Condition { negateCondition(condition) }

  public func exists(
    _ relationship: String, flip: Bool? = nil, scalar: Bool? = nil,
    _ sub: ((AnyQuery) -> AnyQuery)? = nil
  ) -> AST.Condition {
    query.existsCondition(relationship, flip: flip, scalar: scalar, sub)
  }
}

func cmpCondition(
  _ field: String, _ op: AST.SimpleOperator, _ right: AST.ValuePosition
)
  -> AST.Condition
{
  .simple(op: op, left: .column(field), right: right)
}

/// `and(...)`: drops the always-true conditions; one left is itself.
func andConditions(_ conditions: [AST.Condition]) -> AST.Condition {
  let expressions = conditions.filter { !$0.isAlwaysTrue }
  if expressions.count == 1 { return expressions[0] }
  if expressions.contains(where: \.isAlwaysFalse) { return .falseCondition }
  return .and(expressions)
}

/// `or(...)`: drops the always-false conditions; one left is itself.
func orConditions(_ conditions: [AST.Condition]) -> AST.Condition {
  let expressions = conditions.filter { !$0.isAlwaysFalse }
  if expressions.count == 1 { return expressions[0] }
  if expressions.contains(where: \.isAlwaysTrue) { return .trueCondition }
  return .or(expressions)
}

/// `not`: pushes the negation down to the leaves (De Morgan).
func negateCondition(_ condition: AST.Condition) -> AST.Condition {
  switch condition {
  case .and(let c): .or(c.map(negateCondition))
  case .or(let c): .and(c.map(negateCondition))
  case .correlatedSubquery(let related, let op, let flip, let scalar):
    .correlatedSubquery(
      related: related, op: op == .exists ? .notExists : .exists, flip: flip, scalar: scalar)
  case .simple(let op, let left, let right):
    .simple(op: op.negated, left: left, right: right)
  }
}

extension AST.SimpleOperator {
  var negated: AST.SimpleOperator {
    switch self {
    case .eq: .ne
    case .ne: .eq
    case .lt: .ge
    case .gt: .le
    case .ge: .lt
    case .le: .gt
    case .in: .notIn
    case .notIn: .in
    case .like: .notLike
    case .notLike: .like
    case .ilike: .notIlike
    case .notIlike: .ilike
    case .is: .isNot
    case .isNot: .is
    }
  }
}
