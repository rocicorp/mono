/// The ZQL abstract syntax tree (`packages/zero-protocol/src/ast.ts`).
public struct AST: Sendable, Hashable {
  public var schema: String?
  public var table: String
  /// Set on subqueries, to tell apart two subqueries of the same table.
  public var alias: String?
  public var `where`: Condition?
  public var related: [CorrelatedSubquery]?
  public var start: Bound?
  public var limit: Double?
  public var orderBy: Ordering?

  public init(
    schema: String? = nil,
    table: String,
    alias: String? = nil,
    where: Condition? = nil,
    related: [CorrelatedSubquery]? = nil,
    start: Bound? = nil,
    limit: Double? = nil,
    orderBy: Ordering? = nil
  ) {
    self.schema = schema
    self.table = table
    self.alias = alias
    self.where = `where`
    self.related = related
    self.start = start
    self.limit = limit
    self.orderBy = orderBy
  }

  public enum System: String, Sendable, Hashable {
    case permissions, client, test
  }

  public struct Bound: Sendable, Hashable {
    /// The cursor's columns: those the query orders by.
    public var row: JSONObject
    public var exclusive: Bool

    public init(row: JSONObject, exclusive: Bool) {
      self.row = row
      self.exclusive = exclusive
    }
  }

  public enum Direction: String, Codable, Sendable, Hashable {
    case asc, desc
  }

  public struct OrderPart: Sendable, Hashable {
    public var field: String
    public var direction: Direction

    public init(_ field: String, _ direction: Direction) {
      self.field = field
      self.direction = direction
    }
  }

  public typealias Ordering = [OrderPart]

  public enum SimpleOperator: String, Sendable, Hashable, CaseIterable {
    case eq = "="
    case ne = "!="
    case `is` = "IS"
    case isNot = "IS NOT"
    case lt = "<"
    case gt = ">"
    case le = "<="
    case ge = ">="
    case like = "LIKE"
    case notLike = "NOT LIKE"
    case ilike = "ILIKE"
    case notIlike = "NOT ILIKE"
    case `in` = "IN"
    case notIn = "NOT IN"
  }

  public enum CorrelatedSubqueryOperator: String, Sendable, Hashable {
    case exists = "EXISTS"
    case notExists = "NOT EXISTS"
  }

  public enum Anchor: String, Sendable, Hashable {
    case authData
    case preMutationRow
  }

  public enum StaticField: Sendable, Hashable {
    case single(String)
    case path([String])
  }

  /// One side of a simple condition. A literal is a string, number, boolean,
  /// null, or an array of strings, numbers, and booleans.
  public enum ValuePosition: Sendable, Hashable {
    case literal(JSONValue)
    case column(String)
    case `static`(anchor: Anchor, field: StaticField)
  }

  public indirect enum Condition: Sendable, Hashable {
    case simple(op: SimpleOperator, left: ValuePosition, right: ValuePosition)
    case and([Condition])
    case or([Condition])
    case correlatedSubquery(
      related: CorrelatedSubquery, op: CorrelatedSubqueryOperator, flip: Bool?, scalar: Bool?)
  }

  public struct Correlation: Sendable, Hashable {
    public var parentField: [String]
    public var childField: [String]

    public init(parentField: [String], childField: [String]) {
      self.parentField = parentField
      self.childField = childField
    }
  }

  public struct CorrelatedSubquery: Sendable, Hashable {
    public var correlation: Correlation
    public var subquery: AST
    public var system: System?
    /// Hidden hops (junction tables) are left out of the output view; their
    /// children are not.
    public var hidden: Bool?

    public init(
      correlation: Correlation, subquery: AST, system: System? = nil, hidden: Bool? = nil
    ) {
      self.correlation = correlation
      self.subquery = subquery
      self.system = system
      self.hidden = hidden
    }
  }
}

extension AST.Condition {
  public var conditions: [AST.Condition]? {
    switch self {
    case .and(let c), .or(let c): c
    default: nil
    }
  }
}
