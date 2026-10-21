/// A row struct of a table, as `zero-swift codegen` generates it: the
/// table's columns as stored properties, and an optional property per
/// relationship that a query fills when it asks for it with `related`.
public protocol ZeroRow: Sendable {
  /// The client name of the row's table.
  static var tableName: String { get }
  /// The client column name of each column property.
  static var columns: KeyPathNames<Self> { get }
  /// The relationship name of each relationship property.
  static var relationships: KeyPathNames<Self> { get }

  /// Reads a row of a materialized view.
  init(entry: ViewEntry) throws(ZeroValueError)
  /// The row's columns, keyed by client column name.
  var columnValues: Row { get }
}

/// Names by key path: the columns or relationships of a row struct. Key
/// paths are immutable, so the table is safe to share.
public struct KeyPathNames<Root>: @unchecked Sendable, ExpressibleByDictionaryLiteral {
  let names: [PartialKeyPath<Root>: String]

  public init(dictionaryLiteral elements: (PartialKeyPath<Root>, String)...) {
    names = Dictionary(uniqueKeysWithValues: elements)
  }

  public subscript(keyPath: PartialKeyPath<Root>) -> String? { names[keyPath] }
}

extension ZeroRow {
  static func column(_ keyPath: PartialKeyPath<Self>) -> String {
    guard let name = columns[keyPath] else {
      preconditionFailure("\(keyPath) is not a column of \(tableName)")
    }
    return name
  }

  static func relationship(_ keyPath: PartialKeyPath<Self>) -> String {
    guard let name = relationships[keyPath] else {
      preconditionFailure("\(keyPath) is not a relationship of \(tableName)")
    }
    return name
  }
}

extension ViewEntry {
  /// A column of the row, for generated `init(entry:)`s. A null in a
  /// column that can't be null reads as `V.valueForNull`.
  @inlinable
  public func column<V: ZeroValue>(_ name: String) throws(ZeroValueError) -> V {
    try Self.decode(name, row[name] ?? .null)
  }

  /// A column of the row, for generated `init(entry:)`s: `index` is its
  /// position in the table's schema, where the row has it unless it came
  /// from elsewhere. Reading it there skips looking the name up.
  @inlinable
  public func column<V: ZeroValue>(_ name: String, _ index: Int) throws(ZeroValueError) -> V {
    let names = row.layout.names
    if index < names.count && names[index] == name {
      return try Self.decode(name, row[slot: index] ?? .null)
    }
    return try Self.decode(name, row[name] ?? .null)
  }

  @inlinable
  static func decode<V: ZeroValue>(_ name: String, _ value: JSONValue) throws(ZeroValueError) -> V {
    if case .null = value, let fallback = V.valueForNull { return fallback }
    do {
      return try V(zeroValue: value)
    } catch {
      throw ZeroValueError("\(name): \(error.description)")
    }
  }

  /// A many relationship: nil when the query didn't ask for it.
  public func relationship<R: ZeroRow>(_ name: String) throws(ZeroValueError) -> [R]? {
    guard let value = relationships[name] else { return nil }
    var out: [R] = []
    for e in value.entries { out.append(try R(entry: e)) }
    return out
  }

  /// A one relationship: nil when the query didn't ask for it or no row
  /// matches.
  public func relationship<R: ZeroRow>(_ name: String) throws(ZeroValueError) -> R? {
    guard let entry = relationships[name]?.entries.first else { return nil }
    return try R(entry: entry)
  }
}

/// What a query materializes to: `[Row]`, or `Row?` after `one()`.
public protocol QueryResult: Sendable {
  associatedtype Row: ZeroRow
  init(view: ViewValue) throws(ZeroValueError)
  /// The result of rows already read, in the view's order.
  init(rows: [Row])
}

extension Array: QueryResult where Element: ZeroRow {
  public typealias Row = Element

  @inlinable
  public init(view: ViewValue) throws(ZeroValueError) {
    let entries = view.entries
    var out: [Element] = []
    out.reserveCapacity(entries.count)
    for e in entries { out.append(try Element(entry: e)) }
    self = out
  }

  public init(rows: [Element]) { self = rows }
}

extension Optional: QueryResult where Wrapped: ZeroRow {
  public typealias Row = Wrapped

  public init(view: ViewValue) throws(ZeroValueError) {
    self = try view.entries.first.map { (e) throws(ZeroValueError) in try Wrapped(entry: e) }
  }

  public init(rows: [Wrapped]) { self = rows.first }
}

/// A query over the rows of one table, typed by its generated row struct
/// (`Query` in `packages/zql/src/query/query.ts`). Columns and relationships
/// are named by key path.
///
/// ```swift
/// zql.issue
///   .where(\.status, .open)
///   .where { $0.priority >= 2 && ($0.assignee == me || $0.assignee == nil) }
///   .whereExists(\.labels) { $0.where(\.name, "bug") }
///   .related(\.comments) { $0.orderBy(\.created, .desc).limit(10) }
///   .orderBy(\.modified, .desc)
///   .limit(50)
/// ```
public struct Query<Row: ZeroRow, Result: QueryResult>: Sendable where Result.Row == Row {
  /// The untyped query this one builds.
  public let any: AnyQuery

  public init(_ any: AnyQuery) {
    self.any = any
  }

  public var ast: AST { any.ast }
  package var format: Format { any.format }
  public var hash: String { any.hash }

  /// Compares a column with a value. Comparing with nil uses `IS`, since
  /// `= NULL` matches nothing.
  public func `where`<V: ZeroValue>(_ column: KeyPath<Row, V>, _ value: V) -> Self {
    Self(any.where((Column<V>(Row.column(column)) == value).ast))
  }

  public func `where`<V: ZeroValue>(
    _ column: KeyPath<Row, V>, _ op: AST.SimpleOperator, _ value: V
  ) -> Self {
    Self(any.where(Row.column(column), op, value.zeroValue))
  }

  /// Compares a column with a value that may be nil. nil compares with
  /// `op` as JS compares `undefined`, so `.eq` with nil matches no row.
  public func `where`<V: ZeroValue>(
    _ column: KeyPath<Row, V>, _ op: AST.SimpleOperator, _ value: V?
  ) -> Self {
    Self(any.where(Row.column(column), op, value?.zeroValue ?? .null))
  }

  /// `IN` and `NOT IN`.
  public func `where`<V: ZeroValue>(
    _ column: KeyPath<Row, V>, _ op: AST.SimpleOperator, _ values: [V]
  ) -> Self {
    Self(any.where(Row.column(column), op, .array(values.map(\.zeroValue))))
  }

  public func `where`<V: ZeroValue>(
    _ column: KeyPath<Row, V>, _ op: AST.SimpleOperator, _ param: StaticParameter
  ) -> Self {
    Self(any.where(Row.column(column), op, param))
  }

  public func `where`(_ condition: (RowExpression<Row>) -> Condition) -> Self {
    Self(any.where { condition(RowExpression(builder: $0)).ast })
  }

  public func whereExists<R: ZeroRow>(
    _ relationship: KeyPath<Row, [R]?>, flip: Bool? = nil, scalar: Bool? = nil,
    _ sub: ((Query<R, [R]>) -> Query<R, [R]>)? = nil
  ) -> Self {
    Self(any.whereExists(Row.relationship(relationship), flip: flip, scalar: scalar, untyped(sub)))
  }

  public func whereExists<R: ZeroRow>(
    _ relationship: KeyPath<Row, R?>, flip: Bool? = nil, scalar: Bool? = nil,
    _ sub: ((Query<R, [R]>) -> Query<R, [R]>)? = nil
  ) -> Self {
    Self(any.whereExists(Row.relationship(relationship), flip: flip, scalar: scalar, untyped(sub)))
  }

  /// Fills a many relationship of each row.
  public func related<R: ZeroRow, S: QueryResult>(
    _ relationship: KeyPath<Row, [R]?>, _ sub: @escaping (Query<R, [R]>) -> Query<R, S>
  ) -> Self where S.Row == R {
    Self(any.related(Row.relationship(relationship)) { sub(Query<R, [R]>($0)).any })
  }

  public func related<R: ZeroRow>(_ relationship: KeyPath<Row, [R]?>) -> Self {
    Self(any.related(Row.relationship(relationship)))
  }

  /// Fills a one relationship of each row.
  public func related<R: ZeroRow, S: QueryResult>(
    _ relationship: KeyPath<Row, R?>, _ sub: @escaping (Query<R, [R]>) -> Query<R, S>
  ) -> Self where S.Row == R {
    Self(any.related(Row.relationship(relationship)) { sub(Query<R, [R]>($0)).any })
  }

  public func related<R: ZeroRow>(_ relationship: KeyPath<Row, R?>) -> Self {
    Self(any.related(Row.relationship(relationship)))
  }

  public func orderBy<V: ZeroValue>(
    _ column: KeyPath<Row, V>, _ direction: AST.Direction
  )
    -> Self
  {
    Self(any.orderBy(Row.column(column), direction))
  }

  public func limit(_ limit: Int) -> Self {
    Self(any.limit(limit))
  }

  /// Starts after `row`, or at it when `inclusive`.
  public func start(_ row: Row, inclusive: Bool = false) -> Self {
    Self(any.start(row.columnValues, inclusive: inclusive))
  }

  /// Starts after the row with these column values, or at it when
  /// `inclusive`: a cursor that holds only the columns the query orders by,
  /// as in `.start([\.created: page.created, \.id: page.id])`.
  public func start(
    _ values: [PartialKeyPath<Row>: any ZeroValue], inclusive: Bool = false
  ) -> Self {
    var row: [String: JSONValue] = [:]
    for (keyPath, value) in values { row[Row.column(keyPath)] = value.zeroValue }
    return Self(any.start(row, inclusive: inclusive))
  }

  /// At most one row.
  public func one() -> Query<Row, Row?> {
    Query<Row, Row?>(any.one())
  }

  private func untyped<R: ZeroRow>(
    _ sub: ((Query<R, [R]>) -> Query<R, [R]>)?
  )
    -> ((AnyQuery) -> AnyQuery)?
  {
    sub.map { sub in { sub(Query<R, [R]>($0)).any } }
  }
}

/// A condition of a `where`.
public struct Condition: Sendable, Hashable {
  public let ast: AST.Condition

  public init(_ ast: AST.Condition) {
    self.ast = ast
  }

  public static func && (a: Condition, b: Condition) -> Condition {
    Condition(andConditions([a.ast, b.ast]))
  }

  public static func || (a: Condition, b: Condition) -> Condition {
    Condition(orConditions([a.ast, b.ast]))
  }

  public static prefix func ! (c: Condition) -> Condition {
    Condition(negateCondition(c.ast))
  }

  /// Always true.
  public static var `true`: Condition { Condition(.trueCondition) }
  /// Always false.
  public static var `false`: Condition { Condition(.falseCondition) }
}

/// A column in a `where` expression.
public struct Column<Value: ZeroValue>: Sendable, Hashable {
  public let name: String

  public init(_ name: String) {
    self.name = name
  }

  func cmp(_ op: AST.SimpleOperator, _ value: JSONValue) -> Condition {
    Condition(cmpCondition(name, op, .literal(value)))
  }

  public static func == (c: Column, v: Value) -> Condition {
    let value = v.zeroValue
    return c.cmp(value == .null ? .is : .eq, value)
  }

  public static func != (c: Column, v: Value) -> Condition {
    let value = v.zeroValue
    return c.cmp(value == .null ? .isNot : .ne, value)
  }

  public static func < (c: Column, v: Value) -> Condition { c.cmp(.lt, v.zeroValue) }
  public static func <= (c: Column, v: Value) -> Condition { c.cmp(.le, v.zeroValue) }
  public static func > (c: Column, v: Value) -> Condition { c.cmp(.gt, v.zeroValue) }
  public static func >= (c: Column, v: Value) -> Condition { c.cmp(.ge, v.zeroValue) }

  public func `is`(_ v: Value) -> Condition { cmp(.is, v.zeroValue) }
  public func isNot(_ v: Value) -> Condition { cmp(.isNot, v.zeroValue) }
  public func `in`(_ values: [Value]) -> Condition { cmp(.in, .array(values.map(\.zeroValue))) }
  public func notIn(_ values: [Value]) -> Condition {
    cmp(.notIn, .array(values.map(\.zeroValue)))
  }

  public func compare(_ op: AST.SimpleOperator, _ param: StaticParameter) -> Condition {
    Condition(cmpCondition(name, op, param.position))
  }
}

extension Column where Value == String {
  public func like(_ pattern: String) -> Condition { cmp(.like, .string(pattern)) }
  public func notLike(_ pattern: String) -> Condition { cmp(.notLike, .string(pattern)) }
  public func ilike(_ pattern: String) -> Condition { cmp(.ilike, .string(pattern)) }
  public func notIlike(_ pattern: String) -> Condition { cmp(.notIlike, .string(pattern)) }
}

extension Column where Value == String? {
  public func like(_ pattern: String) -> Condition { cmp(.like, .string(pattern)) }
  public func notLike(_ pattern: String) -> Condition { cmp(.notLike, .string(pattern)) }
  public func ilike(_ pattern: String) -> Condition { cmp(.ilike, .string(pattern)) }
  public func notIlike(_ pattern: String) -> Condition { cmp(.notIlike, .string(pattern)) }
}

/// The row a `where` expression is about: its columns by key path, and
/// `exists` over its relationships.
@dynamicMemberLookup
public struct RowExpression<Row: ZeroRow>: Sendable {
  let builder: ExpressionBuilder

  public subscript<V: ZeroValue>(dynamicMember column: KeyPath<Row, V>) -> Column<V> {
    Column(Row.column(column))
  }

  public func exists<R: ZeroRow>(
    _ relationship: KeyPath<Row, [R]?>, flip: Bool? = nil, scalar: Bool? = nil,
    _ sub: ((Query<R, [R]>) -> Query<R, [R]>)? = nil
  ) -> Condition {
    exists(Row.relationship(relationship), flip: flip, scalar: scalar, sub)
  }

  public func exists<R: ZeroRow>(
    _ relationship: KeyPath<Row, R?>, flip: Bool? = nil, scalar: Bool? = nil,
    _ sub: ((Query<R, [R]>) -> Query<R, [R]>)? = nil
  ) -> Condition {
    exists(Row.relationship(relationship), flip: flip, scalar: scalar, sub)
  }

  private func exists<R: ZeroRow>(
    _ name: String, flip: Bool?, scalar: Bool?, _ sub: ((Query<R, [R]>) -> Query<R, [R]>)?
  ) -> Condition {
    Condition(
      builder.exists(
        name, flip: flip, scalar: scalar, sub.map { sub in { sub(Query<R, [R]>($0)).any } }))
  }

  /// All of `conditions`, leaving out the nil ones; true when there are
  /// none.
  public func and(_ conditions: Condition?...) -> Condition { and(conditions) }

  public func and(_ conditions: [Condition?]) -> Condition {
    Condition(andConditions(conditions.compactMap { $0?.ast }))
  }

  /// Any of `conditions`, leaving out the nil ones; false when there are
  /// none.
  public func or(_ conditions: Condition?...) -> Condition { or(conditions) }

  public func or(_ conditions: [Condition?]) -> Condition {
    Condition(orConditions(conditions.compactMap { $0?.ast }))
  }

  public func not(_ condition: Condition) -> Condition { !condition }
}
