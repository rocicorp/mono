/// Row predicates from conditions (`packages/zql/src/builder/filter.ts`).
package typealias RowPredicate = (Row) -> Bool

/// An error building a pipeline, where JS throws.
package struct PipelineError: Error, CustomStringConvertible {
  package let description: String
  package init(_ description: String) { self.description = description }
}

/// `createPredicate(condition)` for a condition without subqueries.
func createPredicate(_ condition: AST.Condition) throws(PipelineError) -> RowPredicate {
  switch condition {
  case .and(let conditions):
    var predicates: [RowPredicate] = []
    for c in conditions { predicates.append(try createPredicate(c)) }
    return { row in
      for p in predicates where !p(row) { return false }
      return true
    }
  case .or(let conditions):
    var predicates: [RowPredicate] = []
    for c in conditions { predicates.append(try createPredicate(c)) }
    return { row in
      for p in predicates where p(row) { return true }
      return false
    }
  case .correlatedSubquery:
    throw PipelineError("createPredicate: unexpected correlated subquery")
  case .simple(let op, let left, let right):
    guard case .literal(let rhs) = right else {
      if case .static = right {
        throw PipelineError("static values should be resolved before creating predicates")
      }
      throw PipelineError("createPredicate: right side must be a literal")
    }
    if case .static = left {
      throw PipelineError("static values should be resolved before creating predicates")
    }
    switch op {
    case .is, .isNot:
      let isNot = op == .isNot
      let impl: (Value?) -> Bool = { lhs in
        ValueCompare.identical(lhs, rhs) != isNot
      }
      if case .literal(let l) = left {
        let result = impl(l)
        return { _ in result }
      }
      guard case .column(let name) = left else { preconditionFailure() }
      let column = ColumnReader(name)
      return { row in impl(column.read(row)) }
    default:
      break
    }
    if rhs == .null { return { _ in false } }
    let impl = try createPredicateImpl(rhs, op)
    if case .literal(let l) = left {
      if l == .null { return { _ in false } }
      let result = impl(l)
      return { _ in result }
    }
    guard case .column(let name) = left else { preconditionFailure() }
    let column = ColumnReader(name)
    return { row in
      guard let lhs = column.read(row), lhs != .null else { return false }
      return impl(lhs)
    }
  }
}

/// Reads a column of rows, looking its index up once for each layout
/// rather than hashing its name for each row.
final class ColumnReader {
  let name: String
  // Unchecked: the engine runs pipelines on the main actor, so no read of
  // the cache overlaps a write to it.
  @exclusivity(unchecked) private var column: ColumnRef?

  init(_ name: String) { self.name = name }

  func read(_ row: Row) -> Value? {
    let layout = row.layout
    if let column, column.layout === layout { return row[column: column] }
    let column = layout.column(name)
    self.column = column
    return row[column: column]
  }
}

/// `createPredicateImpl`: `lhs` and `rhs` are not null.
func createPredicateImpl(
  _ rhs: Value, _ op: AST.SimpleOperator
) throws(PipelineError) -> (Value) -> Bool {
  switch op {
  case .eq: return { ValueCompare.identical($0, rhs) }
  case .ne: return { !ValueCompare.identical($0, rhs) }
  // compareValues orders strings by UTF-8, matching ORDER BY and SQLite.
  case .lt: return { ValueCompare.compare($0, rhs) < 0 }
  case .le: return { ValueCompare.compare($0, rhs) <= 0 }
  case .gt: return { ValueCompare.compare($0, rhs) > 0 }
  case .ge: return { ValueCompare.compare($0, rhs) >= 0 }
  case .like: return try likePredicate(rhs, caseInsensitive: false)
  case .notLike:
    let p = try likePredicate(rhs, caseInsensitive: false)
    return { !p($0) }
  case .ilike: return try likePredicate(rhs, caseInsensitive: true)
  case .notIlike:
    let p = try likePredicate(rhs, caseInsensitive: true)
    return { !p($0) }
  case .in, .notIn:
    guard case .array(let values) = rhs else {
      throw PipelineError("Expected rhs to be an array for \(op.rawValue) operator")
    }
    let isIn = op == .in
    return { lhs in sameValueZeroContains(values, lhs) == isIn }
  case .is, .isNot:
    preconditionFailure("handled by createPredicate")
  }
}

/// `new Set(values).has(lhs)`: SameValueZero, arrays and objects by
/// reference (so never).
func sameValueZeroContains(_ values: [Value], _ lhs: Value) -> Bool {
  switch lhs {
  case .array, .object: return false
  case .number(let n) where n.isNaN:
    return values.contains { if case .number(let m) = $0 { m.isNaN } else { false } }
  default:
    return values.contains { ValueCompare.identical($0, lhs) }
  }
}

/// `getLikePredicate(pattern, flags)`. JS converts the pattern with
/// `String()` and throws on a non-string left side; Swift treats that as no
/// match.
func likePredicate(_ pattern: Value, caseInsensitive: Bool) throws(PipelineError) -> (Value) -> Bool
{
  let source: String
  switch pattern {
  case .string(let s): source = s
  case .number(let n): source = JSNumber.format(n)
  case .bool(let b): source = b ? "true" : "false"
  case .null: source = "null"
  case .array, .object: source = JSString.from(pattern)
  }
  let like: LikePattern
  do {
    like = try LikePattern(source, caseInsensitive: caseInsensitive)
  } catch {
    throw PipelineError(error.description)
  }
  return { lhs in
    guard case .string(let s) = lhs else { return false }
    return like.matches(s)
  }
}

/// JS `String(value)` for JSON values.
enum JSString {
  static func from(_ v: Value) -> String {
    switch v {
    case .null: "null"
    case .bool(let b): b ? "true" : "false"
    case .number(let n): JSNumber.format(n)
    case .string(let s): s
    case .array(let a): a.map { e in e == .null ? "" : from(e) }.joined(separator: ",")
    case .object: "[object Object]"
    }
  }
}

/// `transformFilters`: a condition without subqueries that matches a
/// superset of the rows `filters` matches, and whether anything was removed.
func transformFilters(
  _ filters: AST.Condition?
) -> (filters: AST.Condition?, conditionsRemoved: Bool) {
  guard let filters else { return (nil, false) }
  switch filters {
  case .simple:
    return (filters, false)
  case .correlatedSubquery:
    return (nil, true)
  case .and(let conditions), .or(let conditions):
    let isOr = if case .or = filters { true } else { false }
    var transformed: [AST.Condition] = []
    var removed = false
    for c in conditions {
      let t = transformFilters(c)
      // If any branch of an OR ends up empty, the whole OR goes.
      if t.filters == nil && isOr { return (nil, true) }
      removed = removed || t.conditionsRemoved
      if let f = t.filters { transformed.append(f) }
    }
    return (simplifyCondition(isOr ? .or(transformed) : .and(transformed)), removed)
  }
}

/// `simplifyCondition` (`packages/zql/src/query/expression.ts`).
package func simplifyCondition(_ c: AST.Condition) -> AST.Condition {
  switch c {
  case .simple, .correlatedSubquery:
    return c
  case .and(let conditions), .or(let conditions):
    let isAnd = if case .and = c { true } else { false }
    if conditions.count == 1 { return simplifyCondition(conditions[0]) }
    let flattened = flatten(isAnd, conditions.map(simplifyCondition))
    if isAnd && flattened.contains(where: \.isAlwaysFalse) { return .falseCondition }
    if !isAnd && flattened.contains(where: \.isAlwaysTrue) { return .trueCondition }
    return isAnd ? .and(flattened) : .or(flattened)
  }
}

/// `flatten(type, conditions)`: inlines children of the same type.
func flatten(_ isAnd: Bool, _ conditions: [AST.Condition]) -> [AST.Condition] {
  var out: [AST.Condition] = []
  for c in conditions {
    switch c {
    case .and(let inner) where isAnd: out.append(contentsOf: inner)
    case .or(let inner) where !isAnd: out.append(contentsOf: inner)
    default: out.append(c)
    }
  }
  return out
}

extension AST.Condition {
  /// `TRUE`: an empty AND.
  package static var trueCondition: AST.Condition { .and([]) }
  /// `FALSE`: an empty OR.
  package static var falseCondition: AST.Condition { .or([]) }

  var isAlwaysTrue: Bool {
    if case .and(let c) = self { c.isEmpty } else { false }
  }

  var isAlwaysFalse: Bool {
    if case .or(let c) = self { c.isEmpty } else { false }
  }
}
