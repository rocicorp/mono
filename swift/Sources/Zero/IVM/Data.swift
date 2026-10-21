/// Value comparison for IVM (`packages/zql/src/ivm/data.ts`).
package enum ValueCompare {
  package struct IncomparableError: Error, CustomStringConvertible {
    package let description: String
  }

  /// `compareValues(a, b)`: strings by UTF-8 bytes, numbers numerically,
  /// `false < true`, and `null` before everything. Values of different
  /// non-null types, and arrays or objects, can't be compared: JS throws.
  package static func compareChecked(_ a: Value, _ b: Value) throws(IncomparableError) -> Int {
    switch (a, b) {
    case (.string(let x), .string(let y)):
      return StringCompare.utf8(x, y)
    case (.number(let x), .number(let y)):
      return x < y ? -1 : x > y ? 1 : 0
    case (.bool(let x), .bool(let y)):
      return x == y ? 0 : x ? 1 : -1
    case (.null, .null):
      return 0
    case (.null, _):
      return -1
    case (_, .null):
      return 1
    default:
      if a.typeName != b.typeName {
        throw IncomparableError(
          description: "Cannot compare values of different types: \(a.typeName) and \(b.typeName)")
      }
      throw IncomparableError(description: "Unsupported type: \(a.typeName)")
    }
  }

  /// `compareChecked`, with a total order for the inputs JS throws on: by
  /// type, then by JSON encoding. Pipelines use this so a bad `orderBy` (on
  /// a json column) degrades instead of trapping.
  @inlinable
  package static func compare(_ a: Value, _ b: Value) -> Int {
    switch (a, b) {
    case (.string(let x), .string(let y)):
      return StringCompare.utf8(x, y)
    case (.number(let x), .number(let y)):
      return x < y ? -1 : x > y ? 1 : 0
    case (.bool(let x), .bool(let y)):
      return x == y ? 0 : x ? 1 : -1
    case (.null, .null):
      return 0
    case (.null, _):
      return -1
    case (_, .null):
      return 1
    default:
      return fallbackCompare(a, b)
    }
  }

  @usableFromInline
  static func fallbackCompare(_ a: Value, _ b: Value) -> Int {
    func rank(_ v: Value) -> Int {
      switch v {
      case .null: 0
      case .bool: 1
      case .number: 2
      case .string: 3
      case .array: 4
      case .object: 5
      }
    }
    let r = rank(a) - rank(b)
    if r != 0 { return r }
    return StringCompare.utf8(JSONSerializer.stringify(a), JSONSerializer.stringify(b))
  }

  /// `valuesEqual(a, b)`: like `===`, except `null` equals nothing (as in
  /// SQL joins). Arrays and objects are never equal: JS compares them by
  /// reference.
  @inlinable
  package static func equal(_ a: Value, _ b: Value) -> Bool {
    switch (a, b) {
    case (.null, _), (_, .null): return false
    case (.string(let x), .string(let y)): return StringCompare.equal(x, y)
    case (.number(let x), .number(let y)): return x == y
    case (.bool(let x), .bool(let y)): return x == y
    default: return false
    }
  }

  /// `compareValues` on possibly missing values: `undefined` reads as `null`.
  @inlinable
  package static func compare(_ a: Value?, _ b: Value?) -> Int {
    compare(a ?? .null, b ?? .null)
  }

  /// `valuesEqual` on possibly missing values: `undefined` equals nothing.
  @inlinable
  package static func equal(_ a: Value?, _ b: Value?) -> Bool {
    guard let a, let b else { return false }
    return equal(a, b)
  }

  /// JS `===` on possibly missing values. Strings compare by code unit;
  /// arrays and objects are never identical, since rows are decoded fresh.
  @inlinable
  package static func identical(_ a: Value?, _ b: Value?) -> Bool {
    switch (a, b) {
    case (nil, nil): return true
    case (nil, _), (_, nil): return false
    case (.null?, .null?): return true
    case (.string(let x)?, .string(let y)?): return StringCompare.equal(x, y)
    case (.number(let x)?, .number(let y)?): return x == y
    case (.bool(let x)?, .bool(let y)?): return x == y
    default: return false
    }
  }

  /// A row's value for `column`; a missing column reads as `null`.
  @inlinable
  package static func value(_ row: Row, _ column: String) -> Value {
    row[column] ?? .null
  }
}

/// Orders rows (`Comparator` in `data.ts`).
package typealias Comparator = (Row, Row) -> Int

/// `makeComparator(order, reverse)`. Rows in `layout` read the columns by
/// index.
package func makeComparator(
  _ order: AST.Ordering, reverse: Bool = false, layout: RowLayout = .empty
) -> Comparator {
  if order.count == 1 && order[0].direction == .asc && !reverse {
    let field = layout.column(order[0].field)
    return { a, b in ValueCompare.compare(a[column: field], b[column: field]) }
  }
  let parts = order.map { (layout.column($0.field), $0.direction == .asc) }
  return { a, b in
    for (field, asc) in parts {
      let comp = ValueCompare.compare(a[column: field], b[column: field])
      if comp != 0 {
        let result = asc ? comp : -comp
        return reverse ? -result : result
      }
    }
    return 0
  }
}
