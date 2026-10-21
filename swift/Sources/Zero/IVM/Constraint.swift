/// Column values a fetch must match (`packages/zql/src/ivm/constraint.ts`).
///
/// Keys keep insertion order, like the JS object: a `MemorySource` builds
/// its index sort from it. A value is `nil` where JS has `undefined`.
package struct Constraint {
  package var entries: [(key: String, value: Value?)]

  @inlinable
  package init(_ entries: [(key: String, value: Value?)] = []) {
    self.entries = entries
  }

  @inlinable
  package var keys: [String] { entries.map(\.key) }

  @inlinable
  package var count: Int { entries.count }

  @inlinable
  package func has(_ key: String) -> Bool {
    entries.contains { $0.key == key }
  }

  /// The value for `key`; `nil` when absent or `undefined`.
  @inlinable
  package subscript(key: String) -> Value? {
    get {
      for e in entries where e.key == key { return e.value }
      return nil
    }
  }

  @inlinable
  package mutating func set(_ key: String, _ value: Value?) {
    if let i = entries.firstIndex(where: { $0.key == key }) {
      entries[i].value = value
    } else {
      entries.append((key, value))
    }
  }

  /// `{...self, ...other}`.
  @inlinable
  package func merging(_ other: Constraint) -> Constraint {
    var result = self
    for e in other.entries { result.set(e.key, e.value) }
    return result
  }

  /// `constraintMatchesRow`.
  @inlinable
  package func matches(_ row: Row) -> Bool {
    for e in entries where !ValueCompare.equal(row[e.key], e.value) {
      return false
    }
    return true
  }

  /// `constraintsAreCompatible`: no shared key has different values.
  package func isCompatible(with other: Constraint) -> Bool {
    for e in entries where other.has(e.key) && !ValueCompare.equal(e.value, other[e.key]) {
      return false
    }
    return true
  }

  /// `constraintMatchesPrimaryKey`.
  package func matchesPrimaryKey(_ primaryKey: [String]) -> Bool {
    keyMatchesPrimaryKey(keys, primaryKey)
  }

  /// `{key: row[key]}` for each of `keys`.
  static func from(_ row: Row, _ keys: [String]) -> Constraint {
    Constraint(keys.map { ($0, row[$0]) })
  }
}

/// `keyMatchesPrimaryKey`. The primary key is sorted; `key` need not be.
func keyMatchesPrimaryKey(_ key: [String], _ primaryKey: [String]) -> Bool {
  if key.count != primaryKey.count { return false }
  let sorted = key.sorted(by: StringCompare.utf16Less)
  for i in 0..<sorted.count where !StringCompare.equal(sorted[i], primaryKey[i]) {
    return false
  }
  return true
}

/// `pullSimpleAndComponents`: the top-level AND conjuncts that are simple.
func pullSimpleAndComponents(_ condition: AST.Condition) -> [AST.Condition] {
  switch condition {
  case .and(let conditions):
    return conditions.flatMap(pullSimpleAndComponents)
  case .simple:
    return [condition]
  case .or(let conditions) where conditions.count == 1:
    return pullSimpleAndComponents(conditions[0])
  default:
    return []
  }
}

/// `primaryKeyConstraintFromFilters`: the primary key lookup the filters
/// amount to, if they do.
func primaryKeyConstraintFromFilters(
  _ condition: AST.Condition?, _ primaryKey: [String]
) -> Constraint? {
  guard let condition else { return nil }
  let conditions = pullSimpleAndComponents(condition)
  if conditions.isEmpty { return nil }
  var ret = Constraint()
  for case .simple(let op, let left, let right) in conditions where op == .eq {
    if case .column(let name) = left {
      guard case .literal(let value) = right else {
        preconditionFailure("extractColumn: expected right side to be literal")
      }
      if !primaryKey.contains(name) { continue }
      ret.set(name, value)
    }
  }
  if ret.count != primaryKey.count { return nil }
  return ret
}
