/// AST normalization (`normalizeAST` in `packages/zero-protocol/src/ast.ts`):
/// `related` sorted by alias, `where` flattened and sorted. Two ASTs that
/// describe the same query hash the same once normalized.
extension AST {
  package func normalized() -> AST {
    var ast = self
    ast.where = `where`.map { Self.normalizeSubqueries($0) }.map(Self.normalizeCondition)
    if let related {
      let normalized = related.map { r -> CorrelatedSubquery in
        var r = r
        r.subquery = r.subquery.normalized()
        return r
      }
      ast.related = Self.normalizeRelated(normalized)
    }
    return ast
  }

  static func normalizeSubqueries(_ cond: Condition) -> Condition {
    switch cond {
    case .simple:
      return cond
    case .correlatedSubquery(var related, let op, let flip, let scalar):
      related.subquery = related.subquery.normalized()
      return .correlatedSubquery(related: related, op: op, flip: flip, scalar: scalar)
    case .and(let c):
      return .and(c.map(normalizeSubqueries))
    case .or(let c):
      return .or(c.map(normalizeSubqueries))
    }
  }

  static func normalizeRelated(_ related: [CorrelatedSubquery]) -> [CorrelatedSubquery] {
    for i in related.indices.dropFirst() where cmpRelated(related[i - 1], related[i]) > 0 {
      return JSSort.sorted(related, by: cmpRelated)
    }
    return related
  }

  package static func normalizeCondition(_ cond: Condition) -> Condition {
    switch cond {
    case .simple, .correlatedSubquery:
      return cond
    case .and(let children), .or(let children):
      let isAnd: Bool
      if case .and = cond { isAnd = true } else { isAnd = false }
      var conditions: [Condition] = []
      for c in children {
        let n = normalizeCondition(c)
        switch (isAnd, n) {
        case (true, .and(let nested)), (false, .or(let nested)):
          conditions.append(contentsOf: nested)
        default:
          conditions.append(n)
        }
      }
      // A singleton conjunction is the condition itself. Empty ones are
      // meaningful: an empty `and` is true and an empty `or` is false.
      if conditions.count == 1 {
        return conditions[0]
      }
      conditions = JSSort.sorted(conditions, by: cmpCondition)
      return isAnd ? .and(conditions) : .or(conditions)
    }
  }

  // MARK: - Comparators, ported verbatim (including their asymmetries, since
  // the sort order decides the hash).

  static func typeName(_ c: Condition) -> String {
    switch c {
    case .simple: "simple"
    case .and: "and"
    case .or: "or"
    case .correlatedSubquery: "correlatedSubquery"
    }
  }

  static func cmpCondition(_ a: Condition, _ b: Condition) -> Int {
    if case .simple(let aop, let al, let ar) = a {
      guard case .simple(let bop, let bl, let br) = b else { return -1 }
      let l = compareValuePosition(al, bl)
      if l != 0 { return l }
      let o = StringCompare.utf8(aop.rawValue, bop.rawValue)
      if o != 0 { return o }
      return compareValuePosition(ar, br)
    }
    if case .simple = b { return 1 }

    if case .correlatedSubquery(let ar, let aop, let af, let asc) = a {
      guard case .correlatedSubquery(let br, let bop, let bf, let bsc) = b else { return -1 }
      let r = cmpRelated(ar, br)
      if r != 0 { return r }
      let o = StringCompare.utf8(aop.rawValue, bop.rawValue)
      if o != 0 { return o }
      let f = cmpOptionalBool(af, bf)
      if f != 0 { return f }
      return cmpOptionalBool(asc, bsc)
    }
    if case .correlatedSubquery = b { return -1 }

    let t = StringCompare.utf8(typeName(a), typeName(b))
    if t != 0 { return t }
    let ac = a.conditions!
    let bc = b.conditions!
    for (l, r) in zip(ac, bc) {
      let v = cmpCondition(l, r)
      if v != 0 { return v }
    }
    return ac.count - bc.count
  }

  static func valuePositionType(_ v: ValuePosition) -> String {
    switch v {
    case .literal: "literal"
    case .column: "column"
    case .static: "static"
    }
  }

  static func compareValuePosition(_ a: ValuePosition, _ b: ValuePosition) -> Int {
    switch (a, b) {
    case (.literal(let x), .literal(let y)):
      return cmpLiteralValue(x, y)
    case (.column(let x), .column(let y)):
      return StringCompare.utf8(x, y)
    case (.static(let aa, let af), .static(let ba, let bf)):
      let c = StringCompare.utf8(aa.rawValue, ba.rawValue)
      if c != 0 { return c }
      return cmpLiteralValue(af.json, bf.json)
    default:
      return StringCompare.utf8(valuePositionType(a), valuePositionType(b))
    }
  }

  // null < boolean < number < string < array.
  static func literalValueType(_ v: JSONValue) -> Int {
    switch v {
    case .null, .object: 0
    case .bool: 1
    case .number: 2
    case .string: 3
    case .array: 4
    }
  }

  static func cmpLiteralValue(_ a: JSONValue, _ b: JSONValue) -> Int {
    let t = literalValueType(a) - literalValueType(b)
    if t != 0 { return t }
    switch (a, b) {
    case (.array(let x), .array(let y)):
      for (l, r) in zip(x, y) {
        let v = cmpLiteralValue(l, r)
        if v != 0 { return v }
      }
      return x.count - y.count
    case (.string(let x), .string(let y)):
      return StringCompare.utf8(x, y)
    case (.number(let x), .number(let y)):
      return x < y ? -1 : x > y ? 1 : 0
    case (.bool(let x), .bool(let y)):
      return (x ? 1 : 0) - (y ? 1 : 0)
    default:
      return 0
    }
  }

  static func cmpRelated(_ a: CorrelatedSubquery, _ b: CorrelatedSubquery) -> Int {
    StringCompare.utf8(a.subquery.alias ?? "", b.subquery.alias ?? "")
  }

  static func cmpOptionalBool(_ a: Bool?, _ b: Bool?) -> Int {
    func n(_ v: Bool?) -> Int { v == nil ? 0 : v! ? 2 : 1 }
    return n(a) - n(b)
  }
}

extension AST.StaticField {
  var json: JSONValue {
    switch self {
    case .single(let s): .string(s)
    case .path(let p): .array(p.map { .string($0) })
    }
  }
}
