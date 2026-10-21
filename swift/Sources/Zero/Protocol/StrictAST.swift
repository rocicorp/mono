/// `strictASTSchema` (`packages/zero-protocol/src/ast.ts`), for ASTs a
/// client sends: nested at most ``maxDepth`` deep, and parsed in valita's
/// `strict` mode even inside a lenient parse, so an AST with a key zero
/// doesn't know is rejected rather than run as a different query.
enum StrictAST {
  /// `MAX_AST_DEPTH`.
  static let maxDepth = 50

  static func decode(_ v: JSONValue) throws(DecodingError) -> AST {
    if exceedsDepth(v) {
      throw DecodingError("AST exceeds the maximum depth of \(maxDepth)")
    }
    try checkAST(v)
    return try AST(json: v)
  }

  /// `exceedsASTDepth`: every step down counts, except the one from an AST
  /// into its where clause.
  static func exceedsDepth(_ ast: JSONValue, maxDepth: Int = maxDepth) -> Bool {
    astExceedsDepth(ast, 0, maxDepth)
  }

  /// `isPlainObject` (`packages/shared/src/deep-merge.ts`) is true for
  /// arrays too; they have none of the keys looked at.
  private static func plainObject(_ v: JSONValue?) -> [String: JSONValue]? {
    switch v {
    case .object(let o)?: o
    case .array?: [:]
    default: nil
    }
  }

  private static func astExceedsDepth(_ ast: JSONValue?, _ depth: Int, _ maxDepth: Int) -> Bool {
    if depth > maxDepth { return true }
    guard let o = plainObject(ast) else { return false }
    if conditionExceedsDepth(o["where"], depth, maxDepth) { return true }
    guard case .array(let related)? = o["related"] else { return false }
    return related.contains { r in
      guard let r = plainObject(r) else { return false }
      return astExceedsDepth(r["subquery"], depth + 1, maxDepth)
    }
  }

  private static func conditionExceedsDepth(
    _ cond: JSONValue?, _ depth: Int, _ maxDepth: Int
  ) -> Bool {
    if depth > maxDepth { return true }
    guard let o = plainObject(cond) else { return false }
    if case .array(let conditions)? = o["conditions"],
      conditions.contains(where: { conditionExceedsDepth($0, depth + 1, maxDepth) })
    {
      return true
    }
    if let related = plainObject(o["related"]) {
      return astExceedsDepth(related["subquery"], depth + 1, maxDepth)
    }
    return false
  }

  // The keys each object of the AST may have. `AST(json:)` checks the
  // values; this only rejects the keys `strict` mode would.

  private static func only(_ o: [String: JSONValue], _ keys: Set<String>) throws(DecodingError) {
    for k in o.keys where !keys.contains(k) {
      throw DecodingError("Unexpected key", path: [k])
    }
  }

  private static func checkAST(_ v: JSONValue) throws(DecodingError) {
    guard case .object(let o) = v else { return }
    try only(o, ["schema", "table", "alias", "where", "related", "limit", "orderBy", "start"])
    if let w = o["where"] { try checkCondition(w) }
    if case .array(let related)? = o["related"] {
      for r in related { try checkCorrelatedSubquery(r) }
    }
    if case .object(let start)? = o["start"] { try only(start, ["row", "exclusive"]) }
  }

  private static func checkCorrelatedSubquery(_ v: JSONValue) throws(DecodingError) {
    guard case .object(let o) = v else { return }
    try only(o, ["correlation", "hidden", "system", "subquery"])
    if case .object(let c)? = o["correlation"] { try only(c, ["parentField", "childField"]) }
    if let s = o["subquery"] { try checkAST(s) }
  }

  private static func checkCondition(_ v: JSONValue) throws(DecodingError) {
    guard case .object(let o) = v else { return }
    switch o["type"]?.stringValue {
    case "simple":
      try only(o, ["type", "op", "left", "right"])
      if let l = o["left"] { try checkValuePosition(l) }
      if let r = o["right"] { try checkValuePosition(r) }
    case "and", "or":
      try only(o, ["type", "conditions"])
      if case .array(let conditions)? = o["conditions"] {
        for c in conditions { try checkCondition(c) }
      }
    case "correlatedSubquery":
      try only(o, ["type", "related", "op", "flip", "scalar"])
      if let r = o["related"] { try checkCorrelatedSubquery(r) }
    default:
      return
    }
  }

  private static func checkValuePosition(_ v: JSONValue) throws(DecodingError) {
    guard case .object(let o) = v else { return }
    switch o["type"]?.stringValue {
    case "literal": try only(o, ["type", "value"])
    case "column": try only(o, ["type", "name"])
    case "static": try only(o, ["type", "anchor", "field"])
    default: return
    }
  }
}
