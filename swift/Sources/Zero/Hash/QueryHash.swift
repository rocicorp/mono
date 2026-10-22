/// Query identities, ported from `packages/zero-protocol/src/query-hash-visitor.ts`.
///
/// The JS implementation folds an AST (or a name and args) into two xxHash32
/// lanes word by word. These digests are the query IDs the client registers,
/// so the Swift client reproduces them exactly.
package enum QueryHash {
  struct State {
    var h1: UInt32 = XXHash32.prime5
    var h2: UInt32 = XXHash32.prime1

    @inline(__always) mutating func mix(_ w: UInt32) {
      h1 = XXHash32.round(h1, w)
      h2 = XXHash32.round(h2, w)
    }

    @inline(__always) mutating func mix(_ w: Int) {
      mix(UInt32(bitPattern: Int32(truncatingIfNeeded: w)))
    }

    mutating func mixString(_ s: String) {
      let units = Array(s.utf16)
      let n = units.count
      mix(UInt32(truncatingIfNeeded: n) | strMark)
      var i = 0
      while i + 1 < n {
        mix(UInt32(units[i]) | UInt32(units[i + 1]) << 16)
        i += 2
      }
      if i < n {
        mix(UInt32(units[i]))
      }
    }

    mutating func mixNumber(_ n: Double) {
      if JSNumber.isInt32(n) {
        mix(JSNumber.int32Bits(n))
      } else {
        let bits = n.bitPattern  // little-endian words: low first
        mix(UInt32(truncatingIfNeeded: bits))
        mix(UInt32(truncatingIfNeeded: bits >> 32))
      }
    }

    func finalize() -> String {
      func base36(_ v: UInt32) -> String {
        let s = String(v, radix: 36)
        return String(repeating: "0", count: max(0, 7 - s.count)) + s
      }
      return base36(XXHash32.avalanche(h1)) + base36(XXHash32.avalanche(h2))
    }
  }

  static let tagNull: UInt32 = 0x1001
  static let tagFalse: UInt32 = 0x1002
  static let tagTrue: UInt32 = 0x1003
  static let tagInt: UInt32 = 0x1004
  static let tagFloat: UInt32 = 0x1005
  static let tagArr: UInt32 = 0x1007
  static let tagObj: UInt32 = 0x1008
  static let tagEnd: UInt32 = 0x1009
  static let tagUndef: UInt32 = 0x100a

  static let tagLiteral: UInt32 = 0x2001
  static let tagColumn: UInt32 = 0x2002
  static let tagStatic: UInt32 = 0x2003
  static let tagSimple: UInt32 = 0x2004
  static let tagSubquery: UInt32 = 0x2005
  static let tagAnd: UInt32 = 0x2006
  static let tagOr: UInt32 = 0x2007
  static let tagFormat: UInt32 = 0x2008
  static let tagNameArgs: UInt32 = 0x2009
  static let tagSystemPermissions: UInt32 = 0x200a
  static let tagSystemClient: UInt32 = 0x200b
  static let tagSystemTest: UInt32 = 0x200c
  static let tagJunction: UInt32 = 0x200d

  static let strMark: UInt32 = 0x4000_0000

  /// `hashOfAST`: the hash of `ast` after normalizing it.
  package static func hashOfAST(_ ast: AST) -> String {
    hashOfNormalizedAST(ast.normalized())
  }

  /// The hash of an AST that is already normalized.
  package static func hashOfNormalizedAST(_ ast: AST) -> String {
    var s = State()
    s.visitAST(ast)
    return s.finalize()
  }

  /// `hashOfNameAndArgs`: the identity of a named (custom) query.
  package static func hashOfNameAndArgs(_ name: String, _ args: [JSONValue]) -> String {
    var s = State()
    s.mix(tagNameArgs)
    s.mixString(name)
    s.visitValue(.array(args))
    return s.finalize()
  }

  /// `hashOfQueryInternals`: the identity of a query as the client sees it.
  package static func hashOfQueryInternals(
    ast: AST,
    format: JSONValue,
    system: AST.System,
    currentJunction: String?,
    customQueryName: String?,
    customQueryArgs: [JSONValue]?
  ) -> String {
    var s = State()
    s.visitAST(ast)
    s.mix(tagFormat)
    s.visitValue(format)
    s.mixSystem(system)
    if let currentJunction {
      s.mix(tagJunction)
      s.mixString(currentJunction)
    } else {
      s.mix(tagUndef)
    }
    if let customQueryName {
      s.mix(tagNameArgs)
      s.mixString(customQueryName)
      if let customQueryArgs {
        s.visitValue(.array(customQueryArgs))
      }
    } else {
      s.mix(tagUndef)
    }
    return s.finalize()
  }
}

extension QueryHash.State {
  typealias H = QueryHash

  /// Visits `v` without recursion: the arrays and objects it is in are on
  /// a stack, so nesting takes heap, not stack.
  mutating func visitValue(_ v: JSONValue) {
    if visitScalar(v) { return }
    // An array, or an object's entries in `Object.keys(v).sort()` order,
    // and the index of the next value.
    var containers: [(entries: Entries, next: Int)] = []
    var value = v
    nested: while true {
      switch value {
      case .array(let a):
        mix(H.tagArr)
        containers.append((.array(a), 0))
      case .object(let o):
        mix(H.tagObj)
        // By UTF-16 code unit, whatever the order.
        containers.append((.object(o.sorted { StringCompare.utf16Less($0.key, $1.key) }), 0))
      default:
        preconditionFailure("not an array or object")
      }
      // Visits values until one is an array or object, ending the
      // containers that are done.
      while var container = containers.popLast() {
        while container.next < container.entries.count {
          let e: JSONValue
          switch container.entries {
          case .array(let a):
            e = a[container.next]
          case .object(let entries):
            mixString(entries[container.next].key)
            e = entries[container.next].value
          }
          container.next += 1
          if visitScalar(e) { continue }
          containers.append(container)
          value = e
          continue nested
        }
        mix(H.tagEnd)
      }
      return
    }
  }

  enum Entries {
    case array([JSONValue])
    case object([(key: String, value: JSONValue)])

    var count: Int {
      switch self {
      case .array(let a): a.count
      case .object(let o): o.count
      }
    }
  }

  /// Visits `v` unless it is an array or object; returns whether it did.
  private mutating func visitScalar(_ v: JSONValue) -> Bool {
    switch v {
    case .string(let s):
      mixString(s)
    case .number(let n):
      // NaN and Infinity are null on the wire (JSON.stringify writes null).
      if !n.isFinite {
        mix(H.tagNull)
      } else {
        mix(JSNumber.isInt32(n) ? H.tagInt : H.tagFloat)
        mixNumber(n)
      }
    case .bool(let b):
      mix(b ? H.tagTrue : H.tagFalse)
    case .null:
      mix(H.tagNull)
    case .array, .object:
      return false
    }
    return true
  }

  mutating func visitOptionalString(_ s: String?) {
    if let s { mixString(s) } else { mix(H.tagUndef) }
  }

  mutating func mixSystem(_ system: AST.System) {
    switch system {
    case .permissions: mix(H.tagSystemPermissions)
    case .client: mix(H.tagSystemClient)
    case .test: mix(H.tagSystemTest)
    }
  }

  mutating func mixOptionalBool(_ b: Bool?) {
    switch b {
    case nil: mix(H.tagUndef)
    case true?: mix(H.tagTrue)
    case false?: mix(H.tagFalse)
    }
  }

  mutating func visitCompoundKey(_ k: [String]) {
    mix(k.count)
    for s in k { mixString(s) }
  }

  mutating func visitOrdering(_ o: AST.Ordering?) {
    guard let o else {
      mix(H.tagUndef)
      return
    }
    mix(o.count)
    for part in o {
      mixString(part.field)
      mix(part.direction == .asc ? H.tagTrue : H.tagFalse)
    }
  }

  mutating func visitValuePosition(_ v: AST.ValuePosition) {
    switch v {
    case .literal(let value):
      mix(H.tagLiteral)
      visitValue(value)
    case .column(let name):
      mix(H.tagColumn)
      mixString(name)
    case .static(let anchor, let field):
      mix(H.tagStatic)
      mixString(anchor.rawValue)
      switch field {
      case .single(let f): mixString(f)
      case .path(let p): visitCompoundKey(p)
      }
    }
  }

  mutating func visitCondition(_ c: AST.Condition) {
    switch c {
    case .simple(let op, let left, let right):
      mix(H.tagSimple)
      mixString(op.rawValue)
      visitValuePosition(left)
      visitValuePosition(right)
    case .correlatedSubquery(let related, let op, let flip, let scalar):
      mix(H.tagSubquery)
      mixString(op.rawValue)
      mixOptionalBool(flip)
      mixOptionalBool(scalar)
      visitCorrelatedSubquery(related)
    case .and(let conditions), .or(let conditions):
      if case .and = c { mix(H.tagAnd) } else { mix(H.tagOr) }
      mix(conditions.count)
      for cond in conditions { visitCondition(cond) }
    }
  }

  mutating func visitCorrelatedSubquery(_ csq: AST.CorrelatedSubquery) {
    visitCompoundKey(csq.correlation.parentField)
    visitCompoundKey(csq.correlation.childField)
    mixOptionalBool(csq.hidden)
    if let system = csq.system { mixSystem(system) } else { mix(H.tagUndef) }
    visitAST(csq.subquery)
  }

  mutating func visitAST(_ ast: AST) {
    mix(H.tagObj)
    visitOptionalString(ast.schema)
    mixString(ast.table)
    visitOptionalString(ast.alias)
    if let w = ast.where { visitCondition(w) } else { mix(H.tagUndef) }
    if let related = ast.related {
      mix(related.count)
      for r in related { visitCorrelatedSubquery(r) }
    } else {
      mix(H.tagUndef)
    }
    if let start = ast.start {
      mix(start.exclusive ? H.tagTrue : H.tagFalse)
      visitValue(.object(start.row))
    } else {
      mix(H.tagUndef)
    }
    if let limit = ast.limit {
      mix(H.tagInt)
      mixNumber(limit)
    } else {
      mix(H.tagUndef)
    }
    visitOrdering(ast.orderBy)
    mix(H.tagEnd)
  }
}
