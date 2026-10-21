/// JSON encoding of the AST, following `astSchema`.
extension AST: JSONCodable {
  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "AST")
    schema = try r.optionalString("schema")
    table = try r.string("table")
    alias = try r.optionalString("alias")
    self.where = try r.optional("where", Condition.init(json:))
    related = try r.optional("related") { (v) throws(DecodingError) in
      try JSONDecode.array(v, CorrelatedSubquery.init(json:))
    }
    limit = try r.optionalNumber("limit")
    orderBy = try r.optional("orderBy") { (v) throws(DecodingError) in
      try JSONDecode.array(v) { (e) throws(DecodingError) -> OrderPart in
        guard case .array(let t) = e, t.count == 2, case .string(let field) = t[0],
          case .string(let dir) = t[1], let direction = Direction(rawValue: dir)
        else { throw DecodingError("Expected [field, 'asc' | 'desc']") }
        return OrderPart(field, direction)
      }
    }
    start = try r.optional("start") { (v) throws(DecodingError) -> Bound in
      let b = try JSONObjectReader(v, "bound")
      return Bound(
        row: try b.required("row", JSONDecode.object), exclusive: try b.bool("exclusive"))
    }
  }

  package var json: JSONValue {
    var o = JSONObjectBuilder()
    o.set("schema", schema)
    o.set("table", .string(table))
    o.set("alias", alias)
    o.set("where", self.where?.json)
    o.set("related", related.map { JSONValue.array($0.map(\.json)) })
    o.set(
      "start",
      start.map { JSONValue.object(["row": .object($0.row), "exclusive": .bool($0.exclusive)]) })
    o.set("limit", limit)
    o.set(
      "orderBy",
      orderBy.map {
        JSONValue.array($0.map { .array([.string($0.field), .string($0.direction.rawValue)]) })
      })
    return o.value
  }
}

extension AST.CorrelatedSubquery: JSONCodable {
  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "correlated subquery")
    correlation = try r.required("correlation") { (v) throws(DecodingError) in
      let c = try JSONObjectReader(v, "correlation")
      return AST.Correlation(
        parentField: try c.required("parentField", JSONDecode.compoundKey),
        childField: try c.required("childField", JSONDecode.compoundKey))
    }
    subquery = try r.required("subquery", AST.init(json:))
    system = try r.optional("system") { (v) throws(DecodingError) in
      let s = try JSONDecode.string(v)
      guard let system = AST.System(rawValue: s) else {
        throw DecodingError("Invalid system \"\(s)\"")
      }
      return system
    }
    hidden = try r.optionalBool("hidden")
  }

  package var json: JSONValue {
    var o = JSONObjectBuilder()
    o.set(
      "correlation",
      .object([
        "parentField": correlation.parentField.json, "childField": correlation.childField.json,
      ]))
    o.set("subquery", subquery.json)
    o.set("system", system?.rawValue)
    o.set("hidden", hidden)
    return o.value
  }
}

extension AST.ValuePosition: JSONCodable {
  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "value position")
    let type = try r.string("type")
    switch type {
    case "literal":
      let value = try r.required("value") { (v) throws(DecodingError) -> JSONValue in
        switch v {
        case .string, .number, .bool, .null: return v
        case .array(let a):
          for e in a {
            switch e {
            case .string, .number, .bool: continue
            default: throw DecodingError("Invalid literal array element")
            }
          }
          return v
        case .object: throw DecodingError("Invalid literal value")
        }
      }
      self = .literal(value)
    case "column":
      self = .column(try r.string("name"))
    case "static":
      let anchorName = try r.string("anchor")
      guard let anchor = AST.Anchor(rawValue: anchorName) else {
        throw DecodingError("Invalid anchor \"\(anchorName)\"", path: ["anchor"])
      }
      let field = try r.required("field") { (v) throws(DecodingError) -> AST.StaticField in
        switch v {
        case .string(let s): return .single(s)
        case .array: return .path(try JSONDecode.array(v, JSONDecode.string))
        default: throw DecodingError("Expected string or string array")
        }
      }
      self = .static(anchor: anchor, field: field)
    default:
      throw DecodingError("Invalid value position type \"\(type)\"", path: ["type"])
    }
  }

  package var json: JSONValue {
    switch self {
    case .literal(let v): ["type": "literal", "value": v]
    case .column(let name): ["type": "column", "name": .string(name)]
    case .static(let anchor, let field):
      ["type": "static", "anchor": .string(anchor.rawValue), "field": field.json]
    }
  }
}

extension AST.Condition: JSONCodable {
  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "condition")
    let type = try r.string("type")
    switch type {
    case "simple":
      let opName = try r.string("op")
      guard let op = AST.SimpleOperator(rawValue: opName) else {
        throw DecodingError("Invalid operator \"\(opName)\"", path: ["op"])
      }
      let left = try r.required("left", AST.ValuePosition.init(json:))
      let right = try r.required("right", AST.ValuePosition.init(json:))
      if case .column = right {
        throw DecodingError("The right side of a condition can't be a column", path: ["right"])
      }
      self = .simple(op: op, left: left, right: right)
    case "and", "or":
      let conditions = try r.required("conditions") { (v) throws(DecodingError) in
        try JSONDecode.array(v, AST.Condition.init(json:))
      }
      self = type == "and" ? .and(conditions) : .or(conditions)
    case "correlatedSubquery":
      let opName = try r.string("op")
      guard let op = AST.CorrelatedSubqueryOperator(rawValue: opName) else {
        throw DecodingError("Invalid operator \"\(opName)\"", path: ["op"])
      }
      self = .correlatedSubquery(
        related: try r.required("related", AST.CorrelatedSubquery.init(json:)),
        op: op,
        flip: try r.optionalBool("flip"),
        scalar: try r.optionalBool("scalar"))
    default:
      throw DecodingError("Invalid condition type \"\(type)\"", path: ["type"])
    }
  }

  package var json: JSONValue {
    switch self {
    case .simple(let op, let left, let right):
      return ["type": "simple", "op": .string(op.rawValue), "left": left.json, "right": right.json]
    case .and(let c):
      return ["type": "and", "conditions": .array(c.map(\.json))]
    case .or(let c):
      return ["type": "or", "conditions": .array(c.map(\.json))]
    case .correlatedSubquery(let related, let op, let flip, let scalar):
      var o = JSONObjectBuilder()
      o.set("type", "correlatedSubquery")
      o.set("related", related.json)
      o.set("op", .string(op.rawValue))
      o.set("flip", flip)
      o.set("scalar", scalar)
      return o.value
    }
  }
}
