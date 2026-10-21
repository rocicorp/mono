/// A change to a set of queries as zero-cache sends it: a client's desired
/// queries or the queries whose rows are synced
/// (`patchOpSchema` in `packages/zero-protocol/src/queries-patch.ts`).
package enum QueriesPatchOp: Sendable, Hashable, JSONCodable {
  case put(hash: String, ttl: Double?)
  case del(hash: String)
  case clear

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "queries patch op")
    switch try r.string("op") {
    case "put": self = .put(hash: try r.string("hash"), ttl: try r.optionalNumber("ttl"))
    case "del": self = .del(hash: try r.string("hash"))
    case "clear": self = .clear
    case let op: throw DecodingError("Invalid queries patch op \"\(op)\"", path: ["op"])
    }
  }

  package var json: JSONValue {
    switch self {
    case .put(let hash, let ttl):
      var o = JSONObjectBuilder()
      o.set("op", "put")
      o.set("hash", hash)
      o.set("ttl", ttl)
      return o.value
    case .del(let hash): return ["op": "del", "hash": .string(hash)]
    case .clear: return ["op": "clear"]
    }
  }
}

/// `queriesPatchSchema`.
package typealias QueriesPatch = [QueriesPatchOp]

/// A query a client wants synced (`upPutOpSchema`). A client query carries its
/// `ast`; a custom query its `name` and `args`.
package struct UpQueriesPut: Sendable, Hashable {
  package var hash: String
  package var ttl: Double?
  package var ast: AST?
  package var name: String?
  package var args: [JSONValue]?

  package init(
    hash: String, ttl: Double? = nil, ast: AST? = nil, name: String? = nil,
    args: [JSONValue]? = nil
  ) {
    self.hash = hash
    self.ttl = ttl
    self.ast = ast
    self.name = name
    self.args = args
  }
}

/// A change to a client's desired queries, as the client sends it
/// (`upPatchOpSchema` in `packages/zero-protocol/src/queries-patch.ts`).
package enum UpQueriesPatchOp: Sendable, Hashable, JSONCodable {
  case put(UpQueriesPut)
  case del(hash: String)
  case clear

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "queries patch op")
    switch try r.string("op") {
    case "put":
      self = .put(
        UpQueriesPut(
          hash: try r.string("hash"),
          ttl: try r.optionalNumber("ttl"),
          ast: try r.optional("ast", StrictAST.decode),
          name: try r.optionalString("name"),
          args: try r.optional("args", JSONDecode.array)))
    case "del": self = .del(hash: try r.string("hash"))
    case "clear": self = .clear
    case let op: throw DecodingError("Invalid queries patch op \"\(op)\"", path: ["op"])
    }
  }

  package var json: JSONValue {
    switch self {
    case .put(let put):
      var o = JSONObjectBuilder()
      o.set("op", "put")
      o.set("hash", put.hash)
      o.set("ttl", put.ttl)
      o.set("ast", put.ast?.json)
      o.set("name", put.name)
      o.set("args", put.args.map { JSONValue.array($0) })
      return o.value
    case .del(let hash): return ["op": "del", "hash": .string(hash)]
    case .clear: return ["op": "clear"]
    }
  }
}

/// `upQueriesPatchSchema`.
package typealias UpQueriesPatch = [UpQueriesPatchOp]

extension JSONDecode {
  static func queriesPatch(_ v: JSONValue) throws(DecodingError) -> QueriesPatch {
    try array(v, QueriesPatchOp.init(json:))
  }

  static func upQueriesPatch(_ v: JSONValue) throws(DecodingError) -> UpQueriesPatch {
    try array(v, UpQueriesPatchOp.init(json:))
  }
}
