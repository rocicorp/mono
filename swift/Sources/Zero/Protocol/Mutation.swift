/// The name of every CRUD mutation (`CRUD_MUTATION_NAME` in
/// `packages/zero-protocol/src/mutation.ts`).
package let crudMutationName = "_zero_crud"

/// One operation of a CRUD mutation (`crudOpSchema` in
/// `packages/zero-protocol/src/mutation.ts`).
package enum CRUDOp: Sendable, Hashable, JSONCodable {
  case insert(tableName: String, primaryKey: [String], value: RowRecord)
  case upsert(tableName: String, primaryKey: [String], value: RowRecord)
  /// `value` is a partial row with at least the primary key columns.
  case update(tableName: String, primaryKey: [String], value: RowRecord)
  /// `value` holds the primary key columns.
  case delete(tableName: String, primaryKey: [String], value: PrimaryKeyValueRecord)

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "CRUD op")
    let op = try r.string("op")
    switch op {
    case "insert", "upsert", "update", "delete":
      let tableName = try r.string("tableName")
      let primaryKey = try r.required("primaryKey", JSONDecode.compoundKey)
      switch op {
      case "insert":
        self = .insert(
          tableName: tableName, primaryKey: primaryKey,
          value: try r.required("value", JSONDecode.row))
      case "upsert":
        self = .upsert(
          tableName: tableName, primaryKey: primaryKey,
          value: try r.required("value", JSONDecode.row))
      case "update":
        self = .update(
          tableName: tableName, primaryKey: primaryKey,
          value: try r.required("value", JSONDecode.row))
      default:
        self = .delete(
          tableName: tableName, primaryKey: primaryKey,
          value: try r.required("value", JSONDecode.primaryKeyValueRecord))
      }
    default:
      throw DecodingError("Invalid CRUD op \"\(op)\"", path: ["op"])
    }
  }

  package var json: JSONValue {
    let (op, tableName, primaryKey, value): (String, String, [String], RowRecord) =
      switch self {
      case .insert(let t, let pk, let v): ("insert", t, pk, v)
      case .upsert(let t, let pk, let v): ("upsert", t, pk, v)
      case .update(let t, let pk, let v): ("update", t, pk, v)
      case .delete(let t, let pk, let v): ("delete", t, pk, v)
      }
    return [
      "op": .string(op), "tableName": .string(tableName), "primaryKey": primaryKey.json,
      "value": .object(value),
    ]
  }
}

/// A mutation of the legacy CRUD mutators (`crudMutationSchema`). Its name is
/// always ``crudMutationName`` and its args are `[{ops}]`.
package struct CRUDMutation: Sendable, Hashable {
  package var id: Double
  package var clientID: String
  package var ops: [CRUDOp]
  package var timestamp: Double

  package init(id: Double, clientID: String, ops: [CRUDOp], timestamp: Double) {
    self.id = id
    self.clientID = clientID
    self.ops = ops
    self.timestamp = timestamp
  }
}

/// A mutation of a custom mutator (`customMutationSchema`).
package struct CustomMutation: Sendable, Hashable {
  package var id: Double
  package var clientID: String
  package var name: String
  package var args: [JSONValue]
  package var timestamp: Double

  package init(id: Double, clientID: String, name: String, args: [JSONValue], timestamp: Double) {
    self.id = id
    self.clientID = clientID
    self.name = name
    self.args = args
    self.timestamp = timestamp
  }
}

/// A mutation as pushed to zero-cache (`mutationSchema`).
package enum Mutation: Sendable, Hashable, JSONCodable {
  case crud(CRUDMutation)
  case custom(CustomMutation)

  package var id: Double {
    switch self {
    case .crud(let m): m.id
    case .custom(let m): m.id
    }
  }

  package var clientID: String {
    switch self {
    case .crud(let m): m.clientID
    case .custom(let m): m.clientID
    }
  }

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "mutation")
    switch try r.literalUnion("type", MutationType.self) {
    case .crud:
      try r.literal("name", crudMutationName)
      let ops = try r.required("args") { (v) throws(DecodingError) -> [CRUDOp] in
        let args = try JSONDecode.array(v)
        guard args.count == 1 else {
          throw DecodingError("Expected array of length 1, got \(args.count)")
        }
        do throws(DecodingError) {
          let arg = try JSONObjectReader(args[0], "CRUD arg")
          return try arg.required("ops") { (v) throws(DecodingError) in
            try JSONDecode.array(v, CRUDOp.init(json:))
          }
        } catch {
          throw error.at("0")
        }
      }
      self = .crud(
        CRUDMutation(
          id: try r.number("id"), clientID: try r.string("clientID"), ops: ops,
          timestamp: try r.number("timestamp")))
    case .custom:
      self = .custom(
        CustomMutation(
          id: try r.number("id"), clientID: try r.string("clientID"), name: try r.string("name"),
          args: try r.required("args", JSONDecode.array), timestamp: try r.number("timestamp")))
    }
  }

  package var json: JSONValue {
    switch self {
    case .crud(let m):
      return [
        "type": "crud", "id": .number(m.id), "clientID": .string(m.clientID),
        "name": .string(crudMutationName),
        "args": [["ops": .array(m.ops.map(\.json))]],
        "timestamp": .number(m.timestamp),
      ]
    case .custom(let m):
      return [
        "type": "custom", "id": .number(m.id), "clientID": .string(m.clientID),
        "name": .string(m.name), "args": .array(m.args), "timestamp": .number(m.timestamp),
      ]
    }
  }
}

/// A failed mutation's result (`mutationErrorSchema` in
/// `packages/zero-protocol/src/mutation.ts`). The fields are kept as sent:
/// see ``MutationResult`` for which results are errors.
package struct MutationError: Sendable, Hashable {
  /// `"app"` for an error the mutator threw; `"oooMutation"` or
  /// `"alreadyProcessed"` for one from zero.
  package var error: JSONValue
  package var message: JSONValue?
  package var details: JSONValue?

  package init(error: JSONValue, message: JSONValue? = nil, details: JSONValue? = nil) {
    self.error = error
    self.message = message
    self.details = details
  }

  /// An application error (`appErrorSchema`).
  package static func app(message: String? = nil, details: JSONValue? = nil) -> MutationError {
    MutationError(error: "app", message: message.map { .string($0) }, details: details)
  }
}

/// The result of running a mutation on the API server
/// (`mutationResultSchema` in `packages/zero-protocol/src/mutation.ts`).
///
/// This follows how the JS client reads a result. It parses it in
/// `passthrough` mode and tells errors apart with `'error' in result`
/// (`packages/zero-client/src/client/mutation-tracker.ts`). The union's OK
/// branch, `{data?}`, has no discriminator and accepts every object, so it
/// is what valita returns: the object as sent if it has no `data`, and just
/// `{data}` if it does, because valita copies only the keys it knows once a
/// field's matcher returns a value, as `jsonSchema` always does. So a result
/// with `data` is OK even if it has an `error`, and any other result with an
/// `error`, whatever its value, is an error.
package enum MutationResult: Sendable, Hashable, JSONCodable {
  /// `mutationOkSchema`, with the data the mutator returned, if any.
  case ok(data: JSONValue?)
  case error(MutationError)

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "mutation result")
    if let data = r.object["data"] {
      self = .ok(data: data)
    } else if let error = r.object["error"] {
      self = .error(
        MutationError(error: error, message: r.object["message"], details: r.object["details"]))
    } else {
      self = .ok(data: nil)
    }
  }

  package var json: JSONValue {
    var o = JSONObjectBuilder()
    switch self {
    case .ok(let data):
      o.set("data", data)
    case .error(let e):
      o.set("error", e.error)
      o.set("message", e.message)
      o.set("details", e.details)
    }
    return o.value
  }
}

/// `mutationResponseSchema`.
package struct MutationResponse: Sendable, Hashable, JSONCodable {
  package var id: MutationID
  package var result: MutationResult

  package init(id: MutationID, result: MutationResult) {
    self.id = id
    self.result = result
  }

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "mutation response")
    id = try r.required("id", MutationID.init(json:))
    result = try r.required("result", MutationResult.init(json:))
  }

  package var json: JSONValue {
    ["id": id.json, "result": result.json]
  }
}

/// A change to the mutation results zero-cache tracks for a client
/// (`patchOpSchema` in `packages/zero-protocol/src/mutations-patch.ts`).
package enum MutationPatchOp: Sendable, Hashable, JSONCodable {
  case put(MutationResponse)
  case del(MutationID)

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "mutations patch op")
    switch try r.string("op") {
    case "put": self = .put(try r.required("mutation", MutationResponse.init(json:)))
    case "del": self = .del(try r.required("id", MutationID.init(json:)))
    case let op: throw DecodingError("Invalid mutations patch op \"\(op)\"", path: ["op"])
    }
  }

  package var json: JSONValue {
    switch self {
    case .put(let m): ["op": "put", "mutation": m.json]
    case .del(let id): ["op": "del", "id": id.json]
    }
  }
}

/// `mutationsPatchSchema`.
package typealias MutationsPatch = [MutationPatchOp]
