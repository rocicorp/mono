/// The body of a `push` message (`pushBodySchema` in
/// `packages/zero-protocol/src/push.ts`).
package struct PushBody: Sendable, Hashable, JSONCodable {
  package var clientGroupID: String
  package var mutations: [Mutation]
  package var pushVersion: Double
  /// The schema version of CRUD mutations. Custom mutations leave schema
  /// versioning to the API server.
  package var schemaVersion: Double?
  package var timestamp: Double
  package var requestID: String
  /// Deprecated: auth is set for the client group by `initConnection` and
  /// `updateAuth`.
  package var auth: String?
  /// A W3C traceparent header for distributed tracing.
  package var traceparent: String?

  package init(
    clientGroupID: String, mutations: [Mutation], pushVersion: Double,
    schemaVersion: Double? = nil, timestamp: Double, requestID: String, auth: String? = nil,
    traceparent: String? = nil
  ) {
    self.clientGroupID = clientGroupID
    self.mutations = mutations
    self.pushVersion = pushVersion
    self.schemaVersion = schemaVersion
    self.timestamp = timestamp
    self.requestID = requestID
    self.auth = auth
    self.traceparent = traceparent
  }

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "push body")
    clientGroupID = try r.string("clientGroupID")
    mutations = try r.required("mutations") { (v) throws(DecodingError) in
      try JSONDecode.array(v, Mutation.init(json:))
    }
    pushVersion = try r.number("pushVersion")
    schemaVersion = try r.optionalNumber("schemaVersion")
    timestamp = try r.number("timestamp")
    requestID = try r.string("requestID")
    auth = try r.optionalString("auth")
    traceparent = try r.optionalString("traceparent")
  }

  package var json: JSONValue {
    var o = JSONObjectBuilder()
    o.set("clientGroupID", clientGroupID)
    o.set("mutations", .array(mutations.map(\.json)))
    o.set("pushVersion", pushVersion)
    o.set("schemaVersion", schemaVersion)
    o.set("timestamp", timestamp)
    o.set("requestID", requestID)
    o.set("auth", auth)
    o.set("traceparent", traceparent)
    return o.value
  }
}

/// The deprecated error responses to a push (`pushErrorSchema` in
/// `packages/zero-protocol/src/push.ts`). zero-cache now reports push
/// failures with `error` messages.
package enum PushError: Sendable, Hashable, JSONCodable {
  case unsupportedPushVersion(mutationIDs: [MutationID]?)
  case unsupportedSchemaVersion(mutationIDs: [MutationID]?)
  case http(status: Double, details: String, mutationIDs: [MutationID]?)
  case zeroPusher(details: String, mutationIDs: [MutationID]?)

  package var mutationIDs: [MutationID]? {
    switch self {
    case .unsupportedPushVersion(let ids), .unsupportedSchemaVersion(let ids): ids
    case .http(_, _, let ids), .zeroPusher(_, let ids): ids
    }
  }

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "push error")
    let error = try r.string("error")
    let ids = { () throws(DecodingError) in
      try r.optional("mutationIDs") { (v) throws(DecodingError) in
        try JSONDecode.array(v, MutationID.init(json:))
      }
    }
    switch error {
    case "unsupportedPushVersion": self = .unsupportedPushVersion(mutationIDs: try ids())
    case "unsupportedSchemaVersion": self = .unsupportedSchemaVersion(mutationIDs: try ids())
    case "http":
      self = .http(
        status: try r.number("status"), details: try r.string("details"), mutationIDs: try ids())
    case "zeroPusher":
      self = .zeroPusher(details: try r.string("details"), mutationIDs: try ids())
    default:
      throw DecodingError("Invalid push error \"\(error)\"", path: ["error"])
    }
  }

  package var json: JSONValue {
    var o = JSONObjectBuilder()
    switch self {
    case .unsupportedPushVersion: o.set("error", "unsupportedPushVersion")
    case .unsupportedSchemaVersion: o.set("error", "unsupportedSchemaVersion")
    case .http(let status, let details, _):
      o.set("error", "http")
      o.set("status", status)
      o.set("details", details)
    case .zeroPusher(let details, _):
      o.set("error", "zeroPusher")
      o.set("details", details)
    }
    o.set("mutationIDs", mutationIDs.map { JSONValue.array($0.map(\.json)) })
    return o.value
  }
}

/// The body of a `pushResponse` message (`pushResponseBodySchema` in
/// `packages/zero-protocol/src/push.ts`).
///
/// valita tries the OK variant first, and it has no discriminator: a body
/// with a valid `mutations` array is OK even if it also has an `error` key.
package enum PushResponseBody: Sendable, Hashable, JSONCodable {
  case ok(mutations: [MutationResponse])
  case error(PushError)

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "pushResponse body")
    do {
      self = .ok(
        mutations: try r.required("mutations") { (v) throws(DecodingError) in
          try JSONDecode.array(v, MutationResponse.init(json:))
        })
    } catch {
      self = .error(try PushError(json: json))
    }
  }

  package var json: JSONValue {
    switch self {
    case .ok(let mutations): ["mutations": .array(mutations.map(\.json))]
    case .error(let e): e.json
    }
  }
}
