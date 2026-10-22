/// The body of a `changeDesiredQueries` message
/// (`packages/zero-protocol/src/change-desired-queries.ts`).
package struct ChangeDesiredQueriesBody: Sendable, Hashable, JSONCodable {
  package var desiredQueriesPatch: UpQueriesPatch
  /// A W3C traceparent header for distributed tracing.
  package var traceparent: String?

  package init(desiredQueriesPatch: UpQueriesPatch, traceparent: String? = nil) {
    self.desiredQueriesPatch = desiredQueriesPatch
    self.traceparent = traceparent
  }

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "changeDesiredQueries body")
    desiredQueriesPatch = try r.required("desiredQueriesPatch", JSONDecode.upQueriesPatch)
    traceparent = try r.optionalString("traceparent")
  }

  package var json: JSONValue {
    var o = JSONObjectBuilder()
    o.set("desiredQueriesPatch", .array(desiredQueriesPatch.map(\.json)))
    o.set("traceparent", traceparent)
    return o.value
  }
}

/// The body of a `pull` request (`pullRequestBodySchema` in
/// `packages/zero-protocol/src/pull.ts`).
package struct PullRequestBody: Sendable, Hashable, JSONCodable {
  package var clientGroupID: String
  package var cookie: Version?
  package var requestID: String

  package init(clientGroupID: String, cookie: Version?, requestID: String) {
    self.clientGroupID = clientGroupID
    self.cookie = cookie
    self.requestID = requestID
  }

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "pull request body")
    clientGroupID = try r.string("clientGroupID")
    cookie = try r.required("cookie", NullableVersion.decode)
    requestID = try r.string("requestID")
  }

  package var json: JSONValue {
    [
      "clientGroupID": .string(clientGroupID), "cookie": NullableVersion.json(cookie),
      "requestID": .string(requestID),
    ]
  }
}

/// The body of a `pull` response (`pullResponseBodySchema` in
/// `packages/zero-protocol/src/pull.ts`). Pull is only used to recover
/// mutations, so it carries no patch.
package struct PullResponseBody: Sendable, Hashable, JSONCodable {
  package var cookie: Version
  /// The `requestID` of the pull request this answers.
  package var requestID: String
  package var lastMutationIDChanges: [JSKey: Double]

  package init(cookie: Version, requestID: String, lastMutationIDChanges: [JSKey: Double]) {
    self.cookie = cookie
    self.requestID = requestID
    self.lastMutationIDChanges = lastMutationIDChanges
  }

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "pull response body")
    cookie = try r.string("cookie")
    requestID = try r.string("requestID")
    lastMutationIDChanges = try r.required("lastMutationIDChanges", JSONDecode.numberRecord)
  }

  package var json: JSONValue {
    [
      "cookie": .string(cookie), "requestID": .string(requestID),
      "lastMutationIDChanges": .numberRecord(lastMutationIDChanges),
    ]
  }
}

/// The body of a `deleteClients` message, in either direction
/// (`packages/zero-protocol/src/delete-clients.ts`).
package struct DeleteClientsBody: Sendable, Hashable, JSONCodable {
  package var clientIDs: [String]?
  package var clientGroupIDs: [String]?

  package init(clientIDs: [String]? = nil, clientGroupIDs: [String]? = nil) {
    self.clientIDs = clientIDs
    self.clientGroupIDs = clientGroupIDs
  }

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "deleteClients body")
    clientIDs = try r.optional("clientIDs", JSONDecode.strings)
    clientGroupIDs = try r.optional("clientGroupIDs", JSONDecode.strings)
  }

  package var json: JSONValue {
    var o = JSONObjectBuilder()
    o.set("clientIDs", clientIDs.map(JSONValue.strings))
    o.set("clientGroupIDs", clientGroupIDs.map(JSONValue.strings))
    return o.value
  }
}

/// The body of an `updateAuth` message
/// (`packages/zero-protocol/src/update-auth.ts`).
package struct UpdateAuthBody: Sendable, Hashable, JSONCodable {
  package var auth: String

  package init(auth: String) {
    self.auth = auth
  }

  package init(json: JSONValue) throws(DecodingError) {
    auth = try JSONObjectReader(json, "updateAuth body").string("auth")
  }

  package var json: JSONValue { ["auth": .string(auth)] }
}

/// A custom query the API server failed to transform (`erroredQuerySchema`
/// in `packages/zero-protocol/src/custom-queries.ts`).
public enum ErroredQuery: Sendable, Hashable, JSONCodable {
  /// The query function threw. `message` is optional for backwards
  /// compatibility.
  case app(id: String, name: String, message: String?, details: JSONValue?)
  /// The query's arguments failed to parse.
  case parse(id: String, name: String, message: String, details: JSONValue?)

  public var id: String {
    switch self {
    case .app(let id, _, _, _), .parse(let id, _, _, _): id
    }
  }

  public var name: String {
    switch self {
    case .app(_, let name, _, _), .parse(_, let name, _, _): name
    }
  }

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "errored query")
    switch try r.string("error") {
    case "app":
      self = .app(
        id: try r.string("id"), name: try r.string("name"),
        message: try r.optionalString("message"),
        details: r.optionalJSON("details"))
    case "parse":
      self = .parse(
        id: try r.string("id"), name: try r.string("name"), message: try r.string("message"),
        details: r.optionalJSON("details"))
    case let error: throw DecodingError("Invalid query error \"\(error)\"", path: ["error"])
    }
  }

  package var json: JSONValue {
    var o = JSONObjectBuilder()
    switch self {
    case .app(let id, let name, let message, let details):
      o.set("error", "app")
      o.set("id", id)
      o.set("name", name)
      o.set("message", message)
      o.set("details", details)
    case .parse(let id, let name, let message, let details):
      o.set("error", "parse")
      o.set("id", id)
      o.set("name", name)
      o.set("message", message)
      o.set("details", details)
    }
    return o.value
  }
}

extension JSONDecode {
  /// The body of a `transformError` message.
  static func erroredQueries(_ v: JSONValue) throws(DecodingError) -> [ErroredQuery] {
    try array(v, ErroredQuery.init(json:))
  }
}
