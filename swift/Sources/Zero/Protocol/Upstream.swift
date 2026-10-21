/// A message from the client to zero-cache (`upstreamSchema` in
/// `packages/zero-protocol/src/up.ts`).
package enum Upstream: Sendable, Hashable, JSONCodable {
  case initConnection(InitConnectionBody)
  case ping
  case deleteClients(DeleteClientsBody)
  case changeDesiredQueries(ChangeDesiredQueriesBody)
  case pull(PullRequestBody)
  case updateAuth(UpdateAuthBody)
  case push(PushBody)
  /// Sent before the client closes the socket. The body is any array.
  case closeConnection([JSONValue])
  case inspect(InspectUpBody)
  /// Tells zero-cache the client has seen the mutation results up to the
  /// given ID, so it can delete them.
  case ackMutationResponses(MutationID)
  /// A message whose string tag this build doesn't know, kept as sent.
  /// `upstreamSchema` is an open `taggedTupleUnion`, so a newer client's
  /// messages parse (`v.UnrecognizedTuple`).
  case unrecognized([JSONValue])

  package var tag: String {
    switch self {
    case .initConnection: "initConnection"
    case .ping: "ping"
    case .deleteClients: "deleteClients"
    case .changeDesiredQueries: "changeDesiredQueries"
    case .pull: "pull"
    case .updateAuth: "updateAuth"
    case .push: "push"
    case .closeConnection: "closeConnection"
    case .inspect: "inspect"
    case .ackMutationResponses: "ackMutationResponses"
    case .unrecognized(let a): a.first?.stringValue ?? ""
    }
  }

  package init(json: JSONValue) throws(DecodingError) {
    guard case .array(let a) = json else { throw DecodingError("Expected array") }
    guard case .string(let tag)? = a.first else {
      throw DecodingError("Expected string", path: ["0"])
    }
    // A known tag picks its tuple, which must have exactly two elements.
    func decode<T>(_ decode: (JSONValue) throws(DecodingError) -> T) throws(DecodingError) -> T {
      let body = try JSONDecode.body(of: json, tag: tag)
      do {
        return try decode(body)
      } catch {
        throw error.at("1")
      }
    }
    switch tag {
    case "initConnection": self = .initConnection(try decode(InitConnectionBody.init(json:)))
    case "ping":
      _ = try decode(JSONDecode.object)
      self = .ping
    case "deleteClients": self = .deleteClients(try decode(DeleteClientsBody.init(json:)))
    case "changeDesiredQueries":
      self = .changeDesiredQueries(try decode(ChangeDesiredQueriesBody.init(json:)))
    case "pull": self = .pull(try decode(PullRequestBody.init(json:)))
    case "updateAuth": self = .updateAuth(try decode(UpdateAuthBody.init(json:)))
    case "push": self = .push(try decode(PushBody.init(json:)))
    case "closeConnection": self = .closeConnection(try decode(JSONDecode.array))
    case "inspect": self = .inspect(try decode(InspectUpBody.init(json:)))
    case "ackMutationResponses":
      self = .ackMutationResponses(try decode(MutationID.init(json:)))
    default: self = .unrecognized(a)
    }
  }

  /// Parses message text, as zero-cache receives it.
  package init(text: String) throws(DecodingError) {
    try self.init(json: try JSONValue.parseMessage(text))
  }

  package var json: JSONValue {
    switch self {
    case .initConnection(let b): .message(tag, b.json)
    case .ping: .message(tag, [:])
    case .deleteClients(let b): .message(tag, b.json)
    case .changeDesiredQueries(let b): .message(tag, b.json)
    case .pull(let b): .message(tag, b.json)
    case .updateAuth(let b): .message(tag, b.json)
    case .push(let b): .message(tag, b.json)
    case .closeConnection(let b): .message(tag, .array(b))
    case .inspect(let b): .message(tag, b.json)
    case .ackMutationResponses(let id): .message(tag, id.json)
    case .unrecognized(let a): .array(a)
    }
  }

  /// The message text to send.
  package var text: String { JSONSerializer.stringify(json) }
}
