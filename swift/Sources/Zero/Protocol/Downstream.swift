/// A message from zero-cache to the client (`downstreamSchema` in
/// `packages/zero-protocol/src/down.ts`).
///
/// The union is closed: the client treats a message with a tag it doesn't
/// know as invalid and disconnects (`#onMessage` in
/// `packages/zero-client/src/client/zero.ts`, which parses in `passthrough`
/// mode).
package enum Downstream: Sendable, Hashable, JSONCodable {
  case connected(ConnectedBody)
  case error(ErrorBody)
  case pong
  case pokeStart(PokeStartBody)
  case pokePart(PokePartBody)
  case pokeEnd(PokeEndBody)
  case pull(PullResponseBody)
  case deleteClients(DeleteClientsBody)
  case pushResponse(PushResponseBody)
  case inspect(InspectDownBody)
  /// Custom queries the API server failed to transform.
  case transformError([ErroredQuery])

  package var tag: String {
    switch self {
    case .connected: "connected"
    case .error: "error"
    case .pong: "pong"
    case .pokeStart: "pokeStart"
    case .pokePart: "pokePart"
    case .pokeEnd: "pokeEnd"
    case .pull: "pull"
    case .deleteClients: "deleteClients"
    case .pushResponse: "pushResponse"
    case .inspect: "inspect"
    case .transformError: "transformError"
    }
  }

  package init(json: JSONValue) throws(DecodingError) {
    guard case .array(let a) = json else { throw DecodingError("Expected array") }
    guard a.count == 2, case .string(let tag) = a[0] else {
      throw DecodingError("Expected [tag, body] tuple")
    }
    func decode<T>(_ decode: (JSONValue) throws(DecodingError) -> T) throws(DecodingError) -> T {
      do {
        return try decode(a[1])
      } catch {
        throw error.at("1")
      }
    }
    switch tag {
    case "connected": self = .connected(try decode(ConnectedBody.init(json:)))
    case "error": self = .error(try decode(ErrorBody.init(json:)))
    case "pong":
      _ = try decode(JSONDecode.object)
      self = .pong
    case "pokeStart": self = .pokeStart(try decode(PokeStartBody.init(json:)))
    case "pokePart": self = .pokePart(try decode(PokePartBody.init(json:)))
    case "pokeEnd": self = .pokeEnd(try decode(PokeEndBody.init(json:)))
    case "pull": self = .pull(try decode(PullResponseBody.init(json:)))
    case "deleteClients": self = .deleteClients(try decode(DeleteClientsBody.init(json:)))
    case "pushResponse": self = .pushResponse(try decode(PushResponseBody.init(json:)))
    case "inspect": self = .inspect(try decode(InspectDownBody.init(json:)))
    case "transformError": self = .transformError(try decode(JSONDecode.erroredQueries))
    default:
      throw DecodingError("Unknown message type \"\(tag)\"", path: ["0"])
    }
  }

  /// Parses message text, as the client receives it.
  package init(text: String) throws(DecodingError) {
    try self.init(json: try JSONValue.parseMessage(text))
  }

  package var json: JSONValue {
    switch self {
    case .connected(let b): .message(tag, b.json)
    case .error(let b): .message(tag, b.json)
    case .pong: .message(tag, [:])
    case .pokeStart(let b): .message(tag, b.json)
    case .pokePart(let b): .message(tag, b.json)
    case .pokeEnd(let b): .message(tag, b.json)
    case .pull(let b): .message(tag, b.json)
    case .deleteClients(let b): .message(tag, b.json)
    case .pushResponse(let b): .message(tag, b.json)
    case .inspect(let b): .message(tag, b.json)
    case .transformError(let b): .message(tag, .array(b.map(\.json)))
    }
  }

  /// The message text to send.
  package var text: String { JSONSerializer.stringify(json) }
}
