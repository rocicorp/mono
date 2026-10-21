/// The body of an `error` message (`errorBodySchema` in
/// `packages/zero-protocol/src/error.ts`).
package enum ErrorBody: Sendable, Hashable, JSONCodable {
  case basic(BasicErrorBody)
  case backoff(BackoffErrorBody)
  case pushFailed(PushFailedBody)
  case transformFailed(TransformFailedBody)

  package var kind: ErrorKind {
    switch self {
    case .basic(let b): b.kind
    case .backoff(let b): b.kind
    case .pushFailed: .pushFailed
    case .transformFailed: .transformFailed
    }
  }

  /// Where the error came from; nil from servers too old to say.
  package var origin: ErrorOrigin? {
    switch self {
    case .basic(let b): b.origin
    case .backoff(let b): b.origin
    case .pushFailed(let b): b.cause.origin
    case .transformFailed(let b): b.cause.origin
    }
  }

  package var message: String {
    switch self {
    case .basic(let b): b.message
    case .backoff(let b): b.message
    case .pushFailed(let b): b.message
    case .transformFailed(let b): b.message
    }
  }

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "error body")
    let kind = try r.literalUnion("kind", ErrorKind.self)
    if ErrorKind.basic.contains(kind) {
      self = .basic(
        BasicErrorBody(
          kind: kind, message: try r.string("message"),
          origin: try r.optional("origin") { (v) throws(DecodingError) in
            let origin = try JSONDecode.literalUnion(v, ErrorOrigin.self)
            guard origin != .client else { throw DecodingError("Unexpected origin \"client\"") }
            return origin
          }))
    } else if ErrorKind.backoff.contains(kind) {
      self = .backoff(
        BackoffErrorBody(
          kind: kind, message: try r.string("message"),
          minBackoffMs: try r.optionalNumber("minBackoffMs"),
          maxBackoffMs: try r.optionalNumber("maxBackoffMs"),
          reconnectParams: try r.optional("reconnectParams", JSONDecode.stringRecord),
          origin: try r.optional("origin") { (v) throws(DecodingError) in
            try JSONDecode.literal(v, ErrorOrigin.zeroCache.rawValue)
            return ErrorOrigin.zeroCache
          }))
    } else if kind == .pushFailed {
      self = .pushFailed(
        PushFailedBody(
          message: try r.string("message"),
          mutationIDs: try r.required("mutationIDs") { (v) throws(DecodingError) in
            try JSONDecode.array(v, MutationID.init(json:))
          },
          details: r.optionalJSON("details"),
          cause: try FailureCause(r, serverReasons: PushFailedBody.serverReasons)))
    } else {
      self = .transformFailed(
        TransformFailedBody(
          message: try r.string("message"),
          queryIDs: try r.required("queryIDs", JSONDecode.strings),
          details: r.optionalJSON("details"),
          cause: try FailureCause(r, serverReasons: TransformFailedBody.serverReasons)))
    }
  }

  package var json: JSONValue {
    var o = JSONObjectBuilder()
    o.set("kind", .string(kind.rawValue))
    o.set("message", message)
    switch self {
    case .basic(let b):
      o.set("origin", b.origin?.rawValue)
    case .backoff(let b):
      o.set("minBackoffMs", b.minBackoffMs)
      o.set("maxBackoffMs", b.maxBackoffMs)
      o.set("reconnectParams", b.reconnectParams.map(JSONValue.stringRecord))
      o.set("origin", b.origin?.rawValue)
    case .pushFailed(let b):
      o.set("mutationIDs", .array(b.mutationIDs.map(\.json)))
      o.set("details", b.details)
      b.cause.write(into: &o)
    case .transformFailed(let b):
      o.set("queryIDs", .strings(b.queryIDs))
      o.set("details", b.details)
      b.cause.write(into: &o)
    }
    return o.value
  }
}

/// `basicErrorBodySchema`. `origin` is `server` or `zeroCache` when set.
package struct BasicErrorBody: Sendable, Hashable {
  package var kind: ErrorKind
  package var message: String
  package var origin: ErrorOrigin?

  package init(kind: ErrorKind, message: String, origin: ErrorOrigin? = nil) {
    self.kind = kind
    self.message = message
    self.origin = origin
  }
}

/// `backoffBodySchema`: zero-cache asks the client to reconnect later.
/// `origin` is `zeroCache` when set.
package struct BackoffErrorBody: Sendable, Hashable {
  package var kind: ErrorKind
  package var message: String
  package var minBackoffMs: Double?
  package var maxBackoffMs: Double?
  /// Query parameters to send in the next reconnect only.
  package var reconnectParams: [String: String]?
  package var origin: ErrorOrigin?

  package init(
    kind: ErrorKind, message: String, minBackoffMs: Double? = nil, maxBackoffMs: Double? = nil,
    reconnectParams: [String: String]? = nil, origin: ErrorOrigin? = nil
  ) {
    self.kind = kind
    self.message = message
    self.minBackoffMs = minBackoffMs
    self.maxBackoffMs = maxBackoffMs
    self.reconnectParams = reconnectParams
    self.origin = origin
  }
}

/// Why a push or transform failed: the `origin` and `reason` of
/// `pushFailedBodySchema` and `transformFailedBodySchema`, whose three
/// variants each allow different combinations.
package enum FailureCause: Sendable, Hashable {
  /// `origin: 'server'`: the API server reported the failure.
  case server(reason: ErrorReason)
  /// `origin: 'zeroCache', reason: 'http'`: the API server answered with a
  /// non-OK status.
  case http(status: Double, bodyPreview: String?)
  /// `origin: 'zeroCache'` with reason `timeout`, `parse`, or `internal`.
  case zeroCache(reason: ErrorReason)

  package var origin: ErrorOrigin {
    if case .server = self { return .server }
    return .zeroCache
  }

  package var reason: ErrorReason {
    switch self {
    case .server(let reason), .zeroCache(let reason): reason
    case .http: .http
    }
  }

  static let zeroCacheReasons: Set<ErrorReason> = [.timeout, .parse, .internal]

  init(_ r: JSONObjectReader, serverReasons: Set<ErrorReason>) throws(DecodingError) {
    // valita tries the variants in order; at most one accepts.
    let origin = try r.required("origin") { (v) throws(DecodingError) in
      let origin = try JSONDecode.literalUnion(v, ErrorOrigin.self)
      guard origin != .client else { throw DecodingError("Unexpected origin \"client\"") }
      return origin
    }
    if origin == .server {
      self = .server(reason: try r.oneOf("reason", serverReasons))
    } else if try r.literalUnion("reason", ErrorReason.self) == .http {
      self = .http(status: try r.number("status"), bodyPreview: try r.optionalString("bodyPreview"))
    } else {
      self = .zeroCache(reason: try r.oneOf("reason", Self.zeroCacheReasons))
    }
  }

  func write(into o: inout JSONObjectBuilder) {
    o.set("origin", origin.rawValue)
    o.set("reason", reason.rawValue)
    if case .http(let status, let bodyPreview) = self {
      o.set("status", status)
      o.set("bodyPreview", bodyPreview)
    }
  }
}

/// `pushFailedBodySchema`: the API server or zero-cache failed to process
/// a push. `mutationIDs` may be a subset of the pushed mutations.
package struct PushFailedBody: Sendable, Hashable {
  package var message: String
  package var mutationIDs: [MutationID]
  package var details: JSONValue?
  package var cause: FailureCause

  static let serverReasons: Set<ErrorReason> = [
    .database, .parse, .outOfOrderMutation, .unsupportedPushVersion, .internal,
  ]

  package init(
    message: String, mutationIDs: [MutationID], details: JSONValue? = nil, cause: FailureCause
  ) {
    self.message = message
    self.mutationIDs = mutationIDs
    self.details = details
    self.cause = cause
  }
}

/// `transformFailedBodySchema`: the API server or zero-cache failed to
/// transform custom queries.
package struct TransformFailedBody: Sendable, Hashable {
  package var message: String
  package var queryIDs: [String]
  package var details: JSONValue?
  package var cause: FailureCause

  static let serverReasons: Set<ErrorReason> = [.database, .parse, .internal]

  package init(
    message: String, queryIDs: [String], details: JSONValue? = nil, cause: FailureCause
  ) {
    self.message = message
    self.queryIDs = queryIDs
    self.details = details
    self.cause = cause
  }
}
