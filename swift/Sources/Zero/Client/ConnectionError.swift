/// The kinds of error the client raises itself
/// (`packages/zero-client/src/client/client-error-kind-enum.ts`).
package enum ClientErrorKind: String, Sendable, Hashable, CaseIterable {
  case abruptClose = "AbruptClose"
  case cleanClose = "CleanClose"
  case clientClosed = "ClientClosed"
  case connectTimeout = "ConnectTimeout"
  case hidden = "Hidden"
  case `internal` = "Internal"
  case invalidMessage = "InvalidMessage"
  case noSocketOrigin = "NoSocketOrigin"
  case offline = "Offline"
  case pingTimeout = "PingTimeout"
  case pullTimeout = "PullTimeout"
  case unexpectedBaseCookie = "UnexpectedBaseCookie"
  case userDisconnect = "UserDisconnect"
}

/// Why a connection ended or could not be made: an error the client raised
/// (`ClientError`) or one the server sent (`ProtocolError`), together
/// `ZeroError` in `packages/zero-client/src/client/error.ts`.
package enum ConnectionError: Error, Sendable, Hashable, CustomStringConvertible {
  case client(ClientErrorKind, message: String)
  case server(ErrorBody)

  package var message: String {
    switch self {
    case .client(_, let message): message
    case .server(let body): body.message
    }
  }

  /// The kind's name, a `ClientErrorKind` or an `ErrorKind`.
  package var kindName: String {
    switch self {
    case .client(let kind, _): kind.rawValue
    case .server(let body): body.kind.rawValue
    }
  }

  package var description: String { "\(kindName): \(message)" }

  var clientKind: ClientErrorKind? {
    if case .client(let kind, _) = self { return kind }
    return nil
  }

  var serverKind: ErrorKind? {
    if case .server(let body) = self { return body.kind }
    return nil
  }

  /// `isZeroError`: a client error, or a server error that says where it
  /// came from. Older servers send errors without an origin.
  var isZeroError: Bool {
    switch self {
    case .client: true
    case .server(let body): body.origin == .server || body.origin == .zeroCache
    }
  }

  /// `isAuthError`: the server rejected the credentials, or the API server
  /// answered a push or transform with 401 or 403.
  var isAuthError: Bool {
    guard isZeroError, case .server(let body) = self else { return false }
    switch body {
    case .basic(let b): return b.kind == .authInvalidated || b.kind == .unauthorized
    case .pushFailed(let b): return Self.isHTTPAuthFailure(b.cause)
    case .transformFailed(let b): return Self.isHTTPAuthFailure(b.cause)
    case .backoff: return false
    }
  }

  static func isHTTPAuthFailure(_ cause: FailureCause) -> Bool {
    if case .http(let status, _) = cause { return status == 401 || status == 403 }
    return false
  }

  /// How a rejected mutation describes the client (`getStateDescription`).
  var stateDescription: String {
    switch clientKind {
    case .offline?: "offline"
    case .clientClosed?: "closed"
    default: "in error state"
    }
  }

  /// `getBackoffParams`.
  var backoff: BackoffErrorBody? {
    if isZeroError, case .server(.backoff(let b)) = self { return b }
    return nil
  }
}

/// What a connection error does to the connection status
/// (`getErrorConnectionTransition`).
enum ConnectionTransition {
  /// Keep the status and retry.
  case none
  case needsAuth
  case error
  case disconnected
  case closed

  /// The transition and the reason to record. An error without an origin
  /// is unexpected: it becomes an internal client error.
  static func of(_ error: ConnectionError) -> (ConnectionTransition, ConnectionError) {
    guard error.isZeroError else {
      return (
        .error, .client(.internal, message: "Unexpected internal error: " + error.message)
      )
    }
    return (ConnectionTransition(error), error)
  }

  init(_ error: ConnectionError) {
    if error.isAuthError {
      self = .needsAuth
      return
    }
    switch error {
    case .client(let kind, _):
      switch kind {
      case .abruptClose, .cleanClose, .connectTimeout, .pingTimeout, .pullTimeout,
        .unexpectedBaseCookie:
        self = .none
      case .internal, .invalidMessage, .userDisconnect:
        self = .error
      case .hidden, .offline, .noSocketOrigin:
        self = .disconnected
      case .clientClosed:
        self = .closed
      }
    case .server(let body):
      switch body.kind {
      case .clientNotFound, .invalidConnectionRequest, .invalidConnectionRequestBaseCookie,
        .invalidConnectionRequestLastMutationID, .invalidConnectionRequestClientDeleted,
        .invalidMessage, .invalidPush, .versionNotSupported, .schemaVersionNotSupported,
        .internal, .pushFailed, .transformFailed:
        self = .error
      case .rebalance, .rehome, .serverOverloaded:
        self = .none
      case .authInvalidated, .unauthorized:
        self = .needsAuth
      case .mutationRateLimited, .mutationFailed:
        self = .none
      }
    }
  }
}
