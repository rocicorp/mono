/// The kind of a protocol error
/// (`packages/zero-protocol/src/error-kind-enum.ts`).
package enum ErrorKind: String, Sendable, Hashable, CaseIterable {
  case authInvalidated = "AuthInvalidated"
  case clientNotFound = "ClientNotFound"
  case invalidConnectionRequest = "InvalidConnectionRequest"
  case invalidConnectionRequestBaseCookie = "InvalidConnectionRequestBaseCookie"
  case invalidConnectionRequestLastMutationID = "InvalidConnectionRequestLastMutationID"
  case invalidConnectionRequestClientDeleted = "InvalidConnectionRequestClientDeleted"
  case invalidMessage = "InvalidMessage"
  case invalidPush = "InvalidPush"
  case pushFailed = "PushFailed"
  case mutationFailed = "MutationFailed"
  case mutationRateLimited = "MutationRateLimited"
  case rebalance = "Rebalance"
  case rehome = "Rehome"
  case transformFailed = "TransformFailed"
  case unauthorized = "Unauthorized"
  case versionNotSupported = "VersionNotSupported"
  case schemaVersionNotSupported = "SchemaVersionNotSupported"
  case serverOverloaded = "ServerOverloaded"
  case `internal` = "Internal"

  /// The kinds of a basic error body (`basicErrorKindSchema` in
  /// `packages/zero-protocol/src/error.ts`).
  package static let basic: Set<ErrorKind> = [
    .authInvalidated, .clientNotFound, .invalidConnectionRequest,
    .invalidConnectionRequestBaseCookie, .invalidConnectionRequestLastMutationID,
    .invalidConnectionRequestClientDeleted, .invalidMessage, .invalidPush,
    .mutationRateLimited, .mutationFailed, .unauthorized, .versionNotSupported,
    .schemaVersionNotSupported, .internal,
  ]

  /// The kinds whose body carries backoff parameters
  /// (`backoffErrorKindSchema`).
  package static let backoff: Set<ErrorKind> = [.rebalance, .rehome, .serverOverloaded]
}

/// Where an error came from
/// (`packages/zero-protocol/src/error-origin-enum.ts`).
package enum ErrorOrigin: String, Sendable, Hashable, CaseIterable {
  case client
  case server
  case zeroCache
}

/// Why a push or transform failed
/// (`packages/zero-protocol/src/error-reason-enum.ts`).
package enum ErrorReason: String, Sendable, Hashable, CaseIterable {
  case database
  case parse
  case outOfOrderMutation = "oooMutation"
  case unsupportedPushVersion
  case `internal`
  case http
  case timeout
}

/// `packages/zero-protocol/src/mutation-type-enum.ts`.
package enum MutationType: String, Sendable, Hashable, CaseIterable {
  case crud
  case custom
}
