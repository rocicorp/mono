/// The sync protocol version this client speaks.
///
/// `zero-cache` supports every protocol version from
/// `MIN_SERVER_SUPPORTED_SYNC_PROTOCOL` through its own `PROTOCOL_VERSION`
/// (see `packages/zero-protocol/src/protocol-version.ts`), so this can lag the
/// JS client.
public let protocolVersion = 53
