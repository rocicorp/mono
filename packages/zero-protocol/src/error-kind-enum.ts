// Note: Metric names depend on these values,
// so if you add or change on here a corresponding dashboard
// change will likely be needed.

export const AuthInvalidated = 'AuthInvalidated';
export const ClientNotFound = 'ClientNotFound';
export const InvalidConnectionRequest = 'InvalidConnectionRequest';
export const InvalidConnectionRequestBaseCookie =
  'InvalidConnectionRequestBaseCookie';
export const InvalidConnectionRequestLastMutationID =
  'InvalidConnectionRequestLastMutationID';
export const InvalidConnectionRequestClientDeleted =
  'InvalidConnectionRequestClientDeleted';
export const InvalidMessage = 'InvalidMessage';
export const InvalidPush = 'InvalidPush';
export const PushFailed = 'PushFailed';
export const MutationFailed = 'MutationFailed';
export const MutationRateLimited = 'MutationRateLimited';
export const Rebalance = 'Rebalance';
export const Rehome = 'Rehome';
export const TransformFailed = 'TransformFailed';
export const Unauthorized = 'Unauthorized';
export const VersionNotSupported = 'VersionNotSupported';
export const SchemaVersionNotSupported = 'SchemaVersionNotSupported';
export const ServerOverloaded = 'ServerOverloaded';
export const Internal = 'Internal';

/**
 * The app rejected the client's auth token (used in CRUD mutators).
 * @deprecated TODO(0xcadams): document the deprecation better
 */
export type AuthInvalidated = typeof AuthInvalidated;
/**
 * zero-cache no longer has CVR state for the client; forces a full resync from scratch.
 */
export type ClientNotFound = typeof ClientNotFound;
/**
 * Handshake metadata is invalid or incomplete, so the client must rebuild local state.
 */
export type InvalidConnectionRequest = typeof InvalidConnectionRequest;
/**
 * Client's base cookie is ahead of the cache snapshot; drop local cache and reconnect.
 */
export type InvalidConnectionRequestBaseCookie =
  typeof InvalidConnectionRequestBaseCookie;
/**
 * Client's last mutation ID is ahead of the cache; wipe local history and restart sync.
 */
export type InvalidConnectionRequestLastMutationID =
  typeof InvalidConnectionRequestLastMutationID;
/**
 * Legacy signal that the server deleted the client; reconstruct local storage before retrying.
 */
export type InvalidConnectionRequestClientDeleted =
  typeof InvalidConnectionRequestClientDeleted;
/**
 * Upstream message failed schema validation or JSON parsing; socket closes immediately.
 */
export type InvalidMessage = typeof InvalidMessage;
/**
 * Push payload could not be applied (version mismatch, out-of-order mutation); transitions connection to error.
 */
export type InvalidPush = typeof InvalidPush;
/**
 * Push failed during processing; transitions connection to error.
 */
export type PushFailed = typeof PushFailed;
/**
 * Transform failed during processing; transitions connection to error.
 */
export type TransformFailed = typeof TransformFailed;
/**
 * Legacy CRUD mutator failure signal.
 * @deprecated
 */
export type MutationFailed = typeof MutationFailed;
/**
 * Legacy CRUD mutator rate limit signal.
 * @deprecated
 */
export type MutationRateLimited = typeof MutationRateLimited;
/**
 * Cache is rebalancing ownership; client should retry after instructed backoff.
 */
export type Rebalance = typeof Rebalance;
/**
 * Replica ownership moved; reconnect using the hinted parameters after backoff.
 */
export type Rehome = typeof Rehome;
/**
 * JWT validation failure mapped to needs-auth (used in CRUD mutators).
 * @deprecated
 */
export type Unauthorized = typeof Unauthorized;
/**
 * Client requested unsupported protocol version; disconnect and prompt for upgrade.
 */
export type VersionNotSupported = typeof VersionNotSupported;
/**
 * Client schema hash or version is outside zero-cache window; refresh with a supported schema.
 */
export type SchemaVersionNotSupported = typeof SchemaVersionNotSupported;
/**
 * Cache is overloaded and instructs clients to back off before retrying.
 */
export type ServerOverloaded = typeof ServerOverloaded;
/**
 * Unhandled zero-cache exception; connection transitions to error for manual recovery.
 */
export type Internal = typeof Internal;
