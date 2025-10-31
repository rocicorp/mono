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
 * @deprecated auth errors are now represented as ['error', { ... }] messages
 */
export type AuthInvalidated = typeof AuthInvalidated;
/**
 * zero-cache no longer has CVR state for the client.
 */
export type ClientNotFound = typeof ClientNotFound;
/**
 * Handshake metadata is invalid or incomplete.
 */
export type InvalidConnectionRequest = typeof InvalidConnectionRequest;
/**
 * Client's base cookie is ahead of the replica snapshot.
 */
export type InvalidConnectionRequestBaseCookie =
  typeof InvalidConnectionRequestBaseCookie;
/**
 * Client's last mutation ID is ahead of the replica.
 */
export type InvalidConnectionRequestLastMutationID =
  typeof InvalidConnectionRequestLastMutationID;
/**
 * The server deleted the client.
 */
export type InvalidConnectionRequestClientDeleted =
  typeof InvalidConnectionRequestClientDeleted;
/**
 * Upstream message failed schema validation or JSON parsing.
 */
export type InvalidMessage = typeof InvalidMessage;
/**
 * Push payload could not be applied (version mismatch, out-of-order mutation).
 */
export type InvalidPush = typeof InvalidPush;
/**
 * Push failed during processing.
 */
export type PushFailed = typeof PushFailed;
/**
 * Transform failed during processing.
 */
export type TransformFailed = typeof TransformFailed;
/**
 * CRUD mutator failure.
 * @deprecated
 */
export type MutationFailed = typeof MutationFailed;
/**
 * CRUD mutator rate limit.
 * @deprecated
 */
export type MutationRateLimited = typeof MutationRateLimited;
/**
 * Cache is rebalancing ownership.
 */
export type Rebalance = typeof Rebalance;
/**
 * Replica ownership moved.
 */
export type Rehome = typeof Rehome;
/**
 * JWT validation failure (used in CRUD mutators).
 * @deprecated
 */
export type Unauthorized = typeof Unauthorized;
/**
 * Client requested unsupported protocol version.
 */
export type VersionNotSupported = typeof VersionNotSupported;
/**
 * Client schema hash or version is outside zero-cache window.
 */
export type SchemaVersionNotSupported = typeof SchemaVersionNotSupported;
/**
 * zero-cache is overloaded.
 */
export type ServerOverloaded = typeof ServerOverloaded;
/**
 * Unhandled zero-cache exception.
 */
export type Internal = typeof Internal;
