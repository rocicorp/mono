/**
 * There is a new client group due to a another tab loading new code which
 * cannot sync locally with this tab until it updates to the new code. This tab
 * can still sync with the zero-cache.
 */
export const NewClientGroup = 'NewClientGroup';
export type NewClientGroup = typeof NewClientGroup;

/**
 * This client was unable to connect to the zero-cache because it is using a
 * protocol version that the zero-cache does not support.
 */
export const VersionNotSupported = 'VersionNotSupported';
export type VersionNotSupported = typeof VersionNotSupported;

/**
 * This client was unable to connect to the zero-cache because it is using a
 * schema version (see {@codelink Schema}) that the zero-cache does not support.
 */
export const SchemaVersionNotSupported = 'SchemaVersionNotSupported';
export type SchemaVersionNotSupported = typeof SchemaVersionNotSupported;
