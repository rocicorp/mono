/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/await-thenable, @typescript-eslint/no-misused-promises, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-floating-promises, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/require-await, @typescript-eslint/no-empty-object-type, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error */
/* eslint-disable @typescript-eslint/naming-convention */

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
