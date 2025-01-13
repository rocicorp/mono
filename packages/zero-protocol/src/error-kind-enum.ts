/* eslint-disable @typescript-eslint/naming-convention */

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
export const MutationFailed = 'MutationFailed';
export const MutationRateLimited = 'MutationRateLimited';
export const Unauthorized = 'Unauthorized';
export const VersionNotSupported = 'VersionNotSupported';
export const SchemaVersionNotSupported = 'SchemaVersionNotSupported';
export const ServerOverloaded = 'ServerOverloaded';
export const Internal = 'Internal';

export type AuthInvalidated = typeof AuthInvalidated;
export type ClientNotFound = typeof ClientNotFound;
export type InvalidConnectionRequest = typeof InvalidConnectionRequest;
export type InvalidConnectionRequestBaseCookie =
  typeof InvalidConnectionRequestBaseCookie;
export type InvalidConnectionRequestLastMutationID =
  typeof InvalidConnectionRequestLastMutationID;
export type InvalidConnectionRequestClientDeleted =
  typeof InvalidConnectionRequestClientDeleted;
export type InvalidMessage = typeof InvalidMessage;
export type InvalidPush = typeof InvalidPush;
export type MutationFailed = typeof MutationFailed;
export type MutationRateLimited = typeof MutationRateLimited;
export type Unauthorized = typeof Unauthorized;
export type VersionNotSupported = typeof VersionNotSupported;
export type SchemaVersionNotSupported = typeof SchemaVersionNotSupported;
export type ServerOverloaded = typeof ServerOverloaded;
export type Internal = typeof Internal;
