import * as v from 'shared/valita.js';

// Note: Metric names depend on these values,
// so if you add or change on here a corresponding dashboard
// change will likely needed.
//
// TODO: Remove the items here that are only used for metrics or are not used
// at all. Metrics should have its own set of constants, which can alias these
// where desired.

export const enum ErrorKind {
  AuthInvalidated = 'AuthInvalidated',
  ClientNotFound = 'ClientNotFound',
  ConnectTimeout = 'ConnectTimeout',
  InvalidConnectionRequest = 'InvalidConnectionRequest',
  InvalidConnectionRequestBaseCookie = 'InvalidConnectionRequestBaseCookie',
  InvalidConnectionRequestLastMutationID = 'InvalidConnectionRequestLastMutationID',
  InvalidMessage = 'InvalidMessage',
  InvalidPush = 'InvalidPush',
  PingTimeout = 'PingTimeout',
  PullTimeout = 'PullTimeout',
  RoomClosed = 'RoomClosed',
  RoomNotFound = 'RoomNotFound',
  Unauthorized = 'Unauthorized',
  UnexpectedBaseCookie = 'UnexpectedBaseCookie',
  UnexpectedLastMutationID = 'UnexpectedLastMutationID',
  VersionNotSupported = 'VersionNotSupported',
}

// TODO: Just generate this from the schema like every other place we do enums.
// Argh fritz.
export const errorKindSchema = v.union(
  v.literal(ErrorKind.AuthInvalidated),
  v.literal(ErrorKind.ClientNotFound),
  v.literal(ErrorKind.ConnectTimeout),
  v.literal(ErrorKind.InvalidConnectionRequest),
  v.literal(ErrorKind.InvalidConnectionRequestBaseCookie),
  v.literal(ErrorKind.InvalidConnectionRequestLastMutationID),
  v.literal(ErrorKind.InvalidMessage),
  v.literal(ErrorKind.InvalidPush),
  v.literal(ErrorKind.PingTimeout),
  v.literal(ErrorKind.PullTimeout),
  v.literal(ErrorKind.RoomClosed),
  v.literal(ErrorKind.RoomNotFound),
  v.literal(ErrorKind.Unauthorized),
  v.literal(ErrorKind.UnexpectedBaseCookie),
  v.literal(ErrorKind.UnexpectedLastMutationID),
  v.literal(ErrorKind.VersionNotSupported),
);

export const errorMessageSchema = v.tuple([
  v.literal('error'),
  errorKindSchema,
  v.string(),
]);

export type ErrorMessage = ['error', ErrorKind, string];
