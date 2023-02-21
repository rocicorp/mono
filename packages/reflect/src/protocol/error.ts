import * as s from 'superstruct';

// Keep this in sync with reflect server.
//
// Also note that metric names depend on these values,
// so if you add or change on here a corresponding dashboard
// change will likely needed.

export const enum ErrorKind {
  AuthInvalidated = 'AuthInvalidated',
  ClientNotFound = 'ClientNotFound',
  ConnectTimeout = 'ConnectTimeout',
  InvalidConnectionRequest = 'InvalidConnectionRequest',
  InvalidMessage = 'InvalidMessage',
  InvalidPush = 'InvalidPush',
  PingTimeout = 'PingTimeout',
  RoomClosed = 'RoomClosed',
  RoomNotFound = 'RoomNotFound',
  Unauthorized = 'Unauthorized',
  UnexpectedBaseCookie = 'UnexpectedBaseCookie',
  UnexpectedLastMutationID = 'UnexpectedLastMutationID',
  VersionNotSupported = 'VersionNotSupported',
}

export const errorKindSchema = s.union([
  s.literal(ErrorKind.AuthInvalidated),
  s.literal(ErrorKind.ClientNotFound),
  s.literal(ErrorKind.ConnectTimeout),
  s.literal(ErrorKind.InvalidConnectionRequest),
  s.literal(ErrorKind.InvalidMessage),
  s.literal(ErrorKind.InvalidPush),
  s.literal(ErrorKind.PingTimeout),
  s.literal(ErrorKind.RoomClosed),
  s.literal(ErrorKind.RoomNotFound),
  s.literal(ErrorKind.Unauthorized),
  s.literal(ErrorKind.UnexpectedBaseCookie),
  s.literal(ErrorKind.UnexpectedLastMutationID),
  s.literal(ErrorKind.VersionNotSupported),
]);

export const errorMessageSchema = s.tuple([
  s.literal('error'),
  errorKindSchema,
  s.string(),
]);

export type ErrorMessage = ['error', ErrorKind, string];
