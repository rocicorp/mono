import {z} from 'zod';

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

export const errorKindSchema = z.union([
  z.literal(ErrorKind.AuthInvalidated),
  z.literal(ErrorKind.ClientNotFound),
  z.literal(ErrorKind.ConnectTimeout),
  z.literal(ErrorKind.InvalidConnectionRequest),
  z.literal(ErrorKind.InvalidMessage),
  z.literal(ErrorKind.InvalidPush),
  z.literal(ErrorKind.PingTimeout),
  z.literal(ErrorKind.RoomClosed),
  z.literal(ErrorKind.RoomNotFound),
  z.literal(ErrorKind.Unauthorized),
  z.literal(ErrorKind.UnexpectedBaseCookie),
  z.literal(ErrorKind.UnexpectedLastMutationID),
  z.literal(ErrorKind.VersionNotSupported),
]);

export const errorMessageSchema = z.tuple([
  z.literal('error'),
  errorKindSchema,
  z.string(),
]);

export type ErrorMessage = ['error', ErrorKind, string];
