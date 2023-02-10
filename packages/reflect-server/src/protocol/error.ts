import * as s from 'superstruct';

// Keep this in sync with reflect client

export const enum ErrorKind {
  AuthInvalidated = 'AuthInvalidated',
  ClientNotFound = 'ClientNotFound',
  ConnectTimeout = 'ConnectTimeout',
  InvalidConnectionRequest = 'InvalidConnectionRequest',
  InvalidMessage = 'InvalidMessage',
  PingTimeout = 'PingTimeout',
  RoomClosed = 'RoomClosed',
  RoomNotFound = 'RoomNotFound',
  Unauthorized = 'Unauthorized',
  UnexpectedBaseCookie = 'UnexpectedBaseCookie',
  UnexpectedLastMutationID = 'UnexpectedLastMutationID',
}

export const errorKindSchema = s.union([
  s.literal(ErrorKind.AuthInvalidated),
  s.literal(ErrorKind.ClientNotFound),
  s.literal(ErrorKind.ConnectTimeout),
  s.literal(ErrorKind.InvalidConnectionRequest),
  s.literal(ErrorKind.InvalidMessage),
  s.literal(ErrorKind.PingTimeout),
  s.literal(ErrorKind.RoomClosed),
  s.literal(ErrorKind.RoomNotFound),
  s.literal(ErrorKind.Unauthorized),
  s.literal(ErrorKind.UnexpectedBaseCookie),
  s.literal(ErrorKind.UnexpectedLastMutationID),
]);

export const errorMessageSchema = s.tuple([
  s.literal('error'),
  errorKindSchema,
  s.string(),
]);

export type ErrorMessage = s.Infer<typeof errorMessageSchema>;
