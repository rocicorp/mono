import * as v from 'shared/src/valita.js';

// Note: Metric names depend on these values,
// so if you add or change on here a corresponding dashboard
// change will likely be needed.

export enum ErrorKind {
  AuthInvalidated = 'AuthInvalidated',
  ClientNotFound = 'ClientNotFound',
  InvalidConnectionRequest = 'InvalidConnectionRequest',
  InvalidConnectionRequestBaseCookie = 'InvalidConnectionRequestBaseCookie',
  InvalidConnectionRequestLastMutationID = 'InvalidConnectionRequestLastMutationID',
  InvalidConnectionRequestClientDeleted = 'InvalidConnectionRequestClientDeleted',
  InvalidMessage = 'InvalidMessage',
  InvalidPush = 'InvalidPush',
  MutationFailed = 'MutationFailed',
  Unauthorized = 'Unauthorized',
  VersionNotSupported = 'VersionNotSupported',
  Internal = 'Internal',
}

export const errorKindSchema: v.Type<ErrorKind> = v.union(
  v.literal(ErrorKind.AuthInvalidated),
  v.literal(ErrorKind.ClientNotFound),
  v.literal(ErrorKind.InvalidConnectionRequest),
  v.literal(ErrorKind.InvalidConnectionRequestBaseCookie),
  v.literal(ErrorKind.InvalidConnectionRequestLastMutationID),
  v.literal(ErrorKind.InvalidConnectionRequestClientDeleted),
  v.literal(ErrorKind.InvalidMessage),
  v.literal(ErrorKind.InvalidPush),
  v.literal(ErrorKind.MutationFailed),
  v.literal(ErrorKind.Unauthorized),
  v.literal(ErrorKind.VersionNotSupported),
  v.literal(ErrorKind.Internal),
);

export const errorMessageSchema = v.tuple([
  v.literal('error'),
  errorKindSchema,
  v.string(),
]);

export type ErrorMessage = ['error', ErrorKind, string];
