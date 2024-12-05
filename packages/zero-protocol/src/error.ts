import * as v from '../../shared/src/valita.js';

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
  MutationRateLimited = 'MutationRateLimited',
  Unauthorized = 'Unauthorized',
  VersionNotSupported = 'VersionNotSupported',
  SchemaVersionNotSupported = 'SchemaVersionNotSupported',
  ServerOverloaded = 'ServerOverloaded',
  Internal = 'Internal',
}

export const errorKindSchema = v.union(
  v.literal(ErrorKind.AuthInvalidated),
  v.literal(ErrorKind.ClientNotFound),
  v.literal(ErrorKind.InvalidConnectionRequest),
  v.literal(ErrorKind.InvalidConnectionRequestBaseCookie),
  v.literal(ErrorKind.InvalidConnectionRequestLastMutationID),
  v.literal(ErrorKind.InvalidConnectionRequestClientDeleted),
  v.literal(ErrorKind.InvalidMessage),
  v.literal(ErrorKind.InvalidPush),
  v.literal(ErrorKind.MutationRateLimited),
  v.literal(ErrorKind.MutationFailed),
  v.literal(ErrorKind.Unauthorized),
  v.literal(ErrorKind.VersionNotSupported),
  v.literal(ErrorKind.SchemaVersionNotSupported),
  v.literal(ErrorKind.Internal),
);

const basicErrorBodySchema = v.object({
  kind: errorKindSchema,
  message: v.string(),
});

const serverOverloadedBodySchema = v.object({
  kind: v.literal(ErrorKind.ServerOverloaded),
  message: v.string(),
  minBackoffMs: v.number().optional(),
});

export const errorBodySchema = v.union(
  basicErrorBodySchema,
  serverOverloadedBodySchema,
);

export type ServerOverloadedBody = v.Infer<typeof serverOverloadedBodySchema>;

export type ErrorBody = v.Infer<typeof errorBodySchema>;

export const errorMessageSchema: v.Type<ErrorMessage> = v.tuple([
  v.literal('error'),
  errorBodySchema,
]);

export type ErrorMessage = ['error', ErrorBody];
