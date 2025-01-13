import * as v from '../../shared/src/valita.js';
import * as ErrorKind from './error-kind-enum.js';

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
