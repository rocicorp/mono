import * as v from '../../shared/src/valita.js';
import * as ErrorKind from './error-kind-enum.js';

const basicErrorKindSchema = v.union(
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
  kind: basicErrorKindSchema,
  message: v.string(),
});

const backoffErrorKindSchema = v.union(
  v.literal(ErrorKind.Rebalance),
  v.literal(ErrorKind.Rehome),
  v.literal(ErrorKind.ServerOverloaded),
);

const backoffBodySchema = v.object({
  kind: backoffErrorKindSchema,
  message: v.string(),
  minBackoffMs: v.number().optional(),
  maxBackoffMs: v.number().optional(),
  // Query parameters to send in the next reconnect. In the event of
  // a conflict, these will be overridden by the parameters used by
  // the client; it is the responsibility of the server to avoid
  // parameter name conflicts.
  //
  // The parameters will only be added to the immediately following
  // reconnect, and not after that.
  reconnectParams: v.record(v.string()).optional(),
});

export const errorBodySchema = v.union(basicErrorBodySchema, backoffBodySchema);

export type BackoffBody = v.Infer<typeof backoffBodySchema>;

export type ErrorBody = v.Infer<typeof errorBodySchema>;

export const errorMessageSchema: v.Type<ErrorMessage> = v.tuple([
  v.literal('error'),
  errorBodySchema,
]);

export type ErrorMessage = ['error', ErrorBody];
