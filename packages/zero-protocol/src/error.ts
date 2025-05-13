import * as v from '../../shared/src/valita.ts';
import {ErrorKind} from './error-kind.ts';

const basicErrorKindSchema = v.literalUnion(
  ErrorKind.AuthInvalidated,
  ErrorKind.ClientNotFound,
  ErrorKind.InvalidConnectionRequest,
  ErrorKind.InvalidConnectionRequestBaseCookie,
  ErrorKind.InvalidConnectionRequestLastMutationID,
  ErrorKind.InvalidConnectionRequestClientDeleted,
  ErrorKind.InvalidMessage,
  ErrorKind.InvalidPush,
  ErrorKind.MutationRateLimited,
  ErrorKind.MutationFailed,
  ErrorKind.Unauthorized,
  ErrorKind.VersionNotSupported,
  ErrorKind.SchemaVersionNotSupported,
  ErrorKind.Internal,
);

const basicErrorBodySchema = v.object({
  kind: basicErrorKindSchema,
  message: v.string(),
});

const backoffErrorKindSchema = v.literalUnion(
  ErrorKind.Rebalance,
  ErrorKind.Rehome,
  ErrorKind.ServerOverloaded,
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

export const errorKindSchema: v.Type<ErrorKind> = v.union(
  basicErrorKindSchema,
  backoffErrorKindSchema,
);

export const errorBodySchema = v.union(basicErrorBodySchema, backoffBodySchema);

export type BackoffBody = v.Infer<typeof backoffBodySchema>;

export type ErrorBody = v.Infer<typeof errorBodySchema>;

export const errorMessageSchema: v.Type<ErrorMessage> = v.tuple([
  v.literal('error'),
  errorBodySchema,
]);

export type ErrorMessage = ['error', ErrorBody];
