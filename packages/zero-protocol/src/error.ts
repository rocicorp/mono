import {jsonSchema} from '../../shared/src/json-schema.ts';
import * as v from '../../shared/src/valita.ts';
import {mutationIDSchema} from './mutation-id.ts';
import {ErrorKind} from './error-kind.ts';
import {ErrorOrigin} from './error-origin.ts';

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
  // TODO(0xcadams): make this required once we cut the protocol version
  origin: v.literalUnion(ErrorOrigin.Server, ErrorOrigin.ZeroCache).optional(),
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
  origin: v.literalUnion(ErrorOrigin.ZeroCache).optional(),
});

const pushFailedErrorKindSchema = v.literal(ErrorKind.PushFailed);
const transformFailedErrorKindSchema = v.literal(ErrorKind.TransformFailed);

export const errorKindSchema: v.Type<ErrorKind> = v.union(
  basicErrorKindSchema,
  backoffErrorKindSchema,
  pushFailedErrorKindSchema,
  transformFailedErrorKindSchema,
);

const pushFailedBaseSchema = v.object({
  kind: pushFailedErrorKindSchema,
  details: jsonSchema.optional(),
  mutationIDs: v.array(mutationIDSchema),
  message: v.string(),
});

export const pushFailedBodySchema = v.union(
  pushFailedBaseSchema.extend({
    origin: v.literal(ErrorOrigin.Server),
    type: v.literalUnion(
      'database',
      'parse',
      'oooMutation',
      'unsupportedPushVersion',
      'internal',
    ),
  }),
  pushFailedBaseSchema.extend({
    origin: v.literal(ErrorOrigin.ZeroCache),
    type: v.literal('http'),
    status: v.number(),
    bodyPreview: v.string().optional(),
  }),
  pushFailedBaseSchema.extend({
    origin: v.literal(ErrorOrigin.ZeroCache),
    type: v.literalUnion('timeout', 'parse', 'internal'),
  }),
);

const transformFailedBaseSchema = v.object({
  kind: transformFailedErrorKindSchema,
  details: jsonSchema.optional(),
  queryIDs: v.array(v.string()),
  message: v.string(),
});

export const transformFailedBodySchema = v.union(
  transformFailedBaseSchema.extend({
    origin: v.literal(ErrorOrigin.Server),
    type: v.literalUnion('database', 'parse', 'internal'),
  }),
  transformFailedBaseSchema.extend({
    origin: v.literal(ErrorOrigin.ZeroCache),
    type: v.literal('http'),
    status: v.number(),
    bodyPreview: v.string().optional(),
  }),
  transformFailedBaseSchema.extend({
    origin: v.literal(ErrorOrigin.ZeroCache),
    type: v.literalUnion('timeout', 'parse', 'internal'),
  }),
);

export const errorBodySchema = v.union(
  basicErrorBodySchema,
  backoffBodySchema,
  pushFailedBodySchema,
  transformFailedBodySchema,
);

export type BackoffBody = v.Infer<typeof backoffBodySchema>;
export type PushFailedBody = v.Infer<typeof pushFailedBodySchema>;
export type TransformFailedBody = v.Infer<typeof transformFailedBodySchema>;
export type ErrorBody = v.Infer<typeof errorBodySchema>;

export const errorMessageSchema: v.Type<ErrorMessage> = v.tuple([
  v.literal('error'),
  errorBodySchema,
]);

export type ErrorMessage = ['error', ErrorBody];

/**
 * Represents an error used in zero-client, zero-cache, and zero-server.
 */
export class ProtocolError<
  const T extends {
    kind: string;
    message: string;
    // TODO(0xcadams): this should eventually be required
    origin?: ErrorOrigin | undefined;
  } = ErrorBody,
> extends Error {
  readonly errorBody: T;

  constructor(errorBody: T, options?: ErrorOptions) {
    super(errorBody.message, options);
    this.name = 'ProtocolError';
    this.errorBody = errorBody;
  }

  get kind(): T['kind'] {
    return this.errorBody.kind;
  }
}

export function isProtocolError(error: unknown): error is ProtocolError {
  return error instanceof ProtocolError;
}
