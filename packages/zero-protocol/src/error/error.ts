import * as v from '../../../shared/src/valita.ts';
import {ErrorOrigin} from './error-origin.ts';
import {ErrorRecoveryStrategy} from './error-recovery-strategy.ts';
import {ErrorScope} from './error-scope.ts';
import {ErrorSeverity} from './error-severity.ts';
import {ErrorSource} from './error-source.ts';

export const errorSeveritySchema = v.literalUnion(
  ErrorSeverity.Info,
  ErrorSeverity.Warn,
  ErrorSeverity.Error,
);

const baseErrorSchema = v.object({
  id: v.string(),
  timestamp: v.number(),
  severity: errorSeveritySchema,
  message: v.string(),
});

const recoveryStrategySchema = v.union(
  v.object({
    type: v.literal(ErrorRecoveryStrategy.Auto),
    minBackoffMs: v.number().optional(),
    maxBackoffMs: v.number().optional(),
    reconnectParams: v.record(v.string()).optional(),
  }),
  v.object({
    type: v.literal(ErrorRecoveryStrategy.ManualReconnect),
  }),
  v.object({
    type: v.literal(ErrorRecoveryStrategy.ManualRefetch),
  }),
  v.object({
    type: v.literal(ErrorRecoveryStrategy.PageReload),
    clearStorage: v.boolean(),
    disableClientGroup: v.boolean(),
  }),
);

/**
 * Connection errors happen when the client is
 * unable to open a websocket connection to zero-cache.
 */
export const connectionErrorSchema = baseErrorSchema.extend({
  origin: v.literal(ErrorOrigin.Client),
  scope: v.literal(ErrorScope.Connection),
  recoveryStrategy: recoveryStrategySchema,
  source: v.literal(ErrorSource.HTTP),
  details: v.object({
    type: v.literal('Websocket'),
    wasClean: v.boolean(),
    websocketID: v.string(),
    attempt: v.number(),
  }),
});

export type ConnectionError = v.Infer<typeof connectionErrorSchema>;

// The base query error body is used for both HTTP and application query errors.
// These only originate from the server.
export const baseQueryErrorSchema = baseErrorSchema.extend({
  origin: v.literalUnion(
    ErrorOrigin.Server,
    ErrorOrigin.ZeroCache,
    ErrorOrigin.Client,
  ),
  scope: v.literal(ErrorScope.Query),
  recoveryStrategy: recoveryStrategySchema,
  query: v.object({
    id: v.string(),
    name: v.string(),
    args: v.unknown(),
  }),
});

export type BaseQueryError = v.Infer<typeof baseQueryErrorSchema>;

export const httpErrorDetailsSchema = v.union(
  v.object({
    type: v.literal('Network'),
  }),
  v.object({
    type: v.literal('Auth'),
    status: v.literalUnion(401, 403),
    body: v.string().optional(),
  }),
  v.object({
    type: v.literal('ServerError'),
    status: v.number(),
    body: v.string().optional(),
  }),
);

export type HTTPErrorDetails = v.Infer<typeof httpErrorDetailsSchema>;

/**
 * Batch query errors happen when zero-cache is
 * unable to fetch all queries due to an error.
 *
 * This can be an HTTP error (timeout, etc.),
 * a non-200 status code from the API server,
 * a JWT validation error, or an internal zero-cache error.
 */
export const queryBatchErrorSchema = v.union(
  baseQueryErrorSchema.extend({
    source: v.literal(ErrorSource.HTTP),
    details: httpErrorDetailsSchema,
  }),
  baseQueryErrorSchema.extend({
    source: v.literal(ErrorSource.App),
    details: v.union(
      v.object({
        /** @deprecated */
        type: v.literal('JWTValidation'),
      }),
    ),
  }),
  baseQueryErrorSchema.extend({
    source: v.literal(ErrorSource.Zero),
    details: v.union(
      v.object({
        type: v.literal('Internal'),
      }),
    ),
  }),
);

export type QueryBatchError = v.Infer<typeof queryBatchErrorSchema>;

/**
 * Item query errors happen when zero-cache is
 * unable to execute an individual query due to an error.
 *
 * This can be a validation error for the parameters, or if an app error occurs during
 * the execution of the query (e.g. a permission error, a schema mismatch, etc.).
 */
export const queryItemErrorSchema = baseQueryErrorSchema.extend({
  source: v.literal(ErrorSource.App),
  details: v.union(
    v.object({
      type: v.literal('Validation'),
      // TODO add validation errors?
    }),
    v.object({
      type: v.literal('Execution'),
    }),
  ),
});

export type QueryItemError = v.Infer<typeof queryItemErrorSchema>;

/**
 * Query errors happen when zero-cache is unable to execute a single
 * query or a batch of queries.
 */
export const queryErrorSchema = v.union(
  queryBatchErrorSchema,
  queryItemErrorSchema,
);

export type QueryError = v.Infer<typeof queryErrorSchema>;

// The base mutation error body is used for both HTTP and application mutation errors.
const baseMutationErrorSchema = baseErrorSchema.extend({
  origin: v.literalUnion(
    ErrorOrigin.Server,
    ErrorOrigin.ZeroCache,
    ErrorOrigin.Client,
  ),
  scope: v.literal(ErrorScope.Mutation),
  recoveryStrategy: recoveryStrategySchema,
  mutations: v.array(
    v.object({
      id: v.number(),
      clientID: v.string(),
      name: v.string(),
      args: v.unknown(),
    }),
  ),
});

/**
 * Batch mutation errors happen when zero-cache is
 * unable to execute a batch of mutations due to
 * an error outside of the DB transaction
 * boundary.
 *
 * This can be a network error (timeout, etc.),
 * a non-200 status code from the API server,
 * or an internal zero-cache error.
 */
export const mutationBatchErrorSchema = v.union(
  baseMutationErrorSchema.extend({
    source: v.literal(ErrorSource.HTTP),
    details: httpErrorDetailsSchema,
  }),
  baseMutationErrorSchema.extend({
    source: v.literal(ErrorSource.App),
    details: v.object({
      /** @deprecated */
      type: v.literal('JWTValidation'),
    }),
  }),
  baseMutationErrorSchema.extend({
    source: v.literal(ErrorSource.Zero),
    details: v.union(
      v.object({
        type: v.literal('OutOfOrder'),
      }),
      v.object({
        type: v.literal('Internal'),
      }),
    ),
  }),
);

export type MutationBatchError = v.Infer<typeof mutationBatchErrorSchema>;

/**
 * Mutation errors happen when zero-cache is unable to execute
 * an individual mutation inside of the DB transaction boundary.
 *
 * This can be if an error is thrown from the app during the execution of the mutation
 * (e.g. a permission error, constraint violation, JWT validation error, etc.),
 * the mutation was already processed, or an internal zero-cache error.
 */
export const mutationItemErrorSchema = baseMutationErrorSchema.extend({
  source: v.literalUnion(ErrorSource.App, ErrorSource.Zero),
  details: v.union(
    v.object({
      type: v.literal('Validation'),
    }),
    v.object({
      type: v.literal('Execution'),
    }),
    v.object({
      type: v.literal('AlreadyProcessed'),
    }),
    v.object({
      type: v.literal('Internal'),
    }),
  ),
});

export type MutationItemError = v.Infer<typeof mutationItemErrorSchema>;

/**
 * Mutation errors happen when zero-cache is unable to execute a single
 * mutation or a batch of mutations.
 */
export const mutationErrorSchema = v.union(
  mutationBatchErrorSchema,
  mutationItemErrorSchema,
);

export type MutationError = v.Infer<typeof mutationErrorSchema>;

/**
 * Sync errors happen when zero-cache or the client has a problem
 * with sync data.
 *
 * This can be due to a missing CVR/client, the client being
 * ahead/behind zero-cache, a protocol mismatch, or
 * some other sync-layer issue.
 */
export const syncErrorSchema = baseErrorSchema.extend({
  scope: v.literal(ErrorScope.Sync),
  origin: v.literal(ErrorOrigin.ZeroCache),
  recoveryStrategy: recoveryStrategySchema,
  details: v.union(
    v.object({
      // This was renamed from ClientNotFound
      type: v.literal('MissingClientState'),
    }),
    v.object({
      // This was renamed from InvalidConnectionRequest{BaseCookie,LastMutationID}
      type: v.literal('ClientOutOfSync'),
      clientStatus: v.literalUnion('Ahead', 'Behind'),
    }),
    v.object({
      // This was renamed from VersionNotSupported and SchemaVersionNotSupported
      type: v.literal('ProtocolMismatch'),
      clientProtocolVersion: v.literalUnion('Ahead', 'Behind'),
      // TODO check if Schema is still even used? it used to be
      // a manual version bump that users needed to do
      category: v.literalUnion('Protocol', 'Schema'),
    }),
    v.object({
      type: v.literal('Other'),
    }),
  ),
});

export type SyncError = v.Infer<typeof syncErrorSchema>;

/**
 * Backoff errors happen when zero-cache requests the client
 * to reconnect.
 *
 * This can be due to
 */
export const backoffErrorSchema = baseErrorSchema.extend({
  scope: v.literal(ErrorScope.Backoff),
  origin: v.literal(ErrorOrigin.ZeroCache),
  recoveryStrategy: recoveryStrategySchema,
  details: v.union(
    v.object({
      type: v.literalUnion('Rebalance', 'Rehome', 'ServerOverloaded'),
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
    }),
  ),
});

export type BackoffError = v.Infer<typeof backoffErrorSchema>;

/**
 * Internal errors happen when there is an unexpected
 * error in zero. This is a fallback for when the client
 * or zero-cache is unable to determine the specific error.
 */
export const internalErrorSchema = baseErrorSchema.extend({
  scope: v.literal(ErrorScope.Internal),
  origin: v.literalUnion(ErrorOrigin.Client, ErrorOrigin.ZeroCache),
  details: v.object({
    component: v.string(),
  }),
});

export type InternalError = v.Infer<typeof internalErrorSchema>;

export const detailedErrorBodySchema = v.union(
  connectionErrorSchema,
  queryErrorSchema,
  mutationErrorSchema,
  syncErrorSchema,
  backoffErrorSchema,
  internalErrorSchema,
);

export type DetailedErrorBody = v.Infer<typeof detailedErrorBodySchema>;

export const detailedErrorMessageSchema: v.Type<DetailedErrorMessage> = v.tuple(
  [v.literal('detailedError'), detailedErrorBodySchema],
);

export type DetailedErrorMessage = ['detailedError', DetailedErrorBody];
