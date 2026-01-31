import {jsonSchema} from '../../shared/src/json-schema.ts';
import * as v from '../../shared/src/valita.ts';
import type {NameMapper} from '../../zero-types/src/name-mapper.ts';
import {rowSchema} from './data.ts';
import {pushFailedBodySchema} from './error.ts';
import {mutationIDSchema} from './mutation-id.ts';
import * as MutationType from './mutation-type-enum.ts';
import {primaryKeySchema, primaryKeyValueRecordSchema} from './primary-key.ts';

// NOTE! If you change this name you must also change the
// string in `replicache-impl.ts` But CRUD mutators are being
// deleted soon so this should not happen.
export const CRUD_MUTATION_NAME = '_zero_crud';

// Internal mutation name for cleaning up mutation results.
// This mutation is sent from zero-cache to zero-server to delete
// acknowledged mutation results from the upstream database.
export const CLEANUP_RESULTS_MUTATION_NAME = '_zero_cleanupResults';

export const cleanupResultsArgSchema = v.union(
  // Legacy format (no type field) - treat as single
  v.object({
    clientGroupID: v.string(),
    clientID: v.string(),
    upToMutationID: v.number(),
  }),
  // Explicit single: delete up to a specific mutation ID for one client
  v.object({
    type: v.literal('single'),
    clientGroupID: v.string(),
    clientID: v.string(),
    upToMutationID: v.number(),
  }),
  // Bulk: delete all mutations for multiple clients
  v.object({
    type: v.literal('bulk'),
    clientGroupID: v.string(),
    clientIDs: v.tuple([v.string()]).concat(v.array(v.string())),
  }),
);

/**
 * Inserts if entity with id does not already exist.
 */
const insertOpSchema = v.object({
  op: v.literal('insert'),
  tableName: v.string(),
  primaryKey: primaryKeySchema,
  value: rowSchema,
});

/**
 * Upsert semantics. Inserts if entity with id does not already exist,
 * otherwise updates existing entity with id.
 *
 * By default, conflict detection uses the primary key. If `conflictColumns`
 * is provided, those columns are used instead (for unique constraints).
 */
const upsertOpSchema = v.object({
  op: v.literal('upsert'),
  tableName: v.string(),
  primaryKey: primaryKeySchema,
  value: rowSchema,
  // Optional: columns to use for ON CONFLICT instead of primary key
  conflictColumns: v.array(v.string()).optional(),
});

/**
 * Updates if entity with id exists, otherwise does nothing.
 */
const updateOpSchema = v.object({
  op: v.literal('update'),
  tableName: v.string(),
  primaryKey: primaryKeySchema,
  // Partial value with at least the primary key fields
  value: rowSchema,
});

/**
 * Deletes entity with id if it exists, otherwise does nothing.
 */
const deleteOpSchema = v.object({
  op: v.literal('delete'),
  tableName: v.string(),
  primaryKey: primaryKeySchema,
  // Partial value representing the primary key
  value: primaryKeyValueRecordSchema,
});

const crudOpSchema = v.union(
  insertOpSchema,
  upsertOpSchema,
  updateOpSchema,
  deleteOpSchema,
);

const crudArgSchema = v.object({
  ops: v.array(crudOpSchema),
});

const crudArgsSchema = v.tuple([crudArgSchema]);

export const crudMutationSchema = v.object({
  type: v.literal(MutationType.CRUD),
  id: v.number(),
  clientID: v.string(),
  name: v.literal(CRUD_MUTATION_NAME),
  args: crudArgsSchema,
  timestamp: v.number(),
});

export const customMutationSchema = v.object({
  type: v.literal(MutationType.Custom),
  id: v.number(),
  clientID: v.string(),
  name: v.string(),
  args: v.array(jsonSchema),
  timestamp: v.number(),
});

export const mutationSchema = v.union(crudMutationSchema, customMutationSchema);

export const pushBodySchema = v.object({
  clientGroupID: v.string(),
  mutations: v.array(mutationSchema),
  pushVersion: v.number(),
  // For legacy (CRUD) mutations, the schema is tied to the client group /
  // sync connection. For custom mutations, schema versioning is delegated
  // to the custom protocol / api-server.
  schemaVersion: v.number().optional(),
  timestamp: v.number(),
  requestID: v.string(),
});

export const pushMessageSchema = v.tuple([v.literal('push'), pushBodySchema]);

const appErrorSchema = v.object({
  error: v.literal('app'),
  // The user can return any additional data here
  message: v.string().optional(),
  details: jsonSchema.optional(),
});
const zeroErrorSchema = v.object({
  error: v.union(
    /** @deprecated push oooMutation errors are now represented as ['error', { ... }] messages */
    v.literal('oooMutation'),
    v.literal('alreadyProcessed'),
  ),
  details: jsonSchema.optional(),
});

const mutationOkSchema = v.object({
  // The user can return any additional data here
  data: jsonSchema.optional(),
});
const mutationErrorSchema = v.union(appErrorSchema, zeroErrorSchema);

export const mutationResultSchema = v.union(
  mutationOkSchema,
  mutationErrorSchema,
);

export const mutationResponseSchema = v.object({
  id: mutationIDSchema,
  result: mutationResultSchema,
});

const pushOkSchema = v.object({
  mutations: v.array(mutationResponseSchema),
});

/**
 * @deprecated push errors are now represented as ['error', { ... }] messages
 */
const unsupportedPushVersionSchema = v.object({
  /** @deprecated */
  error: v.literal('unsupportedPushVersion'),
  /** @deprecated */
  mutationIDs: v.array(mutationIDSchema).optional(),
});
/**
 * @deprecated push errors are now represented as ['error', { ... }] messages
 */
const unsupportedSchemaVersionSchema = v.object({
  /** @deprecated */
  error: v.literal('unsupportedSchemaVersion'),
  /** @deprecated */
  mutationIDs: v.array(mutationIDSchema).optional(),
});
/**
 * @deprecated push http errors are now represented as ['error', { ... }] messages
 */
const httpErrorSchema = v.object({
  /** @deprecated */
  error: v.literal('http'),
  /** @deprecated */
  status: v.number(),
  /** @deprecated */
  details: v.string(),
  /** @deprecated */
  mutationIDs: v.array(mutationIDSchema).optional(),
});
/**
 * @deprecated push zero errors are now represented as ['error', { ... }] messages
 */
const zeroPusherErrorSchema = v.object({
  /** @deprecated */
  error: v.literal('zeroPusher'),
  /** @deprecated */
  details: v.string(),
  /** @deprecated */
  mutationIDs: v.array(mutationIDSchema).optional(),
});
/**
 * @deprecated push errors are now represented as ['error', { ... }] messages
 */
const pushErrorSchema = v.union(
  unsupportedPushVersionSchema,
  unsupportedSchemaVersionSchema,
  httpErrorSchema,
  zeroPusherErrorSchema,
);

export const pushResponseBodySchema = v.union(pushOkSchema, pushErrorSchema);

export const pushResponseSchema = v.union(
  pushResponseBodySchema,
  pushFailedBodySchema,
);
export const pushResponseMessageSchema = v.tuple([
  v.literal('pushResponse'),
  pushResponseBodySchema,
]);

export const ackMutationResponsesMessageSchema = v.tuple([
  v.literal('ackMutationResponses'),
  mutationIDSchema,
]);

/**
 * The schema for the querystring parameters of the custom push endpoint.
 */
export const pushParamsSchema = v.object({
  schema: v.string(),
  appID: v.string(),
});

export type InsertOp = v.Infer<typeof insertOpSchema>;
export type UpsertOp = v.Infer<typeof upsertOpSchema>;
export type UpdateOp = v.Infer<typeof updateOpSchema>;
export type DeleteOp = v.Infer<typeof deleteOpSchema>;
export type CRUDOp = v.Infer<typeof crudOpSchema>;
export type CRUDOpKind = CRUDOp['op'];
export type CRUDMutationArg = v.Infer<typeof crudArgSchema>;
export type CRUDMutation = v.Infer<typeof crudMutationSchema>;
export type CustomMutation = v.Infer<typeof customMutationSchema>;
export type Mutation = v.Infer<typeof mutationSchema>;
export type PushBody = v.Infer<typeof pushBodySchema>;
export type PushMessage = v.Infer<typeof pushMessageSchema>;
export type PushResponseBody = v.Infer<typeof pushResponseBodySchema>;
export type PushResponse = v.Infer<typeof pushResponseSchema>;
export type PushResponseMessage = v.Infer<typeof pushResponseMessageSchema>;
export type MutationResponse = v.Infer<typeof mutationResponseSchema>;
export type MutationOk = v.Infer<typeof mutationOkSchema>;
export type MutationError = v.Infer<typeof mutationErrorSchema>;
/**
 * @deprecated push errors are now represented as ['error', { ... }] messages
 */
export type PushError = v.Infer<typeof pushErrorSchema>;
export type PushOk = v.Infer<typeof pushOkSchema>;
export type MutationResult = v.Infer<typeof mutationResultSchema>;
export type AckMutationMessage = v.Infer<
  typeof ackMutationResponsesMessageSchema
>;
export type CleanupResultsArg = v.Infer<typeof cleanupResultsArgSchema>;
export type {MutationID} from './mutation-id.ts';

export function mapCRUD(
  arg: CRUDMutationArg,
  map: NameMapper,
): CRUDMutationArg {
  return {
    ops: arg.ops.map(
      ({op, tableName, primaryKey, value}) =>
        ({
          op,
          tableName: map.tableName(tableName),
          primaryKey: map.columns(tableName, primaryKey),
          value: map.row(tableName, value),
          // The cast is necessary because ts objects to the `value` field
          // for "delete" ops being different.
        }) as unknown as CRUDOp,
    ),
  };
}
