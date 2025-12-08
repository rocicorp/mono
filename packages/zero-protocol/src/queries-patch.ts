import {jsonSchema} from '../../shared/src/json-schema.ts';
import * as v from '../../shared/src/valita.ts';
import {astSchema} from './ast.ts';

export const putOpSchemaBase = v.object({
  op: v.literal('put'),
  hash: v.string(),
  ttl: v.number().optional(),
});

export const putOpSchema = putOpSchemaBase.extend({
  // If set, the query is in an error state, and errorVersion is set.
  errorMessage: v.string().optional(),
  // An opaque orderable version string representing the version at which
  // the error occurred. If this version is strictly greater than the
  // retryErrorVersion sent by the client, it indicates that a new error
  // has occurred subsequent to the error the client is attempting to retry,
  // meaning the retry attempt has failed.
  errorVersion: v.string().optional(),
});

export const upPutOpSchema = putOpSchemaBase.extend({
  // All fields are optional in this transitional period.
  // - ast is filled in for client queries
  // - name and args are filled in for custom queries
  ast: astSchema.optional(),
  name: v.string().optional(),
  args: v.readonly(v.array(jsonSchema)).optional(),
  // If set, the client requests a retry for this query. The retry will only be
  // attempted if the server's current error version for the query matches this
  // value. A mismatch implies that the server has already retried (and either
  // succeeded or failed with a new error version) since the client received
  // the error, so the request is ignored to prevent redundant retries.
  retryErrorVersion: v.string().optional(),
});

const delOpSchema = v.object({
  op: v.literal('del'),
  hash: v.string(),
});

const clearOpSchema = v.object({
  op: v.literal('clear'),
});

const patchOpSchema = v.union(putOpSchema, delOpSchema, clearOpSchema);
const upPatchOpSchema = v.union(upPutOpSchema, delOpSchema, clearOpSchema);

export const queriesPatchSchema = v.array(patchOpSchema);
export const upQueriesPatchSchema = v.array(upPatchOpSchema);

export type QueriesPutOp = v.Infer<typeof putOpSchema>;
export type QueriesDelOp = v.Infer<typeof delOpSchema>;
export type QueriesClearOp = v.Infer<typeof clearOpSchema>;
export type QueriesPatchOp = v.Infer<typeof patchOpSchema>;
export type UpQueriesPatchOp = v.Infer<typeof upPatchOpSchema>;
export type QueriesPatch = v.Infer<typeof queriesPatchSchema>;
export type UpQueriesPatch = v.Infer<typeof upQueriesPatchSchema>;
