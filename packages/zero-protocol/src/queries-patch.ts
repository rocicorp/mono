import {jsonSchema} from '../../shared/src/json-schema.ts';
import * as v from '../../shared/src/valita.ts';
import {astSchema} from './ast.ts';

export const putOpSchemaBase = v.object({
  op: v.literal('put'),
  hash: v.string(),
});

export const putDesiredQueryOpSchema = putOpSchemaBase.extend({
  // All fields are optional in this transitional period.
  // - ast is filled in for client queries
  // - name and args are filled in for custom queries
  ast: astSchema.optional(),
  name: v.string().optional(),
  args: v.readonly(v.array(jsonSchema)).optional(),
  ttl: v.number().optional(),
  // If set, the client requests a retry for this query. The retry will only be
  // attempted if the server's current error version for the query matches this
  // value. A mismatch implies that the server has already retried (and either
  // succeeded or failed with a new error version) since the client received
  // the error, so the request is ignored to prevent redundant retries.
  retryErrorVersion: v.string().optional(),
});

export const queryErrorSchema = v.object({
  // A human-readable error message.
  message: v.string(),
  // An opaque orderable version string representing the version at which
  // the error occurred. If this version is strictly greater than the
  // `retryErrorVersion` sent by the client, it indicates that a new error
  // has occurred subsequent to the error the client is attempting to retry,
  // meaning the retry attempt has failed.
  version: v.string(),
});

export const putGotQueryOpSchema = putOpSchemaBase.extend({
  // If set, the query is in an error state.  It can be retried by sending a
  // `putDesiredQueryOpSchema` with a `retryErrorVersion` set to the version
  // specified in the error.
  error: queryErrorSchema.optional(),
});

const delOpSchema = v.object({
  op: v.literal('del'),
  hash: v.string(),
});

const clearOpSchema = v.object({
  op: v.literal('clear'),
});

const patchGotQueryOpSchema = v.union(
  putGotQueryOpSchema,
  delOpSchema,
  clearOpSchema,
);
const patchDesiredQueryOpSchema = v.union(
  putDesiredQueryOpSchema,
  delOpSchema,
  clearOpSchema,
);

export type DesiredQueriesPatchOp = v.Infer<typeof patchDesiredQueryOpSchema>;
export type GotQueriesPatchOp = v.Infer<typeof patchGotQueryOpSchema>;

export const gotQueriesPatchSchema = v.array(patchGotQueryOpSchema);
export const desiredQueriesPatchSchema = v.array(patchDesiredQueryOpSchema);

export type DesiredQueriesPatch = v.Infer<typeof desiredQueriesPatchSchema>;
export type GotQueriesPatch = v.Infer<typeof gotQueriesPatchSchema>;
