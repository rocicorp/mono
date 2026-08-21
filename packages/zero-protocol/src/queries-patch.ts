import {jsonSchema} from '../../shared/src/json-schema.ts';
import * as v from '../../shared/src/valita.ts';
import {unsupportedLegacyQueryASTSchema} from './legacy-query.ts';

export const putOpSchema = v.object({
  op: v.literal('put'),
  hash: v.string(),
  ttl: v.number().optional(),
});

export const upPutOpSchema = putOpSchema.extend({
  // Keep the field in the inferred wire type so legacy clients can still be
  // built, but reject it without inspecting or recursively parsing its value.
  // This prevents untrusted legacy ASTs from reaching zero-cache internals.
  ast: unsupportedLegacyQueryASTSchema.optional(),
  name: v.string().optional(),
  args: v.readonly(v.array(jsonSchema)).optional(),
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
