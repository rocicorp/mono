import {jsonObjectSchema} from '../../shared/src/json-schema.js';
import * as v from '../../shared/src/valita.js';
import {primaryKeyValueRecordSchema} from './primary-key.js';

const putOpSchema = v.object({
  op: v.literal('put'),
  tableName: v.string(),
  // TODO: Remove entityID, we can use value
  entityID: primaryKeyValueRecordSchema,
  value: jsonObjectSchema,
});

const updateOpSchema = v.object({
  op: v.literal('update'),
  tableName: v.string(),
  // TODO: Rename to id
  entityID: primaryKeyValueRecordSchema,
  merge: jsonObjectSchema.optional(),
  constrain: v.array(v.string()).optional(),
});

const delOpSchema = v.object({
  op: v.literal('del'),
  tableName: v.string(),
  // TODO: Rename to id
  entityID: primaryKeyValueRecordSchema,
});

const clearOpSchema = v.object({
  op: v.literal('clear'),
});

const rowPatchOpSchema = v.union(
  putOpSchema,
  updateOpSchema,
  delOpSchema,
  clearOpSchema,
);

export const rowsPatchSchema = v.array(rowPatchOpSchema);
export type RowsPatchOp = v.Infer<typeof rowPatchOpSchema>;
