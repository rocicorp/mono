import {jsonObjectSchema} from '../../shared/src/json-schema.js';
import * as v from '../../shared/src/valita.js';
import {rowSchema} from './data.js';
import {primaryKeyValueRecordSchema} from './primary-key.js';

const putOpSchema = v.object({
  op: v.literal('put'),
  tableName: v.string(),
  value: rowSchema,
});

const updateOpSchema = v.object({
  op: v.literal('update'),
  tableName: v.string(),
  id: primaryKeyValueRecordSchema,
  merge: jsonObjectSchema.optional(),
  constrain: v.array(v.string()).optional(),
});

const delOpSchema = v.object({
  op: v.literal('del'),
  tableName: v.string(),
  id: primaryKeyValueRecordSchema,
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
export type RowPatchOp = v.Infer<typeof rowPatchOpSchema>;
