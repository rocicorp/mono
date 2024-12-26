import {jsonObjectSchema} from '../../shared/src/json-schema.js';
import * as v from '../../shared/src/valita.js';
import {rowSchema} from './data.js';
import {primaryKeyValueRecordSchema} from './primary-key.js';

const putOpSchema = v.object({
  op: v.literal('put'),
  tableName: v.string(),

  // `id` and `rest` comprise the full row value
  // TODO: make these non-optional with next PROTOCOL_VERSION bump.
  id: primaryKeyValueRecordSchema.optional(),
  rest: rowSchema.optional(),

  // TODO: remove with next PROTOCOL_VERSION bump.
  value: rowSchema.optional(),
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
