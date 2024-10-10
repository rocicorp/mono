import {jsonObjectSchema} from '../../shared/src/json-schema.js';
import * as v from '../../shared/src/valita.js';
import {primaryKeyValueRecordSchema} from './primary-key.js';

const putOpSchema = v.object({
  op: v.literal('put'),
  entityType: v.string(),
  entityID: primaryKeyValueRecordSchema,
  value: jsonObjectSchema,
});

const updateOpSchema = v.object({
  op: v.literal('update'),
  entityType: v.string(),
  entityID: primaryKeyValueRecordSchema,
  merge: jsonObjectSchema.optional(),
  constrain: v.array(v.string()).optional(),
});

const delOpSchema = v.object({
  op: v.literal('del'),
  entityType: v.string(),
  entityID: primaryKeyValueRecordSchema,
});

const clearOpSchema = v.object({
  op: v.literal('clear'),
});

const entityPatchOpSchema = v.union(
  putOpSchema,
  updateOpSchema,
  delOpSchema,
  clearOpSchema,
);

// TODO: Rename to entities -> row?
export const entitiesPatchSchema = v.array(entityPatchOpSchema);
export type EntitiesPutOp = v.Infer<typeof putOpSchema>;
export type EntitiesUpdateOp = v.Infer<typeof updateOpSchema>;
export type EntitiesDelOp = v.Infer<typeof delOpSchema>;
export type EntitiesClearOp = v.Infer<typeof clearOpSchema>;
export type EntitiesPatchOp = v.Infer<typeof entityPatchOpSchema>;
export type EntitiesPatch = v.Infer<typeof entitiesPatchSchema>;
