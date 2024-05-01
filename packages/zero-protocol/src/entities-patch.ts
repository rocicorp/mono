import {jsonSchema} from 'shared/src/json-schema.js';
import * as v from 'shared/src/valita.js';
import {entityIDSchema} from './entity.js';

const putOpSchema = v.object({
  op: v.literal('put'),
  entityType: v.string(),
  entityID: entityIDSchema,
  value: jsonSchema,
});

const patchOpSchema = v.object({
  op: v.literal('patch'),
  entityType: v.string(),
  entityID: entityIDSchema,
  merge: jsonSchema.optional(),
  constrain: v.array(v.string()).optional(),
});

const delOpSchema = v.object({
  op: v.literal('del'),
  entityType: v.string(),
  entityID: entityIDSchema,
});

const clearOpSchema = v.object({
  op: v.literal('clear'),
});

const entityPatchOpSchema = v.union(
  putOpSchema,
  patchOpSchema,
  delOpSchema,
  clearOpSchema,
);

export const entitiesPatchSchema = v.array(entityPatchOpSchema);
export type EntitiesPutOp = v.Infer<typeof putOpSchema>;
export type EntitiesDelOp = v.Infer<typeof delOpSchema>;
export type EntitiesClearOp = v.Infer<typeof clearOpSchema>;
export type EntitiesPatchOp = v.Infer<typeof entityPatchOpSchema>;
export type EntitiesPatch = v.Infer<typeof entitiesPatchSchema>;
