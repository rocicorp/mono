import {jsonSchema} from 'shared/src/json-schema.js';
import * as v from 'shared/src/valita.js';

const putOpSchema = v.object({
  op: v.literal('put'),
  key: v.string(),
  value: jsonSchema,
});

const delOpSchema = v.object({
  op: v.literal('del'),
  key: v.string(),
});

const clearOpSchema = v.object({
  op: v.literal('clear'),
});

const patchOpSchema = v.union(putOpSchema, delOpSchema, clearOpSchema);

export const entitiesPatchSchema = v.array(patchOpSchema);
export type EntitiesPutOp = v.Infer<typeof putOpSchema>;
export type EntitiesDelOp = v.Infer<typeof delOpSchema>;
export type EntitiesClearOp = v.Infer<typeof clearOpSchema>;
export type EntitiesPatchOp = v.Infer<typeof patchOpSchema>;
export type EntitiesPatch = v.Infer<typeof entitiesPatchSchema>;
