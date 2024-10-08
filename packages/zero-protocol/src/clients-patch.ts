import * as v from '../../shared/src/valita.js';

const putOpSchema = v.object({
  op: v.literal('put'),
  clientID: v.string(),
});

const delOpSchema = v.object({
  op: v.literal('del'),
  clientID: v.string(),
});

const clearOpSchema = v.object({
  op: v.literal('clear'),
});

const patchOpSchema = v.union(putOpSchema, delOpSchema, clearOpSchema);

export const clientsPatchSchema = v.array(patchOpSchema);

export type ClientsPutOp = v.Infer<typeof putOpSchema>;
export type ClientsDelOp = v.Infer<typeof delOpSchema>;
export type ClientsClearOp = v.Infer<typeof clearOpSchema>;
export type ClientsPatchOp = v.Infer<typeof patchOpSchema>;
export type ClientsPatch = v.Infer<typeof clientsPatchSchema>;
