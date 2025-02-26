import * as v from '../../shared/src/valita.ts';
import {astSchema} from './ast.ts';

export const putOpSchema = v.object({
  op: v.literal('put'),
  hash: v.string(),
  ast: astSchema,
  ttl: v.number().optional(),
});

const delOpSchema = v.object({
  op: v.literal('del'),
  hash: v.string(),
});

const clearOpSchema = v.object({
  op: v.literal('clear'),
});

const patchOpSchema = v.union(putOpSchema, delOpSchema, clearOpSchema);

export const queriesPatchSchema = v.array(patchOpSchema);

export type QueriesPutOp = v.Infer<typeof putOpSchema>;
export type QueriesDelOp = v.Infer<typeof delOpSchema>;
export type QueriesClearOp = v.Infer<typeof clearOpSchema>;
export type QueriesPatchOp = v.Infer<typeof patchOpSchema>;
export type QueriesPatch = v.Infer<typeof queriesPatchSchema>;
