import * as s from 'superstruct';
import {jsonSchema} from './json.js';

const putOpSchema = s.object({
  op: s.literal('put'),
  key: s.string(),
  value: jsonSchema,
});

const delOpSchema = s.object({
  op: s.literal('del'),
  key: s.string(),
});

const patchOpSchema = s.union([putOpSchema, delOpSchema]);

export const patchSchema = s.array(patchOpSchema);
export type PutOp = s.Infer<typeof putOpSchema>;
export type DelOp = s.Infer<typeof delOpSchema>;
export type PatchOp = s.Infer<typeof patchOpSchema>;
export type Patch = s.Infer<typeof patchSchema>;
