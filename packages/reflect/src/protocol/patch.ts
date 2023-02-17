import {z} from 'zod';
import {jsonSchema} from './json.js';

const putOpSchema = z.object({
  op: z.literal('put'),
  key: z.string(),
  value: jsonSchema,
});

const delOpSchema = z.object({
  op: z.literal('del'),
  key: z.string(),
});

const patchOpSchema = z.union([putOpSchema, delOpSchema]);

export const patchSchema = z.array(patchOpSchema);
export type PutOp = z.infer<typeof putOpSchema>;
export type DelOp = z.infer<typeof delOpSchema>;
export type PatchOp = z.infer<typeof patchOpSchema>;
export type Patch = z.infer<typeof patchSchema>;
