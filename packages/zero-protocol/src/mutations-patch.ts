import * as v from '../../shared/src/valita.ts';
import {mutationIDSchema, mutationResponseSchema} from './push.ts';

export const putOpSchema = v.object({
  op: v.literal('put'),
  mutation: mutationResponseSchema,
});

export const delOpSchema = v.object({
  op: v.literal('del'),
  mutationID: mutationIDSchema,
});

const patchOpSchema = v.union(putOpSchema, delOpSchema);
export const mutationsPatchSchema = v.array(patchOpSchema);
