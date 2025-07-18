import * as v from '../../shared/src/valita.ts';
import {mutationResponseSchema} from './push.ts';

export const putOpSchema = v.object({
  op: v.literal('put'),
  mutation: mutationResponseSchema,
});

const patchOpSchema = putOpSchema;
export const mutationsPatchSchema = v.array(patchOpSchema);
