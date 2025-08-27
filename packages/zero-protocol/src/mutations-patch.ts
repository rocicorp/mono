import * as v from '../../shared/src/valita.ts';
import {mutationIDSchema, mutationResponseSchema} from './push.ts';

/**
 * Mutation results are stored ephemerally in the client
 * hence why we only have the `put` operation.
 *
 * On put the mutation promise is resolved/rejected
 * and reference released.
 */
export const putOpSchema = v.object({
  op: v.literal('put'),
  mutation: mutationResponseSchema,
});
export const delOpSchema = v.object({
  op: v.literal('del'),
  id: mutationIDSchema,
});

const patchOpSchema = v.union(putOpSchema, delOpSchema);
export const mutationsPatchSchema = v.array(patchOpSchema);
export type MutationPatch = v.Infer<typeof patchOpSchema>;
