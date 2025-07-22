import * as v from '../../shared/src/valita.ts';
import {mutationResponseSchema} from './push.ts';

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

const patchOpSchema = putOpSchema;
export const mutationsPatchSchema = v.array(patchOpSchema);
export type MutationPatch = v.Infer<typeof patchOpSchema>;
