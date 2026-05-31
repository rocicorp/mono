import * as v from '../../shared/src/valita.ts';

export const mutationIDSchema = v.object({
  id: v.number(),
  clientID: v.string(),
});

export type MutationID = v.Infer<typeof mutationIDSchema>;
