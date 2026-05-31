import * as v from '../../shared/src/valita.ts';

const updateAuthBodySchema = v.object({
  auth: v.string(),
});

export const updateAuthMessageSchema = v.tuple([
  v.literal('updateAuth'),
  updateAuthBodySchema,
]);

export type UpdateAuthBody = v.Infer<typeof updateAuthBodySchema>;
export type UpdateAuthMessage = v.Infer<typeof updateAuthMessageSchema>;
