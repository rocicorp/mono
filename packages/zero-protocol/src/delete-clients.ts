import * as v from '../../shared/src/valita.js';

const deleteClientsBodySchema = v.object({
  clientIDs: v.array(v.string()),
});

export const deleteClientsMessageSchema = v.tuple([
  v.literal('deleteClients'),
  deleteClientsBodySchema,
]);

export type DeleteClientsBody = v.Infer<typeof deleteClientsBodySchema>;
export type DeleteClientsMessage = v.Infer<typeof deleteClientsMessageSchema>;
