import * as v from '../../shared/src/valita.ts';

export const deleteClientsBodySchema = v.union(
  v.readonlyObject({
    clientIDs: v.readonlyArray(v.string()).optional(),
    clientGroupIDs: v.readonlyArray(v.string()).optional(),
  }),
);

export const deleteClientsMessageSchema = v.tuple([
  v.literal('deleteClients'),
  deleteClientsBodySchema,
]);

export type DeleteClientsBody = v.Infer<typeof deleteClientsBodySchema>;
export type DeleteClientsMessage = v.Infer<typeof deleteClientsMessageSchema>;
