import * as v from 'shared/src/valita.js';

export const connectionsResponseSchema = v.array(
  v.object({
    userID: v.string(),
    clientID: v.string(),
  }),
);

export type ConnectionsResponse = v.Infer<typeof connectionsResponseSchema>;
