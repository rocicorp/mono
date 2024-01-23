import * as v from 'shared/src/valita.js';

export const disconnectBeaconSchema = v.object({
  lastMutationID: v.number(),
});

export type DisconnectBeacon = v.Infer<typeof disconnectBeaconSchema>;

export const disconnectBeaconQueryParamsSchema = v.object({
  roomID: v.string(),
  userID: v.string(),
  clientID: v.string(),
});

export type DisconnectBeaconQueryParams = v.Infer<
  typeof disconnectBeaconQueryParamsSchema
>;
