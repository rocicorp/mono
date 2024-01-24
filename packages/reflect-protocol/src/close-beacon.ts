import * as v from 'shared/src/valita.js';

export const closeBeaconSchema = v.object({
  lastMutationID: v.number(),
});

export type CloseBeacon = v.Infer<typeof closeBeaconSchema>;

export const closeBeaconQueryParamsSchema = v.object({
  roomID: v.string(),
  userID: v.string(),
  clientID: v.string(),
});

export type CloseBeaconQueryParams = v.Infer<
  typeof closeBeaconQueryParamsSchema
>;
