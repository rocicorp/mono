import * as v from 'shared/src/valita.js';
import {pushBodySchema} from './push.js';

export const disconnectBeaconSchema = pushBodySchema;

export type DisconnectBeacon = v.Infer<typeof disconnectBeaconSchema>;

export const disconnectBeaconQueryParamsSchema = v.object({
  roomID: v.string(),
  userID: v.string(),
  clientID: v.string(),
});
