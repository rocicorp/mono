import * as valita from 'shared/src/valita.js';

export const disconnectBeaconQueryParamsSchema = valita.object({
  roomID: valita.string(),
  userID: valita.string(),
  clientID: valita.string(),
});
