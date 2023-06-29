import type {WriteTransaction} from 'reflect-types/src/mod.js';
import type {JSONValue} from '../src/mod.js';
import {createReflectServer} from '../src/mod.js';

const mutators = {
  async addData(tx: WriteTransaction, object: {[key: string]: JSONValue}) {
    for (const [key, value] of Object.entries(object)) {
      await tx.put(key, value);
    }
  },
};

// eslint-disable-next-line require-await
const authHandler = async (auth: string, _roomID: string) => {
  if (auth) {
    // A real implementation could:
    // 1. if using session auth make a fetch call to a service to
    //    look up the userID by `auth` in a session database.
    // 2. if using stateless JSON Web Token auth, decrypt and validate the token
    //    and return the sub field value for userID (i.e. subject field).
    // It should also check that the user with userID is authorized
    // to access the room with roomID.
    return {
      userID: auth,
    };
  }
  throw Error('Unauthorized');
};

const {worker, RoomDO, AuthDO} = createReflectServer(_env => ({
  mutators,
  authHandler,
}));
export {AuthDO, RoomDO, worker as default};
