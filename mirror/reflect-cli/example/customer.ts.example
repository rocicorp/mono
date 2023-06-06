import {version, type WriteTransaction} from '@rocicorp/reflect-server';

export const mutators = {
  async init(tx: WriteTransaction) {
    await tx.put('init', {});
    await tx.put('version', version);
  },
};

// eslint-disable-next-line require-await
export async function authHandler(auth: string, _roomID: string) {
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
  return null;
}

// declare function createReflectServer<
//   Env extends ReflectServerBaseEnv,
//   MD extends MutatorDefs,
// >(makeOptions: (env: Env) => ReflectServerOptions<MD>);

function makeOptions() {
  return {
    mutators: {},
    authHandler,
  };
}

export {makeOptions as default};
