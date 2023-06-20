import {callFirebase} from './call-firebase.js';
import {readAuthConfigFile} from './auth-config.js';
import type {EnsureUserRequest} from 'mirror-protocol/user.js';
import {ensureUserResponseSchema} from 'mirror-protocol/user.js';
import jwt_decode from 'jwt-decode';

export async function statusHandler() {
  const {idToken} = readAuthConfigFile();
  if (!idToken) {
    throw new Error(
      'No idToken found. Please run `@rocicorp/reflect auth` to authenticate.',
    );
  }
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const token = jwt_decode.default<{user_id: string}>(idToken);
  const data: EnsureUserRequest = {
    requester: {
      userAgent: {
        type: 'reflect-cli',
        version: '0.0.1',
      },
      userID: token.user_id,
    },
  };
  const user = await callFirebase(
    'user-ensure',
    data,
    ensureUserResponseSchema,
    idToken,
  );
  console.log(user);
}
