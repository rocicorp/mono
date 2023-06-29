import jwtDecode from 'jwt-decode';
import {ensureUserResponseSchema} from 'mirror-protocol/src/user.js';
import {mustReadAuthConfigFile} from './auth-config.js';
import {callFirebase} from './call-firebase.js';
import {makeRequester} from './requester.js';

export async function statusHandler() {
  const config = mustReadAuthConfigFile();
  if (!config) {
    throw new Error(
      'No config file found. Please run `@rocicorp/reflect auth` to authenticate.',
    );
  }

  // eslint-disable-next-line @typescript-eslint/naming-convention
  const token = jwtDecode.default<{user_id: string}>(config.idToken);
  const data = {requester: makeRequester(token.user_id)};
  const user = await callFirebase(
    'user-ensure',
    data,
    ensureUserResponseSchema,
    config.idToken,
  );
  console.log(user);
}
