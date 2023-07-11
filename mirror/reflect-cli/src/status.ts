import {ensureUserResponseSchema} from 'mirror-protocol/src/user.js';
import {callFirebase} from 'shared/src/mirror/call-firebase.js';
import {getUserIDFromConfig, mustReadAuthConfigFile} from './auth-config.js';
import {makeRequester} from './requester.js';

export async function statusHandler() {
  const config = mustReadAuthConfigFile();
  const userID = getUserIDFromConfig(config);
  const data = {requester: makeRequester(userID)};
  const result = await callFirebase(
    'user-ensure',
    data,
    config.idToken,
    ensureUserResponseSchema,
  );
  console.log(result);
}
