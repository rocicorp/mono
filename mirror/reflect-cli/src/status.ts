import {ensureUser} from 'mirror-protocol/src/user.js';
import {authenticate} from './auth-config.js';
import {makeRequester} from './requester.js';
import {getFirestore} from './firebase.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export async function statusHandler(yargs: YargvToInterface<CommonYargsArgv>) {
  const {userID} = await authenticate(yargs);
  const data = {requester: makeRequester(userID)};
  const result = await ensureUser(data);
  console.log('Status:', result);
  const userDoc = (await getFirestore().doc(`users/${userID}`).get()).data();
  console.log('User doc', userDoc);
}
