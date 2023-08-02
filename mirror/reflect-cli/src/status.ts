import {ensureUser} from 'mirror-protocol/src/user.js';
import {authenticate} from './auth-config.js';
import {makeRequester} from './requester.js';
import {getFirestore} from './firebase.js';

export async function statusHandler() {
  const user = await authenticate();
  const data = {requester: makeRequester(user.uid)};
  const result = await ensureUser(data);
  console.log('Status:', result);
  const userDoc = (await getFirestore().doc(`users/${user.uid}`).get()).data();
  console.log('User doc', userDoc);
}
