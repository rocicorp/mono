import {ensureUser} from 'mirror-protocol/src/user.js';
import {authenticate} from './auth-config.js';
import {makeRequester} from './requester.js';

export async function statusHandler() {
  const user = await authenticate();
  const data = {requester: makeRequester(user.uid)};
  const result = await ensureUser(data);
  console.log(
    result.success ? `Logged in as ${user.displayName}` : 'Not logged in',
  );
}
