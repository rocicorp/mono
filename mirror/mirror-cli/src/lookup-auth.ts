import {getServiceAccountAuth} from './firebase.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function lookupAuthOptions(yargs: CommonYargsArgv) {
  return yargs
    .positional('uid', {
      describe: 'UID of auth user to look up.',
      type: 'string',
      demandOption: true,
    })
    .positional('email', {
      describe: 'Email of auth user to look up.',
      type: 'string',
      demandOption: true,
    });
}

type LookupAuthHandlerArgs = YargvToInterface<
  ReturnType<typeof lookupAuthOptions>
>;

export async function lookupAuthHandler(yargs: LookupAuthHandlerArgs) {
  const {email, uid} = yargs;

  const auth = getServiceAccountAuth('super-granter', yargs);
  const user = email.includes('@')
    ? await auth.getUserByEmail(email)
    : await auth.getUser(uid);

  console.log(user);
}
