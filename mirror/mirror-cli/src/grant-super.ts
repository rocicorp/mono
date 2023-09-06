import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {getServiceAccountAuth} from './firebase.js';

export function grantSuperOptions(yargs: CommonYargsArgv) {
  return yargs
    .positional('email', {
      describe: 'Email address of account to grant super powers to',
      type: 'string',
      demandOption: true,
    })
    .option('minutes', {
      describe: 'Number of minutes to grant super powers for.',
      type: 'number',
      default: 10,
    });
}

type GrantSuperHandlerArgs = YargvToInterface<
  ReturnType<typeof grantSuperOptions>
>;

export async function grantSuperHandler(yargs: GrantSuperHandlerArgs) {
  const {email, minutes} = yargs;
  const auth = getServiceAccountAuth('super-granter', yargs);
  const {uid} = await auth.getUserByEmail(email);
  const expiration = Date.now() + minutes * 60 * 1000;
  await auth.setCustomUserClaims(uid, {superUntil: expiration});
  const {customClaims} = await auth.getUser(uid);

  console.log(
    `${email} (${uid}) has super powers until ${new Date(
      expiration,
    ).toLocaleTimeString()}`,
    customClaims,
  );
}
