import color from 'picocolors';

import {
  createAppKey,
  isValidAppKeyName,
  listAppKeys,
} from 'mirror-protocol/src/app-keys.js';
import {ensureAppInstantiated} from '../app-config.js';
import {authenticate} from '../auth-config.js';
import {checkbox} from '../inquirer.js';
import {makeRequester} from '../requester.js';

import type {CommonYargsArgv, YargvToInterface} from '../yarg-types.js';

export function createAppKeyOptions(yargs: CommonYargsArgv) {
  return yargs.positional('name', {
    describe:
      'Unique name for the key. Must be alphanumeric, optionally with hyphens.',
    type: 'string',
    demandOption: true,
  });
}

type CreateAppKeyHandlerArgs = YargvToInterface<
  ReturnType<typeof createAppKeyOptions>
>;

export async function createAppKeyHandler(
  yargs: CreateAppKeyHandlerArgs,
): Promise<void> {
  const {name} = yargs;
  if (!isValidAppKeyName(name)) {
    console.error(
      color.yellow(`Invalid name "${name}"\n`) +
        'Names must be lowercased alphanumeric, starting with a letter and not ending with a hyphen.',
    );
    process.exit(-1);
  }

  const {userID} = await authenticate(yargs);
  const {appID} = await ensureAppInstantiated(yargs);
  const requester = makeRequester(userID);

  const {defaultPermissions} = await listAppKeys({
    requester,
    appID,
    show: false,
  });
  const allPerms = Object.keys(defaultPermissions);
  const perms =
    allPerms.length <= 1
      ? allPerms
      : await checkbox({
          message: `Select permissions for the "${color.bold(name)}" key:`,
          choices: allPerms.map(perm => ({name: perm, value: perm})),
          instructions: false,
          pageSize: 1000,
          required: true,
        });

  const {value} = await createAppKey({
    requester,
    appID,
    name,
    permissions: Object.fromEntries(perms.map(perm => [perm, true])),
  });
  console.log(`Created app key "${color.bold(name)}": ${value}`);
}
