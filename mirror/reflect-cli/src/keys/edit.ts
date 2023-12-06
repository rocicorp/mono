import color from 'picocolors';

import {editAppKey, listAppKeys} from 'mirror-protocol/src/app-keys.js';
import {ensureAppInstantiated} from '../app-config.js';
import {authenticate} from '../auth-config.js';
import {checkbox} from '../inquirer.js';
import {makeRequester} from '../requester.js';

import type {CommonYargsArgv, YargvToInterface} from '../yarg-types.js';

export function editAppKeyOptions(yargs: CommonYargsArgv) {
  return yargs.positional('name', {
    describe: 'Name of the key to edit.',
    type: 'string',
    demandOption: true,
  });
}

type EditAppKeyHandlerArgs = YargvToInterface<
  ReturnType<typeof editAppKeyOptions>
>;

export async function editAppKeyHandler(
  yargs: EditAppKeyHandlerArgs,
): Promise<void> {
  const {name} = yargs;
  const {userID} = await authenticate(yargs);
  const {appID} = await ensureAppInstantiated(yargs);
  const requester = makeRequester(userID);

  const {keys, defaultPermissions} = await listAppKeys({
    requester,
    appID,
    show: false,
  });
  const key = keys.find(key => key.name === name);
  if (!key) {
    console.warn(color.yellow(`Key named "${name}" was not found.`));
    process.exit(-1);
  }
  const perms = await checkbox({
    message: `Select permissions for the "${color.bold(name)}" key:`,
    choices: Object.keys(defaultPermissions).map(perm => ({
      name: perm,
      value: perm,
      checked: key.permissions[perm],
    })),
    pageSize: 1000,
  });
  if (perms.length === 0) {
    // TODO: Update version of @inquirer/checkbox that includes validation.
    console.error(color.yellow('You must select at least one permission.'));
    process.exit(-1);
  }

  await editAppKey({
    requester,
    appID,
    name,
    permissions: Object.fromEntries(perms.map(perm => [perm, true])),
  });
  console.log(`Permissions set for app key "${color.bold(name)}".`);
}
