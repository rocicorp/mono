import color from 'picocolors';

import {editAppKey, listAppKeys} from 'mirror-protocol/src/app-keys.js';
import {ensureAppInstantiated} from '../app-config.js';
import {checkbox} from '../inquirer.js';
import {makeRequester} from '../requester.js';

import type {AuthContext} from '../handler.js';
import {padColumns} from '../table.js';
import type {CommonYargsArgv, YargvToInterface} from '../yarg-types.js';
import {stripDescriptionsIfValid} from './create.js';

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
  authContext: AuthContext,
): Promise<void> {
  const {name} = yargs;
  const {userID} = authContext.user;
  const {appID} = await ensureAppInstantiated(authContext);
  const requester = makeRequester(userID);

  const {keys, allPermissions} = await listAppKeys.call({
    requester,
    appID,
    show: false,
  });
  const key = keys.find(key => key.name === name);
  if (!key) {
    console.warn(color.yellow(`Key named "${name}" was not found.`));
    process.exit(-1);
  }
  const desc = padColumns(Object.entries(allPermissions));
  const perms = await checkbox({
    message: `Select permissions for the "${color.bold(name)}" key:`,
    choices: Object.keys(allPermissions).map((perm, i) => ({
      name: `${desc[i][0]}     ${color.gray(desc[i][1])}`,
      value: perm,
      checked: key.permissions[perm],
    })),
    pageSize: 1000,
    instructions: false,
    required: true,
    validate: stripDescriptionsIfValid,
  });

  await editAppKey.call({
    requester,
    appID,
    name,
    permissions: Object.fromEntries(perms.map(perm => [perm, true])),
  });
  console.log(`Permissions set for app key "${color.bold(name)}".`);
}
