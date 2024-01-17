import color from 'picocolors';

import {
  createAppKey,
  isValidApiKeyName,
  listAppKeys,
} from 'mirror-protocol/src/app-keys.js';
import {ensureAppInstantiated} from '../app-config.js';
import {checkbox, type Choice, type Item} from '../inquirer.js';
import {makeRequester} from '../requester.js';

import type {AuthContext} from '../handler.js';
import {padColumns} from '../table.js';
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
  authContext: AuthContext,
): Promise<void> {
  const {name} = yargs;
  if (!isValidApiKeyName(name)) {
    console.error(
      color.yellow(`Invalid name "${name}"\n`) +
        'Names must be lowercased alphanumeric, starting with a letter and not ending with a hyphen.',
    );
    process.exit(-1);
  }

  const {userID} = authContext.user;
  const {appID} = await ensureAppInstantiated(authContext);
  const requester = makeRequester(userID);

  const {allPermissions} = await listAppKeys.call({
    requester,
    appID,
    show: false,
  });
  const allPerms = Object.keys(allPermissions);
  const desc = padColumns(Object.entries(allPermissions));
  const perms =
    allPerms.length <= 1
      ? allPerms
      : await checkbox({
          message: `Select permissions for the "${color.bold(name)}" key:`,
          choices: allPerms.map((perm, i) => ({
            name: `${desc[i][0]}     ${color.gray(desc[i][1])}`,
            value: perm,
          })),
          instructions: false,
          pageSize: 1000,
          required: true,
          validate: stripDescriptionsIfValid,
        });

  const {value} = await createAppKey.call({
    requester,
    appID,
    name,
    permissions: Object.fromEntries(perms.map(perm => [perm, true])),
  });
  console.log(`Created app key "${color.bold(name)}": ${value}`);
}

export function stripDescriptionsIfValid(
  items: readonly Item<string>[],
): boolean {
  const choices = items as Choice<string>[]; // We don't use Separators
  if (!choices.some(choice => choice.checked)) {
    return false; // `required` will ask the user to pick at least one choice.
  }
  // Before proceeding, delete the `name` fields ('permission    description') so that
  // the resulting output lists the `value` fields ('permission' only).
  choices.forEach(choice => delete choice.name);
  return true;
}
