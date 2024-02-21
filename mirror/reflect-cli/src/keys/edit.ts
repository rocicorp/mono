import {getFirestore} from 'firebase/firestore';
import {editApiKey, listApiKeys} from 'mirror-protocol/src/api-keys.js';
import color from 'picocolors';
import type {AuthContext} from '../handler.js';
import {makeRequester} from '../requester.js';
import {getSingleTeam} from '../teams.js';
import type {CommonYargsArgv, YargvToInterface} from '../yarg-types.js';
import {promptForKeyConfiguration} from './create.js';
import {getLogger} from '../logger.js';

export function editKeyOptions(yargs: CommonYargsArgv) {
  return yargs.positional('name', {
    describe: 'Name of the key to edit',
    type: 'string',
    demandOption: true,
  });
}

type EditKeyHandlerArgs = YargvToInterface<ReturnType<typeof editKeyOptions>>;

export async function editKeyHandler(
  yargs: EditKeyHandlerArgs,
  authContext: AuthContext,
): Promise<void> {
  const {name} = yargs;
  const {userID} = authContext.user;
  const firestore = getFirestore();
  const teamID = await getSingleTeam(firestore, userID, 'admin');
  const requester = makeRequester(userID);

  const {keys, allPermissions} = await listApiKeys.call({
    requester,
    teamID,
    show: false,
  });
  const key = keys.find(key => key.name === name);
  if (!key) {
    getLogger().warn(color.yellow(`Key named "${name}" was not found.`));
    process.exit(-1);
  }

  const {appIDs, perms} = await promptForKeyConfiguration(
    firestore,
    teamID,
    name,
    allPermissions,
    perm => key.permissions[perm],
    app => app.id in key.apps,
  );

  await editApiKey.call({
    requester,
    teamID,
    name,
    permissions: Object.fromEntries(perms.map(perm => [perm, true])),
    appIDs: {
      add: appIDs.filter(id => !(id in key.apps)),
      remove: Object.keys(key.apps).filter(id => !appIDs.includes(id)),
    },
  });
  getLogger().log(`Edited key "${color.bold(name)}".`);
}
