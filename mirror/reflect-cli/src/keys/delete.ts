import {getFirestore} from 'firebase/firestore';
import {deleteApiKeys} from 'mirror-protocol/src/api-keys.js';
import type {AuthContext} from '../handler.js';
import {getLogger} from '../logger.js';
import {makeRequester} from '../requester.js';
import {getSingleTeam} from '../teams.js';
import type {CommonYargsArgv, YargvToInterface} from '../yarg-types.js';

export function deleteKeysOptions(yargs: CommonYargsArgv) {
  return yargs.positional('names', {
    describe: 'Space-separated names of keys to delete',
    type: 'string',
    array: true,
    demandOption: true,
  });
}

type DeleteKeysHandlerArgs = YargvToInterface<
  ReturnType<typeof deleteKeysOptions>
>;

export async function deleteKeysHandler(
  yargs: DeleteKeysHandlerArgs,
  authContext: AuthContext,
): Promise<void> {
  const {names} = yargs;
  const {userID} = authContext.user;
  const firestore = getFirestore();
  const teamID = await getSingleTeam(firestore, authContext, 'admin');

  const {deleted} = await deleteApiKeys.call({
    requester: makeRequester(userID),
    teamID,
    names,
  });
  if (deleted.length === 0) {
    getLogger().warn(
      `No keys with the specified names (${asList(
        names,
      )}) were found. They may have already been deleted.`,
    );
  } else {
    getLogger().log(`Deleted ${asList(deleted)}.`);
  }
}

function asList(names: string[]) {
  return names.map(name => `"${name}"`).join(',');
}
