import {deleteAppKeys} from 'mirror-protocol/src/app-keys.js';
import {ensureAppInstantiated} from '../app-config.js';
import {makeRequester} from '../requester.js';
import type {CommonYargsArgv, YargvToInterface} from '../yarg-types.js';
import type {AuthContext} from '../handler.js';

export function deleteAppKeysOptions(yargs: CommonYargsArgv) {
  return yargs.positional('names', {
    describe: 'Space-separated names of keys to delete',
    type: 'string',
    array: true,
    demandOption: true,
  });
}

type DeleteAppKeysHandlerArgs = YargvToInterface<
  ReturnType<typeof deleteAppKeysOptions>
>;

export async function deleteAppKeysHandler(
  yargs: DeleteAppKeysHandlerArgs,
  authContext: AuthContext,
): Promise<void> {
  const {names} = yargs;
  const {userID} = authContext.user;
  const {appID} = await ensureAppInstantiated(authContext);

  const {deleted} = await deleteAppKeys({
    requester: makeRequester(userID),
    appID,
    names,
  });
  if (deleted.length === 0) {
    console.warn(
      `No app keys with the specified names (${asList(
        names,
      )}) were found. They may have already been deleted.`,
    );
  } else {
    console.log(`Deleted ${asList(deleted)}.`);
  }
}

function asList(names: string[]) {
  return names.map(name => `"${name}"`).join(',');
}
