import {getFirestore} from 'firebase/firestore';
import {listApiKeys} from 'mirror-protocol/src/api-keys.js';
import {APP_CREATE_PERMISSION} from 'mirror-schema/src/external/api-key.js';
import color from 'picocolors';
import type {AuthContext} from '../handler.js';
import {makeRequester} from '../requester.js';
import {padColumns} from '../table.js';
import {getSingleTeam} from '../teams.js';
import type {CommonYargsArgv, YargvToInterface} from '../yarg-types.js';

export function listKeysOptions(yargs: CommonYargsArgv) {
  return yargs.option('show', {
    desc: 'Show the values of the keys',
    type: 'boolean',
    default: false,
  });
}

type ListKeysHandlerArgs = YargvToInterface<ReturnType<typeof listKeysOptions>>;

export const CREATED_APPS = '(created apps)';

export async function listKeysHandler(
  yargs: ListKeysHandlerArgs,
  authContext: AuthContext,
): Promise<void> {
  const {show} = yargs;

  const {userID} = authContext.user;
  const firestore = getFirestore();
  const teamID = await getSingleTeam(firestore, userID, 'admin');

  const {keys, allPermissions} = await listApiKeys.call({
    requester: makeRequester(userID),
    teamID,
    show,
  });
  const now = Date.now();
  const table = [
    ['name', 'value', 'last used', 'apps', 'permissions'],
    ...keys.map(key => {
      // The "app:create" permission is handled specially, shown as "(created apps)" in the apps column.
      const apps = Object.values(key.apps);
      if (key.permissions[APP_CREATE_PERMISSION]) {
        apps.unshift(CREATED_APPS);
        delete key.permissions[APP_CREATE_PERMISSION];
      }
      return [
        color.bold(key.name),
        key.value === null ? color.italic(color.gray('Hidden')) : key.value,
        key.lastUseTime === null ? '' : timeAgo(key.lastUseTime, now),
        apps.join(','),
        Object.keys(allPermissions)
          .filter(perm => key.permissions[perm])
          .join(','),
      ];
    }),
  ];

  padColumns(table).forEach((row, i) => {
    if (i === 0) {
      row = row.map(header => color.gray(header));
    }
    console.log(row.join('     '));
  });
}

const TIME_AGO = new Intl.RelativeTimeFormat(undefined, {numeric: 'always'});

function timeAgo(past: number, now: number) {
  const msAgo = past - now;
  if (msAgo > -60000) {
    return TIME_AGO.format(Math.round(msAgo / 1000), 'seconds');
  }
  if (msAgo > -3600 * 1000) {
    return TIME_AGO.format(Math.round(msAgo / 60_000), 'minutes');
  }
  if (msAgo > -24 * 3600 * 1000) {
    return TIME_AGO.format(Math.round(msAgo / 3600_000), 'hours');
  }
  return TIME_AGO.format(Math.round(msAgo / (24 * 3600 * 1000)), 'days');
}
