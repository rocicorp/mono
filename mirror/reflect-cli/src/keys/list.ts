import {listAppKeys} from 'mirror-protocol/src/app-keys.js';
import color from 'picocolors';
import {ensureAppInstantiated} from '../app-config.js';
import {authenticate} from '../auth-config.js';
import {makeRequester} from '../requester.js';
import {padColumns} from '../table.js';
import type {CommonYargsArgv, YargvToInterface} from '../yarg-types.js';

export function listAppKeysOptions(yargs: CommonYargsArgv) {
  return yargs.option('show', {
    desc: 'Show the values of the keys',
    type: 'boolean',
    default: false,
  });
}

type ListAppKeysHandlerArgs = YargvToInterface<
  ReturnType<typeof listAppKeysOptions>
>;

export async function listAppKeysHandler(
  yargs: ListAppKeysHandlerArgs,
): Promise<void> {
  const {show} = yargs;
  const {userID} = await authenticate(yargs);
  const {appID} = await ensureAppInstantiated(yargs);

  const {keys, defaultPermissions} = await listAppKeys({
    requester: makeRequester(userID),
    appID,
    show,
  });
  const now = Date.now();
  const table = [
    ['name', 'value', 'last used', 'permissions'],
    ...keys.map(key => [
      color.bold(key.name),
      key.value === null ? color.italic(color.gray('Hidden')) : key.value,
      key.lastUseTime === null ? '' : timeAgo(key.lastUseTime, now),
      Object.keys(defaultPermissions)
        .filter(perm => key.permissions[perm])
        .join(','),
    ]),
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
