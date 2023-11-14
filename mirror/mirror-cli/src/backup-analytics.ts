import {Analytics} from 'cloudflare-api/src/analytics.js';
import {getStorage} from 'firebase-admin/storage';
import {backupWeekBefore} from 'mirror-server/src/metrics/backup.js';
import {getProviderConfig} from './cf.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function backupAnalyticsOptions(yargs: CommonYargsArgv) {
  return yargs
    .positional('table', {
      desc: 'Table to backup',
      choices: ['RunningConnectionSeconds', 'ConnectionLifetimes'],
      type: 'string',
      demandOption: true,
    })
    .option('week-before', {
      desc: "Date before which the Sunday-Saturday's worth of data is to be backed up. Defaults to now()",
      type: 'string',
    });
}

type BackupAnalyticsHandlerArgs = YargvToInterface<
  ReturnType<typeof backupAnalyticsOptions>
>;

export async function backupAnalyticsHandler(
  yargs: BackupAnalyticsHandlerArgs,
) {
  const {stack, table, weekBefore: endDate} = yargs;
  const config = await getProviderConfig(yargs);
  const analytics = new Analytics(config);
  const now = endDate ? new Date(endDate) : new Date();

  const storage = getStorage();
  const bucket = storage.bucket(`reflect-mirror-${stack}-dataset-archive`);

  await backupWeekBefore(now, analytics, table, bucket);
}
