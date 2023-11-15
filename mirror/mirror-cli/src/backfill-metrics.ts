import {Analytics} from 'cloudflare-api/src/analytics.js';
import {getFirestore} from 'firebase-admin/firestore';
import {aggregateHourBefore} from 'mirror-server/src/metrics/aggregate.js';
import {sleep} from 'shared/src/sleep.js';
import {getProviderConfig} from './cf.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function backfillMetricsOptions(yargs: CommonYargsArgv) {
  return yargs
    .option('start-date', {
      desc: 'Date from which to begin the backfill, hour by hour',
      type: 'string',
      default: '2023-10-14',
    })
    .option('max-runs', {
      desc: 'Maximum number of hours to backfill',
      type: 'number',
      default: Number.MAX_SAFE_INTEGER,
    });
}

type BackfillMetricsHandlerArgs = YargvToInterface<
  ReturnType<typeof backfillMetricsOptions>
>;

export async function backfillMetricsHandler(
  yargs: BackfillMetricsHandlerArgs,
) {
  const {startDate, maxRuns} = yargs;
  const firestore = getFirestore();
  const config = await getProviderConfig(yargs);
  const analytics = new Analytics(config);
  const startTime = new Date(startDate);
  const now = Date.now();

  for (let runs = 0; startTime.getTime() < now && runs < maxRuns; runs++) {
    (await aggregateHourBefore(firestore, analytics, startTime)).forEach(
      result => {
        if (result.status === 'rejected') {
          throw result.reason;
        }
      },
    );
    startTime.setHours(startTime.getHours() + 1);
    await sleep(1000);
  }
}
