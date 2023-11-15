import type {Firestore} from '@google-cloud/firestore';
import type {Analytics} from 'cloudflare-api/src/analytics.js';
import {logger} from 'firebase-functions';
import {runningConnectionSeconds} from 'mirror-schema/src/datasets.js';
import {CONNECTION_SECONDS, ROOM_SECONDS} from 'mirror-schema/src/metrics.js';
import * as v from 'shared/src/valita.js';
import {Ledger} from './ledger.js';

export const sums = {
  schema: v.object({
    teamID: v.string(),
    appID: v.string(),
    totalElapsed: v.number(),
    totalPeriod: v.number(),
  }),
  expr: {
    totalElapsed: 'SUM(elapsed)',
    totalPeriod: 'SUM(period)',
  },
} as const;

export async function aggregateHourBefore(
  firestore: Firestore,
  analytics: Analytics,
  date: Date,
) {
  const end = new Date(
    Date.UTC(
      date.getUTCFullYear(),
      date.getUTCMonth(),
      date.getUTCDate(),
      date.getUTCHours(),
    ),
  );
  const start = new Date(end.getTime() - 3600 * 1000);
  const results = await analytics.query(
    runningConnectionSeconds
      .selectStar()
      .where('timestamp', '>=', start)
      .and('timestamp', '<', end)
      .select(sums)
      .groupBy('teamID', 'appID'),
  );

  logger.info(
    `Aggregated connection seconds from ${start.toISOString()} for ${
      results.rows
    } apps`,
  );
  return Promise.allSettled(
    results.data.map(({teamID, appID, totalElapsed, totalPeriod}) =>
      new Ledger(firestore).set(
        teamID,
        appID,
        start,
        new Map([
          [CONNECTION_SECONDS, totalElapsed],
          [ROOM_SECONDS, totalPeriod],
        ]),
      ),
    ),
  );
}
