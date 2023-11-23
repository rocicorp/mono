import type {Analytics} from 'cloudflare-api/src/analytics.js';
import type {Firestore} from 'firebase-admin/firestore';
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
    totalPeriod: 'SUM(adjustedPeriod)',
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
      .selectStarPlus({
        schema: v.object({
          adjustedPeriod: v.number(),
        }),
        expr: {
          // Prior to https://github.com/rocicorp/mono/commit/cb0d845f3720ec647f634c85ec1fc408e35df87f,
          // older versions of the reflect-server used a semantics for "period" in which time
          // continued to be counted for 10 seconds after the last connection closed.
          // This adjustment aggregates `min(period, elapsed)` to make the resulting semantics
          // of `adjustedPeriod` closer to that of "active room seconds".
          adjustedPeriod: 'IF(period > elapsed, elapsed, period)',
        },
      })
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
