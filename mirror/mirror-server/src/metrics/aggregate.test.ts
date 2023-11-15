import {afterEach, describe, expect, jest, test} from '@jest/globals';
import {Analytics} from 'cloudflare-api/src/analytics.js';
import {initializeApp} from 'firebase-admin/app';
import {getFirestore} from 'firebase-admin/firestore';
import {monthMetricsPath, totalMetricsPath} from 'mirror-schema/src/metrics.js';
import {FetchMocker} from 'shared/src/fetch-mocker.js';
import {aggregateHourBefore} from './aggregate.js';

const QUERY_RESULT = {
  meta: [
    {name: 'teamID', type: 'String'},
    {name: 'appID', type: 'String'},
    {name: 'totalElapsed', type: 'Float64'},
    {name: 'totalPeriod', type: 'Float64'},
  ],
  data: [
    {
      teamID: 'Itbj8PWpHEm',
      appID: 'ln2b4mz2',
      totalElapsed: 8321.108,
      totalPeriod: 7401.156,
    },
    {
      teamID: 'CV9Uwl0iwnd',
      appID: 'louhf2oo',
      totalElapsed: 109.055,
      totalPeriod: 116.241,
    },
    {
      teamID: 'CzCv9TyiF7u',
      appID: 'lmjdhbbp',
      totalElapsed: 9778.55,
      totalPeriod: 3255.52,
    },
    {
      teamID: '7b4eqY3OWih',
      appID: 'lof5fld8',
      totalElapsed: 1368521.747,
      totalPeriod: 72572.993,
    },
    {
      teamID: 'LySsDFFXARU',
      appID: 'lox33rd9',
      totalElapsed: 101.94,
      totalPeriod: 124.589,
    },
  ],
  rows: 5,
  // eslint-disable-next-line @typescript-eslint/naming-convention
  rows_before_limit_at_least: 16146,
};

describe('aggregateHourBefore', () => {
  initializeApp({projectId: 'metrics-aggregation-test'});
  const firestore = getFirestore();

  afterEach(() => {
    jest.clearAllMocks();
  });

  test('success', async () => {
    const fetcher = new FetchMocker().result(
      'POST',
      '/analytics',
      QUERY_RESULT,
    );

    const date = new Date(Date.UTC(2023, 9, 10, 13, 14, 15, 16));
    const startTime = Date.UTC(2023, 9, 10, 12) / 1000;
    const endTime = Date.UTC(2023, 9, 10, 13) / 1000;

    await aggregateHourBefore(
      firestore,
      new Analytics({apiToken: 'api-token', accountID: 'cf-account'}),
      date,
    );

    expect(fetcher.bodys()).toEqual([
      `SELECT
          teamID,
          appID,
          SUM(elapsed) AS totalElapsed,
          SUM(period) AS totalPeriod
          FROM (
          SELECT
          blob1 AS teamID,
          blob2 AS appID,
          double1 AS elapsed,
          double2 AS period,
          timestamp
          FROM RunningConnectionSeconds
          WHERE (timestamp >= toDateTime(${startTime})) AND (timestamp < toDateTime(${endTime}))
          )
          GROUP BY teamID, appID
          FORMAT JSON`,
    ]);

    for (const usage of QUERY_RESULT.data) {
      const {teamID, appID, totalElapsed, totalPeriod} = usage;
      const expectedTeamMonth = {
        teamID,
        appID: null,
        yearMonth: 202310,
        total: {
          cs: totalElapsed,
          rs: totalPeriod,
        },
        day: {
          '10': {
            hour: {
              '12': {
                cs: totalElapsed,
                rs: totalPeriod,
              },
            },
            total: {
              cs: totalElapsed,
              rs: totalPeriod,
            },
          },
        },
      };
      expect(
        (
          await firestore.doc(monthMetricsPath('2023', '10', teamID)).get()
        ).data(),
      ).toEqual(expectedTeamMonth);
      expect(
        (
          await firestore
            .doc(monthMetricsPath('2023', '10', teamID, appID))
            .get()
        ).data(),
      ).toEqual({
        ...expectedTeamMonth,
        appID,
      });

      const expectedTeamTotal = {
        teamID,
        appID: null,
        total: {
          cs: totalElapsed,
          rs: totalPeriod,
        },
        year: {
          '2023': {
            cs: totalElapsed,
            rs: totalPeriod,
          },
        },
      };
      expect(
        (await firestore.doc(totalMetricsPath(teamID)).get()).data(),
      ).toEqual(expectedTeamTotal);
      expect(
        (await firestore.doc(totalMetricsPath(teamID, appID)).get()).data(),
      ).toEqual({
        ...expectedTeamTotal,
        appID,
      });
    }
  });
});
