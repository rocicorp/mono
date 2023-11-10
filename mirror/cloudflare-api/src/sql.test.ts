import {describe, expect, test} from '@jest/globals';
import * as v from 'shared/src/valita.js';
import {Dataset, timestampSchema} from './dataset.js';

const runningConnectionSeconds = new Dataset(
  'RunningConnectionSeconds',
  v.object({
    teamID: v.string(),
    appID: v.string(),
    elapsed: v.number(),
    interval: v.number(),
  }),
);

describe('sql', () => {
  describe('selectStar', () => {
    const selectStar = runningConnectionSeconds.selectStar();

    test('toString', () => {
      expect(selectStar.toString()).toBe(
        `SELECT
          blob1 AS teamID,
          blob2 AS appID,
          double1 AS elapsed,
          double2 AS interval,
          timestamp
          FROM RunningConnectionSeconds
          FORMAT JSON`,
      );
    });

    test('schema', () => {
      expect(
        selectStar.schema.parse({
          teamID: '198SeL9eaaF',
          appID: 'lm3bjejn',
          elapsed: 75.389,
          interval: 60.001,
          timestamp: '2023-11-01 04:29:19',
        }),
      ).toEqual({
        teamID: '198SeL9eaaF',
        appID: 'lm3bjejn',
        elapsed: 75.389,
        interval: 60.001,
        timestamp: new Date(Date.UTC(2023, 10, 1, 4, 29, 19)),
      });
    });

    test('where', () => {
      expect(selectStar.where('teamID', '=', 'foo').toString()).toBe(
        `SELECT
          blob1 AS teamID,
          blob2 AS appID,
          double1 AS elapsed,
          double2 AS interval,
          timestamp
          FROM RunningConnectionSeconds
          WHERE teamID = 'foo'
          FORMAT JSON`,
      );

      expect(
        selectStar
          .where('teamID', '=', 'foo')
          .and('appID', '=', `'quotes' and "double quotes"`)
          .toString(),
      ).toBe(
        `SELECT
          blob1 AS teamID,
          blob2 AS appID,
          double1 AS elapsed,
          double2 AS interval,
          timestamp
          FROM RunningConnectionSeconds
          WHERE (teamID = 'foo') AND (appID = '\\'quotes\\' and \\"double quotes\\"')
          FORMAT JSON`,
      );

      expect(
        selectStar
          .where('teamID', '=', 'foo')
          .and('appID', '=', `'quotes' and "double quotes"`)
          .or('elapsed', '>', 20)
          .toString(),
      ).toBe(
        `SELECT
          blob1 AS teamID,
          blob2 AS appID,
          double1 AS elapsed,
          double2 AS interval,
          timestamp
          FROM RunningConnectionSeconds
          WHERE ((teamID = 'foo') AND (appID = '\\'quotes\\' and \\"double quotes\\"')) OR (elapsed > 20)
          FORMAT JSON`,
      );

      expect(
        selectStar
          .where('teamID', '=', 'foo')
          .and('appID', '=', `'quotes' and "double quotes"`)
          .or('elapsed', '>', 20)
          .or('timestamp', '>', new Date(Date.UTC(2023, 9, 10)))
          .toString(),
      ).toBe(
        `SELECT
          blob1 AS teamID,
          blob2 AS appID,
          double1 AS elapsed,
          double2 AS interval,
          timestamp
          FROM RunningConnectionSeconds
          WHERE (((teamID = 'foo') AND (appID = '\\'quotes\\' and \\"double quotes\\"')) OR (elapsed > 20)) OR (timestamp > toDateTime(1696896000))
          FORMAT JSON`,
      );
    });

    test('group by', () => {
      expect(
        selectStar
          .where('teamID', '=', 'foo')
          .groupBy('teamID', 'appID')
          .toString(),
      ).toBe(
        `SELECT
          blob1 AS teamID,
          blob2 AS appID,
          double1 AS elapsed,
          double2 AS interval,
          timestamp
          FROM RunningConnectionSeconds
          WHERE teamID = 'foo'
          GROUP BY teamID, appID
          FORMAT JSON`,
      );
    });

    test('order by', () => {
      expect(
        selectStar
          .where('teamID', '=', 'foo')
          .groupBy('teamID', 'appID')
          .orderBy('timestamp')
          .toString(),
      ).toBe(
        `SELECT
          blob1 AS teamID,
          blob2 AS appID,
          double1 AS elapsed,
          double2 AS interval,
          timestamp
          FROM RunningConnectionSeconds
          WHERE teamID = 'foo'
          GROUP BY teamID, appID
          ORDER BY timestamp ASC
          FORMAT JSON`,
      );

      expect(
        selectStar
          .where('appID', '=', `string with 'quotes' and "double quotes"`)
          .groupBy('teamID', 'appID')
          .orderBy('timestamp', 'DESC')
          .toString(),
      ).toBe(
        `SELECT
          blob1 AS teamID,
          blob2 AS appID,
          double1 AS elapsed,
          double2 AS interval,
          timestamp
          FROM RunningConnectionSeconds
          WHERE appID = 'string with \\'quotes\\' and \\"double quotes\\"'
          GROUP BY teamID, appID
          ORDER BY timestamp DESC
          FORMAT JSON`,
      );
    });

    test('limit', () => {
      expect(
        selectStar
          .where('elapsed', '>', 20)
          .groupBy('teamID', 'appID')
          .orderBy('timestamp')
          .limit(100)
          .toString(),
      ).toBe(
        `SELECT
          blob1 AS teamID,
          blob2 AS appID,
          double1 AS elapsed,
          double2 AS interval,
          timestamp
          FROM RunningConnectionSeconds
          WHERE elapsed > 20
          GROUP BY teamID, appID
          ORDER BY timestamp ASC
          LIMIT 100
          FORMAT JSON`,
      );

      expect(
        selectStar
          .where('elapsed', '>', 20)
          .groupBy('teamID', 'appID')
          .orderBy('timestamp')
          .limit(undefined)
          .toString(),
      ).toBe(
        `SELECT
          blob1 AS teamID,
          blob2 AS appID,
          double1 AS elapsed,
          double2 AS interval,
          timestamp
          FROM RunningConnectionSeconds
          WHERE elapsed > 20
          GROUP BY teamID, appID
          ORDER BY timestamp ASC
          LIMIT ALL
          FORMAT JSON`,
      );
    });
  });

  describe('selectStarPlus', () => {
    const selectStarPlus = runningConnectionSeconds.selectStarPlus({
      schema: v.object({averageConnections: v.number()}),
      expr: {averageConnections: 'elapsed / interval'},
    });

    test('toString', () => {
      expect(selectStarPlus.toString()).toBe(
        `SELECT
          blob1 AS teamID,
          blob2 AS appID,
          double1 AS elapsed,
          double2 AS interval,
          timestamp,
          elapsed / interval AS averageConnections
          FROM RunningConnectionSeconds
          FORMAT JSON`,
      );
    });

    test('where', () => {
      expect(
        selectStarPlus
          .where('teamID', '=', 'foo')
          .and('averageConnections', '>', 2)
          .toString(),
      ).toBe(
        `SELECT
          blob1 AS teamID,
          blob2 AS appID,
          double1 AS elapsed,
          double2 AS interval,
          timestamp,
          elapsed / interval AS averageConnections
          FROM RunningConnectionSeconds
          WHERE (teamID = 'foo') AND (averageConnections > 2)
          FORMAT JSON`,
      );
    });
  });

  describe('custom select', () => {
    const selectCustom = runningConnectionSeconds.select({
      schema: v.object({
        teamID: v.string(),
        appID: v.string(),
        averageConnections: v.number(),
        timestamp: timestampSchema,
      }),
      expr: {
        teamID: 'blob1',
        appID: 'blob2',
        averageConnections: 'double1 / double2',
        timestamp: '',
      },
    });

    test('toString', () => {
      expect(selectCustom.toString()).toBe(
        `SELECT
          blob1 AS teamID,
          blob2 AS appID,
          double1 / double2 AS averageConnections,
          timestamp
          FROM RunningConnectionSeconds
          FORMAT JSON`,
      );
    });

    test('schema', () => {
      expect(
        selectCustom.schema.parse({
          teamID: '198SeL9eaaF',
          appID: 'lm3bjejn',
          averageConnections: 2.1,
          timestamp: '2023-11-01 05:10:41',
        }),
      ).toEqual({
        teamID: '198SeL9eaaF',
        appID: 'lm3bjejn',
        averageConnections: 2.1,
        timestamp: new Date(Date.UTC(2023, 10, 1, 5, 10, 41)),
      });
    });

    test('where', () => {
      expect(selectCustom.where('averageConnections', '>', 2).toString()).toBe(
        `SELECT
          blob1 AS teamID,
          blob2 AS appID,
          double1 / double2 AS averageConnections,
          timestamp
          FROM RunningConnectionSeconds
          WHERE averageConnections > 2
          FORMAT JSON`,
      );
    });
  });

  describe('aggregation', () => {
    const selectAggregation = runningConnectionSeconds.select({
      schema: v.object({
        teamID: v.string(),
        appID: v.string(),
        totalElapsed: v.number(),
        totalInterval: v.number(),
      }),
      expr: {
        teamID: 'blob1',
        appID: 'blob2',
        totalElapsed: 'SUM(double1)',
        totalInterval: 'SUM(double2)',
      },
    });

    test('where backing column', () => {
      expect(
        selectAggregation
          .where('timestamp', '>=', new Date(Date.UTC(2023, 5, 1)))
          .and('timestamp', '<', new Date(Date.UTC(2023, 6, 1)))
          .groupBy('teamID', 'appID')
          .toString(),
      ).toBe(
        `SELECT
          blob1 AS teamID,
          blob2 AS appID,
          SUM(double1) AS totalElapsed,
          SUM(double2) AS totalInterval
          FROM RunningConnectionSeconds
          WHERE (timestamp >= toDateTime(1685577600)) AND (timestamp < toDateTime(1688169600))
          GROUP BY teamID, appID
          FORMAT JSON`,
      );
    });

    test('schema', () => {
      expect(
        selectAggregation.schema.parse({
          appID: 'foo-app',
          teamID: 'bar-team',
          totalInterval: 60,
          totalElapsed: 45.23,
        }),
      ).toEqual({
        appID: 'foo-app',
        teamID: 'bar-team',
        totalInterval: 60,
        totalElapsed: 45.23,
      });
    });
  });

  describe('select subquery', () => {
    const selectAggregation = runningConnectionSeconds
      .select({
        schema: v.object({
          teamID: v.string(),
          appID: v.string(),
          totalElapsed: v.number(),
          totalInterval: v.number(),
        }),
        expr: {
          teamID: 'blob1',
          appID: 'blob2',
          totalElapsed: 'SUM(double1)',
          totalInterval: 'SUM(double2)',
        },
      })
      .where('totalElapsed', '>', 1000);

    const superSelect = selectAggregation
      .select({
        schema: v.object({
          teamID: v.string(),
          appID: v.string(),
          avgConnections: v.number(),
        }),
        expr: {
          teamID: '',
          appID: '',
          avgConnections: 'totalElapsed / totalInterval',
        },
      })
      .where('avgConnections', '>', 3);

    test('toString', () => {
      expect(superSelect.toString()).toBe(
        `SELECT
          teamID,
          appID,
          totalElapsed / totalInterval AS avgConnections
          FROM (
          SELECT
          blob1 AS teamID,
          blob2 AS appID,
          SUM(double1) AS totalElapsed,
          SUM(double2) AS totalInterval
          FROM RunningConnectionSeconds
          WHERE totalElapsed > 1000
          )
          WHERE avgConnections > 3
          FORMAT JSON`,
      );
    });
  });
});
