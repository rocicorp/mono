import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  test,
} from '@jest/globals';
import type postgres from 'postgres';
import {TestDBs} from '../../../test/db.js';
import {createSilentLogContext} from '../../../test/logger.js';
import {initSyncSchema} from './sync-schema.js';

describe('sync schema', () => {
  type Case = {
    name: string;
    preState?: Record<string, object[]>;
    postState: Record<string, object[]>;
  };

  const cases: Case[] = [
    {
      name: 'sync schema meta',
      postState: {
        ['zero.schema_meta']: [
          {
            // Update these as necessary.
            version: 1,
            maxVersion: 1,
            minSafeRollbackVersion: 1,
          },
        ],
      },
    },
  ];

  const testDBs = new TestDBs();
  let db: postgres.Sql;
  beforeAll(async () => {
    db = await testDBs.createAndConnect('sync_schema_test');
  });

  afterEach(async () => {
    await db.begin(tx => [tx`DROP TABLE IF EXISTS zero.schema_meta`]);
  });

  afterAll(async () => {
    await testDBs.end();
  });

  for (const c of cases) {
    test(c.name, async () => {
      for (const [table, rows] of Object.entries(c.preState ?? {})) {
        await db.begin(tx =>
          rows.map(row => tx`INSERT INTO ${tx(table)} ${tx(row)}`),
        );
      }

      await initSyncSchema(createSilentLogContext(), db);

      for (const [table, expected] of Object.entries(c.postState)) {
        if (expected.length === 0) {
          expect(await db`SELECT COUNT(*) FROM ${db(table)}`).toEqual([
            {count: 0},
          ]);
        } else {
          const columns = Object.keys(expected[0]);
          const actual = await db`SELECT ${db(columns)} FROM ${db(table)}`;
          expect(actual).toEqual(expected);
        }
      }
    });
  }
});
