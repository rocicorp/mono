import {resolver} from '@rocicorp/resolver';
import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Actives} from '../../server/anonymous-otel-start.ts';
import {test, type PgTest} from '../../test/db.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {cvrSchema} from '../../types/shards.ts';
import {ActiveUsersGauge} from './active-users-gauge.ts';
import {setupCVRTables, type InstancesRow} from './schema/cvr.ts';
import {ttlClockFromNumber} from './ttl-clock.ts';

const APP_ID = 'zapp';
const SHARD_NUM = 3;
const SHARD = {appID: APP_ID, shardNum: SHARD_NUM};

const DAY_MS = 1000 * 60 * 60 * 24;

describe('view-syncer/active-users-gauge', () => {
  type DBState = {
    instances: (Partial<InstancesRow> &
      Pick<
        InstancesRow,
        'clientGroupID' | 'version' | 'clientSchema' | 'ttlClock'
      > & {deleted: boolean})[];
  };

  function addDBState(db: PostgresDB, state: Partial<DBState>): Promise<void> {
    return db.begin(async tx => {
      for (const [table, rows] of Object.entries(state)) {
        for (const row of rows) {
          await tx`INSERT INTO ${tx(`${cvrSchema(SHARD)}.` + table)} ${tx(
            row,
          )}`;
        }
      }
    });
  }

  const lc = createSilentLogContext();
  let cvrDb: PostgresDB;
  let gauge: ActiveUsersGauge;
  let activesGetterPromise: Promise<() => Actives>;

  beforeEach<PgTest>(async ({testDBs}) => {
    cvrDb = await testDBs.create('active_users_gauge_test_db');
    await cvrDb.begin(tx => setupCVRTables(lc, tx, SHARD));

    const actives = resolver<() => Actives>();
    activesGetterPromise = actives.promise;
    gauge = new ActiveUsersGauge(lc, cvrDb, SHARD, {}, actives.resolve);

    const now = Date.now();
    for (const [clientGroupID, lastActive, profileID, deleted] of [
      ['active', now, 'p92d90asd9fs', false],
      ['active-same-profile', now, 'p92d90asd9fs', false],
      ['active-legacy', now, 'cgactive-legacy', false],
      ['active3day', now - 3 * DAY_MS, 'p2382e9083230', true],
      ['active3day-same-profile', now - 3 * DAY_MS, 'p92d90asd9fs', true],
      ['active3day-legacy', now - 3 * DAY_MS, 'cgactive3day-legacy', true],
      ['active8day', now - 8 * DAY_MS, 'p7e9efh9fhdfjd', false],
      ['active8day-legacy', now - 8 * DAY_MS, 'cgactive8day-legacy', false],
      ['active31day', now - 31 * DAY_MS, 'p8d2fhfhd0fsihdf', false],
      ['active31day-legacy', now - 31 * DAY_MS, 'cgactive31day-legacy', false],
    ] as [string, number, string, boolean][]) {
      await addDBState(cvrDb, {
        instances: [
          {
            clientGroupID,
            version: '1aa',
            lastActive,
            ttlClock: ttlClockFromNumber(Date.UTC(2024, 3, 23)),
            clientSchema: null,
            deleted,
            profileID,
          },
        ],
      });
    }

    void gauge.run();

    return async () => {
      await testDBs.drop(cvrDb);
      await gauge.stop();
    };
  });

  test('computes active users', async () => {
    const activesGetter = await activesGetterPromise;
    const actives = activesGetter();
    expect(actives).toEqual({
      active_users_last_day: 3, // No de-duping
      users_1da: 1, // deduped p92d90asd9fs
      users_1da_legacy: 2, // deduped p92d90asd9fs + cgactive-legacy
      users_7da: 2, // adds p2382e9083230, but dedupes p92d90asd9fs
      users_7da_legacy: 4, // adds p2382e9083230 + cgactive3day-legacy
      users_30da: 3, // adds p7e9efh9fhdfjd
      users_30da_legacy: 6, // adds p7e9efh9fhdfjd + cgactive8day-legacy
    });
  });
});
