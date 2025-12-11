import {resolver} from '@rocicorp/resolver';
import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {DEFAULT_TTL_MS} from '../../../../zql/src/query/ttl.ts';
import {test, type PgTest} from '../../test/db.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {cvrSchema} from '../../types/shards.ts';
import {CVRPurger} from './cvr-purger.ts';
import {
  setupCVRTables,
  type ClientsRow,
  type DesiresRow,
  type InstancesRow,
  type QueriesRow,
  type RowsRow,
  type RowsVersionRow,
} from './schema/cvr.ts';
import {ttlClockFromNumber} from './ttl-clock.ts';

const APP_ID = 'zapp';
const SHARD_NUM = 3;
const SHARD = {appID: APP_ID, shardNum: SHARD_NUM};

describe('view-syncer/cvr-purger', () => {
  type DBState = {
    instances: (Partial<InstancesRow> &
      Pick<
        InstancesRow,
        'clientGroupID' | 'version' | 'clientSchema' | 'ttlClock'
      > & {deleted: boolean})[];
    clients: ClientsRow[];
    queries: QueriesRow[];
    desires: DesiresRow[];
    rows: RowsRow[];
    rowsVersion?: RowsVersionRow[];
  };

  function addDBState(db: PostgresDB, state: Partial<DBState>): Promise<void> {
    return db.begin(async tx => {
      const {instances, rowsVersion} = state;
      if (instances && !rowsVersion) {
        state = {
          rowsVersion: instances.map(({clientGroupID, version}) => ({
            clientGroupID,
            version,
          })),
          ...state,
        };
      }

      for (const [table, rows] of Object.entries(state)) {
        for (const row of rows) {
          await tx`INSERT INTO ${tx(`${cvrSchema(SHARD)}.` + table)} ${tx(
            row,
          )}`;
        }
      }
    });
  }

  async function getAllState(db: PostgresDB): Promise<DBState> {
    const [instances, clients, queries, desires, rows] = await Promise.all([
      db`SELECT * FROM ${db('zapp_3/cvr.instances')}`,
      db`SELECT * FROM ${db('zapp_3/cvr.clients')}`,
      db`SELECT * FROM ${db('zapp_3/cvr.queries')}`,
      db`SELECT * FROM ${db('zapp_3/cvr.desires')}`,
      db`SELECT * FROM ${db('zapp_3/cvr.rows')}`,
    ]);

    desires.forEach(row => {
      // expiresAt is deprecated. It is still in the db but we do not
      // want it in the js objects.
      delete row.expiresAt;
    });
    return {
      instances,
      clients,
      queries,
      desires,
      rows,
    } as unknown as DBState;
  }

  const INACTIVITY_THRESHOLD_MS = 1000 * 60 * 60 * 24;
  const TOMBSTONE_THRESHOLD_MS = 1000 * 60 * 60 * 24 * 7;

  const lc = createSilentLogContext();
  let cvrDb: PostgresDB;
  let purger: CVRPurger;

  beforeEach<PgTest>(async ({testDBs}) => {
    cvrDb = await testDBs.create('cvr_purger_test_db');
    await cvrDb.begin(tx => setupCVRTables(lc, tx, SHARD));

    purger = new CVRPurger(
      lc,
      cvrDb,
      SHARD,
      {
        inactivityThresholdMs: INACTIVITY_THRESHOLD_MS,
        initialBatchSize: 25,
        initialIntervalMs: 60000,
      },
      TOMBSTONE_THRESHOLD_MS,
    );

    for (const [clientGroupID, lastActive, deleted] of [
      ['new-1', Date.now(), false],
      ['old-1', Date.now() - INACTIVITY_THRESHOLD_MS - 1000, false],
      ['new-2', Date.now() - INACTIVITY_THRESHOLD_MS / 2, false],
      ['old-2', Date.now() - TOMBSTONE_THRESHOLD_MS + 30000, false],
      ['new-3', Date.now() - INACTIVITY_THRESHOLD_MS + 60000, false],
      ['old-3', Date.now() - TOMBSTONE_THRESHOLD_MS + 60000, false],
      ['new-tombstone', Date.now() - TOMBSTONE_THRESHOLD_MS + 10000, true],
      ['old-tombstone', Date.now() - TOMBSTONE_THRESHOLD_MS - 60000, true],
    ] as [string, number, boolean][]) {
      if (deleted) {
        await addDBState(cvrDb, {
          instances: [
            {
              clientGroupID,
              version: '1aa',
              replicaVersion: null,
              lastActive,
              ttlClock: ttlClockFromNumber(Date.UTC(2024, 3, 23)),
              clientSchema: null,
              deleted: true,
            },
          ],
        });
      } else {
        await addDBState(cvrDb, {
          instances: [
            {
              clientGroupID,
              version: '1aa',
              replicaVersion: null,
              lastActive,
              ttlClock: ttlClockFromNumber(Date.UTC(2024, 3, 23)),
              clientSchema: null,
              deleted: false,
            },
          ],
          clients: [
            {
              clientGroupID,
              clientID: 'fooClient',
            },
          ],
          queries: [
            {
              clientGroupID,
              queryArgs: null,
              queryName: null,
              queryHash: 'oneHash',
              clientAST: {table: 'issues'},
              transformationHash: null,
              transformationVersion: null,
              patchVersion: null,
              internal: null,
              deleted: null,
            },
          ],
          desires: [
            {
              clientGroupID,
              clientID: 'fooClient',
              queryHash: 'oneHash',
              patchVersion: '1a9:01',
              deleted: null,
              inactivatedAt: null,
              ttl: DEFAULT_TTL_MS,
            },
          ],
          rows: [
            {
              clientGroupID,
              rowKey: {a: 'b'},
              rowVersion: '03',
              refCounts: {oneHash: 1},
              patchVersion: '1a0',
              schema: 'public',
              table: 'issues',
            },
          ],
        });
      }
    }

    return () => testDBs.drop(cvrDb);
  });

  test('complete purge', async () => {
    expect(await purger.purgeInactiveCVRs(1000)).toEqual({
      purged: 3,
      remaining: 0,
    });
    expect(await getAllState(cvrDb)).toMatchObject({
      clients: [
        {clientGroupID: 'new-1'},
        {clientGroupID: 'new-2'},
        {clientGroupID: 'new-3'},
      ],
      desires: [
        {clientGroupID: 'new-1'},
        {clientGroupID: 'new-2'},
        {clientGroupID: 'new-3'},
      ],
      instances: [
        {clientGroupID: 'new-1', deleted: false},
        {clientGroupID: 'new-2', deleted: false},
        {clientGroupID: 'new-3', deleted: false},
        {clientGroupID: 'new-tombstone', deleted: true},
        {clientGroupID: 'old-1', deleted: true},
        {clientGroupID: 'old-2', deleted: true},
        {clientGroupID: 'old-3', deleted: true},
      ],
      queries: [
        {clientGroupID: 'new-1'},
        {clientGroupID: 'new-2'},
        {clientGroupID: 'new-3'},
      ],
      rows: [
        {clientGroupID: 'new-1'},
        {clientGroupID: 'new-2'},
        {clientGroupID: 'new-3'},
      ],
    });
  });

  test('incremental purge', async () => {
    expect(await purger.purgeInactiveCVRs(2)).toEqual({
      purged: 2,
      remaining: 1,
    });
    expect(await getAllState(cvrDb)).toMatchObject({
      clients: [
        {clientGroupID: 'new-1'},
        {clientGroupID: 'old-1'},
        {clientGroupID: 'new-2'},
        {clientGroupID: 'new-3'},
      ],
      desires: [
        {clientGroupID: 'new-1'},
        {clientGroupID: 'old-1'},
        {clientGroupID: 'new-2'},
        {clientGroupID: 'new-3'},
      ],
      instances: [
        {clientGroupID: 'new-1', deleted: false},
        {clientGroupID: 'old-1', deleted: false},
        {clientGroupID: 'new-2', deleted: false},
        {clientGroupID: 'new-3', deleted: false},
        {clientGroupID: 'new-tombstone', deleted: true},
        {clientGroupID: 'old-2', deleted: true},
        {clientGroupID: 'old-3', deleted: true},
      ],
      queries: [
        {clientGroupID: 'new-1'},
        {clientGroupID: 'old-1'},
        {clientGroupID: 'new-2'},
        {clientGroupID: 'new-3'},
      ],
      rows: [
        {clientGroupID: 'new-1'},
        {clientGroupID: 'old-1'},
        {clientGroupID: 'new-2'},
        {clientGroupID: 'new-3'},
      ],
    });

    expect(await purger.purgeInactiveCVRs(2)).toEqual({
      purged: 1,
      remaining: 0,
    });
    expect(await getAllState(cvrDb)).toMatchObject({
      clients: [
        {clientGroupID: 'new-1'},
        {clientGroupID: 'new-2'},
        {clientGroupID: 'new-3'},
      ],
      desires: [
        {clientGroupID: 'new-1'},
        {clientGroupID: 'new-2'},
        {clientGroupID: 'new-3'},
      ],
      instances: [
        {clientGroupID: 'new-1', deleted: false},
        {clientGroupID: 'new-2', deleted: false},
        {clientGroupID: 'new-3', deleted: false},
        {clientGroupID: 'new-tombstone', deleted: true},
        {clientGroupID: 'old-2', deleted: true},
        {clientGroupID: 'old-3', deleted: true},
        {clientGroupID: 'old-1', deleted: true},
      ],
      queries: [
        {clientGroupID: 'new-1'},
        {clientGroupID: 'new-2'},
        {clientGroupID: 'new-3'},
      ],
      rows: [
        {clientGroupID: 'new-1'},
        {clientGroupID: 'new-2'},
        {clientGroupID: 'new-3'},
      ],
    });
  });

  test('purge succeeds if cvrs are locked', async () => {
    const {promise: rowLocked, resolve: signalRowLocked} = resolver();
    const {promise: canReleaseRowLock, resolve: releaseRowLock} = resolver();

    // Simulate a concurrent view-syncer update of cvr "old-3", which otherwise
    // looks eligible for purging.
    void cvrDb.begin(async sql => {
      await sql`
        SELECT * FROM ${sql(cvrSchema(SHARD))}.instances
          WHERE "clientGroupID" = 'old-3'
          FOR UPDATE`;

      signalRowLocked();

      // Hold the FOR UPDATE row lock until the test calls releaseRowLock().
      await canReleaseRowLock;
    });

    await rowLocked;
    try {
      expect(await purger.purgeInactiveCVRs(1000)).toEqual({
        purged: 2,
        remaining: 1,
      });
      expect(await getAllState(cvrDb)).toMatchObject({
        clients: [
          {clientGroupID: 'new-1'},
          {clientGroupID: 'new-2'},
          {clientGroupID: 'new-3'},
          {clientGroupID: 'old-3'},
        ],
        desires: [
          {clientGroupID: 'new-1'},
          {clientGroupID: 'new-2'},
          {clientGroupID: 'new-3'},
          {clientGroupID: 'old-3'},
        ],
        instances: [
          {clientGroupID: 'new-1', deleted: false},
          {clientGroupID: 'new-2', deleted: false},
          {clientGroupID: 'new-3', deleted: false},
          {clientGroupID: 'old-3', deleted: false},
          {clientGroupID: 'new-tombstone', deleted: true},
          {clientGroupID: 'old-1', deleted: true},
          {clientGroupID: 'old-2', deleted: true},
        ],
        queries: [
          {clientGroupID: 'new-1'},
          {clientGroupID: 'new-2'},
          {clientGroupID: 'new-3'},
          {clientGroupID: 'old-3'},
        ],
        rows: [
          {clientGroupID: 'new-1'},
          {clientGroupID: 'new-2'},
          {clientGroupID: 'new-3'},
          {clientGroupID: 'old-3'},
        ],
      });
    } finally {
      // Let the locking transaction complete.
      releaseRowLock();
    }
  });
});
