import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {
  expectTables,
  getConnectionURI,
  initDB,
  type PgTest,
  test,
} from '../../../test/db.ts';
import {
  DbFile,
  expectTables as expectLiteTables,
  expectMatchingObjectsInTables,
  initDB as initLiteDB,
} from '../../../test/lite.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {replicationSlotExpression} from './schema/shard.ts';
import {initSyncSchema} from './sync-schema.ts';

const APP_ID = 'zeroz';
const SHARD_NUM = 9;

// Update as necessary.
const CURRENT_SCHEMA_VERSIONS = {
  dataVersion: 8,
  schemaVersion: 8,
  minSafeVersion: 1,
  lock: 1, // Internal column, always 1
};
const WATERMARK_REGEX = /[0-9a-z]{4,}/;

describe('change-streamer/pg/sync-schema', () => {
  type Case = {
    name: string;

    upstreamSetup?: string;
    requestedPublications?: string[];
    upstreamPreState?: Record<string, object[]>;
    upstreamPostState?: Record<string, object[]>;

    replicaSetup?: string;
    replicaPreState?: Record<string, object[]>;
    replicaPostState: Record<string, object[]>;
  };

  const cases: Case[] = [
    {
      name: 'initial tables',
      upstreamPostState: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
      },
      replicaPostState: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
        ['_zero.versionHistory']: [CURRENT_SCHEMA_VERSIONS],
      },
    },
    {
      name: 'sync partially published upstream data',
      upstreamSetup: `
        CREATE TABLE unpublished(issue_id INTEGER, org_id INTEGER, PRIMARY KEY (org_id, issue_id));
        CREATE TABLE users("userID" INTEGER, password TEXT, handle TEXT, PRIMARY KEY ("userID"));
        CREATE PUBLICATION zero_custom FOR TABLE users ("userID", handle);
    `,
      requestedPublications: ['zero_custom'],
      upstreamPreState: {
        users: [
          {userID: 123, password: 'not-replicated', handle: '@zoot'},
          {userID: 456, password: 'super-secret', handle: '@bonk'},
        ],
      },
      upstreamPostState: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
      },
      replicaPostState: {
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
        ['_zero.versionHistory']: [CURRENT_SCHEMA_VERSIONS],
        users: [
          {userID: 123, handle: '@zoot', ['_0_version']: WATERMARK_REGEX},
          {userID: 456, handle: '@bonk', ['_0_version']: WATERMARK_REGEX},
        ],
      },
    },
  ];

  let upstream: PostgresDB;
  let replicaFile: DbFile;

  beforeEach<PgTest>(async ({testDBs}) => {
    upstream = await testDBs.create('sync_schema_migration_upstream');
    replicaFile = new DbFile('sync_schema_migration_replica');

    return async () => {
      await testDBs.drop(upstream);
      replicaFile.delete();
    };
  });

  const lc = createSilentLogContext();

  for (const c of cases) {
    test(
      c.name,
      async () => {
        const replica = replicaFile.connect(lc);
        await initDB(upstream, c.upstreamSetup, c.upstreamPreState);
        initLiteDB(replica, c.replicaSetup, c.replicaPreState);

        const shard = {
          appID: APP_ID,
          shardNum: SHARD_NUM,
          publications: c.requestedPublications ?? [],
        };

        await initSyncSchema(
          createSilentLogContext(),
          'test',
          shard,
          replicaFile.path,
          getConnectionURI(upstream),
          {tableCopyWorkers: 5},
        );

        await expectTables(upstream, c.upstreamPostState);
        expectMatchingObjectsInTables(replica, c.replicaPostState);

        expectLiteTables(replica, {
          ['_zero.changeLog2']: [],
        });

        // Slot should still exist.
        const slots = await upstream`SELECT slot_name FROM pg_replication_slots 
          WHERE slot_name LIKE ${replicationSlotExpression(shard)}`.values();
        expect(slots).toHaveLength(1);
      },
      10000,
    );
  }
});
