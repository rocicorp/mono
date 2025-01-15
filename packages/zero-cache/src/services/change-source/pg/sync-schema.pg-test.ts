import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.js';
import {
  expectTables,
  getConnectionURI,
  initDB,
  testDBs,
} from '../../../test/db.js';
import {
  DbFile,
  expectTables as expectLiteTables,
  expectMatchingObjectsInTables,
  initDB as initLiteDB,
} from '../../../test/lite.js';
import type {PostgresDB} from '../../../types/pg.js';
import {replicationSlot} from './initial-sync.js';
import {initSyncSchema} from './sync-schema.js';

const SHARD_ID = 'sync_schema_test_id';

// Update as necessary.
const CURRENT_SCHEMA_VERSIONS = {
  dataVersion: 2,
  schemaVersion: 2,
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
        [`zero_${SHARD_ID}.clients`]: [],
        ['zero.schemaVersions']: [
          {lock: true, minSupportedVersion: 1, maxSupportedVersion: 1},
        ],
      },
      replicaPostState: {
        [`zero_${SHARD_ID}.clients`]: [],
        ['zero.schemaVersions']: [
          {
            lock: 1,
            minSupportedVersion: 1,
            maxSupportedVersion: 1,
            ['_0_version']: WATERMARK_REGEX,
          },
        ],
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
        [`zero_${SHARD_ID}.clients`]: [],
        ['zero.schemaVersions']: [
          {lock: true, minSupportedVersion: 1, maxSupportedVersion: 1},
        ],
      },
      replicaPostState: {
        [`zero_${SHARD_ID}.clients`]: [],
        ['zero.schemaVersions']: [
          {
            lock: 1,
            minSupportedVersion: 1,
            maxSupportedVersion: 1,
            ['_0_version']: WATERMARK_REGEX,
          },
        ],
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

  beforeEach(async () => {
    upstream = await testDBs.create('sync_schema_migration_upstream');
    replicaFile = new DbFile('sync_schema_migration_replica');
  });

  afterEach(async () => {
    await testDBs.drop(upstream);
    replicaFile.delete();
  }, 10000);
  const lc = createSilentLogContext();

  for (const c of cases) {
    test(
      c.name,
      async () => {
        const replica = replicaFile.connect(lc);
        await initDB(upstream, c.upstreamSetup, c.upstreamPreState);
        initLiteDB(replica, c.replicaSetup, c.replicaPreState);

        await initSyncSchema(
          createSilentLogContext(),
          'test',
          {id: SHARD_ID, publications: c.requestedPublications ?? []},
          replicaFile.path,
          getConnectionURI(upstream),
          {tableCopyWorkers: 5, rowBatchSize: 10000},
        );

        await expectTables(upstream, c.upstreamPostState);
        expectMatchingObjectsInTables(replica, c.replicaPostState);

        expectLiteTables(replica, {
          ['_zero.changeLog']: [],
        });

        // Slot should still exist.
        const slots =
          await upstream`SELECT slot_name FROM pg_replication_slots WHERE slot_name = ${replicationSlot(
            SHARD_ID,
          )}`.values();
        expect(slots[0]).toEqual([replicationSlot(SHARD_ID)]);
      },
      10000,
    );
  }
});
