import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {
  dropReplicationSlot,
  expectTables,
  getConnectionURI,
  initDB,
  testDBs,
} from 'zero-cache/src/test/db.js';
import {
  DbFile,
  expectTables as expectLiteTables,
  initDB as initLiteDB,
} from 'zero-cache/src/test/lite.js';
import {PostgresDB} from 'zero-cache/src/types/pg.js';
import {replicationSlot} from './initial-sync.js';
import {initSyncSchema} from './sync-schema.js';

const REPLICA_ID = 'sync_schema_test_id';

// Update as necessary.
const CURRENT_SCHEMA_VERSIONS = {
  version: 2,
  maxVersion: 2,
  minSafeRollbackVersion: 1,
  lock: 1, // Internal column, always 1
};

describe('replicator/schema/sync-schema', () => {
  type Case = {
    name: string;

    upstreamSetup?: string;
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
        ['zero.clients']: [],
      },
      replicaPostState: {
        ['zero.clients']: [],
        ['_zero.SchemaVersions']: [CURRENT_SCHEMA_VERSIONS],
      },
    },
    {
      name: 'sync partially published upstream data',
      upstreamSetup: `
        CREATE TABLE unpublished(issue_id INTEGER, org_id INTEGER, PRIMARY KEY (org_id, issue_id));
        CREATE TABLE users("userID" INTEGER, password TEXT, handle TEXT, PRIMARY KEY ("userID"));
        CREATE PUBLICATION zero_custom FOR TABLE users ("userID", handle);
    `,
      upstreamPreState: {
        users: [
          {userID: 123, password: 'not-replicated', handle: '@zoot'},
          {userID: 456, password: 'super-secret', handle: '@bonk'},
        ],
      },
      upstreamPostState: {
        ['zero.clients']: [],
      },
      replicaPostState: {
        ['_zero.SchemaVersions']: [CURRENT_SCHEMA_VERSIONS],
        ['zero.clients']: [],
        users: [
          {userID: 123, handle: '@zoot', ['_0_version']: '00'},
          {userID: 456, handle: '@bonk', ['_0_version']: '00'},
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
    await dropReplicationSlot(upstream, replicationSlot(REPLICA_ID));
    await testDBs.drop(upstream);
    await replicaFile.unlink();
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
          REPLICA_ID,
          replicaFile.path,
          getConnectionURI(upstream),
        );

        await expectTables(upstream, c.upstreamPostState);
        expectLiteTables(replica, c.replicaPostState);

        expectLiteTables(replica, {
          ['_zero.ChangeLog']: [],
        });

        // Slot should still exist.
        const slots =
          await upstream`SELECT slot_name FROM pg_replication_slots WHERE slot_name = ${replicationSlot(
            REPLICA_ID,
          )}`.values();
        expect(slots[0]).toEqual([replicationSlot(REPLICA_ID)]);
      },
      10000,
    );
  }
});
