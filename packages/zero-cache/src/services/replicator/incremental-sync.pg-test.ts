import {afterEach, beforeEach, describe, expect, test} from '@jest/globals';
import type postgres from 'postgres';
import {sleep} from 'shared/src/sleep.js';
import {
  dropReplicationSlot,
  expectTables,
  initDB,
  testDBs,
} from '../../test/db.js';
import {createSilentLogContext} from '../../test/logger.js';
import {versionFromLexi, type LexiVersion} from '../../types/lexi-version.js';
import {IncrementalSyncer, setupReplicationTables} from './incremental-sync.js';
import {replicationSlot, setupUpstream} from './initial-sync.js';
import {getPublicationInfo} from './tables/published.js';
import type {TableSpec} from './tables/specs.js';

const REPLICA_ID = 'incremental_sync_test_id';

describe('replicator/incremental-sync', () => {
  let upstream: postgres.Sql;
  let replica: postgres.Sql;
  let syncer: IncrementalSyncer;

  beforeEach(async () => {
    upstream = await testDBs.create('incremental_sync_test_upstream');
    replica = await testDBs.create('incremental_sync_test_replica');
    syncer = new IncrementalSyncer(
      'postgres:///incremental_sync_test_upstream',
      REPLICA_ID,
      replica,
    );
  });

  afterEach(async () => {
    await syncer.stop(createSilentLogContext());
    await dropReplicationSlot(upstream, replicationSlot(REPLICA_ID));
    await testDBs.drop(replica, upstream);
  });

  type Case = {
    name: string;
    setupUpstream?: string;
    setupReplica?: string;
    writeUpstream?: string[];
    expectedTransactions?: number;
    specs: Record<string, TableSpec>;
    data: Record<string, object[]>;
  };

  const cases: Case[] = [
    {
      name: 'create tables',
      specs: {},
      data: {
        ['_zero.TxLog']: [],
        ['_zero.ChangeLog']: [],
        ['_zero.InvalidationRegistry']: [],
        ['_zero.InvalidationIndex']: [],
      },
    },
    {
      name: 'alter version columns',
      setupReplica: `
      CREATE TABLE issues(
        "issueID" INTEGER PRIMARY KEY,
        _0_version VARCHAR(38) DEFAULT '00'
      );
      CREATE PUBLICATION zero_data FOR TABLES IN SCHEMA public;

      CREATE SCHEMA zero;
      CREATE TABLE zero.clients(
        "clientID" TEXT PRIMARY KEY,
        "lastMutationID" TEXT,
        _0_version VARCHAR(38) DEFAULT '00'
      );
      CREATE PUBLICATION zero_meta FOR TABLES IN SCHEMA zero;
      `,
      specs: {
        ['public.issues']: {
          schema: 'public',
          name: 'issues',
          columns: {
            issueID: {
              dataType: 'int4',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: true,
            },
            ['_0_version']: {
              dataType: 'varchar',
              characterMaximumLength: 38,
              columnDefault: null,
              notNull: true,
            },
          },
          primaryKey: ['issueID'],
        },
        ['zero.clients']: {
          schema: 'zero',
          name: 'clients',
          columns: {
            clientID: {
              dataType: 'text',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: true,
            },
            lastMutationID: {
              dataType: 'text',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: false,
            },
            ['_0_version']: {
              dataType: 'varchar',
              characterMaximumLength: 38,
              columnDefault: null, // Default should be cleared.
              notNull: true,
            },
          },
          primaryKey: ['clientID'],
        },
      },
      data: {
        ['_zero.TxLog']: [],
        ['_zero.ChangeLog']: [],
        ['_zero.InvalidationRegistry']: [],
        ['_zero.InvalidationIndex']: [],
      },
    },
    {
      name: 'insert rows',
      setupUpstream: `
      CREATE TABLE issues(
        "issueID" INTEGER PRIMARY KEY,
        description TEXT
      );
      CREATE PUBLICATION zero_all FOR TABLES IN SCHEMA public;
      `,
      setupReplica: `
      CREATE TABLE issues(
        "issueID" INTEGER PRIMARY KEY,
        description TEXT,
        _0_version VARCHAR(38) DEFAULT '00'
      );
      CREATE PUBLICATION zero_all FOR TABLES IN SCHEMA public;
      `,
      specs: {
        ['public.issues']: {
          schema: 'public',
          name: 'issues',
          columns: {
            issueID: {
              dataType: 'int4',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: true,
            },
            description: {
              dataType: 'text',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: false,
            },
            ['_0_version']: {
              dataType: 'varchar',
              characterMaximumLength: 38,
              columnDefault: null,
              notNull: true,
            },
          },
          primaryKey: ['issueID'],
        },
      },
      writeUpstream: [
        `
      INSERT INTO issues ("issueID") VALUES (123);
      INSERT INTO issues ("issueID") VALUES (456);
      `,
        `
      INSERT INTO issues ("issueID") VALUES (789);
      `,
      ],
      expectedTransactions: 2,
      data: {
        ['public.issues']: [
          {issueID: 123, description: null, ['_0_version']: '01'},
          {issueID: 456, description: null, ['_0_version']: '01'},
          {issueID: 789, description: null, ['_0_version']: '02'},
        ],
      },
    },
    {
      name: 'update rows with multiple key columns and key value updates',
      setupUpstream: `
      CREATE TABLE issues(
        "issueID" INTEGER,
        "orgID" INTEGER,
        description TEXT,
        PRIMARY KEY("orgID", "issueID")
      );
      CREATE PUBLICATION zero_all FOR TABLES IN SCHEMA public;
      `,
      setupReplica: `
      CREATE TABLE issues(
        "issueID" INTEGER,
        "orgID" INTEGER,
        description TEXT,
        _0_version VARCHAR(38) DEFAULT '00',
        PRIMARY KEY("orgID", "issueID")
      );
      CREATE PUBLICATION zero_all FOR TABLES IN SCHEMA public;
      `,
      specs: {
        ['public.issues']: {
          schema: 'public',
          name: 'issues',
          columns: {
            issueID: {
              dataType: 'int4',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: true,
            },
            orgID: {
              dataType: 'int4',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: true,
            },
            description: {
              dataType: 'text',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: false,
            },
            ['_0_version']: {
              dataType: 'varchar',
              characterMaximumLength: 38,
              columnDefault: null,
              notNull: true,
            },
          },
          primaryKey: ['orgID', 'issueID'],
        },
      },
      writeUpstream: [
        `
      INSERT INTO issues ("orgID", "issueID") VALUES (1, 123);
      INSERT INTO issues ("orgID", "issueID") VALUES (1, 456);
      INSERT INTO issues ("orgID", "issueID") VALUES (2, 789);
      `,
        `
      UPDATE issues SET (description) = ROW('foo') WHERE "issueID" = 456;
      UPDATE issues SET ("orgID", description) = ROW(2, 'bar') WHERE "issueID" = 123;
      `,
      ],
      expectedTransactions: 2,
      data: {
        ['public.issues']: [
          {orgID: 2, issueID: 123, description: 'bar', ['_0_version']: '02'},
          {orgID: 1, issueID: 456, description: 'foo', ['_0_version']: '02'},
          {orgID: 2, issueID: 789, description: null, ['_0_version']: '01'},
        ],
      },
    },
    {
      name: 'delete rows',
      setupUpstream: `
      CREATE TABLE issues(
        "issueID" INTEGER,
        "orgID" INTEGER,
        description TEXT,
        PRIMARY KEY("orgID", "issueID")
      );
      CREATE PUBLICATION zero_all FOR TABLES IN SCHEMA public;
      `,
      setupReplica: `
      CREATE TABLE issues(
        "issueID" INTEGER,
        "orgID" INTEGER,
        description TEXT,
        _0_version VARCHAR(38) DEFAULT '00',
        PRIMARY KEY("orgID", "issueID")
      );
      CREATE PUBLICATION zero_all FOR TABLES IN SCHEMA public;
      `,
      specs: {
        ['public.issues']: {
          schema: 'public',
          name: 'issues',
          columns: {
            issueID: {
              dataType: 'int4',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: true,
            },
            orgID: {
              dataType: 'int4',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: true,
            },
            description: {
              dataType: 'text',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: false,
            },
            ['_0_version']: {
              dataType: 'varchar',
              characterMaximumLength: 38,
              columnDefault: null,
              notNull: true,
            },
          },
          primaryKey: ['orgID', 'issueID'],
        },
      },
      writeUpstream: [
        `
      INSERT INTO issues ("orgID", "issueID") VALUES (1, 123);
      INSERT INTO issues ("orgID", "issueID") VALUES (1, 456);
      INSERT INTO issues ("orgID", "issueID") VALUES (2, 789);
      INSERT INTO issues ("orgID", "issueID") VALUES (2, 987);
      `,
        `
      DELETE FROM issues WHERE "orgID" = 1;
      DELETE FROM issues WHERE "issueID" = 987;
      `,
      ],
      expectedTransactions: 2,
      data: {
        ['public.issues']: [
          {orgID: 2, issueID: 789, description: null, ['_0_version']: '01'},
        ],
      },
    },
    {
      name: 'truncate tables',
      setupUpstream: `
      CREATE TABLE foo(id INTEGER PRIMARY KEY);
      CREATE TABLE bar(id INTEGER PRIMARY KEY);
      CREATE TABLE baz(id INTEGER PRIMARY KEY);
      CREATE PUBLICATION zero_all FOR TABLES IN SCHEMA public;
      `,
      setupReplica: `
      CREATE TABLE foo(
        id INTEGER PRIMARY KEY,
        _0_version VARCHAR(38) DEFAULT '00'
      );
      CREATE TABLE bar(
        id INTEGER PRIMARY KEY,
        _0_version VARCHAR(38) DEFAULT '00'
      );
      CREATE TABLE baz(
        id INTEGER PRIMARY KEY,
        _0_version VARCHAR(38) DEFAULT '00'
      );
      CREATE PUBLICATION zero_all FOR TABLES IN SCHEMA public;
      `,
      specs: {
        ['public.foo']: {
          schema: 'public',
          name: 'foo',
          columns: {
            id: {
              dataType: 'int4',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: true,
            },
            ['_0_version']: {
              dataType: 'varchar',
              characterMaximumLength: 38,
              columnDefault: null,
              notNull: true,
            },
          },
          primaryKey: ['id'],
        },
        ['public.bar']: {
          schema: 'public',
          name: 'bar',
          columns: {
            id: {
              dataType: 'int4',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: true,
            },
            ['_0_version']: {
              dataType: 'varchar',
              characterMaximumLength: 38,
              columnDefault: null,
              notNull: true,
            },
          },
          primaryKey: ['id'],
        },
        ['public.baz']: {
          schema: 'public',
          name: 'baz',
          columns: {
            id: {
              dataType: 'int4',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: true,
            },
            ['_0_version']: {
              dataType: 'varchar',
              characterMaximumLength: 38,
              columnDefault: null,
              notNull: true,
            },
          },
          primaryKey: ['id'],
        },
      },
      writeUpstream: [
        `
      INSERT INTO foo (id) VALUES (1);
      INSERT INTO foo (id) VALUES (2);
      INSERT INTO foo (id) VALUES (3);
      INSERT INTO bar (id) VALUES (4);
      INSERT INTO bar (id) VALUES (5);
      INSERT INTO bar (id) VALUES (6);
      INSERT INTO baz (id) VALUES (7);
      INSERT INTO baz (id) VALUES (8);
      INSERT INTO baz (id) VALUES (9);
      `,
        `
      TRUNCATE foo, baz;
      `,
        `
      INSERT INTO foo (id) VALUES (101);
      `,
      ],
      expectedTransactions: 3,
      data: {
        ['public.foo']: [{id: 101, ['_0_version']: '03'}],
        ['public.bar']: [
          {id: 4, ['_0_version']: '01'},
          {id: 5, ['_0_version']: '01'},
          {id: 6, ['_0_version']: '01'},
        ],
        ['public.baz']: [],
      },
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      await initDB(upstream, c.setupUpstream);
      await initDB(replica, c.setupReplica);

      const lc = createSilentLogContext();
      await setupUpstream(
        lc,
        'postgresql:///incremental_sync_test_upstream',
        replicationSlot(REPLICA_ID),
      );
      await replica.begin(tx =>
        setupReplicationTables(
          lc,
          REPLICA_ID,
          tx,
          'postgresql:///incremental_sync_test_upstream',
        ),
      );

      const syncing = syncer.start(lc);

      for (const query of c.writeUpstream ?? []) {
        await upstream.unsafe(query);
      }

      let versions: string[] = [];
      if (c.expectedTransactions) {
        // TODO: Replace this with the mechanism that will be used to notify ViewSyncers.
        for (let i = 0; i < 100; i++) {
          const result =
            await replica`SELECT "dbVersion" FROM _zero."TxLog"`.values();
          versions = result.flat();
          expect(versions.length).toBeLessThanOrEqual(c.expectedTransactions);
          if (versions.length === c.expectedTransactions) {
            break;
          }
          // Wait or throw any error from the syncer.
          await Promise.race([sleep(50), syncing]);
        }
      }

      const published = await getPublicationInfo(replica, 'zero_');
      expect(published.tables).toEqual(c.specs);

      await expectTables(replica, replaceVersions(c.data, versions));
    });
  }

  function replaceVersions(
    data: Record<string, object[]>,
    versions: string[],
  ): Record<string, object[]> {
    Object.values(data).forEach(table =>
      table.forEach(row => {
        if ('_0_version' in row) {
          const v = row['_0_version'] as LexiVersion;
          const index = Number(versionFromLexi(v));
          if (index > 0) {
            row['_0_version'] = versions[index - 1];
          }
        }
      }),
    );
    return data;
  }
});
