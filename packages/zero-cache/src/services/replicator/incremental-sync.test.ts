import type {LogContext} from '@rocicorp/logger';
import {assert} from 'shared/src/asserts.js';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {sleep} from 'shared/src/sleep.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {
  Mode,
  TransactionPool,
  importSnapshot,
} from 'zero-cache/src/db/transaction-pool.js';
import {
  dropReplicationSlot,
  expectTables,
  getConnectionURI,
  initDB,
  testDBs,
} from '../../test/db.js';
import {versionFromLexi, type LexiVersion} from '../../types/lexi-version.js';
import {toLexiVersion} from '../../types/lsn.js';
import type {PostgresDB} from '../../types/pg.js';
import {IncrementalSyncer} from './incremental-sync.js';
import {replicationSlot, setupUpstream} from './initial-sync.js';
import type {RowChange, VersionChange} from './replicator.js';
import {queryLastLSN, setupReplicationTables} from './schema/replication.js';
import {getPublicationInfo} from './tables/published.js';
import type {FilteredTableSpec} from './tables/specs.js';
import {TransactionTrainService} from './transaction-train.js';

const REPLICA_ID = 'incremental_sync_test_id';
const SNAPSHOT_PATTERN = /([0-9A-F]+-){2}[0-9A-F]/;

describe('replicator/incremental-sync', {retry: 3}, () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  let replica: PostgresDB;
  let train: TransactionTrainService;
  let syncer: IncrementalSyncer;

  beforeEach(async () => {
    lc = createSilentLogContext();
    upstream = await testDBs.create('incremental_sync_test_upstream');
    replica = await testDBs.create('incremental_sync_test_replica');
    train = new TransactionTrainService(lc, replica);
    syncer = new IncrementalSyncer(
      getConnectionURI(upstream, 'external'),
      REPLICA_ID,
      replica,
      train,
    );
  });

  afterEach(async () => {
    await train.stop();
    await syncer.stop(lc);
    await dropReplicationSlot(upstream, replicationSlot(REPLICA_ID));
    await testDBs.drop(replica, upstream);
  });

  type Case = {
    name: string;
    setupUpstream?: string;
    setupReplica?: string;
    writeUpstream?: string[];
    expectedTransactions?: number;
    expectedVersionChanges?: Omit<VersionChange, 'prevSnapshotID'>[];
    coalescedVersionChange?: Omit<VersionChange, 'prevSnapshotID'>;
    specs: Record<string, FilteredTableSpec>;
    data: Record<string, Record<string, unknown>[]>;
  };

  const cases: Case[] = [
    {
      name: 'create tables',
      specs: {},
      data: {
        ['_zero.TxLog']: [],
        ['_zero.ChangeLog']: [],
      },
    },
    {
      name: 'alter version columns',
      setupReplica: `
      CREATE TABLE issues(
        "issueID" INTEGER PRIMARY KEY,
        _0_version VARCHAR(38) DEFAULT '00'
      );
      CREATE TABLE "table-with-special-characters" (
        "id" INTEGER PRIMARY KEY,
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
          filterConditions: [],
        },
        ['public.table-with-special-characters']: {
          schema: 'public',
          name: 'table-with-special-characters',
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
          filterConditions: [],
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
          filterConditions: [],
        },
      },
      data: {
        ['_zero.TxLog']: [],
        ['_zero.ChangeLog']: [],
      },
    },
    {
      name: 'insert rows',
      setupUpstream: `
      CREATE TABLE issues(
        "issueID" INTEGER PRIMARY KEY,
        big BIGINT,
        flt FLOAT8,
        ints INTEGER[],
        bigs BIGINT[],
        time TIMESTAMPTZ,
        description TEXT
      );
      CREATE PUBLICATION zero_all FOR TABLE issues WHERE ("issueID" < 1000);
      `,
      setupReplica: `
      CREATE TABLE issues(
        "issueID" INTEGER PRIMARY KEY,
        big BIGINT,
        flt FLOAT8,
        ints INTEGER[],
        bigs BIGINT[],
        time TIMESTAMPTZ,
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
            big: {
              dataType: 'int8',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: false,
            },
            flt: {
              dataType: 'float8',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: false,
            },
            ints: {
              dataType: 'int4[]',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: false,
            },
            bigs: {
              dataType: 'int8[]',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: false,
            },
            time: {
              dataType: 'timestamptz',
              characterMaximumLength: null,
              columnDefault: null,
              notNull: false,
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
          filterConditions: [],
        },
      },
      writeUpstream: [
        `
      INSERT INTO issues ("issueID") VALUES (123);
      INSERT INTO issues ("issueID", time) VALUES (456, '2024-03-21T18:50:23.646716Z');
      -- Rows > 1000 should be filtered by PG.
      INSERT INTO issues ("issueID") VALUES (1001);
      `,
        `
      INSERT INTO issues ("issueID", big) VALUES (789, 9223372036854775807);
      INSERT INTO issues ("issueID", ints) VALUES (987, '{92233720,123}');
      INSERT INTO issues ("issueID", flt) VALUES (234, 123.456);

      -- Rows > 1000 should be filtered by PG.
      INSERT INTO issues ("issueID") VALUES (2001);

      -- https://github.com/porsager/postgres/issues/837
      -- INSERT INTO issues ("issueID", bigs) VALUES (2468, '{9223372036854775807,123}');
      `,
      ],
      expectedTransactions: 2,
      expectedVersionChanges: [
        {
          prevVersion: '00',
          newVersion: '01',
          invalidations: {},
          changes: [
            {
              rowData: {
                ['_0_version']: '01',
                big: null,
                bigs: null,
                description: null,
                flt: null,
                ints: null,
                issueID: 123,
                time: null,
              },
              rowKey: {issueID: 123},
              schema: 'public',
              table: 'issues',
            },
            {
              rowData: {
                ['_0_version']: '01',
                big: null,
                bigs: null,
                description: null,
                flt: null,
                ints: null,
                issueID: 456,
                time: new Date('2024-03-21T18:50:23.646Z'),
              },
              rowKey: {issueID: 456},
              schema: 'public',
              table: 'issues',
            },
          ],
        },
        {
          prevVersion: '01',
          newVersion: '02',
          invalidations: {},
          changes: [
            {
              rowData: {
                ['_0_version']: '02',
                big: 9223372036854775807n,
                bigs: null,
                description: null,
                flt: null,
                ints: null,
                issueID: 789,
                time: null,
              },
              rowKey: {issueID: 789},
              schema: 'public',
              table: 'issues',
            },
            {
              rowData: {
                ['_0_version']: '02',
                big: null,
                bigs: null,
                description: null,
                flt: null,
                ints: [92233720, 123],
                issueID: 987,
                time: null,
              },
              rowKey: {issueID: 987},
              schema: 'public',
              table: 'issues',
            },
            {
              rowData: {
                ['_0_version']: '02',
                big: null,
                bigs: null,
                description: null,
                flt: 123.456,
                ints: null,
                issueID: 234,
                time: null,
              },
              rowKey: {issueID: 234},
              schema: 'public',
              table: 'issues',
            },
          ],
        },
      ],
      coalescedVersionChange: {
        prevVersion: '00',
        newVersion: '02',
        invalidations: {},
        changes: [
          {
            rowData: {
              ['_0_version']: '01',
              big: null,
              bigs: null,
              description: null,
              flt: null,
              ints: null,
              issueID: 123,
              time: null,
            },
            rowKey: {issueID: 123},
            schema: 'public',
            table: 'issues',
          },
          {
            rowData: {
              ['_0_version']: '01',
              big: null,
              bigs: null,
              description: null,
              flt: null,
              ints: null,
              issueID: 456,
              time: new Date('2024-03-21T18:50:23.646Z'),
            },
            rowKey: {issueID: 456},
            schema: 'public',
            table: 'issues',
          },
          {
            rowData: {
              ['_0_version']: '02',
              big: 9223372036854775807n,
              bigs: null,
              description: null,
              flt: null,
              ints: null,
              issueID: 789,
              time: null,
            },
            rowKey: {issueID: 789},
            schema: 'public',
            table: 'issues',
          },
          {
            rowData: {
              ['_0_version']: '02',
              big: null,
              bigs: null,
              description: null,
              flt: null,
              ints: [92233720, 123],
              issueID: 987,
              time: null,
            },
            rowKey: {issueID: 987},
            schema: 'public',
            table: 'issues',
          },
          {
            rowData: {
              ['_0_version']: '02',
              big: null,
              bigs: null,
              description: null,
              flt: 123.456,
              ints: null,
              issueID: 234,
              time: null,
            },
            rowKey: {issueID: 234},
            schema: 'public',
            table: 'issues',
          },
        ],
      },
      data: {
        ['public.issues']: [
          {
            issueID: 123,
            big: null,
            flt: null,
            ints: null,
            bigs: null,
            time: null,
            description: null,
            ['_0_version']: '01',
          },
          {
            issueID: 456,
            big: null,
            flt: null,
            ints: null,
            bigs: null,
            time: new Date(Date.UTC(2024, 2, 21, 18, 50, 23, 646)), // Note: we lost the microseconds
            description: null,
            ['_0_version']: '01',
          },
          {
            issueID: 789,
            big: 9223372036854775807n,
            ints: null,
            flt: null,
            bigs: null,
            time: null,
            description: null,
            ['_0_version']: '02',
          },
          {
            issueID: 987,
            big: null,
            flt: null,
            ints: [92233720, 123],
            bigs: null,
            time: null,
            description: null,
            ['_0_version']: '02',
          },
          {
            issueID: 234,
            big: null,
            flt: 123.456,
            ints: null,
            bigs: null,
            time: null,
            description: null,
            ['_0_version']: '02',
          },
          // https://github.com/porsager/postgres/issues/837
          // {
          //   issueID: 2468,
          //   big: null,
          //   ints: null,
          //   bigs: [9223372036854775807n, 123n],
          //   time: null,
          //   description: null,
          //   ['_0_version']: '02',
          // },
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '01',
            schema: 'public',
            table: 'issues',
            op: 's',
            rowKey: {issueID: 123},
          },
          {
            stateVersion: '01',
            schema: 'public',
            table: 'issues',
            op: 's',

            rowKey: {issueID: 456},
          },
          {
            stateVersion: '02',
            schema: 'public',
            table: 'issues',
            op: 's',
            rowKey: {issueID: 789},
          },
          {
            stateVersion: '02',
            schema: 'public',
            table: 'issues',
            op: 's',
            rowKey: {issueID: 987},
          },
          {
            stateVersion: '02',
            schema: 'public',
            table: 'issues',
            op: 's',
            rowKey: {issueID: 234},
          },
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
          filterConditions: [],
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
      expectedVersionChanges: [
        {
          prevVersion: '00',
          newVersion: '01',
          invalidations: {},
          changes: [
            {
              rowData: {
                ['_0_version']: '01',
                description: null,
                issueID: 123,
                orgID: 1,
              },
              rowKey: {issueID: 123, orgID: 1},
              schema: 'public',
              table: 'issues',
            },
            {
              rowData: {
                ['_0_version']: '01',
                description: null,
                issueID: 456,
                orgID: 1,
              },
              rowKey: {issueID: 456, orgID: 1},
              schema: 'public',
              table: 'issues',
            },
            {
              rowData: {
                ['_0_version']: '01',
                description: null,
                issueID: 789,
                orgID: 2,
              },
              rowKey: {issueID: 789, orgID: 2},
              schema: 'public',
              table: 'issues',
            },
          ],
        },
        {
          prevVersion: '01',
          newVersion: '02',
          invalidations: {},
          changes: [
            {
              rowData: {
                ['_0_version']: '02',
                description: 'foo',
                issueID: 456,
                orgID: 1,
              },
              rowKey: {issueID: 456, orgID: 1},
              schema: 'public',
              table: 'issues',
            },
            {
              rowData: {
                ['_0_version']: '02',
                description: 'bar',
                issueID: 123,
                orgID: 2,
              },
              rowKey: {issueID: 123, orgID: 2},
              schema: 'public',
              table: 'issues',
            },
            {
              rowData: undefined,
              rowKey: {issueID: 123, orgID: 1},
              schema: 'public',
              table: 'issues',
            },
          ],
        },
      ],
      coalescedVersionChange: {
        prevVersion: '00',
        newVersion: '02',
        invalidations: {},
        changes: [
          {
            rowData: {
              ['_0_version']: '01',
              description: null,
              issueID: 123,
              orgID: 1,
            },
            rowKey: {issueID: 123, orgID: 1},
            schema: 'public',
            table: 'issues',
          },
          {
            rowData: {
              ['_0_version']: '01',
              description: null,
              issueID: 456,
              orgID: 1,
            },
            rowKey: {issueID: 456, orgID: 1},
            schema: 'public',
            table: 'issues',
          },
          {
            rowData: {
              ['_0_version']: '01',
              description: null,
              issueID: 789,
              orgID: 2,
            },
            rowKey: {issueID: 789, orgID: 2},
            schema: 'public',
            table: 'issues',
          },
          {
            rowData: {
              ['_0_version']: '02',
              description: 'foo',
              issueID: 456,
              orgID: 1,
            },
            rowKey: {issueID: 456, orgID: 1},
            schema: 'public',
            table: 'issues',
          },
          {
            rowData: {
              ['_0_version']: '02',
              description: 'bar',
              issueID: 123,
              orgID: 2,
            },
            rowKey: {issueID: 123, orgID: 2},
            schema: 'public',
            table: 'issues',
          },
          {
            rowData: undefined,
            rowKey: {issueID: 123, orgID: 1},
            schema: 'public',
            table: 'issues',
          },
        ],
      },
      data: {
        ['public.issues']: [
          {orgID: 2, issueID: 123, description: 'bar', ['_0_version']: '02'},
          {orgID: 1, issueID: 456, description: 'foo', ['_0_version']: '02'},
          {orgID: 2, issueID: 789, description: null, ['_0_version']: '01'},
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '01',
            schema: 'public',
            table: 'issues',
            op: 's',
            rowKey: {orgID: 2, issueID: 789},
          },
          {
            stateVersion: '02',
            schema: 'public',
            table: 'issues',
            op: 's',
            rowKey: {orgID: 1, issueID: 456},
          },
          {
            stateVersion: '02',
            schema: 'public',
            table: 'issues',
            op: 'd',
            rowKey: {orgID: 1, issueID: 123},
          },
          {
            stateVersion: '02',
            schema: 'public',
            table: 'issues',
            op: 's',
            rowKey: {orgID: 2, issueID: 123},
          },
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
          filterConditions: [],
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
      expectedVersionChanges: [
        {
          prevVersion: '00',
          newVersion: '01',
          invalidations: {},
          changes: [
            {
              rowData: {
                ['_0_version']: '01',
                description: null,
                issueID: 123,
                orgID: 1,
              },
              rowKey: {issueID: 123, orgID: 1},
              schema: 'public',
              table: 'issues',
            },
            {
              rowData: {
                ['_0_version']: '01',
                description: null,
                issueID: 456,
                orgID: 1,
              },
              rowKey: {issueID: 456, orgID: 1},
              schema: 'public',
              table: 'issues',
            },
            {
              rowData: {
                ['_0_version']: '01',
                description: null,
                issueID: 789,
                orgID: 2,
              },
              rowKey: {issueID: 789, orgID: 2},
              schema: 'public',
              table: 'issues',
            },
            {
              rowData: {
                ['_0_version']: '01',
                description: null,
                issueID: 987,
                orgID: 2,
              },
              rowKey: {issueID: 987, orgID: 2},
              schema: 'public',
              table: 'issues',
            },
          ],
        },
        {
          prevVersion: '01',
          newVersion: '02',
          invalidations: {},
          changes: [
            {
              rowData: undefined,
              rowKey: {issueID: 123, orgID: 1},
              schema: 'public',
              table: 'issues',
            },
            {
              rowData: undefined,
              rowKey: {issueID: 456, orgID: 1},
              schema: 'public',
              table: 'issues',
            },
            {
              rowData: undefined,
              rowKey: {issueID: 987, orgID: 2},
              schema: 'public',
              table: 'issues',
            },
          ],
        },
      ],
      coalescedVersionChange: {
        prevVersion: '00',
        newVersion: '02',
        invalidations: {},
        changes: [
          {
            rowData: {
              ['_0_version']: '01',
              description: null,
              issueID: 123,
              orgID: 1,
            },
            rowKey: {issueID: 123, orgID: 1},
            schema: 'public',
            table: 'issues',
          },
          {
            rowData: {
              ['_0_version']: '01',
              description: null,
              issueID: 456,
              orgID: 1,
            },
            rowKey: {issueID: 456, orgID: 1},
            schema: 'public',
            table: 'issues',
          },
          {
            rowData: {
              ['_0_version']: '01',
              description: null,
              issueID: 789,
              orgID: 2,
            },
            rowKey: {issueID: 789, orgID: 2},
            schema: 'public',
            table: 'issues',
          },
          {
            rowData: {
              ['_0_version']: '01',
              description: null,
              issueID: 987,
              orgID: 2,
            },
            rowKey: {issueID: 987, orgID: 2},
            schema: 'public',
            table: 'issues',
          },
          {
            rowData: undefined,
            rowKey: {issueID: 123, orgID: 1},
            schema: 'public',
            table: 'issues',
          },
          {
            rowData: undefined,
            rowKey: {issueID: 456, orgID: 1},
            schema: 'public',
            table: 'issues',
          },
          {
            rowData: undefined,
            rowKey: {issueID: 987, orgID: 2},
            schema: 'public',
            table: 'issues',
          },
        ],
      },
      data: {
        ['public.issues']: [
          {orgID: 2, issueID: 789, description: null, ['_0_version']: '01'},
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '01',
            schema: 'public',
            table: 'issues',
            op: 's',
            rowKey: {orgID: 2, issueID: 789},
          },
          {
            stateVersion: '02',
            schema: 'public',
            table: 'issues',
            op: 'd',
            rowKey: {orgID: 1, issueID: 123},
          },
          {
            stateVersion: '02',
            schema: 'public',
            table: 'issues',
            op: 'd',
            rowKey: {orgID: 1, issueID: 456},
          },
          {
            stateVersion: '02',
            schema: 'public',
            table: 'issues',
            op: 'd',
            rowKey: {orgID: 2, issueID: 987},
          },
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
          filterConditions: [],
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
          filterConditions: [],
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
          filterConditions: [],
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
      TRUNCATE foo, baz;
      TRUNCATE foo;  -- Redundant. Shouldn't cause problems.
      `,
        `
      TRUNCATE foo;
      INSERT INTO foo (id) VALUES (101);
      `,
      ],
      expectedTransactions: 2,
      expectedVersionChanges: [
        {
          prevVersion: '00',
          newVersion: '01',
          invalidations: {},
          changes: [
            {
              schema: 'public',
              table: 'foo',
            },
            {
              rowData: {
                ['_0_version']: '01',
                id: 4,
              },
              rowKey: {id: 4},
              schema: 'public',
              table: 'bar',
            },
            {
              rowData: {
                ['_0_version']: '01',
                id: 5,
              },
              rowKey: {id: 5},
              schema: 'public',
              table: 'bar',
            },
            {
              rowData: {
                ['_0_version']: '01',
                id: 6,
              },
              rowKey: {id: 6},
              schema: 'public',
              table: 'bar',
            },
            {
              schema: 'public',
              table: 'baz',
            },
          ],
        },
        {
          prevVersion: '01',
          newVersion: '02',
          invalidations: {},
          changes: [
            {
              schema: 'public',
              table: 'foo',
            },
            {
              rowData: {
                ['_0_version']: '02',
                id: 101,
              },
              rowKey: {id: 101},
              schema: 'public',
              table: 'foo',
            },
          ],
        },
      ],
      coalescedVersionChange: {
        prevVersion: '00',
        newVersion: '02',
        invalidations: {},
        changes: [
          {
            schema: 'public',
            table: 'foo',
          },
          {
            rowData: {
              ['_0_version']: '01',
              id: 4,
            },
            rowKey: {id: 4},
            schema: 'public',
            table: 'bar',
          },
          {
            rowData: {
              ['_0_version']: '01',
              id: 5,
            },
            rowKey: {id: 5},
            schema: 'public',
            table: 'bar',
          },
          {
            rowData: {
              ['_0_version']: '01',
              id: 6,
            },
            rowKey: {id: 6},
            schema: 'public',
            table: 'bar',
          },
          {
            schema: 'public',
            table: 'baz',
          },
          {
            schema: 'public',
            table: 'foo',
          },
          {
            rowData: {
              ['_0_version']: '02',
              id: 101,
            },
            rowKey: {id: 101},
            schema: 'public',
            table: 'foo',
          },
        ],
      },
      data: {
        ['public.foo']: [{id: 101, ['_0_version']: '02'}],
        ['public.bar']: [
          {id: 4, ['_0_version']: '01'},
          {id: 5, ['_0_version']: '01'},
          {id: 6, ['_0_version']: '01'},
        ],
        ['public.baz']: [],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '01',
            schema: 'public',
            table: 'bar',
            op: 's',
            rowKey: {id: 4},
          },
          {
            stateVersion: '01',
            schema: 'public',
            table: 'bar',
            op: 's',
            rowKey: {id: 5},
          },
          {
            stateVersion: '01',
            schema: 'public',
            table: 'bar',
            op: 's',
            rowKey: {id: 6},
          },
          {
            stateVersion: '01',
            schema: 'public',
            table: 'baz',
            op: 't',
            rowKey: {},
          },
          {
            stateVersion: '02',
            schema: 'public',
            table: 'foo',
            op: 't',
            rowKey: {},
          },
          {
            stateVersion: '02',
            schema: 'public',
            table: 'foo',
            op: 's',
            rowKey: {id: 101},
          },
        ],
      },
    },
    {
      name: 'overwriting updates in the same transaction',
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
          filterConditions: [],
        },
      },
      writeUpstream: [
        `
      INSERT INTO issues ("orgID", "issueID") VALUES (1, 123);
      UPDATE issues SET ("orgID", "issueID") = (1, 456);
      INSERT INTO issues ("orgID", "issueID") VALUES (2, 789);
      DELETE FROM issues WHERE "orgID" = 2;
      UPDATE issues SET "description" = 'foo';
      `,
      ],
      expectedTransactions: 1,
      expectedVersionChanges: [
        {
          prevVersion: '00',
          newVersion: '01',
          invalidations: {},
          changes: [
            {
              rowData: {
                ['_0_version']: '01',
                description: 'foo',
                issueID: 456,
                orgID: 1,
              },
              rowKey: {issueID: 456, orgID: 1},
              schema: 'public',
              table: 'issues',
            },
          ],
        },
      ],
      coalescedVersionChange: {
        prevVersion: '00',
        newVersion: '01',
        invalidations: {},
        changes: [
          {
            rowData: {
              ['_0_version']: '01',
              description: 'foo',
              issueID: 456,
              orgID: 1,
            },
            rowKey: {issueID: 456, orgID: 1},
            schema: 'public',
            table: 'issues',
          },
        ],
      },
      data: {
        ['public.issues']: [
          {orgID: 1, issueID: 456, description: 'foo', ['_0_version']: '01'},
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '01',
            schema: 'public',
            table: 'issues',
            op: 's',
            rowKey: {orgID: 1, issueID: 456},
          },
        ],
      },
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      await initDB(upstream, c.setupUpstream);
      await initDB(replica, c.setupReplica);

      await setupUpstream(lc, upstream, replicationSlot(REPLICA_ID));
      await replica.begin(tx =>
        setupReplicationTables(lc, tx, getConnectionURI(upstream)),
      );

      expect(await queryLastLSN(replica)).toBeNull();

      void train.run();

      const syncing = syncer.run(lc);
      const incrementalVersionSubscription = await syncer.versionChanges();
      const coalescedVersionSubscription = await syncer.versionChanges();

      // Listen concurrently to capture incremental version changes.
      const incrementalVersions = (async () => {
        const versions: VersionChange[] = [];
        if (c.expectedTransactions) {
          for await (const v of incrementalVersionSubscription) {
            // Verify that the snapshot ID can be imported.
            const {init, imported} = importSnapshot(v.prevSnapshotID);
            const txPool = new TransactionPool(lc, Mode.READONLY, init);
            void txPool.run(replica);
            await imported;
            txPool.setDone();

            versions.push(v);
            if (versions.length === c.expectedTransactions) {
              break;
            }
          }
        }
        return versions;
      })();

      for (const query of c.writeUpstream ?? []) {
        await upstream.unsafe(query);
      }

      let versions: string[] = [];
      if (c.expectedTransactions) {
        // TODO: Replace this with the mechanism that will be used to notify ViewSyncers.
        for (let i = 0; i < 100; i++) {
          const result =
            await replica`SELECT "stateVersion" FROM _zero."TxLog"`.values();
          versions = result.flat();
          expect(versions.length).toBeLessThanOrEqual(c.expectedTransactions);
          if (versions.length === c.expectedTransactions) {
            break;
          }
          // Wait or throw any error from the syncer.
          await Promise.race([sleep(50), syncing]);
        }
      }

      if (versions.length) {
        const lsn = await queryLastLSN(replica);
        assert(lsn);
        expect(toLexiVersion(lsn)).toBe(versions.at(-1));
      } else {
        expect(await queryLastLSN(replica)).toBeNull();
      }

      const published = await getPublicationInfo(replica);
      expect(
        Object.fromEntries(
          published.tables.map(table => [
            `${table.schema}.${table.name}`,
            table,
          ]),
        ),
      ).toEqual(c.specs);

      await expectTables(replica, replaceVersions(c.data, versions));

      if (c.expectedVersionChanges) {
        expect(await incrementalVersions).toMatchObject(
          c.expectedVersionChanges.map(v => convertVersionChange(v, versions)),
        );
      }
      if (c.coalescedVersionChange) {
        for await (const v of coalescedVersionSubscription) {
          expect(v).toMatchObject(
            convertVersionChange(c.coalescedVersionChange, versions),
          );
          break;
        }
      }
    });
  }

  function convertVersionChange(
    v: Omit<VersionChange, 'prevSnapshotID'>,
    versions: string[],
  ): VersionChange {
    const convert = (val: string) => {
      const index = Number(versionFromLexi(val));
      return index > 0 ? versions[index - 1] : val;
    };
    return {
      newVersion: convert(v.newVersion),
      prevVersion: convert(v.prevVersion),
      prevSnapshotID: expect.stringMatching(SNAPSHOT_PATTERN),
      invalidations: {},
      changes:
        v.changes === undefined
          ? undefined
          : v.changes.map(c =>
              c.rowData === undefined
                ? c
                : ({
                    ...c,
                    rowData: {
                      ...c.rowData,
                      ['_0_version']: convert(
                        c.rowData['_0_version'] as string,
                      ),
                    },
                  } as RowChange),
            ),
    };
  }

  function replaceVersions(
    data: Record<string, Record<string, unknown>[]>,
    versions: string[],
  ): Record<string, unknown[]> {
    const replace = (key: string, obj: Record<string, unknown>) => {
      const v = obj[key] as LexiVersion;
      const index = Number(versionFromLexi(v));
      if (index > 0) {
        obj[key] = versions[index - 1];
      }
    };
    Object.values(data).forEach(table =>
      table.forEach(row => {
        for (const col of ['_0_version', 'stateVersion']) {
          if (col in row) {
            replace(col, row);
          }
        }
        for (const val of Object.values(row)) {
          if (val !== null && typeof val === 'object' && '_0_version' in val) {
            replace('_0_version', val);
          }
        }
      }),
    );
    return data;
  }
});
