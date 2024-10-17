import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  type MockedFunction,
  test,
  vi,
} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.js';
import {Database} from '../../../../zqlite/src/db.js';
import {listIndexes, listTables} from '../../db/lite-tables.js';
import type {LiteIndexSpec, LiteTableSpec} from '../../db/specs.js';
import {dropReplicationSlot, testDBs} from '../../test/db.js';
import {expectTables, initDB} from '../../test/lite.js';
import type {JSONObject} from '../../types/bigint-json.js';
import type {PostgresDB} from '../../types/pg.js';
import {Subscription} from '../../types/subscription.js';
import type {
  Downstream,
  SubscriberContext,
} from '../change-streamer/change-streamer.js';
import {replicationSlot} from '../change-streamer/pg/initial-sync.js';
import {IncrementalSyncer} from './incremental-sync.js';
import {initChangeLog} from './schema/change-log.js';
import {initReplicationState} from './schema/replication-state.js';
import {ReplicationMessages} from './test-utils.js';

const REPLICA_ID = 'incremental_sync_test_id';

describe('replicator/incremental-sync', () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  let replica: Database;
  let syncer: IncrementalSyncer;
  let downstream: Subscription<Downstream>;
  let subscribeFn: MockedFunction<
    (ctx: SubscriberContext) => Promise<Subscription<Downstream>>
  >;

  beforeEach(async () => {
    lc = createSilentLogContext();
    upstream = await testDBs.create('incremental_sync_test_upstream');
    replica = new Database(lc, ':memory:');
    downstream = Subscription.create();
    subscribeFn = vi.fn();
    syncer = new IncrementalSyncer(
      REPLICA_ID,
      {subscribe: subscribeFn.mockResolvedValue(downstream)},
      replica,
      'CONCURRENT',
    );
  });

  afterEach(async () => {
    await syncer.stop(lc);
    await dropReplicationSlot(upstream, replicationSlot(REPLICA_ID));
    await testDBs.drop(upstream);
  });

  type Case = {
    name: string;
    setup: string;
    downstream: Downstream[];
    data: Record<string, Record<string, unknown>[]>;
    tableSpecs?: LiteTableSpec[];
    indexSpecs?: LiteIndexSpec[];
  };

  const issues = new ReplicationMessages({issues: ['issueID', 'bool']});
  const orgIssues = new ReplicationMessages({
    issues: ['orgID', 'issueID', 'bool'],
  });
  const fooBarBaz = new ReplicationMessages({foo: 'id', bar: 'id', baz: 'id'});

  const cases: Case[] = [
    {
      name: 'insert rows',
      setup: `
      CREATE TABLE issues(
        issueID INTEGER,
        bool BOOL,
        big INTEGER,
        flt REAL,
        description TEXT,
        json JSON,
        time TIMESTAMPTZ,
        bytes bytesa,
        intArray int4[],
        _0_version TEXT NOT NULL,
        PRIMARY KEY(issueID, bool)
      );
      `,
      downstream: [
        ['begin', issues.begin()],
        ['data', issues.insert('issues', {issueID: 123, bool: true})],
        ['data', issues.insert('issues', {issueID: 456, bool: false})],
        ['commit', issues.commit(), {watermark: '06'}],

        ['begin', issues.begin()],
        [
          'data',
          issues.insert('issues', {
            issueID: 789,
            bool: true,
            big: 9223372036854775807n,
            json: [{foo: 'bar', baz: 123}],
            time: 1728345600123456n,
            bytes: Buffer.from('world'),
            intArray: [3, 2, 1],
          } as unknown as Record<string, JSONObject>),
        ],
        ['data', issues.insert('issues', {issueID: 987, bool: true})],
        [
          'data',
          issues.insert('issues', {issueID: 234, bool: false, flt: 123.456}),
        ],
        ['commit', issues.commit(), {watermark: '0b'}],
      ],
      data: {
        issues: [
          {
            issueID: 123n,
            big: null,
            flt: null,
            bool: 1n,
            description: null,
            json: null,
            time: null,
            bytes: null,
            intArray: null,
            ['_0_version']: '02',
          },
          {
            issueID: 456n,
            big: null,
            flt: null,
            bool: 0n,
            description: null,
            json: null,
            time: null,
            bytes: null,
            intArray: null,
            ['_0_version']: '02',
          },
          {
            issueID: 789n,
            big: 9223372036854775807n,
            flt: null,
            bool: 1n,
            description: null,
            json: '[{"foo":"bar","baz":123}]',
            time: 1728345600123456n,
            bytes: Buffer.from('world'),
            intArray: '[3,2,1]',
            ['_0_version']: '06',
          },
          {
            issueID: 987n,
            big: null,
            flt: null,
            bool: 1n,
            description: null,
            json: null,
            time: null,
            bytes: null,
            intArray: null,
            ['_0_version']: '06',
          },
          {
            issueID: 234n,
            big: null,
            flt: 123.456,
            bool: 0n,
            description: null,
            json: null,
            time: null,
            bytes: null,
            intArray: null,
            ['_0_version']: '06',
          },
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '02',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":123}',
          },
          {
            stateVersion: '02',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":0,"issueID":456}',
          },
          {
            stateVersion: '06',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":789}',
          },
          {
            stateVersion: '06',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":987}',
          },
          {
            stateVersion: '06',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":0,"issueID":234}',
          },
        ],
      },
    },
    {
      name: 'update rows with multiple key columns and key value updates',
      setup: `
      CREATE TABLE issues(
        issueID INTEGER,
        orgID INTEGER,
        description TEXT,
        bool BOOL,
        _0_version TEXT NOT NULL,
        PRIMARY KEY("orgID", "issueID", "bool")
      );
      `,
      downstream: [
        ['begin', orgIssues.begin()],
        [
          'data',
          orgIssues.insert('issues', {orgID: 1, issueID: 123, bool: true}),
        ],
        [
          'data',
          orgIssues.insert('issues', {orgID: 1, issueID: 456, bool: true}),
        ],
        [
          'data',
          orgIssues.insert('issues', {orgID: 2, issueID: 789, bool: true}),
        ],
        ['commit', orgIssues.commit(), {watermark: '06'}],

        ['begin', orgIssues.begin()],
        [
          'data',
          orgIssues.update('issues', {
            orgID: 1,
            issueID: 456,
            bool: true,
            description: 'foo',
          }),
        ],
        [
          'data',
          orgIssues.update(
            'issues',
            {
              orgID: 2,
              issueID: 123,
              bool: false,
              description: 'bar',
            },
            {orgID: 1, issueID: 123, bool: true},
          ),
        ],
        ['commit', orgIssues.commit(), {watermark: '0a'}],
      ],
      data: {
        issues: [
          {
            orgID: 2n,
            issueID: 123n,
            description: 'bar',
            bool: 0n,
            ['_0_version']: '06',
          },
          {
            orgID: 1n,
            issueID: 456n,
            description: 'foo',
            bool: 1n,
            ['_0_version']: '06',
          },
          {
            orgID: 2n,
            issueID: 789n,
            description: null,
            bool: 1n,
            ['_0_version']: '02',
          },
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '02',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":789,"orgID":2}',
          },
          {
            stateVersion: '06',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":456,"orgID":1}',
          },
          {
            stateVersion: '06',
            table: 'issues',
            op: 'd',
            rowKey: '{"bool":1,"issueID":123,"orgID":1}',
          },
          {
            stateVersion: '06',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":0,"issueID":123,"orgID":2}',
          },
        ],
      },
    },
    {
      name: 'delete rows',
      setup: `
      CREATE TABLE issues(
        issueID INTEGER,
        orgID INTEGER,
        bool BOOL,
        description TEXT,
        _0_version TEXT NOT NULL,
        PRIMARY KEY("orgID", "issueID","bool")
      );
      `,
      downstream: [
        ['begin', orgIssues.begin()],
        [
          'data',
          orgIssues.insert('issues', {orgID: 1, issueID: 123, bool: true}),
        ],
        [
          'data',
          orgIssues.insert('issues', {orgID: 1, issueID: 456, bool: false}),
        ],
        [
          'data',
          orgIssues.insert('issues', {orgID: 2, issueID: 789, bool: false}),
        ],
        [
          'data',
          orgIssues.insert('issues', {orgID: 2, issueID: 987, bool: true}),
        ],
        ['commit', orgIssues.commit(), {watermark: '07'}],

        ['begin', orgIssues.begin()],
        [
          'data',
          orgIssues.delete('issues', {orgID: 1, issueID: 123, bool: true}),
        ],
        [
          'data',
          orgIssues.delete('issues', {orgID: 1, issueID: 456, bool: false}),
        ],
        [
          'data',
          orgIssues.delete('issues', {orgID: 2, issueID: 987, bool: true}),
        ],
        ['commit', orgIssues.commit(), {watermark: '0c'}],
      ],
      data: {
        issues: [
          {
            orgID: 2n,
            issueID: 789n,
            bool: 0n,
            description: null,
            ['_0_version']: '02',
          },
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '02',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":0,"issueID":789,"orgID":2}',
          },
          {
            stateVersion: '07',
            table: 'issues',
            op: 'd',
            rowKey: '{"bool":1,"issueID":123,"orgID":1}',
          },
          {
            stateVersion: '07',
            table: 'issues',
            op: 'd',
            rowKey: '{"bool":0,"issueID":456,"orgID":1}',
          },
          {
            stateVersion: '07',
            table: 'issues',
            op: 'd',
            rowKey: '{"bool":1,"issueID":987,"orgID":2}',
          },
        ],
      },
    },
    {
      name: 'truncate tables',
      setup: `
      CREATE TABLE foo(id INTEGER PRIMARY KEY, _0_version TEXT NOT NULL);
      CREATE TABLE bar(id INTEGER PRIMARY KEY, _0_version TEXT NOT NULL);
      CREATE TABLE baz(id INTEGER PRIMARY KEY, _0_version TEXT NOT NULL);
      `,
      downstream: [
        ['begin', fooBarBaz.begin()],
        ['data', fooBarBaz.insert('foo', {id: 1})],
        ['data', fooBarBaz.insert('foo', {id: 2})],
        ['data', fooBarBaz.insert('foo', {id: 3})],
        ['data', fooBarBaz.insert('bar', {id: 4})],
        ['data', fooBarBaz.insert('bar', {id: 5})],
        ['data', fooBarBaz.insert('bar', {id: 6})],
        ['data', fooBarBaz.insert('baz', {id: 7})],
        ['data', fooBarBaz.insert('baz', {id: 8})],
        ['data', fooBarBaz.insert('baz', {id: 9})],
        ['data', fooBarBaz.truncate('foo', 'baz')],
        ['data', fooBarBaz.truncate('foo')], // Redundant. Shouldn't cause problems.
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],

        ['begin', fooBarBaz.begin()],
        ['data', fooBarBaz.truncate('foo')],
        ['data', fooBarBaz.insert('foo', {id: 101})],
        ['commit', fooBarBaz.commit(), {watermark: '0i'}],
      ],
      data: {
        foo: [{id: 101n, ['_0_version']: '0e'}],
        bar: [
          {id: 4n, ['_0_version']: '02'},
          {id: 5n, ['_0_version']: '02'},
          {id: 6n, ['_0_version']: '02'},
        ],
        baz: [],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '02',
            table: 'bar',
            op: 's',
            rowKey: '{"id":4}',
          },
          {
            stateVersion: '02',
            table: 'bar',
            op: 's',
            rowKey: '{"id":5}',
          },
          {
            stateVersion: '02',
            table: 'bar',
            op: 's',
            rowKey: '{"id":6}',
          },
          {
            stateVersion: '02',
            table: 'baz',
            op: 't',
            rowKey: '',
          },
          {
            stateVersion: '0e',
            table: 'foo',
            op: 't',
            rowKey: '',
          },
          {
            stateVersion: '0e',
            table: 'foo',
            op: 's',
            rowKey: '{"id":101}',
          },
        ],
      },
    },
    {
      name: 'overwriting updates in the same transaction',
      setup: `
      CREATE TABLE issues(
        issueID INTEGER,
        orgID INTEGER,
        bool BOOL,
        description TEXT,
        _0_version TEXT NOT NULL,
        PRIMARY KEY("orgID", "issueID", "bool")
      );
      `,
      downstream: [
        ['begin', orgIssues.begin()],
        [
          'data',
          orgIssues.insert('issues', {orgID: 1, issueID: 123, bool: true}),
        ],
        [
          'data',
          orgIssues.update(
            'issues',
            {orgID: 1, issueID: 456, bool: false},
            {orgID: 1, issueID: 123, bool: true},
          ),
        ],
        [
          'data',
          orgIssues.insert('issues', {orgID: 2, issueID: 789, bool: false}),
        ],
        [
          'data',
          orgIssues.delete('issues', {orgID: 2, issueID: 789, bool: false}),
        ],
        [
          'data',
          orgIssues.update('issues', {
            orgID: 1,
            issueID: 456,
            bool: false,
            description: 'foo',
          }),
        ],
        ['commit', orgIssues.commit(), {watermark: '08'}],
      ],
      data: {
        issues: [
          {
            orgID: 1n,
            issueID: 456n,
            bool: 0n,
            description: 'foo',
            ['_0_version']: '02',
          },
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '02',
            table: 'issues',
            op: 'd',
            rowKey: '{"bool":1,"issueID":123,"orgID":1}',
          },
          {
            stateVersion: '02',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":0,"issueID":456,"orgID":1}',
          },
          {
            stateVersion: '02',
            table: 'issues',
            op: 'd',
            rowKey: '{"bool":0,"issueID":789,"orgID":2}',
          },
        ],
      },
    },
    {
      name: 'create table',
      setup: ``,
      downstream: [
        ['begin', fooBarBaz.begin()],
        [
          'data',
          fooBarBaz.createTable({
            schema: 'public',
            name: 'foo',
            columns: {
              id: {pos: 0, dataType: 'varchar'},
              count: {pos: 1, dataType: 'int8'},
              bool: {pos: 3, dataType: 'bool'},
            },
            primaryKey: ['id'],
          }),
        ],
        ['data', fooBarBaz.insert('foo', {id: 'bar', count: 2, bool: true})],
        ['data', fooBarBaz.insert('foo', {id: 'baz', count: 3, bool: false})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 'bar', count: 2n, bool: 1n, ['_0_version']: '02'},
          {id: 'baz', count: 3n, bool: 0n, ['_0_version']: '02'},
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '02',
            table: 'foo',
            op: 'r',
            rowKey: null,
          },
          {
            stateVersion: '02',
            table: 'foo',
            op: 's',
            rowKey: '{"id":"bar"}',
          },
          {
            stateVersion: '02',
            table: 'foo',
            op: 's',
            rowKey: '{"id":"baz"}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'varchar',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            count: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: false,
              pos: 2,
            },
            bool: {
              characterMaximumLength: null,
              dataType: 'bool',
              dflt: null,
              notNull: false,
              pos: 3,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 4,
            },
          },
          primaryKey: ['id'],
        },
      ],
      indexSpecs: [],
    },
    {
      name: 'rename table',
      setup: `
        CREATE TABLE foo(id INT8 PRIMARY KEY, _0_version TEXT NOT NULL);
        INSERT INTO foo(id, _0_version) VALUES (1, '00');
        INSERT INTO foo(id, _0_version) VALUES (2, '00');
        INSERT INTO foo(id, _0_version) VALUES (3, '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin()],
        ['data', fooBarBaz.renameTable('foo', 'bar')],
        ['data', fooBarBaz.insert('bar', {id: 4})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        bar: [
          {id: 1n, ['_0_version']: '02'},
          {id: 2n, ['_0_version']: '02'},
          {id: 3n, ['_0_version']: '02'},
          {id: 4n, ['_0_version']: '02'},
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '02',
            table: 'bar',
            op: 'r',
            rowKey: null,
          },
          {
            stateVersion: '02',
            table: 'foo',
            op: 'r',
            rowKey: null,
          },
          {
            stateVersion: '02',
            table: 'bar',
            op: 's',
            rowKey: '{"id":4}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'bar',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 2,
            },
          },
          primaryKey: ['id'],
        },
      ],
      indexSpecs: [],
    },
    {
      name: 'add column',
      setup: `
        CREATE TABLE foo(id INT8 PRIMARY KEY, _0_version TEXT NOT NULL);
        INSERT INTO foo(id, _0_version) VALUES (1, '00');
        INSERT INTO foo(id, _0_version) VALUES (2, '00');
        INSERT INTO foo(id, _0_version) VALUES (3, '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin()],
        [
          'data',
          fooBarBaz.addColumn('foo', 'newInt', {
            pos: 9,
            dataType: 'int8',
            dflt: '123', // DEFAULT should applied for ADD COLUMN
          }),
        ],
        [
          'data',
          fooBarBaz.addColumn('foo', 'newBool', {
            pos: 10,
            dataType: 'bool',
            dflt: 'true', // DEFAULT should applied for ADD COLUMN
          }),
        ],
        ['data', fooBarBaz.insert('foo', {id: 4, newInt: 321, newBool: false})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 1n, newInt: 123n, newBool: 1n, ['_0_version']: '02'},
          {id: 2n, newInt: 123n, newBool: 1n, ['_0_version']: '02'},
          {id: 3n, newInt: 123n, newBool: 1n, ['_0_version']: '02'},
          {id: 4n, newInt: 321n, newBool: 0n, ['_0_version']: '02'},
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '02',
            table: 'foo',
            op: 'r',
            rowKey: null,
          },
          {
            stateVersion: '02',
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 2,
            },
            newInt: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: '123',
              notNull: false,
              pos: 3,
            },
            newBool: {
              characterMaximumLength: null,
              dataType: 'bool',
              dflt: '1',
              notNull: false,
              pos: 4,
            },
          },
          primaryKey: ['id'],
        },
      ],
      indexSpecs: [],
    },
    {
      name: 'drop column',
      setup: `
        CREATE TABLE foo(id INT8 PRIMARY KEY, dropMe TEXT, _0_version TEXT NOT NULL);
        INSERT INTO foo(id, dropMe, _0_version) VALUES (1, 'bye', '00');
        INSERT INTO foo(id, dropMe, _0_version) VALUES (2, 'bye', '00');
        INSERT INTO foo(id, dropMe, _0_version) VALUES (3, 'bye', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin()],
        ['data', fooBarBaz.update('foo', {id: 3, dropMe: 'stillDropped'})],
        ['data', fooBarBaz.dropColumn('foo', 'dropMe')],
        ['data', fooBarBaz.insert('foo', {id: 4})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 1n, ['_0_version']: '02'},
          {id: 2n, ['_0_version']: '02'},
          {id: 3n, ['_0_version']: '02'},
          {id: 4n, ['_0_version']: '02'},
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '02',
            table: 'foo',
            op: 'r',
            rowKey: null,
          },
          {
            stateVersion: '02',
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 2,
            },
          },
          primaryKey: ['id'],
        },
      ],
      indexSpecs: [],
    },
    {
      name: 'rename column',
      setup: `
        CREATE TABLE foo(id INT8 PRIMARY KEY, renameMe TEXT, _0_version TEXT NOT NULL);
        INSERT INTO foo(id, renameMe, _0_version) VALUES (1, 'hel', '00');
        INSERT INTO foo(id, renameMe, _0_version) VALUES (2, 'low', '00');
        INSERT INTO foo(id, renameMe, _0_version) VALUES (3, 'orl', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin()],
        ['data', fooBarBaz.update('foo', {id: 3, renameMe: 'olrd'})],
        [
          'data',
          fooBarBaz.updateColumn(
            'foo',
            {name: 'renameMe', spec: {pos: 1, dataType: 'TEXT'}},
            {name: 'newName', spec: {pos: 1, dataType: 'TEXT'}},
          ),
        ],
        ['data', fooBarBaz.insert('foo', {id: 4, newName: 'yay'})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 1n, newName: 'hel', ['_0_version']: '02'},
          {id: 2n, newName: 'low', ['_0_version']: '02'},
          {id: 3n, newName: 'olrd', ['_0_version']: '02'},
          {id: 4n, newName: 'yay', ['_0_version']: '02'},
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '02',
            table: 'foo',
            op: 'r',
            rowKey: null,
          },
          {
            stateVersion: '02',
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            newName: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              pos: 2,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 3,
            },
          },
          primaryKey: ['id'],
        },
      ],
      indexSpecs: [],
    },
    {
      name: 'retype column',
      setup: `
        CREATE TABLE foo(id INT8 PRIMARY KEY, num TEXT, _0_version TEXT NOT NULL);
        INSERT INTO foo(id, num, _0_version) VALUES (1, '3', '00');
        INSERT INTO foo(id, num, _0_version) VALUES (2, '2', '00');
        INSERT INTO foo(id, num, _0_version) VALUES (3, '3', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin()],
        ['data', fooBarBaz.update('foo', {id: 3, num: '1'})],
        [
          'data',
          fooBarBaz.updateColumn(
            'foo',
            {name: 'num', spec: {pos: 1, dataType: 'TEXT'}},
            {name: 'num', spec: {pos: 1, dataType: 'INT8'}},
          ),
        ],
        ['data', fooBarBaz.insert('foo', {id: 4, num: 23})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 1n, num: 3n, ['_0_version']: '02'},
          {id: 2n, num: 2n, ['_0_version']: '02'},
          {id: 3n, num: 1n, ['_0_version']: '02'},
          {id: 4n, num: 23n, ['_0_version']: '02'},
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '02',
            table: 'foo',
            op: 'r',
            rowKey: null,
          },
          {
            stateVersion: '02',
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            num: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              pos: 3,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 2,
            },
          },
          primaryKey: ['id'],
        },
      ],
      indexSpecs: [],
    },
    {
      name: 'rename and retype column',
      setup: `
        CREATE TABLE foo(id INT8 PRIMARY KEY, numburr TEXT, _0_version TEXT NOT NULL);
        INSERT INTO foo(id, numburr, _0_version) VALUES (1, '3', '00');
        INSERT INTO foo(id, numburr, _0_version) VALUES (2, '2', '00');
        INSERT INTO foo(id, numburr, _0_version) VALUES (3, '3', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin()],
        ['data', fooBarBaz.update('foo', {id: 3, numburr: '1'})],
        [
          'data',
          fooBarBaz.updateColumn(
            'foo',
            {name: 'numburr', spec: {pos: 1, dataType: 'TEXT'}},
            {name: 'number', spec: {pos: 1, dataType: 'INT8'}},
          ),
        ],
        ['data', fooBarBaz.insert('foo', {id: 4, number: 23})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 1n, number: 3n, ['_0_version']: '02'},
          {id: 2n, number: 2n, ['_0_version']: '02'},
          {id: 3n, number: 1n, ['_0_version']: '02'},
          {id: 4n, number: 23n, ['_0_version']: '02'},
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '02',
            table: 'foo',
            op: 'r',
            rowKey: null,
          },
          {
            stateVersion: '02',
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              pos: 1,
            },
            number: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              pos: 3,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: true,
              pos: 2,
            },
          },
          primaryKey: ['id'],
        },
      ],
      indexSpecs: [],
    },
    {
      name: 'drop table',
      setup: `
        CREATE TABLE foo(id INT8 PRIMARY KEY, _0_version TEXT NOT NULL);
        INSERT INTO foo(id, _0_version) VALUES (1, '00');
        INSERT INTO foo(id, _0_version) VALUES (2, '00');
        INSERT INTO foo(id, _0_version) VALUES (3, '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin()],
        ['data', fooBarBaz.insert('foo', {id: 4})],
        ['data', fooBarBaz.dropTable('foo')],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        ['_zero.ChangeLog']: [
          {
            stateVersion: '02',
            table: 'foo',
            op: 'r',
            rowKey: null,
          },
        ],
      },
      tableSpecs: [],
      indexSpecs: [],
    },
    {
      name: 'create index',
      setup: `
        CREATE TABLE foo(id INT8 PRIMARY KEY, handle TEXT, _0_version TEXT NOT NULL);
      `,
      downstream: [
        ['begin', fooBarBaz.begin()],
        [
          'data',
          fooBarBaz.createIndex({
            schemaName: 'public',
            tableName: 'foo',
            name: 'foo_handle_index',
            columns: {
              handle: 'DESC',
            },
            unique: true,
          }),
        ],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        ['_zero.ChangeLog']: [],
      },
      indexSpecs: [
        {
          name: 'foo_handle_index',
          tableName: 'foo',
          columns: {handle: 'DESC'},
          unique: true,
        },
      ],
    },
    {
      name: 'drop index',
      setup: `
        CREATE TABLE foo(id INT8 PRIMARY KEY, handle TEXT, _0_version TEXT NOT NULL);
        CREATE INDEX keep_me ON foo (id DESC, handle ASC);
        CREATE INDEX drop_me ON foo (handle DESC);
      `,
      downstream: [
        ['begin', fooBarBaz.begin()],
        ['data', fooBarBaz.dropIndex('drop_me')],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        ['_zero.ChangeLog']: [],
      },
      indexSpecs: [
        {
          name: 'keep_me',
          tableName: 'foo',
          columns: {
            id: 'DESC',
            handle: 'ASC',
          },
          unique: false,
        },
      ],
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      initDB(replica, c.setup);
      initReplicationState(replica, ['zero_data'], '02');
      initChangeLog(replica);

      const syncing = syncer.run(lc);
      const notifications = syncer.subscribe();
      const versionReady = notifications[Symbol.asyncIterator]();

      await versionReady.next(); // Get the initial nextStateVersion.
      expect(subscribeFn.mock.calls[0][0]).toEqual({
        id: 'incremental_sync_test_id',
        replicaVersion: '02',
        watermark: '02',
        initial: true,
      });

      for (const change of c.downstream) {
        downstream.push(change);
        if (change[0] === 'commit') {
          await Promise.race([versionReady.next(), syncing]);
        }
      }

      expectTables(replica, c.data, 'bigint');

      if (c.tableSpecs) {
        expect(
          listTables(replica).filter(t => !t.name.startsWith('_zero.')),
        ).toEqual(c.tableSpecs);
      }
      if (c.indexSpecs) {
        expect(listIndexes(replica)).toEqual(c.indexSpecs);
      }
    });
  }

  test('retry on initial change-streamer connection failure', async () => {
    initReplicationState(replica, ['zero_data'], '02');

    const {promise: hasRetried, resolve: retried} = resolver<true>();
    const syncer = new IncrementalSyncer(
      REPLICA_ID,
      {
        subscribe: vi
          .fn()
          .mockRejectedValueOnce('error')
          .mockImplementation(() => {
            retried(true);
            return resolver().promise;
          }),
      },
      replica,
      'CONCURRENT',
    );

    void syncer.run(lc);

    expect(await hasRetried).toBe(true);

    void syncer.stop(lc);
  });

  test('retry on error in change-stream', async () => {
    initReplicationState(replica, ['zero_data'], '02');

    const {promise: hasRetried, resolve: retried} = resolver<true>();
    const syncer = new IncrementalSyncer(
      REPLICA_ID,
      {
        subscribe: vi
          .fn()
          .mockImplementationOnce(() => Promise.resolve(downstream))
          .mockImplementation(() => {
            retried(true);
            return resolver().promise;
          }),
      },
      replica,
      'CONCURRENT',
    );

    void syncer.run(lc);

    downstream.fail(new Error('doh'));

    expect(await hasRetried).toBe(true);

    void syncer.stop(lc);
  });
});
