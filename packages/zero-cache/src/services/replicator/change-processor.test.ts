import type {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, test} from 'vitest';
import type {JSONObject} from '../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../shared/src/must.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {
  listIndexes,
  listTables,
  type LiteTableSpecWithReplicationStatus,
} from '../../db/lite-tables.ts';
import type {LiteIndexSpec} from '../../db/specs.ts';
import {StatementRunner} from '../../db/statements.ts';
import {expectTables, initDB} from '../../test/lite.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {ChangeProcessor} from './change-processor.ts';
import {ColumnMetadataStore} from './schema/column-metadata.ts';
import {
  getSubscriptionState,
  initReplicationState,
} from './schema/replication-state.ts';
import {createChangeProcessor, ReplicationMessages} from './test-utils.ts';

describe('replicator/change-processor', () => {
  let lc: LogContext;
  let servingReplica: Database;
  let servingProcessor: ChangeProcessor;
  let backupReplica: Database;
  let backupProcessor: ChangeProcessor;

  beforeEach(() => {
    lc = createSilentLogContext();
    servingReplica = new Database(lc, ':memory:');
    initReplicationState(servingReplica, ['zero_data'], '02');
    servingProcessor = new ChangeProcessor(
      new StatementRunner(servingReplica),
      'serving',
      (_, err) => {
        throw err;
      },
    );
    backupReplica = new Database(lc, ':memory:');
    initReplicationState(backupReplica, ['zero_data'], '02');
    backupProcessor = new ChangeProcessor(
      new StatementRunner(backupReplica),
      'backup',
      (_, err) => {
        throw err;
      },
    );
  });

  type Case = {
    name: string;
    setup: string;
    downstream: ChangeStreamData[];
    data: Record<string, Record<string, unknown>[]>;
    tableSpecs?: LiteTableSpecWithReplicationStatus[];
    indexSpecs?: LiteIndexSpec[];
  };

  const issues = new ReplicationMessages({issues: ['issueID', 'bool']});
  const full = new ReplicationMessages(
    {full: ['id', 'bool', 'desc']},
    'public',
    'full',
  );
  const orgIssues = new ReplicationMessages({
    issues: ['orgID', 'issueID', 'bool'],
  });
  const fooBarBaz = new ReplicationMessages({foo: 'id', bar: 'id', baz: 'id'});
  const tables = new ReplicationMessages({transaction: 'column'});

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
        json2 JSONB,
        time TIMESTAMPTZ,
        bytes bytesa,
        intArray int4[],
        _0_version TEXT,
        PRIMARY KEY(issueID, bool)
      );
      `,
      downstream: [
        ['begin', issues.begin(), {commitWatermark: '06'}],
        ['data', issues.insert('issues', {issueID: 123, bool: true})],
        ['data', issues.insert('issues', {issueID: 456, bool: false})],
        ['commit', issues.commit(), {watermark: '06'}],

        ['begin', issues.begin(), {commitWatermark: '0b'}],
        [
          'data',
          issues.insert('issues', {
            issueID: 789,
            bool: true,
            big: 9223372036854775807n,
            json: [{foo: 'bar', baz: 123}],
            json2: true,
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
            json2: null,
            time: null,
            bytes: null,
            intArray: null,
            ['_0_version']: '06',
          },
          {
            issueID: 456n,
            big: null,
            flt: null,
            bool: 0n,
            description: null,
            json: null,
            json2: null,
            time: null,
            bytes: null,
            intArray: null,
            ['_0_version']: '06',
          },
          {
            issueID: 789n,
            big: 9223372036854775807n,
            flt: null,
            bool: 1n,
            description: null,
            json: '[{"foo":"bar","baz":123}]',
            json2: 'true',
            time: 1728345600123456n,
            bytes: Buffer.from('world'),
            intArray: '[3,2,1]',
            ['_0_version']: '0b',
          },
          {
            issueID: 987n,
            big: null,
            flt: null,
            bool: 1n,
            description: null,
            json: null,
            json2: null,
            time: null,
            bytes: null,
            intArray: null,
            ['_0_version']: '0b',
          },
          {
            issueID: 234n,
            big: null,
            flt: 123.456,
            bool: 0n,
            description: null,
            json: null,
            json2: null,
            time: null,
            bytes: null,
            intArray: null,
            ['_0_version']: '0b',
          },
        ],
        ['_zero.changeLog2']: [
          {
            stateVersion: '06',
            pos: 0n,
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":123}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '06',
            pos: 1n,
            table: 'issues',
            op: 's',
            rowKey: '{"bool":0,"issueID":456}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0b',
            pos: 0n,
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":789}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0b',
            pos: 1n,
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":987}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0b',
            pos: 2n,
            table: 'issues',
            op: 's',
            rowKey: '{"bool":0,"issueID":234}',
            backfillingColumnVersions: '{}',
          },
        ],
      },
    },
    {
      name: 'partial update rows',
      setup: `
      CREATE TABLE issues(
        issueID INTEGER,
        bool BOOL,
        big INTEGER,
        flt REAL,
        description TEXT,
        json JSON,
        _0_version TEXT,
        PRIMARY KEY(issueID, bool)
      );
      INSERT INTO issues (issueID, bool, big, flt, description, json, _0_version)
        VALUES (123, true, 9223372036854775807, 123.456, 'hello', 'world', '06');
      `,
      downstream: [
        ['begin', issues.begin(), {commitWatermark: '0a'}],
        [
          'data',
          issues.update('issues', {
            issueID: 123,
            bool: true,
            description: 'bello',
          }),
        ],
        [
          'data',
          issues.update('issues', {
            issueID: 123,
            bool: true,
            json: {wor: 'ld'},
          }),
        ],
        ['commit', issues.commit(), {watermark: '0a'}],
      ],
      data: {
        issues: [
          {
            issueID: 123n,
            big: 9223372036854775807n,
            flt: 123.456,
            bool: 1n,
            description: 'bello',
            json: '{"wor":"ld"}',
            ['_0_version']: '0a',
          },
        ],
        ['_zero.changeLog2']: [
          {
            stateVersion: '0a',
            pos: 1n,
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":123}',
            backfillingColumnVersions: '{}',
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
        _0_version TEXT,
        PRIMARY KEY("orgID", "issueID", "bool")
      );
      `,
      downstream: [
        ['begin', orgIssues.begin(), {commitWatermark: '06'}],
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

        ['begin', orgIssues.begin(), {commitWatermark: '0a'}],
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
            ['_0_version']: '0a',
          },
          {
            orgID: 1n,
            issueID: 456n,
            description: 'foo',
            bool: 1n,
            ['_0_version']: '0a',
          },
          {
            orgID: 2n,
            issueID: 789n,
            description: null,
            bool: 1n,
            ['_0_version']: '06',
          },
        ],
        ['_zero.changeLog2']: [
          {
            stateVersion: '06',
            pos: 2n,
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":789,"orgID":2}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0a',
            pos: 0n,
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":456,"orgID":1}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0a',
            pos: 1n,
            table: 'issues',
            op: 'd',
            rowKey: '{"bool":1,"issueID":123,"orgID":1}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0a',
            pos: 2n,
            table: 'issues',
            op: 's',
            rowKey: '{"bool":0,"issueID":123,"orgID":2}',
            backfillingColumnVersions: '{}',
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
        _0_version TEXT,
        PRIMARY KEY("orgID", "issueID","bool")
      );
      `,
      downstream: [
        ['begin', orgIssues.begin(), {commitWatermark: '07'}],
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

        ['begin', orgIssues.begin(), {commitWatermark: '0c'}],
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
            ['_0_version']: '07',
          },
        ],
        ['_zero.changeLog2']: [
          {
            stateVersion: '07',
            pos: 2n,
            table: 'issues',
            op: 's',
            rowKey: '{"bool":0,"issueID":789,"orgID":2}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0c',
            pos: 0n,
            table: 'issues',
            op: 'd',
            rowKey: '{"bool":1,"issueID":123,"orgID":1}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0c',
            pos: 1n,
            table: 'issues',
            op: 'd',
            rowKey: '{"bool":0,"issueID":456,"orgID":1}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0c',
            pos: 2n,
            table: 'issues',
            op: 'd',
            rowKey: '{"bool":1,"issueID":987,"orgID":2}',
            backfillingColumnVersions: '{}',
          },
        ],
      },
    },
    {
      name: 'truncate tables',
      setup: `
      CREATE TABLE foo(id INTEGER PRIMARY KEY, _0_version TEXT);
      CREATE TABLE bar(id INTEGER PRIMARY KEY, _0_version TEXT);
      CREATE TABLE baz(id INTEGER PRIMARY KEY, _0_version TEXT);
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
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

        ['begin', fooBarBaz.begin(), {commitWatermark: '0i'}],
        ['data', fooBarBaz.truncate('foo')],
        ['data', fooBarBaz.insert('foo', {id: 101})],
        ['commit', fooBarBaz.commit(), {watermark: '0i'}],
      ],
      data: {
        foo: [{id: 101n, ['_0_version']: '0i'}],
        bar: [
          {id: 4n, ['_0_version']: '0e'},
          {id: 5n, ['_0_version']: '0e'},
          {id: 6n, ['_0_version']: '0e'},
        ],
        baz: [],
        ['_zero.changeLog2']: [
          {
            backfillingColumnVersions: '{}',
            op: 's',
            pos: 0n,
            rowKey: '{"id":1}',
            stateVersion: '0e',
            table: 'foo',
          },
          {
            backfillingColumnVersions: '{}',
            op: 's',
            pos: 1n,
            rowKey: '{"id":2}',
            stateVersion: '0e',
            table: 'foo',
          },
          {
            backfillingColumnVersions: '{}',
            op: 's',
            pos: 2n,
            rowKey: '{"id":3}',
            stateVersion: '0e',
            table: 'foo',
          },
          {
            stateVersion: '0e',
            pos: 3n,
            table: 'bar',
            op: 's',
            rowKey: '{"id":4}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: 4n,
            table: 'bar',
            op: 's',
            rowKey: '{"id":5}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: 5n,
            table: 'bar',
            op: 's',
            rowKey: '{"id":6}',
            backfillingColumnVersions: '{}',
          },
          {
            backfillingColumnVersions: '{}',
            op: 's',
            pos: 6n,
            rowKey: '{"id":7}',
            stateVersion: '0e',
            table: 'baz',
          },
          {
            backfillingColumnVersions: '{}',
            op: 's',
            pos: 7n,
            rowKey: '{"id":8}',
            stateVersion: '0e',
            table: 'baz',
          },
          {
            backfillingColumnVersions: '{}',
            op: 's',
            pos: 8n,
            rowKey: '{"id":9}',
            stateVersion: '0e',
            table: 'baz',
          },
          {
            stateVersion: '0e',
            pos: -1n,
            table: 'foo',
            op: 't',
            rowKey: '0e',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0i',
            pos: -1n,
            table: 'foo',
            op: 't',
            rowKey: '0i',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0i',
            pos: 0n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":101}',
            backfillingColumnVersions: '{}',
          },
        ],
      },
    },
    {
      name: 'replica identity full',
      setup: `
      CREATE TABLE full(
        id "INTEGER|NOT_NULL",
        bool BOOL,
        desc TEXT,
        _0_version TEXT
      );
      CREATE UNIQUE INDEX full_pk ON full (id ASC);
      `,
      downstream: [
        ['begin', full.begin(), {commitWatermark: '06'}],
        ['data', full.insert('full', {id: 123, bool: true, desc: null})],
        ['data', full.insert('full', {id: 456, bool: false, desc: null})],
        ['data', full.insert('full', {id: 789, bool: false, desc: null})],
        ['commit', full.commit(), {watermark: '06'}],

        ['begin', full.begin(), {commitWatermark: '0b'}],
        [
          'data',
          full.update(
            'full',
            {id: 123, bool: false, desc: 'foobar'},
            {id: 123, bool: true, desc: null},
          ),
        ],
        [
          'data',
          full.update(
            'full',
            {id: 987, bool: true, desc: 'barfoo'},
            {id: 456, bool: false, desc: null},
          ),
        ],
        ['data', full.delete('full', {id: 789, bool: false, desc: null})],
        ['commit', issues.commit(), {watermark: '0b'}],
      ],
      data: {
        full: [
          {id: 123n, bool: 0n, desc: 'foobar', ['_0_version']: '0b'},
          {id: 987n, bool: 1n, desc: 'barfoo', ['_0_version']: '0b'},
        ],
        ['_zero.changeLog2']: [
          {
            op: 's',
            rowKey: '{"id":123}',
            stateVersion: '0b',
            pos: 1n,
            table: 'full',
            backfillingColumnVersions: '{}',
          },
          {
            op: 'd',
            rowKey: '{"id":456}',
            stateVersion: '0b',
            pos: 2n,
            table: 'full',
            backfillingColumnVersions: '{}',
          },
          {
            op: 's',
            rowKey: '{"id":987}',
            stateVersion: '0b',
            pos: 3n,
            table: 'full',
            backfillingColumnVersions: '{}',
          },
          {
            op: 'd',
            rowKey: '{"id":789}',
            stateVersion: '0b',
            pos: 4n,
            table: 'full',
            backfillingColumnVersions: '{}',
          },
        ],
      },
    },
    {
      name: 'upsert (resumptive replication)',
      setup: `
      CREATE TABLE foo(
        id INT PRIMARY KEY,
        desc TEXT,
        _0_version TEXT
      );
      INSERT INTO foo (id, desc) VALUES (1, 'one');

      CREATE TABLE full(
        id INT PRIMARY KEY,
        bool BOOL,
        desc TEXT,
        _0_version TEXT
      );
      INSERT INTO full (id, bool, desc) VALUES (2, 0, 'two');
      `,
      downstream: [
        ['begin', full.begin(), {commitWatermark: '06'}],
        ['data', fooBarBaz.insert('foo', {id: 1, desc: 'replaced one'})],
        ['data', fooBarBaz.update('foo', {id: 789, desc: null})],
        ['data', fooBarBaz.update('foo', {id: 234, desc: 'woo'}, {id: 999})],
        ['data', fooBarBaz.delete('foo', {id: 1000})],
        [
          'data',
          full.insert('full', {id: 2, bool: true, desc: 'replaced two'}),
        ],
        [
          'data',
          full.update(
            'full',
            {id: 321, bool: false, desc: 'voo'},
            {id: 333, bool: true, desc: 'did not exist'},
          ),
        ],
        [
          'data',
          full.update(
            'full',
            {id: 456, bool: false, desc: null},
            {id: 456, bool: false, desc: 'did not exist'},
          ),
        ],
        [
          'data',
          full.delete('full', {id: 2000, bool: false, desc: 'does not exist'}),
        ],
        ['commit', full.commit(), {watermark: '06'}],
      ],
      data: {
        foo: [
          {id: 1n, desc: 'replaced one', ['_0_version']: '06'},
          {id: 789n, desc: null, ['_0_version']: '06'},
          {id: 234n, desc: 'woo', ['_0_version']: '06'},
        ],
        full: [
          {id: 2n, bool: 1n, desc: 'replaced two', ['_0_version']: '06'},
          {id: 321n, bool: 0n, desc: 'voo', ['_0_version']: '06'},
          {id: 456n, bool: 0n, desc: null, ['_0_version']: '06'},
        ],
        ['_zero.changeLog2']: [
          {
            op: 's',
            rowKey: '{"id":1}',
            stateVersion: '06',
            pos: 0n,
            table: 'foo',
            backfillingColumnVersions: '{}',
          },
          {
            op: 's',
            rowKey: '{"id":789}',
            stateVersion: '06',
            pos: 1n,
            table: 'foo',
            backfillingColumnVersions: '{}',
          },
          {
            op: 'd',
            rowKey: '{"id":999}',
            stateVersion: '06',
            pos: 2n,
            table: 'foo',
            backfillingColumnVersions: '{}',
          },
          {
            op: 's',
            rowKey: '{"id":234}',
            stateVersion: '06',
            pos: 3n,
            table: 'foo',
            backfillingColumnVersions: '{}',
          },
          {
            op: 'd',
            rowKey: '{"id":1000}',
            stateVersion: '06',
            pos: 4n,
            table: 'foo',
            backfillingColumnVersions: '{}',
          },
          {
            op: 's',
            rowKey: '{"id":2}',
            stateVersion: '06',
            pos: 5n,
            table: 'full',
            backfillingColumnVersions: '{}',
          },
          {
            op: 'd',
            rowKey: '{"id":333}',
            stateVersion: '06',
            pos: 6n,
            table: 'full',
            backfillingColumnVersions: '{}',
          },
          {
            op: 's',
            rowKey: '{"id":321}',
            stateVersion: '06',
            pos: 7n,
            table: 'full',
            backfillingColumnVersions: '{}',
          },
          {
            op: 's',
            rowKey: '{"id":456}',
            stateVersion: '06',
            pos: 9n,
            table: 'full',
            backfillingColumnVersions: '{}',
          },
          {
            op: 'd',
            rowKey: '{"id":2000}',
            stateVersion: '06',
            pos: 10n,
            table: 'full',
            backfillingColumnVersions: '{}',
          },
        ],
      },
    },
    {
      name: 'reserved words in DML',
      setup: `
      CREATE TABLE "transaction" (
        "column" INTEGER PRIMARY KEY,
        "trigger" INTEGER,
        "index" INTEGER,
        _0_version TEXT
      );
      `,
      downstream: [
        ['begin', orgIssues.begin(), {commitWatermark: '07'}],
        ['data', tables.truncate('transaction')],
        [
          'data',
          tables.insert('transaction', {column: 1, trigger: 2, index: 3}),
        ],
        [
          'data',
          tables.update(
            'transaction',
            {column: 2, trigger: 3, index: 4},
            {column: 1},
          ),
        ],
        ['data', tables.delete('transaction', {column: 2})],
        ['commit', orgIssues.commit(), {watermark: '07'}],
      ],
      data: {
        transaction: [],
        ['_zero.changeLog2']: [
          {
            stateVersion: '07',
            pos: -1n,
            table: 'transaction',
            op: 't',
            rowKey: '07',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '07',
            pos: 1n,
            table: 'transaction',
            op: 'd',
            rowKey: '{"column":1}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '07',
            pos: 3n,
            table: 'transaction',
            op: 'd',
            rowKey: '{"column":2}',
            backfillingColumnVersions: '{}',
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
        _0_version TEXT,
        PRIMARY KEY("orgID", "issueID", "bool")
      );
      `,
      downstream: [
        ['begin', orgIssues.begin(), {commitWatermark: '08'}],
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
            ['_0_version']: '08',
          },
        ],
        ['_zero.changeLog2']: [
          {
            stateVersion: '08',
            pos: 1n,
            table: 'issues',
            op: 'd',
            rowKey: '{"bool":1,"issueID":123,"orgID":1}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '08',
            pos: 4n,
            table: 'issues',
            op: 'd',
            rowKey: '{"bool":0,"issueID":789,"orgID":2}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '08',
            pos: 5n,
            table: 'issues',
            op: 's',
            rowKey: '{"bool":0,"issueID":456,"orgID":1}',
            backfillingColumnVersions: '{}',
          },
        ],
      },
    },
    {
      name: 'create table',
      setup: ``,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        [
          'data',
          fooBarBaz.createTable(
            {
              schema: 'public',
              name: 'foo',
              columns: {
                id: {pos: 0, dataType: 'varchar'},
                count: {pos: 1, dataType: 'int8'},
                bool: {pos: 3, dataType: 'bool'},
                serial: {
                  pos: 4,
                  dataType: 'int4',
                  dflt: "nextval('issues_serial_seq'::regclass)",
                  notNull: true,
                },
              },
              primaryKey: ['id'],
            },
            {
              metadata: {
                rowKey: {
                  type: 'index',
                  columns: ['id', 'serial'],
                },
              },
              backfill: {
                serial: {upstreamID: 123},
              },
            },
          ),
        ],
        [
          'data',
          fooBarBaz.insert('foo', {id: 'bar', count: 2, bool: true, serial: 1}),
        ],
        [
          'data',
          fooBarBaz.insert('foo', {
            id: 'baz',
            count: 3,
            bool: false,
            serial: 2,
          }),
        ],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 'bar', count: 2n, bool: 1n, serial: 1n, ['_0_version']: '0e'},
          {id: 'baz', count: 3n, bool: 0n, serial: 2n, ['_0_version']: '0e'},
        ],
        ['_zero.changeLog2']: [
          {
            stateVersion: '0e',
            pos: -1n,
            table: 'foo',
            op: 'r',
            rowKey: '0e',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: 0n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":"bar"}',
            backfillingColumnVersions: '{"serial":"0e"}',
          },
          {
            stateVersion: '0e',
            pos: 1n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":"baz"}',
            backfillingColumnVersions: '{"serial":"0e"}',
          },
        ],
        ['_zero.tableMetadata']: [
          {
            schema: 'public',
            table: 'foo',
            metadata: '{"rowKey":{"type":"index","columns":["id","serial"]}}',
          },
        ],
        ['_zero.column_metadata']: [
          {
            backfill: null,
            character_max_length: null,
            column_name: 'id',
            is_array: 0n,
            is_enum: 0n,
            is_not_null: 0n,
            table_name: 'foo',
            upstream_type: 'varchar',
          },
          {
            backfill: null,
            character_max_length: null,
            column_name: 'count',
            is_array: 0n,
            is_enum: 0n,
            is_not_null: 0n,
            table_name: 'foo',
            upstream_type: 'int8',
          },
          {
            backfill: null,
            character_max_length: null,
            column_name: 'bool',
            is_array: 0n,
            is_enum: 0n,
            is_not_null: 0n,
            table_name: 'foo',
            upstream_type: 'bool',
          },
          {
            backfill: '{"upstreamID":123}',
            character_max_length: null,
            column_name: 'serial',
            is_array: 0n,
            is_enum: 0n,
            is_not_null: 1n,
            table_name: 'foo',
            upstream_type: 'int4',
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
              elemPgTypeClass: null,
              pos: 1,
            },
            count: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
            bool: {
              characterMaximumLength: null,
              dataType: 'bool',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
            serial: {
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 4,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 5,
            },
          },
          backfilling: ['serial'],
        },
      ],
      indexSpecs: [],
    },
    {
      name: 'rename table',
      setup: `
        CREATE TABLE foo(id INT8, _0_version TEXT);
        INSERT INTO foo(id, _0_version) VALUES (1, '00');
        INSERT INTO foo(id, _0_version) VALUES (2, '00');
        INSERT INTO foo(id, _0_version) VALUES (3, '00');

        INSERT INTO "_zero.tableMetadata" ("schema", "table", "metadata")
          VALUES ('public', 'foo', '{"rowKey":{"columns":["id"]}}');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        ['data', fooBarBaz.renameTable('foo', 'bar')],
        ['data', fooBarBaz.insert('bar', {id: 4})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        bar: [
          {id: 1n, ['_0_version']: '0e'},
          {id: 2n, ['_0_version']: '0e'},
          {id: 3n, ['_0_version']: '0e'},
          {id: 4n, ['_0_version']: '0e'},
        ],
        ['_zero.changeLog2']: [
          {
            stateVersion: '0e',
            pos: -1n,
            table: 'foo',
            op: 'r',
            rowKey: '0e',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: 0n,
            table: 'bar',
            op: 's',
            rowKey: '{"id":4}',
            backfillingColumnVersions: '{}',
          },
        ],
        ['_zero.tableMetadata']: [
          {
            schema: 'public',
            table: 'bar',
            metadata: '{"rowKey":{"columns":["id"]}}',
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
              elemPgTypeClass: null,
              pos: 1,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
          },
          backfilling: [],
        },
      ],
      indexSpecs: [],
    },
    {
      name: 'add column',
      setup: `
        CREATE TABLE foo(id INT8, _0_version TEXT);
        INSERT INTO foo(id, _0_version) VALUES (1, '00');
        INSERT INTO foo(id, _0_version) VALUES (2, '00');
        INSERT INTO foo(id, _0_version) VALUES (3, '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
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
        [
          'data',
          fooBarBaz.addColumn(
            'foo',
            'newJSON',
            {
              pos: 10,
              dataType: 'json',
            },
            {
              tableMetadata: {rowKey: {columns: ['id']}},
              backfill: {upstreamID: 988},
            },
          ),
        ],
        [
          'data',
          fooBarBaz.insert('foo', {
            id: 4,
            newInt: 321,
            newBool: false,
            newJSON: true,
          }),
        ],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {
            id: 1n,
            newInt: 123n,
            newBool: 1n,
            newJSON: null,
            ['_0_version']: '0e',
          },
          {
            id: 2n,
            newInt: 123n,
            newBool: 1n,
            newJSON: null,
            ['_0_version']: '0e',
          },
          {
            id: 3n,
            newInt: 123n,
            newBool: 1n,
            newJSON: null,
            ['_0_version']: '0e',
          },
          {
            id: 4n,
            newInt: 321n,
            newBool: 0n,
            newJSON: 'true',
            ['_0_version']: '0e',
          },
        ],
        ['_zero.changeLog2']: [
          {
            stateVersion: '0e',
            pos: -1n,
            table: 'foo',
            op: 'r',
            rowKey: '0e',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: 0n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
            backfillingColumnVersions: '{"newJSON":"0e"}',
          },
        ],
        ['_zero.tableMetadata']: [
          {
            schema: 'public',
            table: 'foo',
            metadata: '{"rowKey":{"columns":["id"]}}',
          },
        ],
        ['_zero.column_metadata']: [
          {
            backfill: null,
            character_max_length: null,
            column_name: 'newInt',
            is_array: 0n,
            is_enum: 0n,
            is_not_null: 0n,
            table_name: 'foo',
            upstream_type: 'int8',
          },
          {
            backfill: null,
            character_max_length: null,
            column_name: 'newBool',
            is_array: 0n,
            is_enum: 0n,
            is_not_null: 0n,
            table_name: 'foo',
            upstream_type: 'bool',
          },
          {
            backfill: '{"upstreamID":988}',
            character_max_length: null,
            column_name: 'newJSON',
            is_array: 0n,
            is_enum: 0n,
            is_not_null: 0n,
            table_name: 'foo',
            upstream_type: 'json',
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
              elemPgTypeClass: null,
              pos: 1,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
            newInt: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: '123',
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
            newBool: {
              characterMaximumLength: null,
              dataType: 'bool',
              dflt: '1',
              notNull: false,
              elemPgTypeClass: null,
              pos: 4,
            },
            newJSON: {
              characterMaximumLength: null,
              dataType: 'json',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 5,
            },
          },
          backfilling: ['newJSON'],
        },
      ],
      indexSpecs: [],
    },
    {
      name: 'drop column',
      setup: `
        CREATE TABLE foo(id INT8, dropMe TEXT, _0_version TEXT);
        INSERT INTO foo(id, dropMe, _0_version) VALUES (1, 'bye', '00');
        INSERT INTO foo(id, dropMe, _0_version) VALUES (2, 'bye', '00');
        INSERT INTO foo(id, dropMe, _0_version) VALUES (3, 'bye', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        ['data', fooBarBaz.update('foo', {id: 3, dropMe: 'stillDropped'})],
        ['data', fooBarBaz.dropColumn('foo', 'dropMe')],
        ['data', fooBarBaz.insert('foo', {id: 4})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 1n, ['_0_version']: '0e'},
          {id: 2n, ['_0_version']: '0e'},
          {id: 3n, ['_0_version']: '0e'},
          {id: 4n, ['_0_version']: '0e'},
        ],
        ['_zero.changeLog2']: [
          {
            stateVersion: '0e',
            pos: 0n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":3}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: -1n,
            table: 'foo',
            op: 'r',
            rowKey: '0e',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: 1n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
            backfillingColumnVersions: '{}',
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
              elemPgTypeClass: null,
              pos: 1,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
          },
          backfilling: [],
        },
      ],
      indexSpecs: [],
    },
    {
      name: 'rename column',
      setup: `
        CREATE TABLE foo(id INT8, renameMe TEXT, _0_version TEXT);
        CREATE UNIQUE INDEX foo_pkey ON foo (id ASC);
        INSERT INTO foo(id, renameMe, _0_version) VALUES (1, 'hel', '00');
        INSERT INTO foo(id, renameMe, _0_version) VALUES (2, 'low', '00');
        INSERT INTO foo(id, renameMe, _0_version) VALUES (3, 'orl', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
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
          {id: 1n, newName: 'hel', ['_0_version']: '0e'},
          {id: 2n, newName: 'low', ['_0_version']: '0e'},
          {id: 3n, newName: 'olrd', ['_0_version']: '0e'},
          {id: 4n, newName: 'yay', ['_0_version']: '0e'},
        ],
        ['_zero.changeLog2']: [
          {
            stateVersion: '0e',
            pos: 0n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":3}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: -1n,
            table: 'foo',
            op: 'r',
            rowKey: '0e',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: 1n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
            backfillingColumnVersions: '{}',
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
              elemPgTypeClass: null,
              pos: 1,
            },
            newName: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
          },
          backfilling: [],
        },
      ],
      indexSpecs: [
        {
          name: 'foo_pkey',
          tableName: 'foo',
          columns: {id: 'ASC'},
          unique: true,
        },
      ],
    },
    {
      name: 'change column nullability',
      setup: `
        CREATE TABLE foo(id INT8, nolz TEXT, _0_version TEXT);
        CREATE UNIQUE INDEX foo_pkey ON foo (id ASC);
        INSERT INTO foo(id, nolz, _0_version) VALUES (1, 'hel', '00');
        INSERT INTO foo(id, nolz, _0_version) VALUES (2, 'low', '00');
        INSERT INTO foo(id, nolz, _0_version) VALUES (3, 'orl', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        ['data', fooBarBaz.update('foo', {id: 3, nolz: 'olrd'})],
        [
          'data',
          fooBarBaz.updateColumn(
            'foo',
            {name: 'nolz', spec: {pos: 1, dataType: 'TEXT'}},
            {name: 'nolz', spec: {pos: 1, dataType: 'TEXT', notNull: true}},
          ),
        ],
        ['data', fooBarBaz.insert('foo', {id: 4, nolz: 'yay'})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 1n, nolz: 'hel', ['_0_version']: '0e'},
          {id: 2n, nolz: 'low', ['_0_version']: '0e'},
          {id: 3n, nolz: 'olrd', ['_0_version']: '0e'},
          {id: 4n, nolz: 'yay', ['_0_version']: '0e'},
        ],
        ['_zero.changeLog2']: [
          {
            stateVersion: '0e',
            pos: 0n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":3}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: -1n,
            table: 'foo',
            op: 'r',
            rowKey: '0e',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: 1n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
            backfillingColumnVersions: '{}',
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
              elemPgTypeClass: null,
              pos: 1,
            },
            nolz: {
              characterMaximumLength: null,
              dataType: 'TEXT|NOT_NULL',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
          },
          backfilling: [],
        },
      ],
      indexSpecs: [
        {
          name: 'foo_pkey',
          tableName: 'foo',
          columns: {id: 'ASC'},
          unique: true,
        },
      ],
    },
    {
      name: 'change column default and nullability',
      setup: `
        CREATE TABLE foo(id INT8, nolz TEXT, _0_version TEXT);
        CREATE UNIQUE INDEX foo_pkey ON foo (id ASC);
        INSERT INTO foo(id, nolz, _0_version) VALUES (1, 'hel', '00');
        INSERT INTO foo(id, nolz, _0_version) VALUES (2, 'low', '00');
        INSERT INTO foo(id, nolz, _0_version) VALUES (3, 'orl', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        ['data', fooBarBaz.update('foo', {id: 3, nolz: 'olrd'})],
        [
          'data',
          fooBarBaz.updateColumn(
            'foo',
            {name: 'nolz', spec: {pos: 1, dataType: 'TEXT'}},
            {
              name: 'nolz',
              spec: {pos: 1, dataType: 'TEXT', notNull: true, dflt: 'now()'},
            },
          ),
        ],
        ['data', fooBarBaz.insert('foo', {id: 4, nolz: 'yay'})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 1n, nolz: 'hel', ['_0_version']: '0e'},
          {id: 2n, nolz: 'low', ['_0_version']: '0e'},
          {id: 3n, nolz: 'olrd', ['_0_version']: '0e'},
          {id: 4n, nolz: 'yay', ['_0_version']: '0e'},
        ],
        ['_zero.changeLog2']: [
          {
            stateVersion: '0e',
            pos: 0n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":3}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: -1n,
            table: 'foo',
            op: 'r',
            rowKey: '0e',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: 1n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
            backfillingColumnVersions: '{}',
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
              elemPgTypeClass: null,
              pos: 1,
            },
            nolz: {
              characterMaximumLength: null,
              dataType: 'TEXT|NOT_NULL',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
          },
          backfilling: [],
        },
      ],
      indexSpecs: [
        {
          name: 'foo_pkey',
          tableName: 'foo',
          columns: {id: 'ASC'},
          unique: true,
        },
      ],
    },
    {
      name: 'rename indexed column',
      setup: `
        CREATE TABLE foo(id INT8, renameMe TEXT, _0_version TEXT);
        CREATE UNIQUE INDEX foo_pkey ON foo (id ASC);
        CREATE UNIQUE INDEX foo_rename_me ON foo (renameMe);
        INSERT INTO foo(id, renameMe, _0_version) VALUES (1, 'hel', '00');
        INSERT INTO foo(id, renameMe, _0_version) VALUES (2, 'low', '00');
        INSERT INTO foo(id, renameMe, _0_version) VALUES (3, 'orl', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
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
          {id: 1n, newName: 'hel', ['_0_version']: '0e'},
          {id: 2n, newName: 'low', ['_0_version']: '0e'},
          {id: 3n, newName: 'olrd', ['_0_version']: '0e'},
          {id: 4n, newName: 'yay', ['_0_version']: '0e'},
        ],
        ['_zero.changeLog2']: [
          {
            stateVersion: '0e',
            pos: 0n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":3}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: -1n,
            table: 'foo',
            op: 'r',
            rowKey: '0e',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: 1n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
            backfillingColumnVersions: '{}',
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
              elemPgTypeClass: null,
              pos: 1,
            },
            newName: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
          },
          backfilling: [],
        },
      ],
      indexSpecs: [
        {
          name: 'foo_pkey',
          tableName: 'foo',
          columns: {id: 'ASC'},
          unique: true,
        },
        {
          name: 'foo_rename_me',
          tableName: 'foo',
          columns: {newName: 'ASC'},
          unique: true,
        },
      ],
    },
    {
      name: 'retype column',
      setup: `
        CREATE TABLE foo(id INT8, num TEXT, _0_version TEXT);
        CREATE UNIQUE INDEX foo_pkey ON foo (id ASC);
        INSERT INTO foo(id, num, _0_version) VALUES (1, '3', '00');
        INSERT INTO foo(id, num, _0_version) VALUES (2, '2', '00');
        INSERT INTO foo(id, num, _0_version) VALUES (3, '3', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
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
          {id: 1n, num: 3n, ['_0_version']: '0e'},
          {id: 2n, num: 2n, ['_0_version']: '0e'},
          {id: 3n, num: 1n, ['_0_version']: '0e'},
          {id: 4n, num: 23n, ['_0_version']: '0e'},
        ],
        ['_zero.changeLog2']: [
          {
            stateVersion: '0e',
            pos: 0n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":3}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: -1n,
            table: 'foo',
            op: 'r',
            rowKey: '0e',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: 1n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
            backfillingColumnVersions: '{}',
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
              elemPgTypeClass: null,
              pos: 1,
            },
            num: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
          },
          backfilling: [],
        },
      ],
      indexSpecs: [
        {
          tableName: 'foo',
          name: 'foo_pkey',
          unique: true,
          columns: {id: 'ASC'},
        },
      ],
    },
    {
      name: 'retype column with indexes',
      setup: `
        CREATE TABLE foo(id INT8, num TEXT, _0_version TEXT);
        CREATE UNIQUE INDEX foo_pkey ON foo (id);
        CREATE UNIQUE INDEX foo_num ON foo (num);
        CREATE UNIQUE INDEX foo_id_num ON foo (id, num);
        INSERT INTO foo(id, num, _0_version) VALUES (1, '3', '00');
        INSERT INTO foo(id, num, _0_version) VALUES (2, '2', '00');
        INSERT INTO foo(id, num, _0_version) VALUES (3, '0', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
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
          {id: 1n, num: 3n, ['_0_version']: '0e'},
          {id: 2n, num: 2n, ['_0_version']: '0e'},
          {id: 3n, num: 1n, ['_0_version']: '0e'},
          {id: 4n, num: 23n, ['_0_version']: '0e'},
        ],
        ['_zero.changeLog2']: [
          {
            stateVersion: '0e',
            pos: 0n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":3}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: -1n,
            table: 'foo',
            op: 'r',
            rowKey: '0e',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: 1n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
            backfillingColumnVersions: '{}',
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
              elemPgTypeClass: null,
              pos: 1,
            },
            num: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
          },
          backfilling: [],
        },
      ],
      indexSpecs: [
        {
          name: 'foo_id_num',
          tableName: 'foo',
          columns: {id: 'ASC', num: 'ASC'},
          unique: true,
        },
        {
          name: 'foo_num',
          tableName: 'foo',
          columns: {num: 'ASC'},
          unique: true,
        },
        {
          name: 'foo_pkey',
          tableName: 'foo',
          columns: {id: 'ASC'},
          unique: true,
        },
      ],
    },
    {
      name: 'rename and retype column',
      setup: `
        CREATE TABLE foo(id INT8, numburr TEXT, _0_version TEXT);
        CREATE UNIQUE INDEX foo_pkey ON foo (id ASC);
        INSERT INTO foo(id, numburr, _0_version) VALUES (1, '3', '00');
        INSERT INTO foo(id, numburr, _0_version) VALUES (2, '2', '00');
        INSERT INTO foo(id, numburr, _0_version) VALUES (3, '3', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
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
          {id: 1n, number: 3n, ['_0_version']: '0e'},
          {id: 2n, number: 2n, ['_0_version']: '0e'},
          {id: 3n, number: 1n, ['_0_version']: '0e'},
          {id: 4n, number: 23n, ['_0_version']: '0e'},
        ],
        ['_zero.changeLog2']: [
          {
            stateVersion: '0e',
            pos: 0n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":3}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: -1n,
            table: 'foo',
            op: 'r',
            rowKey: '0e',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: 1n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
            backfillingColumnVersions: '{}',
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
              elemPgTypeClass: null,
              pos: 1,
            },
            number: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
          },
          backfilling: [],
        },
      ],
      indexSpecs: [
        {
          tableName: 'foo',
          name: 'foo_pkey',
          unique: true,
          columns: {id: 'ASC'},
        },
      ],
    },
    {
      name: 'drop table',
      setup: `
        CREATE TABLE foo(id INT8, _0_version TEXT);
        INSERT INTO foo(id, _0_version) VALUES (1, '00');
        INSERT INTO foo(id, _0_version) VALUES (2, '00');
        INSERT INTO foo(id, _0_version) VALUES (3, '00');
      
        INSERT INTO "_zero.tableMetadata" ("schema", "table", "metadata") VALUES
          ('public', 'foo', '{"rowKey":{"columns":["id"]}}'),
          ('public', 'bar', '{"rowKey":{"columns":["id"]}}');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        ['data', fooBarBaz.insert('foo', {id: 4})],
        ['data', fooBarBaz.dropTable('foo')],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        ['_zero.changeLog2']: [
          {
            stateVersion: '0e',
            pos: 0n,
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
            backfillingColumnVersions: '{}',
          },
          {
            stateVersion: '0e',
            pos: -1n,
            table: 'foo',
            op: 'r',
            rowKey: '0e',
            backfillingColumnVersions: '{}',
          },
        ],
        ['_zero.tableMetadata']: [
          {
            schema: 'public',
            table: 'bar',
            metadata: '{"rowKey":{"columns":["id"]}}',
          },
        ],
      },
      tableSpecs: [],
      indexSpecs: [],
    },
    {
      name: 'update table metadata',
      setup: `
        CREATE TABLE foo(id INT8, id2 INT8, _0_version TEXT);
      
        INSERT INTO "_zero.tableMetadata" ("schema", "table", "metadata") VALUES
          ('public', 'foo', '{"rowKey":{"columns":["id"]}}'),
          ('public', 'bar', '{"rowKey":{"columns":["id"]}}');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        [
          'data',
          {
            tag: 'update-table-metadata',
            table: {schema: 'public', name: 'foo'},
            old: {rowKey: {columns: []}},
            new: {rowKey: {type: 'index', columns: ['id2']}},
          },
        ],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        ['_zero.tableMetadata']: [
          {
            schema: 'public',
            table: 'foo',
            metadata: '{"rowKey":{"type":"index","columns":["id2"]}}',
          },
          {
            schema: 'public',
            table: 'bar',
            metadata: '{"rowKey":{"columns":["id"]}}',
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
              elemPgTypeClass: null,
              pos: 1,
            },
            id2: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
          },
          backfilling: [],
        },
      ],
      indexSpecs: [],
    },
    {
      name: 'create index',
      setup: `
        CREATE TABLE foo(id INT8, handle TEXT, _0_version TEXT);
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        [
          'data',
          fooBarBaz.createIndex({
            schema: 'public',
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
        ['_zero.changeLog2']: [
          {
            stateVersion: '0e',
            pos: -1n,
            table: 'foo',
            op: 'r',
            rowKey: '0e',
            backfillingColumnVersions: '{}',
          },
        ],
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
        CREATE TABLE foo(id INT8, handle TEXT, _0_version TEXT);
        CREATE INDEX keep_me ON foo (id DESC, handle ASC);
        CREATE INDEX drop_me ON foo (handle DESC);
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        ['data', fooBarBaz.dropIndex('drop_me')],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        ['_zero.changeLog2']: [],
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
    {
      name: 'reserved words in DDL',
      setup: ``,
      downstream: [
        ['begin', tables.begin(), {commitWatermark: '07'}],
        [
          'data',
          tables.createTable({
            schema: 'public',
            name: 'transaction',
            columns: {
              column: {pos: 0, dataType: 'int8'},
              commit: {pos: 1, dataType: 'int8'},
            },
            primaryKey: ['column'],
          }),
        ],
        [
          'data',
          tables.addColumn('transaction', 'trigger', {
            dataType: 'text',
            pos: 10,
          }),
        ],
        [
          'data',
          tables.updateColumn(
            'transaction',
            {
              name: 'trigger',
              spec: {dataType: 'text', pos: 10},
            },
            {
              name: 'index',
              spec: {dataType: 'text', pos: 10},
            },
          ),
        ],
        [
          'data',
          tables.updateColumn(
            'transaction',
            {
              name: 'index',
              spec: {dataType: 'text', pos: 10},
            },
            {
              name: 'index',
              spec: {dataType: 'int8', pos: 10},
            },
          ),
        ],
        ['data', tables.dropColumn('transaction', 'commit')],
        ['commit', orgIssues.commit(), {watermark: '07'}],
      ],
      data: {
        transaction: [],
        ['_zero.changeLog2']: [
          {
            stateVersion: '07',
            pos: -1n,
            table: 'transaction',
            op: 'r',
            rowKey: '07',
            backfillingColumnVersions: '{}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'transaction',
          columns: {
            column: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 1,
            },
            index: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
          },
          backfilling: [],
        },
      ],
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      for (const [replica, processor, log] of [
        [servingReplica, servingProcessor, 'all-entries'],
        [backupReplica, backupProcessor, 'only-backfills'],
      ] satisfies [
        Database,
        ChangeProcessor,
        'all-entries' | 'only-backfills',
      ][]) {
        initDB(replica, c.setup);

        for (const change of c.downstream) {
          processor.processMessage(lc, change);
        }

        if (log === 'all-entries') {
          expectTables(replica, c.data, 'bigint');
        } else {
          // Only log entries relevant to backfill
          expectTables(
            replica,
            {
              ...c.data,
              ['_zero.changeLog2']:
                c.data['_zero.changeLog2']?.filter(
                  log => log.backfillingColumnVersions !== '{}',
                ) ?? [],
            },
            'bigint',
          );
        }

        if (c.tableSpecs) {
          expect(
            listTables(replica).filter(t => !t.name.startsWith('_zero.')),
          ).toEqual(c.tableSpecs);
        }
        if (c.indexSpecs) {
          expect(listIndexes(replica)).toEqual(c.indexSpecs);
        }
      }
    });
  }
});

describe('replicator/change-processor-errors', () => {
  let lc: LogContext;
  let replica: Database;

  beforeEach(() => {
    lc = createSilentLogContext();
    replica = new Database(lc, ':memory:');

    replica.exec(`
    CREATE TABLE "foo" (
      id INTEGER PRIMARY KEY,
      big INTEGER,
      _0_version TEXT NOT NULL
    );
    `);

    initReplicationState(replica, ['zero_data', 'zero_metadata'], '02');
  });

  type Case = {
    name: string;
    messages: ChangeStreamData[];
    finalCommit: string;
    expectedVersionChanges: number;
    replicated: Record<string, object[]>;
    expectFailure: boolean;
  };

  const messages = new ReplicationMessages({foo: 'id'});

  const cases: Case[] = [
    {
      name: 'malformed replication stream',
      messages: [
        ['begin', messages.begin(), {commitWatermark: '07'}],
        ['data', messages.insert('foo', {id: 123})],
        ['data', messages.insert('foo', {id: 234})],
        ['commit', messages.commit(), {watermark: '07'}],

        // Induce a failure with a missing 'begin' message.
        ['data', messages.insert('foo', {id: 456})],
        ['data', messages.insert('foo', {id: 345})],
        ['commit', messages.commit(), {watermark: '0a'}],

        // This should be dropped.
        ['begin', messages.begin(), {commitWatermark: '0e'}],
        ['data', messages.insert('foo', {id: 789})],
        ['data', messages.insert('foo', {id: 987})],
        ['commit', messages.commit(), {watermark: '0e'}],
      ],
      finalCommit: '07',
      expectedVersionChanges: 1,
      replicated: {
        foo: [
          {id: 123, big: null, ['_0_version']: '07'},
          {id: 234, big: null, ['_0_version']: '07'},
        ],
      },
      expectFailure: true,
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      const failures: unknown[] = [];
      let versionChanges = 0;

      const processor = createChangeProcessor(
        replica,
        (_: LogContext, err: unknown) => failures.push(err),
      );

      for (const msg of c.messages) {
        if (processor.processMessage(lc, msg)) {
          versionChanges++;
        }
      }

      expect(versionChanges).toBe(c.expectedVersionChanges);
      if (c.expectFailure) {
        expect(failures[0]).toBeInstanceOf(Error);
      } else {
        expect(failures).toHaveLength(0);
      }
      expectTables(replica, c.replicated);

      const {watermark} = getSubscriptionState(new StatementRunner(replica));
      expect(watermark).toBe(c.finalCommit);
    });
  }

  test('rollback', () => {
    const processor = createChangeProcessor(replica);

    expect(replica.inTransaction).toBe(false);
    processor.processMessage(lc, [
      'begin',
      {tag: 'begin'},
      {commitWatermark: '0a'},
    ]);
    expect(replica.inTransaction).toBe(true);
    processor.processMessage(lc, ['rollback', {tag: 'rollback'}]);
    expect(replica.inTransaction).toBe(false);
  });

  test('abort', () => {
    const processor = createChangeProcessor(replica);

    expect(replica.inTransaction).toBe(false);
    processor.processMessage(lc, [
      'begin',
      {tag: 'begin'},
      {commitWatermark: '0e'},
    ]);
    expect(replica.inTransaction).toBe(true);
    processor.abort(lc);
    expect(replica.inTransaction).toBe(false);
  });
});

describe('replicator/column-metadata-integration', () => {
  let lc: LogContext;
  let replica: Database;
  let processor: ChangeProcessor;

  beforeEach(() => {
    lc = createSilentLogContext();
    replica = new Database(lc, ':memory:');
    initReplicationState(replica, ['zero_data', 'zero_metadata'], '02');
    processor = createChangeProcessor(replica);
  });

  test('create table writes metadata for all columns', () => {
    const messages = new ReplicationMessages({foo: 'id'});

    processor.processMessage(lc, [
      'begin',
      messages.begin(),
      {commitWatermark: '0e'},
    ]);
    processor.processMessage(lc, [
      'data',
      messages.createTable({
        schema: 'public',
        name: 'foo',
        columns: {
          id: {pos: 0, dataType: 'int8'},
          name: {pos: 1, dataType: 'varchar', characterMaximumLength: 255},
          active: {pos: 2, dataType: 'bool', notNull: true},
        },
        primaryKey: ['id'],
      }),
    ]);
    processor.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '0e'},
    ]);

    const store = ColumnMetadataStore.getInstance(replica);
    expect(store).toBeDefined();

    const idMetadata = must(store).getColumn('foo', 'id');
    expect(idMetadata).toEqual({
      upstreamType: 'int8',
      isNotNull: false,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
      isBackfilling: false,
    });

    const nameMetadata = must(store).getColumn('foo', 'name');
    expect(nameMetadata).toEqual({
      upstreamType: 'varchar',
      isNotNull: false,
      isEnum: false,
      isArray: false,
      characterMaxLength: 255,
      isBackfilling: false,
    });

    const activeMetadata = must(store).getColumn('foo', 'active');
    expect(activeMetadata).toEqual({
      upstreamType: 'bool',
      isNotNull: true,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
      isBackfilling: false,
    });
  });

  test('rename table updates metadata table name', () => {
    const messages = new ReplicationMessages({foo: 'id'});

    // Create table first
    processor.processMessage(lc, [
      'begin',
      messages.begin(),
      {commitWatermark: '0d'},
    ]);
    processor.processMessage(lc, [
      'data',
      messages.createTable({
        schema: 'public',
        name: 'foo',
        columns: {
          id: {pos: 0, dataType: 'int8'},
          value: {pos: 1, dataType: 'text'},
        },
        primaryKey: ['id'],
      }),
    ]);
    processor.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '0d'},
    ]);

    // Rename table
    processor.processMessage(lc, [
      'begin',
      messages.begin(),
      {commitWatermark: '0e'},
    ]);
    processor.processMessage(lc, ['data', messages.renameTable('foo', 'bar')]);
    processor.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '0e'},
    ]);

    const store = ColumnMetadataStore.getInstance(replica);
    expect(store).toBeDefined();

    // Old table name should not exist
    expect(must(store).getColumn('foo', 'id')).toBeUndefined();
    expect(must(store).getColumn('foo', 'value')).toBeUndefined();

    // New table name should have the metadata
    expect(must(store).getColumn('bar', 'id')).toEqual({
      upstreamType: 'int8',
      isNotNull: false,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
      isBackfilling: false,
    });

    expect(must(store).getColumn('bar', 'value')).toEqual({
      upstreamType: 'text',
      isNotNull: false,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
      isBackfilling: false,
    });
  });

  test('add column writes metadata', () => {
    const messages = new ReplicationMessages({foo: 'id'});

    // Create table first
    processor.processMessage(lc, [
      'begin',
      messages.begin(),
      {commitWatermark: '0d'},
    ]);
    processor.processMessage(lc, [
      'data',
      messages.createTable({
        schema: 'public',
        name: 'foo',
        columns: {
          id: {pos: 0, dataType: 'int8'},
        },
        primaryKey: ['id'],
      }),
    ]);
    processor.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '0d'},
    ]);

    // Add column
    processor.processMessage(lc, [
      'begin',
      messages.begin(),
      {commitWatermark: '0e'},
    ]);
    processor.processMessage(lc, [
      'data',
      messages.addColumn('foo', 'score', {
        pos: 1,
        dataType: 'int4',
        dflt: '0',
        notNull: true,
      }),
    ]);
    processor.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '0e'},
    ]);

    const store = ColumnMetadataStore.getInstance(replica);
    expect(store).toBeDefined();

    const scoreMetadata = must(store).getColumn('foo', 'score');
    expect(scoreMetadata).toEqual({
      upstreamType: 'int4',
      isNotNull: true,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
      isBackfilling: false,
    });
  });

  test('update column (rename) updates metadata', () => {
    const messages = new ReplicationMessages({foo: 'id'});

    // Create table with initial column
    processor.processMessage(lc, [
      'begin',
      messages.begin(),
      {commitWatermark: '0d'},
    ]);
    processor.processMessage(lc, [
      'data',
      messages.createTable({
        schema: 'public',
        name: 'foo',
        columns: {
          id: {pos: 0, dataType: 'int8'},
          oldName: {pos: 1, dataType: 'text'},
        },
        primaryKey: ['id'],
      }),
    ]);
    processor.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '0d'},
    ]);

    // Rename column
    processor.processMessage(lc, [
      'begin',
      messages.begin(),
      {commitWatermark: '0e'},
    ]);
    processor.processMessage(lc, [
      'data',
      messages.updateColumn(
        'foo',
        {name: 'oldName', spec: {pos: 1, dataType: 'text'}},
        {name: 'newName', spec: {pos: 1, dataType: 'text'}},
      ),
    ]);
    processor.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '0e'},
    ]);

    const store = ColumnMetadataStore.getInstance(replica);
    expect(store).toBeDefined();

    // Old column name should not exist
    expect(must(store).getColumn('foo', 'oldName')).toBeUndefined();

    // New column name should have the metadata
    expect(must(store).getColumn('foo', 'newName')).toEqual({
      upstreamType: 'text',
      isNotNull: false,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
      isBackfilling: false,
    });
  });

  test('update column (change type) updates metadata', () => {
    const messages = new ReplicationMessages({foo: 'id'});

    // Create table with initial column
    processor.processMessage(lc, [
      'begin',
      messages.begin(),
      {commitWatermark: '0d'},
    ]);
    processor.processMessage(lc, [
      'data',
      messages.createTable({
        schema: 'public',
        name: 'foo',
        columns: {
          id: {pos: 0, dataType: 'int8'},
          value: {pos: 1, dataType: 'text'},
        },
        primaryKey: ['id'],
      }),
    ]);
    processor.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '0d'},
    ]);

    // Change column type
    processor.processMessage(lc, [
      'begin',
      messages.begin(),
      {commitWatermark: '0e'},
    ]);
    processor.processMessage(lc, [
      'data',
      messages.updateColumn(
        'foo',
        {name: 'value', spec: {pos: 1, dataType: 'text'}},
        {name: 'value', spec: {pos: 1, dataType: 'int8', notNull: true}},
      ),
    ]);
    processor.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '0e'},
    ]);

    const store = ColumnMetadataStore.getInstance(replica);
    expect(store).toBeDefined();

    // Metadata should reflect the new type and nullability
    expect(must(store).getColumn('foo', 'value')).toEqual({
      upstreamType: 'int8',
      isNotNull: true,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
      isBackfilling: false,
    });
  });

  test('drop column deletes metadata', () => {
    const messages = new ReplicationMessages({foo: 'id'});

    // Create table with columns
    processor.processMessage(lc, [
      'begin',
      messages.begin(),
      {commitWatermark: '0d'},
    ]);
    processor.processMessage(lc, [
      'data',
      messages.createTable({
        schema: 'public',
        name: 'foo',
        columns: {
          id: {pos: 0, dataType: 'int8'},
          dropMe: {pos: 1, dataType: 'text'},
          keepMe: {pos: 2, dataType: 'bool'},
        },
        primaryKey: ['id'],
      }),
    ]);
    processor.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '0d'},
    ]);

    // Drop column
    processor.processMessage(lc, [
      'begin',
      messages.begin(),
      {commitWatermark: '0e'},
    ]);
    processor.processMessage(lc, [
      'data',
      messages.dropColumn('foo', 'dropMe'),
    ]);
    processor.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '0e'},
    ]);

    const store = ColumnMetadataStore.getInstance(replica);
    expect(store).toBeDefined();

    // Dropped column metadata should not exist
    expect(must(store).getColumn('foo', 'dropMe')).toBeUndefined();

    // Other columns should still have metadata
    expect(must(store).getColumn('foo', 'id')).toBeDefined();
    expect(must(store).getColumn('foo', 'keepMe')).toBeDefined();
  });

  test('drop table deletes all metadata', () => {
    const messages = new ReplicationMessages({foo: 'id'});

    // Create table with columns
    processor.processMessage(lc, [
      'begin',
      messages.begin(),
      {commitWatermark: '0d'},
    ]);
    processor.processMessage(lc, [
      'data',
      messages.createTable({
        schema: 'public',
        name: 'foo',
        columns: {
          id: {pos: 0, dataType: 'int8'},
          name: {pos: 1, dataType: 'text'},
          count: {pos: 2, dataType: 'int4'},
        },
        primaryKey: ['id'],
      }),
    ]);
    processor.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '0d'},
    ]);

    // Drop table
    processor.processMessage(lc, [
      'begin',
      messages.begin(),
      {commitWatermark: '0e'},
    ]);
    processor.processMessage(lc, ['data', messages.dropTable('foo')]);
    processor.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '0e'},
    ]);

    const store = ColumnMetadataStore.getInstance(replica);
    expect(store).toBeDefined();

    // All metadata should be deleted
    expect(must(store).getColumn('foo', 'id')).toBeUndefined();
    expect(must(store).getColumn('foo', 'name')).toBeUndefined();
    expect(must(store).getColumn('foo', 'count')).toBeUndefined();

    // Table should have no metadata entries
    const tableMetadata = must(store).getTable('foo');
    expect(tableMetadata.size).toBe(0);
  });

  test('metadata tracks array and enum types', () => {
    const messages = new ReplicationMessages({foo: 'id'});

    processor.processMessage(lc, [
      'begin',
      messages.begin(),
      {commitWatermark: '0e'},
    ]);
    processor.processMessage(lc, [
      'data',
      messages.createTable({
        schema: 'public',
        name: 'foo',
        columns: {
          id: {pos: 0, dataType: 'int8'},
          tags: {
            pos: 1,
            dataType: 'text[]',
            elemPgTypeClass: 'b', // base type for text
          },
          status: {
            pos: 2,
            dataType: 'user_status',
            pgTypeClass: 'e', // enum
          },
        },
        primaryKey: ['id'],
      }),
    ]);
    processor.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '0e'},
    ]);

    const store = ColumnMetadataStore.getInstance(replica);
    expect(store).toBeDefined();

    const tagsMetadata = must(store).getColumn('foo', 'tags');
    expect(tagsMetadata).toEqual({
      upstreamType: 'text[]',
      isNotNull: false,
      isEnum: false,
      isArray: true,
      characterMaxLength: null,
      isBackfilling: false,
    });

    const statusMetadata = must(store).getColumn('foo', 'status');
    expect(statusMetadata).toEqual({
      upstreamType: 'user_status',
      isNotNull: false,
      isEnum: true,
      isArray: false,
      characterMaxLength: null,
      isBackfilling: false,
    });
  });

  test('getTable returns all column metadata for a table', () => {
    const messages = new ReplicationMessages({users: 'id'});

    processor.processMessage(lc, [
      'begin',
      messages.begin(),
      {commitWatermark: '0e'},
    ]);
    processor.processMessage(lc, [
      'data',
      messages.createTable({
        schema: 'public',
        name: 'users',
        columns: {
          id: {pos: 0, dataType: 'int8'},
          email: {pos: 1, dataType: 'varchar', characterMaximumLength: 255},
          isActive: {pos: 2, dataType: 'bool', notNull: true},
        },
        primaryKey: ['id'],
      }),
    ]);
    processor.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '0e'},
    ]);

    const store = ColumnMetadataStore.getInstance(replica);
    expect(store).toBeDefined();

    const tableMetadata = must(store).getTable('users');
    expect(tableMetadata.size).toBe(3);

    expect(tableMetadata.get('id')).toEqual({
      upstreamType: 'int8',
      isNotNull: false,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
      isBackfilling: false,
    });

    expect(tableMetadata.get('email')).toEqual({
      upstreamType: 'varchar',
      isNotNull: false,
      isEnum: false,
      isArray: false,
      characterMaxLength: 255,
      isBackfilling: false,
    });

    expect(tableMetadata.get('isActive')).toEqual({
      upstreamType: 'bool',
      isNotNull: true,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
      isBackfilling: false,
    });
  });
});
