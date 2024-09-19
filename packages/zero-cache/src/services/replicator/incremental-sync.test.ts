import {LogContext} from '@rocicorp/logger';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {promiseVoid} from 'shared/src/resolved-promises.js';
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  MockedFunction,
  test,
  vi,
} from 'vitest';
import {expectTables, initDB} from 'zero-cache/src/test/lite.js';
import {Subscription} from 'zero-cache/src/types/subscription.js';
import {Database} from 'zqlite/src/db.js';
import {dropReplicationSlot, testDBs} from '../../test/db.js';
import type {PostgresDB} from '../../types/pg.js';
import {
  Downstream,
  SubscriberContext,
} from '../change-streamer/change-streamer.js';
import {replicationSlot} from '../change-streamer/pg/initial-sync.js';
import {IncrementalSyncer} from './incremental-sync.js';
import {Notifier} from './notifier.js';
import {initChangeLog} from './schema/change-log.js';
import {initReplicationState} from './schema/replication-state.js';
import {ReplicationMessages} from './test-utils.js';

const REPLICA_ID = 'incremental_sync_test_id';

describe('replicator/incremental-sync', {retry: 3}, () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  let replica: Database;
  let syncer: IncrementalSyncer;
  let downstream: Subscription<Downstream>;
  let subscribeFn: MockedFunction<
    (ctx: SubscriberContext) => Subscription<Downstream>
  >;
  let checkpointFn: MockedFunction<
    (commits: number, notifier: Notifier) => Promise<void>
  >;

  beforeEach(async () => {
    lc = createSilentLogContext();
    upstream = await testDBs.create('incremental_sync_test_upstream');
    replica = new Database(lc, ':memory:');
    downstream = Subscription.create();
    subscribeFn = vi.fn();
    checkpointFn = vi.fn();
    syncer = new IncrementalSyncer(
      REPLICA_ID,
      {subscribe: subscribeFn.mockImplementation(() => downstream)},
      replica,
      {
        maybeCheckpoint: checkpointFn.mockReturnValue(promiseVoid),
        stop: vi.fn(),
      },
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
  };

  const issues = new ReplicationMessages({issues: 'issueID'});
  const orgIssues = new ReplicationMessages({issues: ['orgID', 'issueID']});
  const fooBarBaz = new ReplicationMessages({foo: 'id', bar: 'id', baz: 'id'});

  const cases: Case[] = [
    {
      name: 'insert rows',
      setup: `
      CREATE TABLE issues(
        issueID INTEGER PRIMARY KEY,
        big INTEGER,
        flt REAL,
        bool BOOL,
        description TEXT,
        _0_version TEXT NOT NULL
      );
      `,
      downstream: [
        ['begin', issues.begin()],
        ['data', issues.insert('issues', {issueID: 123})],
        ['data', issues.insert('issues', {issueID: 456, bool: false})],
        ['commit', issues.commit(), {watermark: '06'}],

        ['begin', issues.begin()],
        [
          'data',
          issues.insert('issues', {
            issueID: 789,
            big: 9223372036854775807n,
          }),
        ],
        ['data', issues.insert('issues', {issueID: 987, bool: true})],
        ['data', issues.insert('issues', {issueID: 234, flt: 123.456})],
        ['commit', issues.commit(), {watermark: '0b'}],
      ],
      data: {
        issues: [
          {
            issueID: 123n,
            big: null,
            flt: null,
            bool: null,
            description: null,
            ['_0_version']: '02',
          },
          {
            issueID: 456n,
            big: null,
            flt: null,
            bool: 0n,
            description: null,
            ['_0_version']: '02',
          },
          {
            issueID: 789n,
            big: 9223372036854775807n,
            flt: null,
            bool: null,
            description: null,
            ['_0_version']: '06',
          },
          {
            issueID: 987n,
            big: null,
            flt: null,
            bool: 1n,
            description: null,
            ['_0_version']: '06',
          },
          {
            issueID: 234n,
            big: null,
            flt: 123.456,
            bool: null,
            description: null,
            ['_0_version']: '06',
          },
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '02',
            table: 'issues',
            op: 's',
            rowKey: '{"issueID":123}',
          },
          {
            stateVersion: '02',
            table: 'issues',
            op: 's',
            rowKey: '{"issueID":456}',
          },
          {
            stateVersion: '06',
            table: 'issues',
            op: 's',
            rowKey: '{"issueID":789}',
          },
          {
            stateVersion: '06',
            table: 'issues',
            op: 's',
            rowKey: '{"issueID":987}',
          },
          {
            stateVersion: '06',
            table: 'issues',
            op: 's',
            rowKey: '{"issueID":234}',
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
        PRIMARY KEY("orgID", "issueID")
      );
      `,
      downstream: [
        ['begin', orgIssues.begin()],
        ['data', orgIssues.insert('issues', {orgID: 1, issueID: 123})],
        ['data', orgIssues.insert('issues', {orgID: 1, issueID: 456})],
        ['data', orgIssues.insert('issues', {orgID: 2, issueID: 789})],
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
            {orgID: 1, issueID: 123},
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
            bool: null,
            ['_0_version']: '02',
          },
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '02',
            table: 'issues',
            op: 's',
            rowKey: '{"issueID":789,"orgID":2}',
          },
          {
            stateVersion: '06',
            table: 'issues',
            op: 's',
            rowKey: '{"issueID":456,"orgID":1}',
          },
          {
            stateVersion: '06',
            table: 'issues',
            op: 'd',
            rowKey: '{"issueID":123,"orgID":1}',
          },
          {
            stateVersion: '06',
            table: 'issues',
            op: 's',
            rowKey: '{"issueID":123,"orgID":2}',
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
        description TEXT,
        _0_version TEXT NOT NULL,
        PRIMARY KEY("orgID", "issueID")
      );
      `,
      downstream: [
        ['begin', orgIssues.begin()],
        ['data', orgIssues.insert('issues', {orgID: 1, issueID: 123})],
        ['data', orgIssues.insert('issues', {orgID: 1, issueID: 456})],
        ['data', orgIssues.insert('issues', {orgID: 2, issueID: 789})],
        ['data', orgIssues.insert('issues', {orgID: 2, issueID: 987})],
        ['commit', orgIssues.commit(), {watermark: '07'}],

        ['begin', orgIssues.begin()],
        ['data', orgIssues.delete('issues', {orgID: 1, issueID: 123})],
        ['data', orgIssues.delete('issues', {orgID: 1, issueID: 456})],
        ['data', orgIssues.delete('issues', {orgID: 2, issueID: 987})],
        ['commit', orgIssues.commit(), {watermark: '0c'}],
      ],
      data: {
        issues: [
          {orgID: 2n, issueID: 789n, description: null, ['_0_version']: '02'},
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '02',
            table: 'issues',
            op: 's',
            rowKey: '{"issueID":789,"orgID":2}',
          },
          {
            stateVersion: '07',
            table: 'issues',
            op: 'd',
            rowKey: '{"issueID":123,"orgID":1}',
          },
          {
            stateVersion: '07',
            table: 'issues',
            op: 'd',
            rowKey: '{"issueID":456,"orgID":1}',
          },
          {
            stateVersion: '07',
            table: 'issues',
            op: 'd',
            rowKey: '{"issueID":987,"orgID":2}',
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
            rowKey: null,
          },
          {
            stateVersion: '0e',
            table: 'foo',
            op: 't',
            rowKey: null,
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
        description TEXT,
        _0_version TEXT NOT NULL,
        PRIMARY KEY("orgID", "issueID")
      );
      `,
      downstream: [
        ['begin', orgIssues.begin()],
        ['data', orgIssues.insert('issues', {orgID: 1, issueID: 123})],
        [
          'data',
          orgIssues.update(
            'issues',
            {orgID: 1, issueID: 456},
            {orgID: 1, issueID: 123},
          ),
        ],
        ['data', orgIssues.insert('issues', {orgID: 2, issueID: 789})],
        ['data', orgIssues.delete('issues', {orgID: 2, issueID: 789})],
        [
          'data',
          orgIssues.update('issues', {
            orgID: 1,
            issueID: 456,
            description: 'foo',
          }),
        ],
        ['commit', orgIssues.commit(), {watermark: '08'}],
      ],
      data: {
        issues: [
          {orgID: 1n, issueID: 456n, description: 'foo', ['_0_version']: '02'},
        ],
        ['_zero.ChangeLog']: [
          {
            stateVersion: '02',
            table: 'issues',
            op: 'd',
            rowKey: '{"issueID":123,"orgID":1}',
          },
          {
            stateVersion: '02',
            table: 'issues',
            op: 's',
            rowKey: '{"issueID":456,"orgID":1}',
          },
          {
            stateVersion: '02',
            table: 'issues',
            op: 'd',
            rowKey: '{"issueID":789,"orgID":2}',
          },
        ],
      },
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

      let commits = 0;
      for (const change of c.downstream) {
        downstream.push(change);
        if (change[0] === 'commit') {
          // Wait for the transaction to be committed to the replica.
          commits++;
          await Promise.race([versionReady.next(), syncing]);
        }
      }

      expect(checkpointFn).toHaveBeenCalledTimes(commits);
      expectTables(replica, c.data, 'bigint');
    });
  }
});
