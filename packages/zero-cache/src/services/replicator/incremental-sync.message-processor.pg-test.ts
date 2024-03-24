import {
  afterEach,
  beforeEach,
  describe,
  expect,
  jest,
  test,
} from '@jest/globals';
import {Lock} from '@rocicorp/lock';
import type {Pgoutput} from 'pg-logical-replication';
import type postgres from 'postgres';
import {expectTables, testDBs} from '../../test/db.js';
import {createSilentLogContext} from '../../test/logger.js';
import {MessageProcessor, setupReplicationTables} from './incremental-sync.js';

describe('replicator/message-processor', () => {
  let replica: postgres.Sql;

  beforeEach(async () => {
    replica = await testDBs.create('message_processor_test_replica');

    await replica`
    CREATE TABLE "foo" (
      id int4 PRIMARY KEY,
      big int8,
      _0_version VARCHAR(38) NOT NULL
    );
    `;
    await replica.begin(tx =>
      setupReplicationTables(
        createSilentLogContext(),
        'unused_id',
        tx,
        'postgres:///unused_upstream',
      ),
    );
  });

  afterEach(async () => {
    await testDBs.drop(replica);
  });

  const FOO_RELATION: Pgoutput.MessageRelation = {
    tag: 'relation',
    relationOid: 123,
    schema: 'public',
    name: 'foo',
    replicaIdentity: 'default',
    columns: [], // Unused at the moment
    keyColumns: ['id'],
  } as const;

  type Case = {
    name: string;
    messages: Record<string, Pgoutput.Message[]>;
    acknowledged: string[];
    replicated: Record<string, object[]>;
    expectFailure: boolean;
  };

  const cases: Case[] = [
    {
      name: 'apply error (out of range)',
      messages: {
        '0/1': [
          {tag: 'begin', commitLsn: '0/e', commitTime: 123n, xid: 123},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 123}},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 234}},
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '0/e',
            commitEndLsn: '0/11',
            commitTime: 123n,
          },
        ],

        // Induce a failure with an out-of-range integer.
        '0/20': [
          {tag: 'begin', commitLsn: '0/30', commitTime: 125n, xid: 125},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 456}},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 2 ** 34}}, // out of range
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '0/30',
            commitEndLsn: '0/31',
            commitTime: 125n,
          },
        ],

        // This should be dropped.
        '0/40': [
          {tag: 'begin', commitLsn: '0/50', commitTime: 127n, xid: 127},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 789}},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 987}},
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '0/50',
            commitEndLsn: '0/51',
            commitTime: 127n,
          },
        ],
      },
      acknowledged: ['0/e'],
      replicated: {
        foo: [
          {id: 123, big: null, ['_0_version']: '0e'},
          {id: 234, big: null, ['_0_version']: '0e'},
        ],
      },
      expectFailure: true,
    },
    {
      name: 'apply error (duplicate key value)',
      messages: {
        '0/1': [
          {tag: 'begin', commitLsn: '0/e', commitTime: 123n, xid: 123},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 123}},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 234}},
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '0/e',
            commitEndLsn: '0/11',
            commitTime: 123n,
          },
        ],

        // Induce a failure with an out-of-range integer.
        '0/20': [
          {tag: 'begin', commitLsn: '0/30', commitTime: 125n, xid: 125},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 456}},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 234}}, // duplicate key
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '0/30',
            commitEndLsn: '0/31',
            commitTime: 125n,
          },
        ],

        // This should be dropped.
        '0/40': [
          {tag: 'begin', commitLsn: '0/50', commitTime: 127n, xid: 127},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 789}},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 987}},
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '0/50',
            commitEndLsn: '0/51',
            commitTime: 127n,
          },
        ],
      },
      acknowledged: ['0/e'],
      replicated: {
        foo: [
          {id: 123, big: null, ['_0_version']: '0e'},
          {id: 234, big: null, ['_0_version']: '0e'},
        ],
      },
      expectFailure: true,
    },
    {
      name: 'replication stream error',
      messages: {
        '0/1': [
          {tag: 'begin', commitLsn: '0/e', commitTime: 123n, xid: 123},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 123}},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 234}},
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '0/e',
            commitEndLsn: '0/11',
            commitTime: 123n,
          },
        ],

        // Induce a failure with a missing 'begin' message.
        '0/20': [
          {tag: 'insert', relation: FOO_RELATION, new: {id: 456}},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 345}},
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '0/30',
            commitEndLsn: '0/31',
            commitTime: 125n,
          },
        ],

        // This should be dropped.
        '0/40': [
          {tag: 'begin', commitLsn: '0/50', commitTime: 127n, xid: 127},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 789}},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 987}},
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '0/50',
            commitEndLsn: '0/51',
            commitTime: 127n,
          },
        ],
      },
      acknowledged: ['0/e'],
      replicated: {
        foo: [
          {id: 123, big: null, ['_0_version']: '0e'},
          {id: 234, big: null, ['_0_version']: '0e'},
        ],
      },
      expectFailure: true,
    },
    {
      name: 'transaction replay',
      messages: {
        '0/1': [
          {tag: 'begin', commitLsn: '0/a', commitTime: 123n, xid: 123},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 123}},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 234}},
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '0/a',
            commitEndLsn: '0/11',
            commitTime: 123n,
          },
        ],

        // Simulate a reconnect with the replication stream sending the same tx again.
        '0/2': [
          {tag: 'begin', commitLsn: '0/a', commitTime: 123n, xid: 123},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 123}},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 234}},
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '0/a',
            commitEndLsn: '0/11',
            commitTime: 123n,
          },
        ],

        // This should succeed.
        '0/40': [
          {tag: 'begin', commitLsn: '0/f', commitTime: 127n, xid: 127},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 789}},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 987}},
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '0/f',
            commitEndLsn: '0/51',
            commitTime: 127n,
          },
        ],
      },
      acknowledged: [
        '0/a',
        '0/a', // Note: The acknowledgement should be resent.
        '0/f',
      ],
      replicated: {
        foo: [
          {id: 123, big: null, ['_0_version']: '0a'},
          {id: 234, big: null, ['_0_version']: '0a'},
          {id: 789, big: null, ['_0_version']: '0f'},
          {id: 987, big: null, ['_0_version']: '0f'},
        ],
      },
      expectFailure: false,
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const acknowledge = jest.fn();
      const failService = jest.fn();

      const processor = new MessageProcessor(
        replica,
        {
          // Unused in this test.
          publications: [],
          tables: {},
        },
        new Lock(),
        acknowledge,
        failService,
      );

      const lc = createSilentLogContext();
      const pending: Promise<unknown>[] = [];
      for (const [lsn, msgs] of Object.entries(c.messages)) {
        for (const msg of msgs) {
          pending.push(processor.processMessage(lc, lsn, msg));
        }
      }
      await Promise.all(pending);

      expect(acknowledge.mock.calls.map(call => call[0])).toEqual(
        c.acknowledged,
      );
      expect(failService).toHaveBeenCalledTimes(c.expectFailure ? 1 : 0);
      await expectTables(replica, c.replicated);
    });
  }
});
