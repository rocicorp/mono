import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import type {Pgoutput} from 'pg-logical-replication';
import {Queue} from 'shared/src/queue.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {expectTables, testDBs} from '../../test/db.js';
import {createSilentLogContext} from '../../test/logger.js';
import type {PostgresDB} from '../../types/pg.js';
import {MessageProcessor} from './incremental-sync.js';
import {InvalidationFilters} from './invalidation.js';
import type {VersionChange} from './replicator.js';
import {setupReplicationTables} from './tables/replication.js';

describe('replicator/message-processor', () => {
  let replica: PostgresDB;

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
    columns: [
      {
        flags: 1,
        name: 'id',
        typeOid: 23,
        typeMod: -1,
        typeSchema: null,
        typeName: null,
        parser: () => {},
      },
    ],
    keyColumns: ['id'],
  } as const;

  type Case = {
    name: string;
    messages: Record<string, Pgoutput.Message[]>;
    acknowledged: string[];
    versions: VersionChange[];
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
      versions: [{prevVersion: '00', newVersion: '0e', invalidations: {}}],
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
      versions: [{prevVersion: '00', newVersion: '0e', invalidations: {}}],
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
      versions: [{prevVersion: '00', newVersion: '0e', invalidations: {}}],
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
      versions: [
        {prevVersion: '00', newVersion: '0a', invalidations: {}},
        {prevVersion: '0a', newVersion: '0f', invalidations: {}},
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
      const failures = new Queue<unknown>();
      const acknowledgements = new Queue<string>();
      const versionChanges = new Queue<VersionChange>();

      const processor = new MessageProcessor(
        replica,
        {
          // Unused in this test.
          publications: [],
          tables: [],
        },
        new Lock(),
        new InvalidationFilters(),
        (lsn: string) => acknowledgements.enqueue(lsn),
        (v: VersionChange) => versionChanges.enqueue(v),
        (_: LogContext, err: unknown) => failures.enqueue(err),
      );

      const lc = createSilentLogContext();
      for (const [lsn, msgs] of Object.entries(c.messages)) {
        for (const msg of msgs) {
          processor.processMessage(lc, lsn, msg);
        }
      }

      for (const lsn of c.acknowledged) {
        expect(await acknowledgements.dequeue()).toBe(lsn);
      }
      for (const version of c.versions) {
        expect(await versionChanges.dequeue()).toEqual(version);
      }
      if (c.expectFailure) {
        expect(await failures.dequeue()).toBeInstanceOf(Error);
      }
      await expectTables(replica, c.replicated);
    });
  }
});
