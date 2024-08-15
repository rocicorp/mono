import {LogContext} from '@rocicorp/logger';
import {Database} from 'better-sqlite3';
import type {Pgoutput} from 'pg-logical-replication';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {DbFile, expectTables} from 'zero-cache/src/test/lite.js';
import {MessageProcessor} from './incremental-sync.js';
import {initChangeLog} from './schema/change-log.js';
import {
  getReplicationState,
  initReplicationState,
} from './schema/replication.js';

describe('replicator/message-processor', () => {
  let lc: LogContext;
  let replicaFile: DbFile;
  let replica: Database;

  beforeEach(() => {
    lc = createSilentLogContext();
    replicaFile = new DbFile('message_processor_test_replica');
    replica = replicaFile.connect();

    replica.exec(`
    CREATE TABLE "foo" (
      id INTEGER PRIMARY KEY,
      big INTEGER,
      _0_version TEXT NOT NULL
    );
    `);

    initReplicationState(replica, ['zero_data', 'zero_metadata'], '0/2');
    initChangeLog(replica);
  });

  afterEach(async () => {
    await replicaFile.unlink();
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
    expectedVersionChanges: number;
    replicated: Record<string, object[]>;
    expectFailure: boolean;
  };

  const cases: Case[] = [
    {
      name: 'malformed replication stream',
      messages: {
        '0/1': [
          {tag: 'begin', commitLsn: '0/d', commitTime: 123n, xid: 123},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 123}},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 234}},
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '0/d',
            commitEndLsn: '0/e',
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
      expectedVersionChanges: 1,
      replicated: {
        foo: [
          {id: 123, big: null, ['_0_version']: '02'},
          {id: 234, big: null, ['_0_version']: '02'},
        ],
      },
      expectFailure: true,
    },
    {
      name: 'transaction replay',
      messages: {
        '0/1': [
          {tag: 'begin', commitLsn: '0/2', commitTime: 123n, xid: 123},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 123}},
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '0/3',
            commitEndLsn: '0/4',
            commitTime: 123n,
          },
        ],

        '0/5': [
          {tag: 'begin', commitLsn: '0/5', commitTime: 123n, xid: 123},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 234}},
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '0/9',
            commitEndLsn: '0/a',
            commitTime: 123n,
          },
        ],

        // Simulate Postgres resending the first two transactions (e.g. reconnecting after
        // the acknowledgements were lost). Both should be dropped (i.e. rolled back).
        '0/6': [
          {tag: 'begin', commitLsn: '0/2', commitTime: 123n, xid: 123},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 123}},
          // For good measure, add new inserts that didn't appear in the previous transaction.
          // This would not actually happen, but it allows us to confirm that no mutations
          // are applied.
          {tag: 'insert', relation: FOO_RELATION, new: {id: 456}},
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '0/3',
            commitEndLsn: '0/4',
            commitTime: 123n,
          },
        ],

        '0/7': [
          {tag: 'begin', commitLsn: '0/5', commitTime: 123n, xid: 123},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 234}},
          // For good measure, add new inserts that didn't appear in the previous transaction.
          // This would not actually happen, but it allows us to confirm that no mutations
          // are applied.
          {tag: 'insert', relation: FOO_RELATION, new: {id: 654}},
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '0/9',
            commitEndLsn: '0/a',
            commitTime: 123n,
          },
        ],

        // This should succeed.
        '0/40': [
          {tag: 'begin', commitLsn: '0/e', commitTime: 127n, xid: 127},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 789}},
          {tag: 'insert', relation: FOO_RELATION, new: {id: 987}},
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '0/e',
            commitEndLsn: '0/f',
            commitTime: 127n,
          },
        ],
      },
      acknowledged: [
        '0/4',
        '0/a',
        '0/4', // Note: The acknowledgements should be resent
        '0/a', //       so that Postgres can track progress.
        '0/f',
      ],
      expectedVersionChanges: 3,
      replicated: {
        foo: [
          {id: 123, big: null, ['_0_version']: '02'},
          {id: 234, big: null, ['_0_version']: '04'},
          {id: 789, big: null, ['_0_version']: '0a'},
          {id: 987, big: null, ['_0_version']: '0a'},
        ],
      },
      expectFailure: false,
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      const failures: unknown[] = [];
      const acknowledgements: string[] = [];
      let versionChanges = 0;

      const processor = new MessageProcessor(
        replica,
        (lsn: string) => acknowledgements.push(lsn),
        () => versionChanges++,
        (_: LogContext, err: unknown) => failures.push(err),
      );

      for (const [lsn, msgs] of Object.entries(c.messages)) {
        for (const msg of msgs) {
          processor.processMessage(lc, lsn, msg);
        }
      }

      expect(acknowledgements).toEqual(c.acknowledged);
      expect(versionChanges).toBe(c.expectedVersionChanges);
      if (c.expectFailure) {
        expect(failures[0]).toBeInstanceOf(Error);
      } else {
        expect(failures).toHaveLength(0);
      }
      expectTables(replica, c.replicated);

      const {watermark} = getReplicationState(replica);
      expect(watermark).toBe(c.acknowledged.at(-1));
    });
  }
});
