import {LogContext} from '@rocicorp/logger';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {beforeEach, describe, expect, test} from 'vitest';
import {StatementRunner} from 'zero-cache/src/db/statements.js';
import {expectTables} from 'zero-cache/src/test/lite.js';
import {Database} from 'zqlite/src/db.js';
import {Change} from '../change-streamer/schema/change.js';
import {initChangeLog} from './schema/change-log.js';
import {
  getSubscriptionState,
  initReplicationState,
} from './schema/replication-state.js';
import {createMessageProcessor, ReplicationMessages} from './test-utils.js';

describe('replicator/message-processor', () => {
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
    initChangeLog(replica);
  });

  type Case = {
    name: string;
    messages: [watermark: string, change: Change][];
    acknowledged: string[];
    expectedVersionChanges: number;
    replicated: Record<string, object[]>;
    expectFailure: boolean;
  };

  const messages = new ReplicationMessages({foo: 'id'});

  const cases: Case[] = [
    {
      name: 'malformed replication stream',
      messages: [
        ['04', messages.begin()],
        ['05', messages.insert('foo', {id: 123})],
        ['06', messages.insert('foo', {id: 234})],
        ['07', messages.commit()],

        // Induce a failure with a missing 'begin' message.
        ['08', messages.insert('foo', {id: 456})],
        ['09', messages.insert('foo', {id: 345})],
        ['0a', messages.commit()],

        // This should be dropped.
        ['0b', messages.begin()],
        ['0c', messages.insert('foo', {id: 789})],
        ['0d', messages.insert('foo', {id: 987})],
        ['0e', messages.commit()],
      ],
      acknowledged: ['07'],
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
      messages: [
        ['05', messages.begin()],
        ['06', messages.insert('foo', {id: 123})],
        ['08', messages.commit()],

        ['09', messages.begin()],
        ['0a', messages.insert('foo', {id: 234})],
        ['0c', messages.commit()],

        // Simulate Postgres resending the first two transactions (e.g. reconnecting after
        // the acknowledgements were lost). Both should be dropped (i.e. rolled back).
        ['05', messages.begin()],
        ['06', messages.insert('foo', {id: 123})],
        // For good measure, add new inserts that didn't appear in the previous transaction.
        // This would not actually happen, but it allows us to confirm that no mutations
        // are applied.
        ['07', messages.insert('foo', {id: 456})],
        ['08', messages.commit()],

        ['09', messages.begin()],
        ['0a', messages.insert('foo', {id: 234})],
        // For good measure, add new inserts that didn't appear in the previous transaction.
        // This would not actually happen, but it allows us to confirm that no mutations
        // are applied.
        ['0b', messages.insert('foo', {id: 654})],
        ['0c', messages.commit()],

        // This should succeed.
        ['0d', messages.begin()],
        ['0e', messages.insert('foo', {id: 789})],
        ['0f', messages.insert('foo', {id: 987})],
        ['0g', messages.commit()],
      ],
      acknowledged: [
        '08',
        '0c',
        '08', // Note: The acknowledgements should be resent
        '0c', //       so that Postgres can track progress.
        '0g',
      ],
      expectedVersionChanges: 3,
      replicated: {
        foo: [
          {id: 123, big: null, ['_0_version']: '02'},
          {id: 234, big: null, ['_0_version']: '08'},
          {id: 789, big: null, ['_0_version']: '0c'},
          {id: 987, big: null, ['_0_version']: '0c'},
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

      const processor = createMessageProcessor(
        replica,
        (lsn: string) => acknowledgements.push(lsn),
        () => versionChanges++,
        (_: LogContext, err: unknown) => failures.push(err),
      );

      for (const [watermark, msg] of c.messages) {
        processor.processMessage(lc, watermark, msg);
      }

      expect(acknowledgements).toEqual(c.acknowledged);
      expect(versionChanges).toBe(c.expectedVersionChanges);
      if (c.expectFailure) {
        expect(failures[0]).toBeInstanceOf(Error);
      } else {
        expect(failures).toHaveLength(0);
      }
      expectTables(replica, c.replicated);

      const {watermark} = getSubscriptionState(new StatementRunner(replica));
      expect(watermark).toBe(c.acknowledged.at(-1));
    });
  }

  test('abort', () => {
    const processor = createMessageProcessor(replica);

    expect(replica.inTransaction).toBe(false);
    processor.processMessage(lc, '02', {tag: 'begin'});
    expect(replica.inTransaction).toBe(true);
    processor.abort(lc);
    expect(replica.inTransaction).toBe(false);
  });
});
