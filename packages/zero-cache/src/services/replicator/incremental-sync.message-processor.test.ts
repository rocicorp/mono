import {LogContext} from '@rocicorp/logger';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {beforeEach, describe, expect, test} from 'vitest';
import {StatementRunner} from 'zero-cache/src/db/statements.js';
import {expectTables} from 'zero-cache/src/test/lite.js';
import {Database} from 'zqlite/src/db.js';
import {DownstreamChange} from '../change-streamer/change-streamer.js';
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
    messages: DownstreamChange[];
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
        ['begin', messages.begin()],
        ['data', messages.insert('foo', {id: 123})],
        ['data', messages.insert('foo', {id: 234})],
        ['commit', messages.commit(), {watermark: '07'}],

        // Induce a failure with a missing 'begin' message.
        ['data', messages.insert('foo', {id: 456})],
        ['data', messages.insert('foo', {id: 345})],
        ['commit', messages.commit(), {watermark: '0a'}],

        // This should be dropped.
        ['begin', messages.begin()],
        ['data', messages.insert('foo', {id: 789})],
        ['data', messages.insert('foo', {id: 987})],
        ['commit', messages.commit(), {watermark: '0e'}],
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
        ['begin', messages.begin()],
        ['data', messages.insert('foo', {id: 123})],
        ['commit', messages.commit(), {watermark: '08'}],

        ['begin', messages.begin()],
        ['data', messages.insert('foo', {id: 234})],
        ['commit', messages.commit(), {watermark: '0c'}],

        // Simulate Postgres resending the first two transactions (e.g. reconnecting after
        // the acknowledgements were lost). Both should be dropped (i.e. rolled back).
        ['begin', messages.begin()],
        ['data', messages.insert('foo', {id: 123})],
        // For good measure, add new inserts that didn't appear in the previous transaction.
        // This would not actually happen, but it allows us to confirm that no mutations
        // are applied.
        ['data', messages.insert('foo', {id: 456})],
        ['commit', messages.commit(), {watermark: '08'}],

        ['begin', messages.begin()],
        ['data', messages.insert('foo', {id: 234})],
        // For good measure, add new inserts that didn't appear in the previous transaction.
        // This would not actually happen, but it allows us to confirm that no mutations
        // are applied.
        ['data', messages.insert('foo', {id: 654})],
        ['commit', messages.commit(), {watermark: '0c'}],

        // This should succeed.
        ['begin', messages.begin()],
        ['data', messages.insert('foo', {id: 789})],
        ['data', messages.insert('foo', {id: 987})],
        ['commit', messages.commit(), {watermark: '0g'}],
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
        (_: LogContext, err: unknown) => failures.push(err),
      );

      for (const msg of c.messages) {
        if (processor.processMessage(lc, msg) > 0) {
          versionChanges++;
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

      const {watermark} = getSubscriptionState(new StatementRunner(replica));
      expect(watermark).toBe(c.acknowledged.at(-1));
    });
  }

  test('abort', () => {
    const processor = createMessageProcessor(replica);

    expect(replica.inTransaction).toBe(false);
    processor.processMessage(lc, ['begin', {tag: 'begin'}]);
    expect(replica.inTransaction).toBe(true);
    processor.abort(lc);
    expect(replica.inTransaction).toBe(false);
  });
});
