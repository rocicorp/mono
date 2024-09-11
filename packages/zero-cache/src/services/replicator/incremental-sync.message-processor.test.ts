import {LogContext} from '@rocicorp/logger';
import type {Pgoutput} from 'pg-logical-replication';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {beforeEach, describe, expect, test} from 'vitest';
import {StatementRunner} from 'zero-cache/src/db/statements.js';
import {expectTables} from 'zero-cache/src/test/lite.js';
import {Database} from 'zqlite/src/db.js';
import {fromLexiVersion} from '../change-streamer/pg/lsn.js';
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
    messages: Record<string, Pgoutput.Message[]>;
    acknowledged: string[];
    expectedVersionChanges: number;
    replicated: Record<string, object[]>;
    expectFailure: boolean;
  };

  const messages = new ReplicationMessages({foo: 'id'});

  const cases: Case[] = [
    {
      name: 'malformed replication stream',
      messages: {
        '0/1': [
          messages.begin(),
          messages.insert('foo', {id: 123}),
          messages.insert('foo', {id: 234}),
          messages.commit('0/E'),
        ],

        // Induce a failure with a missing 'begin' message.
        '0/20': [
          messages.insert('foo', {id: 456}),
          messages.insert('foo', {id: 345}),
          messages.commit('0/31'),
        ],

        // This should be dropped.
        '0/40': [
          messages.begin(),
          messages.insert('foo', {id: 789}),
          messages.insert('foo', {id: 987}),
          messages.commit('0/51'),
        ],
      },
      acknowledged: ['0/E'],
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
          messages.begin(),
          messages.insert('foo', {id: 123}),
          messages.commit('0/4'),
        ],

        '0/5': [
          messages.begin(),
          messages.insert('foo', {id: 234}),
          messages.commit('0/A'),
        ],

        // Simulate Postgres resending the first two transactions (e.g. reconnecting after
        // the acknowledgements were lost). Both should be dropped (i.e. rolled back).
        '0/6': [
          messages.begin(),
          messages.insert('foo', {id: 123}),
          // For good measure, add new inserts that didn't appear in the previous transaction.
          // This would not actually happen, but it allows us to confirm that no mutations
          // are applied.
          messages.insert('foo', {id: 456}),
          messages.commit('0/4'),
        ],

        '0/7': [
          messages.begin(),
          messages.insert('foo', {id: 234}),
          // For good measure, add new inserts that didn't appear in the previous transaction.
          // This would not actually happen, but it allows us to confirm that no mutations
          // are applied.
          messages.insert('foo', {id: 654}),
          messages.commit('0/A'),
        ],

        // This should succeed.
        '0/40': [
          messages.begin(),
          messages.insert('foo', {id: 789}),
          messages.insert('foo', {id: 987}),
          messages.commit('0/F'),
        ],
      },
      acknowledged: [
        '0/4',
        '0/A',
        '0/4', // Note: The acknowledgements should be resent
        '0/A', //       so that Postgres can track progress.
        '0/F',
      ],
      expectedVersionChanges: 3,
      replicated: {
        foo: [
          {id: 123, big: null, ['_0_version']: '02'},
          {id: 234, big: null, ['_0_version']: '0g'},
          {id: 789, big: null, ['_0_version']: '114'},
          {id: 987, big: null, ['_0_version']: '114'},
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

      const {watermark} = getSubscriptionState(new StatementRunner(replica));
      expect(fromLexiVersion(watermark)).toBe(c.acknowledged.at(-1));
    });
  }
});
