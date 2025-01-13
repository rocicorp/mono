import {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.js';
import {Database} from '../../../../zqlite/src/db.js';
import {StatementRunner} from '../../db/statements.js';
import {expectTables} from '../../test/lite.js';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.js';
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

      const processor = createMessageProcessor(
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
    const processor = createMessageProcessor(replica);

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
    const processor = createMessageProcessor(replica);

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
