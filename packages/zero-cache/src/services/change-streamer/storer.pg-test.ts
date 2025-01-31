import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {testDBs} from '../../test/db.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {Subscription} from '../../types/subscription.ts';
import {type Commit} from '../change-source/protocol/current/downstream.ts';
import type {StatusMessage} from '../change-source/protocol/current/status.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {type Downstream} from './change-streamer.ts';
import * as ErrorType from './error-type-enum.ts';
import {ensureReplicationConfig, setupCDCTables} from './schema/tables.ts';
import {Storer} from './storer.ts';
import {createSubscriber} from './test-utils.ts';

describe('change-streamer/storer', () => {
  const lc = createSilentLogContext();
  let db: PostgresDB;
  let storer: Storer;
  let done: Promise<void>;
  let consumed: Queue<Commit | StatusMessage>;

  const REPLICA_VERSION = '00';

  beforeEach(async () => {
    db = await testDBs.create('change_streamer_storer');
    await db.begin(tx => setupCDCTables(lc, tx));
    await ensureReplicationConfig(
      lc,
      db,
      {replicaVersion: REPLICA_VERSION, publications: []},
      true,
    );
    await db.begin(async tx => {
      await Promise.all(
        [
          {watermark: '03', pos: 0, change: {tag: 'begin', foo: 'bar'}},
          {watermark: '03', pos: 1, change: {tag: 'insert'}},
          {watermark: '03', pos: 2, change: {tag: 'commit', bar: 'baz'}},
          {watermark: '06', pos: 0, change: {tag: 'begin', boo: 'dar'}},
          {watermark: '06', pos: 1, change: {tag: 'update'}},
          {watermark: '06', pos: 2, change: {tag: 'commit', boo: 'far'}},
        ].map(row => tx`INSERT INTO cdc."changeLog" ${tx(row)}`),
      );
      await tx`UPDATE cdc."replicationState" SET "lastWatermark" = '06'`;
    });
    consumed = new Queue();
    storer = new Storer(lc, 'task-id', db, REPLICA_VERSION, msg =>
      consumed.enqueue(msg),
    );
    await storer.assumeOwnership();
    done = storer.run();
  });

  afterEach(async () => {
    await testDBs.drop(db);
    void storer.stop();
    await done;
  });

  async function expectConsumed(...watermarks: string[]) {
    for (const watermark of watermarks) {
      expect((await consumed.dequeue())[2].watermark).toBe(watermark);
    }
  }

  const messages = new ReplicationMessages({issues: 'id'});

  async function drain(sub: Subscription<Downstream>, untilWatermark?: string) {
    const msgs: Downstream[] = [];
    for await (const msg of sub) {
      msgs.push(msg);
      if (msg[0] === 'commit' && msg[2].watermark === untilWatermark) {
        break;
      }
    }
    return msgs;
  }

  test('purge', async () => {
    expect(await storer.purgeRecordsBefore('02')).toBe(0);
    expect(await db`SELECT watermark, pos FROM cdc."changeLog"`).toEqual([
      {watermark: '03', pos: 0n},
      {watermark: '03', pos: 1n},
      {watermark: '03', pos: 2n},
      {watermark: '06', pos: 0n},
      {watermark: '06', pos: 1n},
      {watermark: '06', pos: 2n},
    ]);

    expect(await storer.purgeRecordsBefore('03')).toBe(0);
    expect(await db`SELECT watermark, pos FROM cdc."changeLog"`).toEqual([
      {watermark: '03', pos: 0n},
      {watermark: '03', pos: 1n},
      {watermark: '03', pos: 2n},
      {watermark: '06', pos: 0n},
      {watermark: '06', pos: 1n},
      {watermark: '06', pos: 2n},
    ]);

    // Should be rejected as an invalid watermark.
    expect(await storer.purgeRecordsBefore('04')).toBe(3);
    expect(await db`SELECT watermark, pos FROM cdc."changeLog"`).toEqual([
      {watermark: '06', pos: 0n},
      {watermark: '06', pos: 1n},
      {watermark: '06', pos: 2n},
    ]);

    expect(await storer.purgeRecordsBefore('06')).toBe(0);
    expect(await db`SELECT watermark, pos FROM cdc."changeLog"`).toEqual([
      {watermark: '06', pos: 0n},
      {watermark: '06', pos: 1n},
      {watermark: '06', pos: 2n},
    ]);
  });

  test('no queueing if not in transaction', async () => {
    const [sub, _, stream] = createSubscriber('00');

    // This should be buffered until catchup is complete.
    sub.send(['07', ['begin', messages.begin(), {commitWatermark: '08'}]]);
    sub.send(['08', ['commit', messages.commit(), {watermark: '08'}]]);

    // Catchup should start immediately since there are no txes in progress.
    storer.catchup(sub);

    expect(await drain(stream, '08')).toMatchInlineSnapshot(`
      [
        [
          "begin",
          {
            "foo": "bar",
            "tag": "begin",
          },
          {
            "commitWatermark": "03",
          },
        ],
        [
          "data",
          {
            "tag": "insert",
          },
        ],
        [
          "commit",
          {
            "bar": "baz",
            "tag": "commit",
          },
          {
            "watermark": "03",
          },
        ],
        [
          "begin",
          {
            "boo": "dar",
            "tag": "begin",
          },
          {
            "commitWatermark": "06",
          },
        ],
        [
          "data",
          {
            "tag": "update",
          },
        ],
        [
          "commit",
          {
            "boo": "far",
            "tag": "commit",
          },
          {
            "watermark": "06",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "08",
          },
        ],
        [
          "commit",
          {
            "tag": "commit",
          },
          {
            "watermark": "08",
          },
        ],
      ]
    `);
  });

  test('watermark too old', async () => {
    // '01' is not the replica version, and not a watermark in the changeLog
    const [sub, _, stream] = createSubscriber('01');
    storer.catchup(sub);

    expect(await drain(stream)).toEqual([
      [
        'error',
        {
          type: ErrorType.WatermarkTooOld,
          message: 'earliest supported watermark is 03 (requested 01)',
        },
      ],
    ]);
  });

  test('queued if transaction in progress', async () => {
    const [sub1, _0, stream1] = createSubscriber('03');
    const [sub2, _1, stream2] = createSubscriber('06');

    // This should be buffered until catchup is complete.
    sub1.send(['09', ['begin', messages.begin(), {commitWatermark: '0a'}]]);
    sub1.send([
      '0a',
      ['commit', messages.commit({buffer: 'me'}), {watermark: '0a'}],
    ]);
    sub2.send(['09', ['begin', messages.begin(), {commitWatermark: '0a'}]]);
    sub2.send([
      '0a',
      ['commit', messages.commit({buffer: 'me'}), {watermark: '0a'}],
    ]);

    // Start a transaction before enqueuing catchup.
    storer.store(['07', ['begin', messages.begin(), {commitWatermark: '08'}]]);
    // Enqueue catchup before transaction completes.
    storer.catchup(sub1);
    storer.catchup(sub2);
    // Finish the transaction.
    storer.store([
      '08',
      ['commit', messages.commit({extra: 'stuff'}), {watermark: '08'}],
    ]);

    storer.status(['status', {}, {watermark: '0e'}]);
    storer.status(['status', {}, {watermark: '0f'}]);

    // Catchup should wait for the transaction to complete before querying
    // the database, and start after watermark '03'.
    expect(await drain(stream1, '0a')).toMatchInlineSnapshot(`
      [
        [
          "begin",
          {
            "boo": "dar",
            "tag": "begin",
          },
          {
            "commitWatermark": "06",
          },
        ],
        [
          "data",
          {
            "tag": "update",
          },
        ],
        [
          "commit",
          {
            "boo": "far",
            "tag": "commit",
          },
          {
            "watermark": "06",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "07",
          },
        ],
        [
          "commit",
          {
            "extra": "stuff",
            "tag": "commit",
          },
          {
            "watermark": "08",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "0a",
          },
        ],
        [
          "commit",
          {
            "buffer": "me",
            "tag": "commit",
          },
          {
            "watermark": "0a",
          },
        ],
      ]
    `);

    // Catchup should wait for the transaction to complete before querying
    // the database, and start after watermark '06'.
    expect(await drain(stream2, '0a')).toMatchInlineSnapshot(`
      [
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "07",
          },
        ],
        [
          "commit",
          {
            "extra": "stuff",
            "tag": "commit",
          },
          {
            "watermark": "08",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "0a",
          },
        ],
        [
          "commit",
          {
            "buffer": "me",
            "tag": "commit",
          },
          {
            "watermark": "0a",
          },
        ],
      ]
    `);

    expect(await db`SELECT * FROM cdc."changeLog" ORDER BY watermark, pos`)
      .toMatchInlineSnapshot(`
        Result [
          {
            "change": {
              "foo": "bar",
              "tag": "begin",
            },
            "pos": 0n,
            "precommit": null,
            "watermark": "03",
          },
          {
            "change": {
              "tag": "insert",
            },
            "pos": 1n,
            "precommit": null,
            "watermark": "03",
          },
          {
            "change": {
              "bar": "baz",
              "tag": "commit",
            },
            "pos": 2n,
            "precommit": null,
            "watermark": "03",
          },
          {
            "change": {
              "boo": "dar",
              "tag": "begin",
            },
            "pos": 0n,
            "precommit": null,
            "watermark": "06",
          },
          {
            "change": {
              "tag": "update",
            },
            "pos": 1n,
            "precommit": null,
            "watermark": "06",
          },
          {
            "change": {
              "boo": "far",
              "tag": "commit",
            },
            "pos": 2n,
            "precommit": null,
            "watermark": "06",
          },
          {
            "change": {
              "tag": "begin",
            },
            "pos": 0n,
            "precommit": null,
            "watermark": "07",
          },
          {
            "change": {
              "extra": "stuff",
              "tag": "commit",
            },
            "pos": 1n,
            "precommit": "07",
            "watermark": "08",
          },
        ]
      `);

    await expectConsumed('08', '0e', '0f');
  });

  // Similar to "queued if transaction is in progress" but tests rollback.
  test('queued until transaction is rolled back', async () => {
    const [sub1, _0, stream1] = createSubscriber('03');
    const [sub2, _1, stream2] = createSubscriber('06');

    // This should be buffered until catchup is complete.
    sub1.send(['09', ['begin', messages.begin(), {commitWatermark: '0a'}]]);
    sub1.send([
      '0a',
      ['commit', messages.commit({buffer: 'me'}), {watermark: '0a'}],
    ]);
    sub2.send(['09', ['begin', messages.begin(), {commitWatermark: '0a'}]]);
    sub2.send([
      '0a',
      ['commit', messages.commit({buffer: 'me'}), {watermark: '0a'}],
    ]);

    // Start a transaction before enqueuing catchup.
    storer.store(['07', ['begin', messages.begin(), {commitWatermark: '08'}]]);
    // Enqueue catchup before transaction completes.
    storer.catchup(sub1);
    storer.catchup(sub2);
    // Rollback the transaction.
    storer.store(['08', ['rollback', messages.rollback()]]);

    storer.status(['status', {}, {watermark: '0a'}]);
    storer.status(['status', {}, {watermark: '0c'}]);

    // Catchup should wait for the transaction to complete before querying
    // the database, and start after watermark '03'.
    expect(await drain(stream1, '0a')).toMatchInlineSnapshot(`
      [
        [
          "begin",
          {
            "boo": "dar",
            "tag": "begin",
          },
          {
            "commitWatermark": "06",
          },
        ],
        [
          "data",
          {
            "tag": "update",
          },
        ],
        [
          "commit",
          {
            "boo": "far",
            "tag": "commit",
          },
          {
            "watermark": "06",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "0a",
          },
        ],
        [
          "commit",
          {
            "buffer": "me",
            "tag": "commit",
          },
          {
            "watermark": "0a",
          },
        ],
      ]
    `);

    // Catchup should wait for the transaction to complete before querying
    // the database, and start after watermark '06'.
    expect(await drain(stream2, '0a')).toMatchInlineSnapshot(`
      [
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "0a",
          },
        ],
        [
          "commit",
          {
            "buffer": "me",
            "tag": "commit",
          },
          {
            "watermark": "0a",
          },
        ],
      ]
    `);

    expect(await db`SELECT * FROM cdc."changeLog" ORDER BY watermark, pos`)
      .toMatchInlineSnapshot(`
        Result [
          {
            "change": {
              "foo": "bar",
              "tag": "begin",
            },
            "pos": 0n,
            "precommit": null,
            "watermark": "03",
          },
          {
            "change": {
              "tag": "insert",
            },
            "pos": 1n,
            "precommit": null,
            "watermark": "03",
          },
          {
            "change": {
              "bar": "baz",
              "tag": "commit",
            },
            "pos": 2n,
            "precommit": null,
            "watermark": "03",
          },
          {
            "change": {
              "boo": "dar",
              "tag": "begin",
            },
            "pos": 0n,
            "precommit": null,
            "watermark": "06",
          },
          {
            "change": {
              "tag": "update",
            },
            "pos": 1n,
            "precommit": null,
            "watermark": "06",
          },
          {
            "change": {
              "boo": "far",
              "tag": "commit",
            },
            "pos": 2n,
            "precommit": null,
            "watermark": "06",
          },
        ]
      `);

    await expectConsumed('0a', '0c');
  });

  test('catchup does not include subsequent transactions', async () => {
    const [sub, _0, stream] = createSubscriber('03');

    // This should be buffered until catchup is complete.
    sub.send(['0b', ['begin', messages.begin(), {commitWatermark: '0c'}]]);
    sub.send([
      '0c',
      ['commit', messages.commit({waa: 'hoo'}), {watermark: '0c'}],
    ]);

    // Start a transaction before enqueuing catchup.
    storer.store(['07', ['begin', messages.begin(), {commitWatermark: '08'}]]);
    // Enqueue catchup before transaction completes.
    storer.catchup(sub);
    // Finish the transaction.
    storer.store([
      '08',
      ['commit', messages.commit({extra: 'fields'}), {watermark: '08'}],
    ]);

    // And finish another the transaction. In reality, these would be
    // sent by the forwarder, but we skip it in the test to confirm that
    // catchup doesn't include the next transaction.
    storer.store(['09', ['begin', messages.begin(), {commitWatermark: '0a'}]]);
    storer.store(['0a', ['commit', messages.commit(), {watermark: '0a'}]]);

    storer.status(['status', {}, {watermark: '0d'}]);
    storer.status(['status', {}, {watermark: '0e'}]);

    // Wait for the storer to commit that transaction.
    for (let i = 0; i < 10; i++) {
      const result =
        await db`SELECT * FROM cdc."changeLog" WHERE watermark = '0a'`;
      if (result.length) {
        break;
      }
      await sleep(10);
    }

    // Messages should catchup from after '03' and include '06'
    // from the pending transaction. '07' and '08' should not be included
    // in the snapshot used for catchup. We confirm this by sending the '0c'
    // message and ensuring that that was sent.
    expect(await drain(stream, '0c')).toMatchInlineSnapshot(`
      [
        [
          "begin",
          {
            "boo": "dar",
            "tag": "begin",
          },
          {
            "commitWatermark": "06",
          },
        ],
        [
          "data",
          {
            "tag": "update",
          },
        ],
        [
          "commit",
          {
            "boo": "far",
            "tag": "commit",
          },
          {
            "watermark": "06",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "07",
          },
        ],
        [
          "commit",
          {
            "extra": "fields",
            "tag": "commit",
          },
          {
            "watermark": "08",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "0c",
          },
        ],
        [
          "commit",
          {
            "tag": "commit",
            "waa": "hoo",
          },
          {
            "watermark": "0c",
          },
        ],
      ]
    `);
    await expectConsumed('08', '0a', '0d', '0e');
  });
});
