import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {type PgTest, test} from '../../test/db.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {Subscription} from '../../types/subscription.ts';
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
  let fatalErrors: Queue<Error>;
  let shard: {appID: string; shardNum: number};

  const REPLICA_VERSION = '00';
  const APP_ID = 'xero';
  const SHARD_NUM = 5;

  beforeEach<PgTest>(async ({testDBs}) => {
    db = await testDBs.create('change_streamer_storer');
    shard = {appID: APP_ID, shardNum: SHARD_NUM};
    await db.begin(tx => setupCDCTables(lc, tx, shard));
    await ensureReplicationConfig(
      lc,
      db,
      {
        replicaVersion: REPLICA_VERSION,
        publications: [],
        watermark: REPLICA_VERSION,
      },
      shard,
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
        ].map(row => tx`INSERT INTO "xero_5/cdc"."changeLog" ${tx(row)}`),
      );
      await tx`UPDATE "xero_5/cdc"."replicationState" SET "lastWatermark" = '06'`;
    });
    consumed = new Queue();
    fatalErrors = new Queue();

    return async () => {
      await testDBs.drop(db);
      void storer.stop();
      await done;
    };
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

  describe('protocol: ws', () => {
    beforeEach(async () => {
      storer = new Storer(
        lc,
        shard,
        'task-id',
        'change-streamer:12345',
        'ws',
        db,
        REPLICA_VERSION,
        msg => consumed.enqueue(msg),
        err => fatalErrors.enqueue(err),
      );
      await storer.assumeOwnership();
      done = storer.run();
    });

    test('ownerAddress is set correctly', async () => {
      expect(
        await db`SELECT "ownerAddress" FROM "xero_5/cdc"."replicationState" WHERE owner = 'task-id'`,
      ).toEqual([{ownerAddress: 'change-streamer:12345'}]);
    });

    test('purge', async () => {
      expect(await storer.purgeRecordsBefore('02')).toBe(0);
      expect(
        await db`SELECT watermark, pos FROM "xero_5/cdc"."changeLog"`,
      ).toEqual([
        {watermark: '03', pos: 0n},
        {watermark: '03', pos: 1n},
        {watermark: '03', pos: 2n},
        {watermark: '06', pos: 0n},
        {watermark: '06', pos: 1n},
        {watermark: '06', pos: 2n},
      ]);

      expect(await storer.purgeRecordsBefore('03')).toBe(0);
      expect(
        await db`SELECT watermark, pos FROM "xero_5/cdc"."changeLog"`,
      ).toEqual([
        {watermark: '03', pos: 0n},
        {watermark: '03', pos: 1n},
        {watermark: '03', pos: 2n},
        {watermark: '06', pos: 0n},
        {watermark: '06', pos: 1n},
        {watermark: '06', pos: 2n},
      ]);

      // TODO: Consider rejecting as an invalid watermark?
      expect(await storer.purgeRecordsBefore('04')).toBe(3);
      expect(
        await db`SELECT watermark, pos FROM "xero_5/cdc"."changeLog"`,
      ).toEqual([
        {watermark: '06', pos: 0n},
        {watermark: '06', pos: 1n},
        {watermark: '06', pos: 2n},
      ]);

      expect(await storer.purgeRecordsBefore('06')).toBe(0);
      expect(
        await db`SELECT watermark, pos FROM "xero_5/cdc"."changeLog"`,
      ).toEqual([
        {watermark: '06', pos: 0n},
        {watermark: '06', pos: 1n},
        {watermark: '06', pos: 2n},
      ]);
    });

    test('non-owner purge prevented', async () => {
      await db`UPDATE "xero_5/cdc"."replicationState" SET owner = 'different-task-id'`;

      expect(await storer.purgeRecordsBefore('06')).toBe(0);
      expect(
        await db`SELECT watermark, pos FROM "xero_5/cdc"."changeLog"`,
      ).toEqual([
        {watermark: '03', pos: 0n},
        {watermark: '03', pos: 1n},
        {watermark: '03', pos: 2n},
        {watermark: '06', pos: 0n},
        {watermark: '06', pos: 1n},
        {watermark: '06', pos: 2n},
      ]);
    });

    test('ownership change detected at begin aborts transaction', async () => {
      // Change ownership before storing begin — the storer's pipelined
      // SELECT will read the new owner immediately.
      await db`UPDATE "xero_5/cdc"."replicationState" SET owner = 'other-task'`;

      storer.store([
        '07',
        ['begin', messages.begin(), {commitWatermark: '08'}],
      ]);
      storer.store(['07', ['data', messages.insert('issues', {id: 'foo'})]]);
      storer.store(['08', ['commit', messages.commit(), {watermark: '08'}]]);

      await expect(done).rejects.toThrow(
        'changeLog ownership has been assumed by other-task',
      );
      // Prevent the beforeEach cleanup from re-throwing the rejected done.
      done = Promise.resolve();
    });

    test('ownership change during transaction includes new owner in error', async () => {
      // Start a transaction — this begins a SERIALIZABLE tx that
      // reads replicationState (owner = 'task-id').
      storer.store([
        '07',
        ['begin', messages.begin(), {commitWatermark: '08'}],
      ]);
      storer.store(['07', ['data', messages.insert('issues', {id: 'foo'})]]);

      // Wait for the storer to process 'begin' and start the SERIALIZABLE tx.
      // The pipelined SELECT of replicationState should have executed by now.
      await sleep(100);

      // Change ownership externally — this commits immediately on a separate
      // connection, modifying the replicationState row that the storer's
      // SERIALIZABLE tx has already read.
      await db`UPDATE "xero_5/cdc"."replicationState" SET owner = 'new-owner-task'`;

      // Now send commit. The storer's initial read shows owner = 'task-id'
      // (matching), so it proceeds to UPDATE lastWatermark. But Postgres
      // detects the serialization conflict and throws PG_SERIALIZATION_FAILURE.
      storer.store(['08', ['commit', messages.commit(), {watermark: '08'}]]);

      // The AbortError should include the new owner read from a fresh query.
      await expect(done).rejects.toThrow(
        'changeLog ownership was concurrently assumed by new-owner-task (serialization failure)',
      );
      // Prevent the beforeEach cleanup from re-throwing the rejected done.
      done = Promise.resolve();
    });

    test('abort', async () => {
      storer.store([
        '0b',
        ['begin', messages.begin(), {commitWatermark: '0b'}],
      ]);
      storer.store(['0b', ['data', messages.insert('issues', {id: 'foo'})]]);
      storer.abort();

      storer.store([
        '0a',
        ['begin', messages.begin(), {commitWatermark: '0a'}],
      ]);
      storer.store(['0a', ['data', messages.insert('issues', {id: 'bar'})]]);
      storer.store(['0a', ['commit', messages.commit(), {watermark: '0a'}]]);

      await expectConsumed('0a');

      expect(
        await db`
      SELECT watermark, pos, change FROM "xero_5/cdc"."changeLog"
        WHERE watermark >= '0a'`,
      ).toMatchObject([
        {
          change: {tag: 'begin'},
          pos: 0n,
          watermark: '0a',
        },
        {
          change: {
            tag: 'insert',
            new: {id: 'bar'},
          },
          pos: 1n,
          watermark: '0a',
        },
        {
          change: {tag: 'commit'},
          pos: 2n,
          watermark: '0a',
        },
      ]);
    });

    test('no queueing if not in transaction', async () => {
      const [sub, _, stream] = createSubscriber('00');

      // This should be buffered until catchup is complete.
      sub.send(['07', ['begin', messages.begin(), {commitWatermark: '08'}]]);
      sub.send(['08', ['commit', messages.commit(), {watermark: '08'}]]);

      // Catchup should start immediately since there are no txes in progress.
      storer.catchup(sub, 'backup');

      expect(await drain(stream, '08')).toMatchInlineSnapshot(`
      [
        [
          "status",
          {
            "tag": "status",
          },
        ],
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

    test('watermark too old (serving)', async () => {
      // '01' is not the replica version, and not a watermark in the changeLog
      const [sub, _, stream] = createSubscriber('01');
      storer.catchup(sub, 'serving');

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

    test('watermark too old (backup)', async () => {
      // '01' is not the replica version, and not a watermark in the changeLog
      const [sub] = createSubscriber('01');
      storer.catchup(sub, 'backup');

      expect(await fatalErrors.dequeue()).toMatchInlineSnapshot(
        `[AutoResetSignal: backup replica at watermark 01 is behind change db: 03)]`,
      );
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
      storer.store([
        '07',
        ['begin', messages.begin(), {commitWatermark: '08'}],
      ]);
      // Enqueue catchup before transaction completes.
      storer.catchup(sub1, 'serving');
      storer.catchup(sub2, 'serving');
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
          "status",
          {
            "tag": "status",
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
                  "status",
                  {
                    "tag": "status",
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

      expect(
        await db`SELECT * FROM "xero_5/cdc"."changeLog" ORDER BY watermark, pos`,
      ).toMatchInlineSnapshot(`
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
      storer.store([
        '07',
        ['begin', messages.begin(), {commitWatermark: '08'}],
      ]);
      // Enqueue catchup before transaction completes.
      storer.catchup(sub1, 'backup');
      storer.catchup(sub2, 'serving');
      // Rollback the transaction.
      storer.store(['08', ['rollback', messages.rollback()]]);

      storer.status(['status', {}, {watermark: '0a'}]);
      storer.status(['status', {}, {watermark: '0c'}]);

      // Catchup should wait for the transaction to complete before querying
      // the database, and start after watermark '03'.
      expect(await drain(stream1, '0a')).toMatchInlineSnapshot(`
        [
          [
            "status",
            {
              "tag": "status",
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
                "status",
                {
                  "tag": "status",
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

      expect(
        await db`SELECT * FROM "xero_5/cdc"."changeLog" ORDER BY watermark, pos`,
      ).toMatchInlineSnapshot(`
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
      storer.store([
        '07',
        ['begin', messages.begin(), {commitWatermark: '08'}],
      ]);
      // Enqueue catchup before transaction completes.
      storer.catchup(sub, 'serving');
      // Finish the transaction.
      storer.store([
        '08',
        ['commit', messages.commit({extra: 'fields'}), {watermark: '08'}],
      ]);

      // And finish another the transaction. In reality, these would be
      // sent by the forwarder, but we skip it in the test to confirm that
      // catchup doesn't include the next transaction.
      storer.store([
        '09',
        ['begin', messages.begin(), {commitWatermark: '0a'}],
      ]);
      storer.store(['0a', ['commit', messages.commit(), {watermark: '0a'}]]);

      storer.status(['status', {}, {watermark: '0d'}]);
      storer.status(['status', {}, {watermark: '0e'}]);

      // Wait for the storer to commit that transaction.
      for (let i = 0; i < 10; i++) {
        const result =
          await db`SELECT * FROM "xero_5/cdc"."changeLog" WHERE watermark = '0a'`;
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
          "status",
          {
            "tag": "status",
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

  describe('protocol: wss', () => {
    beforeEach(async () => {
      storer = new Storer(
        lc,
        shard,
        'task-id',
        'change-streamer:12345',
        'wss',
        db,
        REPLICA_VERSION,
        msg => consumed.enqueue(msg),
        err => fatalErrors.enqueue(err),
      );
      await storer.assumeOwnership();
      done = storer.run();
    });

    test('ownerAddress is set correctly', async () => {
      expect(
        await db`SELECT "ownerAddress" FROM "xero_5/cdc"."replicationState" WHERE owner = 'task-id'`,
      ).toEqual([{ownerAddress: 'wss://change-streamer:12345'}]);
    });
  });
});
