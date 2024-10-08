import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.js';
import {Queue} from '../../../../shared/src/queue.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from '../../test/db.js';
import type {PostgresDB} from '../../types/pg.js';
import {Subscription} from '../../types/subscription.js';
import {ReplicationMessages} from '../replicator/test-utils.js';
import type {Commit, Downstream} from './change-streamer.js';
import {setupCDCTables} from './schema/tables.js';
import {Storer} from './storer.js';
import {createSubscriber} from './test-utils.js';

describe('change-streamer/storer', () => {
  const lc = createSilentLogContext();
  let db: PostgresDB;
  let storer: Storer;
  let done: Promise<void>;
  let commits: Queue<Commit>;

  beforeEach(async () => {
    db = await testDBs.create('change_streamer_storer');
    await db.begin(async tx => {
      await setupCDCTables(lc, tx);
      await Promise.all(
        [
          {watermark: '02', pos: 0, change: {tag: 'begin', foo: 'bar'}},
          {watermark: '02', pos: 1, change: {tag: 'insert'}},
          {watermark: '03', pos: 2, change: {tag: 'commit', bar: 'baz'}},
          {watermark: '04', pos: 0, change: {tag: 'begin', boo: 'dar'}},
          {watermark: '04', pos: 1, change: {tag: 'update'}},
          {watermark: '06', pos: 2, change: {tag: 'commit', boo: 'far'}},
        ].map(row => tx`INSERT INTO cdc."ChangeLog" ${tx(row)}`),
      );
    });
    commits = new Queue();
    storer = new Storer(lc, db, commit => commits.enqueue(commit));
    done = storer.run();
  });

  afterEach(async () => {
    await testDBs.drop(db);
    void storer.stop();
    await done;
  });

  const messages = new ReplicationMessages({issues: 'id'});

  async function drainUntilCommit(
    watermark: string,
    sub: Subscription<Downstream>,
  ) {
    const msgs: Downstream[] = [];
    for await (const msg of sub) {
      msgs.push(msg);
      if (msg[0] === 'commit' && msg[2].watermark === watermark) {
        break;
      }
    }
    return msgs;
  }

  test('last stored watermark', async () => {
    expect(await storer.getLastStoredWatermark()).toBe('06');

    await db`TRUNCATE TABLE cdc."ChangeLog"`;

    expect(await storer.getLastStoredWatermark()).toBe(null);
  });

  test('no queueing if not in transaction', async () => {
    const [sub, _, stream] = createSubscriber('00');

    // This should be buffered until catchup is complete.
    sub.send(['07', ['begin', messages.begin()]]);
    sub.send(['08', ['commit', messages.commit(), {watermark: '08'}]]);

    // Catchup should start immediately since there are no txes in progress.
    storer.catchup(sub);

    expect(await drainUntilCommit('08', stream)).toMatchInlineSnapshot(`
      [
        [
          "begin",
          {
            "foo": "bar",
            "tag": "begin",
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

  test('queued if transaction in progress', async () => {
    const [sub1, _0, stream1] = createSubscriber('03');
    const [sub2, _1, stream2] = createSubscriber('06');

    // This should be buffered until catchup is complete.
    sub1.send(['09', ['begin', messages.begin()]]);
    sub1.send([
      '0a',
      ['commit', messages.commit({buffer: 'me'}), {watermark: '0a'}],
    ]);
    sub2.send(['09', ['begin', messages.begin()]]);
    sub2.send([
      '0a',
      ['commit', messages.commit({buffer: 'me'}), {watermark: '0a'}],
    ]);

    // Start a transaction before enqueuing catchup.
    storer.store(['07', ['begin', messages.begin()]]);
    // Enqueue catchup before transaction completes.
    storer.catchup(sub1);
    storer.catchup(sub2);
    // Finish the transaction.
    storer.store([
      '08',
      ['commit', messages.commit({extra: 'stuff'}), {watermark: '08'}],
    ]);

    // Catchup should wait for the transaction to complete before querying
    // the database, and start after watermark '03'.
    expect(await drainUntilCommit('0a', stream1)).toMatchInlineSnapshot(`
      [
        [
          "begin",
          {
            "boo": "dar",
            "tag": "begin",
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
    expect(await drainUntilCommit('0a', stream2)).toMatchInlineSnapshot(`
      [
        [
          "begin",
          {
            "tag": "begin",
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

    expect(await db`SELECT * FROM cdc."ChangeLog" ORDER BY watermark, pos`)
      .toMatchInlineSnapshot(`
      Result [
        {
          "change": {
            "foo": "bar",
            "tag": "begin",
          },
          "pos": 0n,
          "precommit": null,
          "watermark": "02",
        },
        {
          "change": {
            "tag": "insert",
          },
          "pos": 1n,
          "precommit": null,
          "watermark": "02",
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
          "watermark": "04",
        },
        {
          "change": {
            "tag": "update",
          },
          "pos": 1n,
          "precommit": null,
          "watermark": "04",
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
  });

  test('catchup does not include subsequent transactions', async () => {
    const [sub, _0, stream] = createSubscriber('03');

    // This should be buffered until catchup is complete.
    sub.send(['0b', ['begin', messages.begin()]]);
    sub.send([
      '0c',
      ['commit', messages.commit({waa: 'hoo'}), {watermark: '0c'}],
    ]);

    // Start a transaction before enqueuing catchup.
    storer.store(['07', ['begin', messages.begin()]]);
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
    storer.store(['09', ['begin', messages.begin()]]);
    storer.store(['0a', ['commit', messages.commit(), {watermark: '0a'}]]);

    // Messages should catchup from after '03' and include '06'
    // from the pending transaction. '07' and '08' should not be included
    // in the snapshot used for catchup. We confirm this by sending the '0c'
    // message and ensuring that that was sent.
    expect(await drainUntilCommit('0c', stream)).toMatchInlineSnapshot(`
      [
        [
          "begin",
          {
            "boo": "dar",
            "tag": "begin",
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
  });

  test('change positioning and replay detection', async () => {
    storer.store(['07', ['begin', messages.begin()]]);
    storer.store(['08', ['data', messages.truncate('issues')]]);
    storer.store([
      '09',
      ['commit', messages.commit({foo: 'bar'}), {watermark: '09'}],
    ]);
    expect(await commits.dequeue()).toEqual([
      'commit',
      {tag: 'commit', foo: 'bar'},
      {watermark: '09'},
    ]);

    // Simulate a replay.
    storer.store(['07', ['begin', messages.begin()]]);
    storer.store(['08', ['data', messages.truncate('issues')]]);
    storer.store([
      '09',
      ['commit', messages.commit({foo: 'bar'}), {watermark: '09'}],
    ]);
    // ACK should be resent.
    expect(await commits.dequeue()).toEqual([
      'commit',
      {tag: 'commit', foo: 'bar'},
      {watermark: '09'},
    ]);

    // Continue to the next transaction.
    storer.store(['0a', ['begin', messages.begin()]]);
    storer.store(['0b', ['data', messages.truncate('issues')]]);
    storer.store([
      '0c',
      ['commit', messages.commit({bar: 'baz'}), {watermark: '0c'}],
    ]);
    expect(await commits.dequeue()).toEqual([
      'commit',
      {tag: 'commit', bar: 'baz'},
      {watermark: '0c'},
    ]);

    expect(
      await db`SELECT * FROM cdc."ChangeLog" WHERE watermark >= '07' ORDER BY watermark, pos`,
    ).toMatchInlineSnapshot(`
      Result [
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
            "cascade": false,
            "relations": [
              {
                "columns": [
                  {
                    "flags": 1,
                    "name": "id",
                    "typeMod": -1,
                    "typeName": null,
                    "typeOid": 23,
                    "typeSchema": null,
                  },
                ],
                "keyColumns": [
                  "id",
                ],
                "name": "issues",
                "relationOid": 1558331249,
                "replicaIdentity": "default",
                "schema": "public",
                "tag": "relation",
              },
            ],
            "restartIdentity": false,
            "tag": "truncate",
          },
          "pos": 1n,
          "precommit": null,
          "watermark": "07",
        },
        {
          "change": {
            "foo": "bar",
            "tag": "commit",
          },
          "pos": 2n,
          "precommit": "07",
          "watermark": "09",
        },
        {
          "change": {
            "tag": "begin",
          },
          "pos": 0n,
          "precommit": null,
          "watermark": "0a",
        },
        {
          "change": {
            "cascade": false,
            "relations": [
              {
                "columns": [
                  {
                    "flags": 1,
                    "name": "id",
                    "typeMod": -1,
                    "typeName": null,
                    "typeOid": 23,
                    "typeSchema": null,
                  },
                ],
                "keyColumns": [
                  "id",
                ],
                "name": "issues",
                "relationOid": 1558331249,
                "replicaIdentity": "default",
                "schema": "public",
                "tag": "relation",
              },
            ],
            "restartIdentity": false,
            "tag": "truncate",
          },
          "pos": 1n,
          "precommit": null,
          "watermark": "0a",
        },
        {
          "change": {
            "bar": "baz",
            "tag": "commit",
          },
          "pos": 2n,
          "precommit": "0a",
          "watermark": "0c",
        },
      ]
    `);
  });
});
