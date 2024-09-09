import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {Queue} from 'shared/src/queue.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from 'zero-cache/src/test/db.js';
import {PostgresDB} from 'zero-cache/src/types/pg.js';
import {Subscription} from 'zero-cache/src/types/subscription.js';
import {ReplicationMessages} from '../replicator/test-utils.js';
import {Downstream} from './change-streamer.js';
import {setupCDCTables} from './schema/tables.js';
import {Storer} from './storer.js';
import {createSubscriber} from './test-utils.js';

describe('change-streamer/storer', () => {
  const lc = createSilentLogContext();
  let db: PostgresDB;
  let storer: Storer;
  let done: Promise<void>;
  let commits: Queue<unknown>;

  beforeEach(async () => {
    db = await testDBs.create('change_streamer_storer');
    await db.begin(async tx => {
      await setupCDCTables(lc, tx);
      await Promise.all(
        [
          {watermark: '01', pos: 0, change: {foo: 'bar'}},
          {watermark: '01', pos: 1, change: {foo: 'boo'}},
          {watermark: '02', pos: 0, change: {bar: 'boo'}},
          {watermark: '02', pos: 1, change: {baz: 'moo'}},
          {watermark: '03', pos: 0, change: {boo: 'doo'}},
          {watermark: '04', pos: 0, change: {moo: 'foo'}},
        ].map(row => tx`INSERT INTO cdc."ChangeLog" ${tx(row)}`),
      );
    });
    commits = new Queue();
    storer = new Storer(lc, db, ({commitEndLsn}) =>
      commits.enqueue(commitEndLsn),
    );
    done = storer.run();
  });

  afterEach(async () => {
    await testDBs.drop(db);
    void storer.stop();
    await done;
  });

  const messages = new ReplicationMessages({issues: 'id'});

  async function drainUntil(watermark: string, sub: Subscription<Downstream>) {
    const msgs: Downstream[] = [];
    for await (const msg of sub) {
      msgs.push(msg);
      if (msg[0] === 'change' && msg[1].watermark >= watermark) {
        break;
      }
    }
    return msgs;
  }

  test('no queueing if not in transaction', async () => {
    const [sub, _, stream] = createSubscriber('00');

    // This should be buffered until catchup is complete.
    sub.send({watermark: '05', change: messages.begin('123')});

    // Catchup should start immediately since there are no txes in progress.
    storer.catchup(sub);

    expect(await drainUntil('05', stream)).toMatchInlineSnapshot(`
      [
        [
          "change",
          {
            "change": {
              "foo": "bar",
            },
            "watermark": "01",
          },
        ],
        [
          "change",
          {
            "change": {
              "foo": "boo",
            },
            "watermark": "01",
          },
        ],
        [
          "change",
          {
            "change": {
              "bar": "boo",
            },
            "watermark": "02",
          },
        ],
        [
          "change",
          {
            "change": {
              "baz": "moo",
            },
            "watermark": "02",
          },
        ],
        [
          "change",
          {
            "change": {
              "boo": "doo",
            },
            "watermark": "03",
          },
        ],
        [
          "change",
          {
            "change": {
              "moo": "foo",
            },
            "watermark": "04",
          },
        ],
        [
          "change",
          {
            "change": {
              "commitLsn": "123",
              "commitTime": 0n,
              "tag": "begin",
              "xid": 0,
            },
            "watermark": "05",
          },
        ],
      ]
    `);
  });

  test('queued if transaction in progress', async () => {
    const [sub1, _0, stream1] = createSubscriber('02');
    const [sub2, _1, stream2] = createSubscriber('03');

    // This should be buffered until catchup is complete.
    sub1.send({watermark: '07', change: messages.begin('456')});
    sub2.send({watermark: '07', change: messages.begin('456')});

    // Start a transaction before enqueuing catchup.
    storer.store({watermark: '05', change: messages.begin('123')});
    // Enqueue catchup before transaction completes.
    storer.catchup(sub1);
    storer.catchup(sub2);
    // Finish the transaction.
    storer.store({watermark: '06', change: messages.commit('312')});

    // Catchup should wait for the transaction to complete before querying
    // the database, and start after watermark '02'.
    expect(await drainUntil('07', stream1)).toMatchInlineSnapshot(`
      [
        [
          "change",
          {
            "change": {
              "boo": "doo",
            },
            "watermark": "03",
          },
        ],
        [
          "change",
          {
            "change": {
              "moo": "foo",
            },
            "watermark": "04",
          },
        ],
        [
          "change",
          {
            "change": {
              "commitLsn": "123",
              "commitTime": 0,
              "tag": "begin",
              "xid": 0,
            },
            "watermark": "05",
          },
        ],
        [
          "change",
          {
            "change": {
              "commitEndLsn": "312",
              "commitLsn": null,
              "commitTime": 0,
              "flags": 0,
              "tag": "commit",
            },
            "watermark": "06",
          },
        ],
        [
          "change",
          {
            "change": {
              "commitLsn": "456",
              "commitTime": 0n,
              "tag": "begin",
              "xid": 0,
            },
            "watermark": "07",
          },
        ],
      ]
    `);

    // Catchup should wait for the transaction to complete before querying
    // the database, and start after watermark '03'.
    expect(await drainUntil('07', stream2)).toMatchInlineSnapshot(`
      [
        [
          "change",
          {
            "change": {
              "moo": "foo",
            },
            "watermark": "04",
          },
        ],
        [
          "change",
          {
            "change": {
              "commitLsn": "123",
              "commitTime": 0,
              "tag": "begin",
              "xid": 0,
            },
            "watermark": "05",
          },
        ],
        [
          "change",
          {
            "change": {
              "commitEndLsn": "312",
              "commitLsn": null,
              "commitTime": 0,
              "flags": 0,
              "tag": "commit",
            },
            "watermark": "06",
          },
        ],
        [
          "change",
          {
            "change": {
              "commitLsn": "456",
              "commitTime": 0n,
              "tag": "begin",
              "xid": 0,
            },
            "watermark": "07",
          },
        ],
      ]
    `);
  });

  test('catchup does not include subsequent transactions', async () => {
    const [sub, _0, stream] = createSubscriber('04');

    // This should be buffered until catchup is complete.
    sub.send({watermark: '09', change: messages.begin('789')});

    // Start a transaction before enqueuing catchup.
    storer.store({watermark: '05', change: messages.begin('123')});
    // Enqueue catchup before transaction completes.
    storer.catchup(sub);
    // Finish the transaction.
    storer.store({watermark: '06', change: messages.commit('312')});

    // And finish another the transaction. In reality, these would be
    // sent by the forwarder, but we skip it in the test to confirm that
    // catchup doesn't include the next transaction.
    storer.store({watermark: '07', change: messages.begin('456')});
    storer.store({watermark: '08', change: messages.commit('654')});

    // Messages should catchup from after '04' and include '05' and '06'
    // from the pending transaction. '07' and '08' should not be included
    // in the snapshot used for catchup. We confirm this by sending the '09'
    // message and ensuring that that was sent.
    expect(await drainUntil('09', stream)).toMatchInlineSnapshot(`
      [
        [
          "change",
          {
            "change": {
              "commitLsn": "123",
              "commitTime": 0,
              "tag": "begin",
              "xid": 0,
            },
            "watermark": "05",
          },
        ],
        [
          "change",
          {
            "change": {
              "commitEndLsn": "312",
              "commitLsn": null,
              "commitTime": 0,
              "flags": 0,
              "tag": "commit",
            },
            "watermark": "06",
          },
        ],
        [
          "change",
          {
            "change": {
              "commitLsn": "789",
              "commitTime": 0n,
              "tag": "begin",
              "xid": 0,
            },
            "watermark": "09",
          },
        ],
      ]
    `);
  });

  test('change positioning and replay detection', async () => {
    storer.store({watermark: '05', change: messages.begin('123')});
    storer.store({watermark: '05', change: messages.truncate('issues')});
    storer.store({watermark: '06', change: messages.commit('321')});
    expect(await commits.dequeue()).toBe('321');

    // Simulate a replay.
    storer.store({watermark: '05', change: messages.begin('123')});
    storer.store({watermark: '05', change: messages.truncate('issues')});
    storer.store({watermark: '06', change: messages.commit('321')});
    // ACK should be resent.
    expect(await commits.dequeue()).toBe('321');

    // Continue to the next transaction.
    storer.store({watermark: '07', change: messages.begin('456')});
    storer.store({watermark: '07', change: messages.truncate('issues')});
    storer.store({watermark: '08', change: messages.commit('654')});
    expect(await commits.dequeue()).toBe('654');

    expect(await db`SELECT * FROM cdc."ChangeLog" WHERE watermark >= '05'`)
      .toMatchInlineSnapshot(`
      Result [
        {
          "change": {
            "commitLsn": "123",
            "commitTime": 0,
            "tag": "begin",
            "xid": 0,
          },
          "pos": 0n,
          "watermark": "05",
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
          "watermark": "05",
        },
        {
          "change": {
            "commitEndLsn": "321",
            "commitLsn": null,
            "commitTime": 0,
            "flags": 0,
            "tag": "commit",
          },
          "pos": 2n,
          "watermark": "06",
        },
        {
          "change": {
            "commitLsn": "456",
            "commitTime": 0,
            "tag": "begin",
            "xid": 0,
          },
          "pos": 0n,
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
          "watermark": "07",
        },
        {
          "change": {
            "commitEndLsn": "654",
            "commitLsn": null,
            "commitTime": 0,
            "flags": 0,
            "tag": "commit",
          },
          "pos": 2n,
          "watermark": "08",
        },
      ]
    `);
  });
});
