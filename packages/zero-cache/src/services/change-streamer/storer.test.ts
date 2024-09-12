import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {Queue} from 'shared/src/queue.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from 'zero-cache/src/test/db.js';
import {PostgresDB} from 'zero-cache/src/types/pg.js';
import {Subscription} from 'zero-cache/src/types/subscription.js';
import {ReplicationMessages} from '../replicator/test-utils.js';
import {Downstream} from './change-streamer.js';
import {MessageCommit} from './schema/change.js';
import {setupCDCTables} from './schema/tables.js';
import {Storer} from './storer.js';
import {createSubscriber} from './test-utils.js';

describe('change-streamer/storer', () => {
  const lc = createSilentLogContext();
  let db: PostgresDB;
  let storer: Storer;
  let done: Promise<void>;
  let commits: Queue<MessageCommit>;

  beforeEach(async () => {
    db = await testDBs.create('change_streamer_storer');
    await db.begin(async tx => {
      await setupCDCTables(lc, tx);
      await Promise.all(
        [
          {watermark: '01', change: {foo: 'bar'}},
          {watermark: '02', change: {foo: 'boo'}},
          {watermark: '03', change: {bar: 'boo'}},
          {watermark: '04', change: {baz: 'moo'}},
          {watermark: '05', change: {boo: 'doo'}},
          {watermark: '06', change: {moo: 'foo'}},
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
    sub.send({watermark: '07', change: messages.begin()});

    // Catchup should start immediately since there are no txes in progress.
    storer.catchup(sub);

    expect(await drainUntil('07', stream)).toMatchInlineSnapshot(`
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
            "watermark": "02",
          },
        ],
        [
          "change",
          {
            "change": {
              "bar": "boo",
            },
            "watermark": "03",
          },
        ],
        [
          "change",
          {
            "change": {
              "baz": "moo",
            },
            "watermark": "04",
          },
        ],
        [
          "change",
          {
            "change": {
              "boo": "doo",
            },
            "watermark": "05",
          },
        ],
        [
          "change",
          {
            "change": {
              "moo": "foo",
            },
            "watermark": "06",
          },
        ],
        [
          "change",
          {
            "change": {
              "tag": "begin",
            },
            "watermark": "07",
          },
        ],
      ]
    `);
  });

  test('queued if transaction in progress', async () => {
    const [sub1, _0, stream1] = createSubscriber('04');
    const [sub2, _1, stream2] = createSubscriber('05');

    // This should be buffered until catchup is complete.
    sub1.send({watermark: '09', change: messages.begin()});
    sub2.send({watermark: '09', change: messages.begin()});

    // Start a transaction before enqueuing catchup.
    storer.store({watermark: '07', change: messages.begin()});
    // Enqueue catchup before transaction completes.
    storer.catchup(sub1);
    storer.catchup(sub2);
    // Finish the transaction.
    storer.store({watermark: '08', change: messages.commit({extra: 'stuff'})});

    // Catchup should wait for the transaction to complete before querying
    // the database, and start after watermark '02'.
    expect(await drainUntil('09', stream1)).toMatchInlineSnapshot(`
      [
        [
          "change",
          {
            "change": {
              "boo": "doo",
            },
            "watermark": "05",
          },
        ],
        [
          "change",
          {
            "change": {
              "moo": "foo",
            },
            "watermark": "06",
          },
        ],
        [
          "change",
          {
            "change": {
              "tag": "begin",
            },
            "watermark": "07",
          },
        ],
        [
          "change",
          {
            "change": {
              "extra": "stuff",
              "tag": "commit",
            },
            "watermark": "08",
          },
        ],
        [
          "change",
          {
            "change": {
              "tag": "begin",
            },
            "watermark": "09",
          },
        ],
      ]
    `);

    // Catchup should wait for the transaction to complete before querying
    // the database, and start after watermark '03'.
    expect(await drainUntil('09', stream2)).toMatchInlineSnapshot(`
      [
        [
          "change",
          {
            "change": {
              "moo": "foo",
            },
            "watermark": "06",
          },
        ],
        [
          "change",
          {
            "change": {
              "tag": "begin",
            },
            "watermark": "07",
          },
        ],
        [
          "change",
          {
            "change": {
              "extra": "stuff",
              "tag": "commit",
            },
            "watermark": "08",
          },
        ],
        [
          "change",
          {
            "change": {
              "tag": "begin",
            },
            "watermark": "09",
          },
        ],
      ]
    `);
  });

  test('catchup does not include subsequent transactions', async () => {
    const [sub, _0, stream] = createSubscriber('06');

    // This should be buffered until catchup is complete.
    sub.send({watermark: '0b', change: messages.begin()});

    // Start a transaction before enqueuing catchup.
    storer.store({watermark: '07', change: messages.begin()});
    // Enqueue catchup before transaction completes.
    storer.catchup(sub);
    // Finish the transaction.
    storer.store({watermark: '08', change: messages.commit({extra: 'fields'})});

    // And finish another the transaction. In reality, these would be
    // sent by the forwarder, but we skip it in the test to confirm that
    // catchup doesn't include the next transaction.
    storer.store({watermark: '09', change: messages.begin()});
    storer.store({watermark: '0a', change: messages.commit()});

    // Messages should catchup from after '04' and include '05' and '06'
    // from the pending transaction. '07' and '08' should not be included
    // in the snapshot used for catchup. We confirm this by sending the '09'
    // message and ensuring that that was sent.
    expect(await drainUntil('0b', stream)).toMatchInlineSnapshot(`
      [
        [
          "change",
          {
            "change": {
              "tag": "begin",
            },
            "watermark": "07",
          },
        ],
        [
          "change",
          {
            "change": {
              "extra": "fields",
              "tag": "commit",
            },
            "watermark": "08",
          },
        ],
        [
          "change",
          {
            "change": {
              "tag": "begin",
            },
            "watermark": "0b",
          },
        ],
      ]
    `);
  });

  test('change positioning and replay detection', async () => {
    storer.store({watermark: '07', change: messages.begin()});
    storer.store({watermark: '08', change: messages.truncate('issues')});
    storer.store({watermark: '09', change: messages.commit({foo: 'bar'})});
    expect(await commits.dequeue()).toEqual({tag: 'commit', foo: 'bar'});

    // Simulate a replay.
    storer.store({watermark: '07', change: messages.begin()});
    storer.store({watermark: '08', change: messages.truncate('issues')});
    storer.store({watermark: '09', change: messages.commit({foo: 'bar'})});
    // ACK should be resent.
    expect(await commits.dequeue()).toEqual({tag: 'commit', foo: 'bar'});

    // Continue to the next transaction.
    storer.store({watermark: '0a', change: messages.begin()});
    storer.store({watermark: '0b', change: messages.truncate('issues')});
    storer.store({watermark: '0c', change: messages.commit({bar: 'baz'})});
    expect(await commits.dequeue()).toEqual({tag: 'commit', bar: 'baz'});

    expect(await db`SELECT * FROM cdc."ChangeLog" WHERE watermark >= '07'`)
      .toMatchInlineSnapshot(`
      Result [
        {
          "change": {
            "tag": "begin",
          },
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
          "watermark": "08",
        },
        {
          "change": {
            "foo": "bar",
            "tag": "commit",
          },
          "watermark": "09",
        },
        {
          "change": {
            "tag": "begin",
          },
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
          "watermark": "0b",
        },
        {
          "change": {
            "bar": "baz",
            "tag": "commit",
          },
          "watermark": "0c",
        },
      ]
    `);
  });
});
