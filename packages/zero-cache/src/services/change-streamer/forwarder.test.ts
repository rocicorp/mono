import {describe, expect, test} from 'vitest';
import {ReplicationMessages} from '../replicator/test-utils.js';
import {Forwarder} from './forwarder.js';
import {createSubscriber} from './test-utils.js';

describe('change-streamer/forwarder', () => {
  const messages = new ReplicationMessages({issues: 'id'});

  test('in transaction queueing', () => {
    const forwarder = new Forwarder();

    const [sub1, stream1] = createSubscriber('0/0', true);
    const [sub2, stream2] = createSubscriber('0/0', true);
    const [sub3, stream3] = createSubscriber('0/0', true);
    const [sub4, stream4] = createSubscriber('0/0', true);

    forwarder.add(sub1);
    forwarder.forward({watermark: '0/11', change: messages.begin('123')});
    forwarder.add(sub2);
    forwarder.forward({watermark: '0/12', change: messages.truncate('issues')});
    forwarder.forward({watermark: '0/13', change: messages.commit('lsn')});
    forwarder.add(sub3);
    forwarder.forward({watermark: '0/14', change: messages.begin('456')});
    forwarder.add(sub4);

    for (const sub of [sub1, sub2, sub3, sub4]) {
      sub.close();
    }

    // sub1 gets all of the messages, as it was not added in a transaction.
    expect(stream1).toMatchInlineSnapshot(`
      [
        [
          "change",
          {
            "change": {
              "commitLsn": "123",
              "commitTime": 0n,
              "tag": "begin",
              "xid": 0,
            },
            "watermark": "0/11",
          },
        ],
        [
          "change",
          {
            "change": {
              "cascade": false,
              "relations": [
                {
                  "columns": [
                    {
                      "flags": 1,
                      "name": "id",
                      "parser": [Function],
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
            "watermark": "0/12",
          },
        ],
        [
          "change",
          {
            "change": {
              "commitEndLsn": "lsn",
              "commitLsn": null,
              "commitTime": 0n,
              "flags": 0,
              "tag": "commit",
            },
            "watermark": "0/13",
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
            "watermark": "0/14",
          },
        ],
      ]
    `);

    // sub2 and sub3 were added in a transaction. They only see the next
    // transaction.
    expect(stream2).toMatchInlineSnapshot(`
      [
        [
          "change",
          {
            "change": {
              "commitLsn": "456",
              "commitTime": 0n,
              "tag": "begin",
              "xid": 0,
            },
            "watermark": "0/14",
          },
        ],
      ]
    `);
    expect(stream3).toMatchInlineSnapshot(`
      [
        [
          "change",
          {
            "change": {
              "commitLsn": "456",
              "commitTime": 0n,
              "tag": "begin",
              "xid": 0,
            },
            "watermark": "0/14",
          },
        ],
      ]
    `);

    // sub4 was added in during the second transaction. It gets nothing.
    expect(stream4).toMatchInlineSnapshot(`[]`);
  });
});
