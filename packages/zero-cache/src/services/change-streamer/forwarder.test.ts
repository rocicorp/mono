import {describe, expect, test} from 'vitest';
import {ReplicationMessages} from '../replicator/test-utils.js';
import {Forwarder} from './forwarder.js';
import {createSubscriber} from './test-utils.js';

describe('change-streamer/forwarder', () => {
  const messages = new ReplicationMessages({issues: 'id'});

  test('in transaction queueing', () => {
    const forwarder = new Forwarder();

    const [sub1, stream1] = createSubscriber('00', true);
    const [sub2, stream2] = createSubscriber('00', true);
    const [sub3, stream3] = createSubscriber('00', true);
    const [sub4, stream4] = createSubscriber('00', true);

    forwarder.add(sub1);
    forwarder.forward(['11', ['begin', messages.begin()]]);
    forwarder.add(sub2);
    forwarder.forward(['12', ['data', messages.truncate('issues')]]);
    forwarder.forward(['13', ['commit', messages.commit(), {watermark: '13'}]]);
    forwarder.add(sub3);
    forwarder.forward(['14', ['begin', messages.begin()]]);
    forwarder.add(sub4);

    for (const sub of [sub1, sub2, sub3, sub4]) {
      sub.close();
    }

    // sub1 gets all of the messages, as it was not added in a transaction.
    expect(stream1).toMatchInlineSnapshot(`
      [
        [
          "begin",
          {
            "tag": "begin",
          },
        ],
        [
          "data",
          {
            "relations": [
              {
                "keyColumns": [
                  "id",
                ],
                "name": "issues",
                "replicaIdentity": "default",
                "schema": "public",
                "tag": "relation",
              },
            ],
            "tag": "truncate",
          },
        ],
        [
          "commit",
          {
            "tag": "commit",
          },
          {
            "watermark": "13",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
        ],
      ]
    `);

    // sub2 and sub3 were added in a transaction. They only see the next
    // transaction.
    expect(stream2).toMatchInlineSnapshot(`
      [
        [
          "begin",
          {
            "tag": "begin",
          },
        ],
      ]
    `);
    expect(stream3).toMatchInlineSnapshot(`
      [
        [
          "begin",
          {
            "tag": "begin",
          },
        ],
      ]
    `);

    // sub4 was added in during the second transaction. It gets nothing.
    expect(stream4).toMatchInlineSnapshot(`[]`);
  });
});
