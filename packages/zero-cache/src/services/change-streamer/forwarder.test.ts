import {describe, expect, test} from 'vitest';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {Forwarder} from './forwarder.ts';
import {createSubscriber} from './test-utils.ts';

const json = BigIntJSON.stringify;

function nextEventLoopTurn() {
  return new Promise<void>(resolve => setImmediate(resolve));
}

describe('change-streamer/forwarder', () => {
  const messages = new ReplicationMessages({issues: 'id'});

  test('flow control waits on catching-up subscriber backlog', async () => {
    const forwarder = new Forwarder(createSilentLogContext());
    const [sub, _, receiver] = createSubscriber('00', false, {
      backlogHighWaterBytes: 1,
    });

    forwarder.add(sub);

    let released = false;
    const forwarded = forwarder
      .forwardWithFlowControl([
        '11',
        'begin',
        json(['begin', messages.begin(), {commitWatermark: '12'}]),
      ])
      .then(() => {
        released = true;
      });

    await nextEventLoopTurn();
    expect(released).toBe(false);

    const drained = sub.setCaughtUp();
    await nextEventLoopTurn();

    // Status initialization plus the forwarded change are now queued
    // downstream, but the forwarder should still be waiting on consumption.
    expect(receiver.queued).toBe(2);
    expect(released).toBe(false);

    receiver.cancel();
    await forwarded;
    await drained;
    expect(released).toBe(true);
  });

  test('in transaction queueing', () => {
    const forwarder = new Forwarder(createSilentLogContext());

    const [sub1, stream1] = createSubscriber('00', true);
    const [sub2, stream2] = createSubscriber('00', true);
    const [sub3, stream3] = createSubscriber('00', true);
    const [sub4, stream4] = createSubscriber('00', true);

    forwarder.add(sub1);
    forwarder.forward([
      '11',
      'begin',
      json(['begin', messages.begin(), {commitWatermark: '13'}]),
    ]);
    forwarder.add(sub2);
    void forwarder.forwardWithFlowControl([
      '12',
      'truncate',
      json(['data', messages.truncate('issues')]),
    ]);
    void forwarder.forwardWithFlowControl([
      '13',
      'commit',
      json(['commit', messages.commit(), {watermark: '13'}]),
    ]);
    forwarder.add(sub3);
    forwarder.forward([
      '14',
      'begin',
      json(['begin', messages.begin(), {commitWatermark: '15'}]),
    ]);
    forwarder.add(sub4);

    for (const sub of [sub1, sub2, sub3, sub4]) {
      sub.close();
    }

    // sub1 gets all of the messages, as it was not added in a transaction.
    expect(stream1).toMatchInlineSnapshot(`
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
            "commitWatermark": "13",
          },
        ],
        [
          "data",
          {
            "relations": [
              {
                "name": "issues",
                "rowKey": {
                  "columns": [
                    "id",
                  ],
                  "type": "default",
                },
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
          {
            "commitWatermark": "15",
          },
        ],
      ]
    `);

    // sub2 and sub3 were added in a transaction. They only see the next
    // transaction.
    expect(stream2).toMatchInlineSnapshot(`
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
            "commitWatermark": "15",
          },
        ],
      ]
    `);
    expect(stream3).toMatchInlineSnapshot(`
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
            "commitWatermark": "15",
          },
        ],
      ]
    `);

    // sub4 was added in during the second transaction. It gets nothing.
    expect(stream4).toMatchInlineSnapshot(`
      [
        [
          "status",
          {
            "tag": "status",
          },
        ],
      ]
    `);
  });

  test('in transaction queueing, rolled back', () => {
    const forwarder = new Forwarder(createSilentLogContext());

    const [sub1, stream1] = createSubscriber('00', true);
    const [sub2, stream2] = createSubscriber('00', true);
    const [sub3, stream3] = createSubscriber('00', true);
    const [sub4, stream4] = createSubscriber('00', true);

    forwarder.add(sub1);
    forwarder.forward([
      '11',
      'begin',
      json(['begin', messages.begin(), {commitWatermark: '14'}]),
    ]);
    forwarder.add(sub2);
    forwarder.forward([
      '12',
      'truncate',
      json(['data', messages.truncate('issues')]),
    ]);
    void forwarder.forwardWithFlowControl([
      '13',
      'rollback',
      json(['rollback', messages.rollback()]),
    ]);
    forwarder.add(sub3);
    forwarder.forward([
      '14',
      'begin',
      json(['begin', messages.begin(), {commitWatermark: '15'}]),
    ]);
    forwarder.add(sub4);

    for (const sub of [sub1, sub2, sub3, sub4]) {
      sub.close();
    }

    // sub1 gets all of the messages, as it was not added in a transaction.
    expect(stream1).toMatchInlineSnapshot(`
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
            "commitWatermark": "14",
          },
        ],
        [
          "data",
          {
            "relations": [
              {
                "name": "issues",
                "rowKey": {
                  "columns": [
                    "id",
                  ],
                  "type": "default",
                },
                "schema": "public",
                "tag": "relation",
              },
            ],
            "tag": "truncate",
          },
        ],
        [
          "rollback",
          {
            "tag": "rollback",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "15",
          },
        ],
      ]
    `);

    // sub2 and sub3 were added in a transaction. They only see the next
    // transaction.
    expect(stream2).toMatchInlineSnapshot(`
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
            "commitWatermark": "15",
          },
        ],
      ]
    `);
    expect(stream3).toMatchInlineSnapshot(`
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
            "commitWatermark": "15",
          },
        ],
      ]
    `);

    // sub4 was added in during the second transaction. It gets nothing.
    expect(stream4).toMatchInlineSnapshot(`
      [
        [
          "status",
          {
            "tag": "status",
          },
        ],
      ]
    `);
  });
});
