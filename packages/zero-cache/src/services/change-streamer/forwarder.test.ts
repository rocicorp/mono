import {describe, expect, test, vi} from 'vitest';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {Forwarder} from './forwarder.ts';
import {createSubscriber} from './test-utils.ts';

const json = BigIntJSON.stringify;

describe('change-streamer/forwarder', () => {
  const messages = new ReplicationMessages({issues: 'id'});

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

  test('sendStatus flushes pending data before queued subscribers activate', () => {
    vi.useFakeTimers();

    try {
      const forwarder = new Forwarder(createSilentLogContext());

      const [sub1, stream1] = createSubscriber('00', true);
      const [sub2, stream2] = createSubscriber('00', true);

      forwarder.add(sub1);
      forwarder.forward([
        '11',
        'begin',
        json(['begin', messages.begin(), {commitWatermark: '13'}]),
      ]);
      forwarder.add(sub2);
      forwarder.forward([
        '12',
        'truncate',
        json(['data', messages.truncate('issues')]),
      ]);

      expect(vi.getTimerCount()).toBe(1);
      forwarder.sendStatus({
        tag: 'status',
        lagReport: {nextSendTimeMs: 123},
      });
      expect(vi.getTimerCount()).toBe(0);

      forwarder.forward([
        '13',
        'commit',
        json(['commit', messages.commit(), {watermark: '13'}]),
      ]);
      forwarder.sendStatus({
        tag: 'status',
        lagReport: {nextSendTimeMs: 456},
      });

      sub1.close();
      sub2.close();

      expect(stream1.map(([tag]) => tag)).toEqual([
        'status',
        'begin',
        'data',
        'status',
        'commit',
        'status',
      ]);
      expect(stream1[3]).toEqual([
        'status',
        {tag: 'status', lagReport: {nextSendTimeMs: 123}},
      ]);
      expect(stream1[5]).toEqual([
        'status',
        {tag: 'status', lagReport: {nextSendTimeMs: 456}},
      ]);

      expect(stream2).toEqual([
        ['status', {tag: 'status'}],
        ['status', {tag: 'status', lagReport: {nextSendTimeMs: 456}}],
      ]);
    } finally {
      vi.useRealTimers();
    }
  });
});
