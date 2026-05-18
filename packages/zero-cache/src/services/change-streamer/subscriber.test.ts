import {describe, expect, test} from 'vitest';
import type {StringifiedStreamPayload} from '../../types/streams.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {CHANGE_STREAMER_V7_PROTOCOL_VERSION} from './change-streamer-protocol.ts';
import {createSubscriber} from './test-utils.ts';

const json = JSON.stringify;

function expandPayload(payload: StringifiedStreamPayload): readonly string[] {
  return typeof payload === 'string' ? [payload] : payload;
}

describe('change-streamer/subscriber', () => {
  const messages = new ReplicationMessages({issues: 'id'});

  test('catchup and backlog', () => {
    const [sub, stream] = createSubscriber('00');

    // Send some messages while it is catching up.
    void sub.send([
      '11',
      'begin',
      json(['begin', messages.begin(), {commitWatermark: '12'}]),
    ]);
    void sub.send([
      '12',
      'commit',
      json(['commit', messages.commit(), {watermark: '12'}]),
    ]);

    // Status messages before initialization should be ignored.
    sub.sendStatus({tag: 'status', lagReport: {nextSendTimeMs: 123}});

    // Send catchup messages.
    void sub.catchup([
      '01',
      'begin',
      json(['begin', messages.begin(), {commitWatermark: '02'}]),
    ]);

    // Status messages after initialization are sent. These can happen
    // within a transaction.
    sub.sendStatus({tag: 'status', lagReport: {nextSendTimeMs: 234}});

    void sub.catchup([
      '02',
      'commit',
      json(['commit', messages.commit(), {watermark: '02'}]),
    ]);

    sub.setCaughtUp();

    // Send some messages after catchup.
    void sub.send([
      '21',
      'begin',
      json(['begin', messages.begin(), {commitWatermark: '22'}]),
    ]);
    void sub.send([
      '22',
      'commit',
      json(['commit', messages.commit(), {watermark: '22'}]),
    ]);

    sub.sendStatus({tag: 'status', lagReport: {nextSendTimeMs: 456}});

    sub.close();

    expect(stream).toMatchInlineSnapshot(`
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
            "commitWatermark": "02",
          },
        ],
        [
          "status",
          {
            "lagReport": {
              "nextSendTimeMs": 234,
            },
            "tag": "status",
          },
        ],
        [
          "commit",
          {
            "tag": "commit",
          },
          {
            "watermark": "02",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "12",
          },
        ],
        [
          "commit",
          {
            "tag": "commit",
          },
          {
            "watermark": "12",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "22",
          },
        ],
        [
          "commit",
          {
            "tag": "commit",
          },
          {
            "watermark": "22",
          },
        ],
        [
          "status",
          {
            "lagReport": {
              "nextSendTimeMs": 456,
            },
            "tag": "status",
          },
        ],
      ]
    `);
  });

  test('watermark filtering', () => {
    const [sub, stream] = createSubscriber('123');

    // Technically, catchup should never send any messages if the subscriber
    // is ahead, since the watermark query would return no results. But pretend it
    // does just to ensure that catchup messages are subject to the filter.
    void sub.catchup([
      '01',
      'begin',
      json(['begin', messages.begin(), {commitWatermark: '02'}]),
    ]);
    void sub.catchup([
      '02',
      'commit',
      json(['commit', messages.commit(), {watermark: '02'}]),
    ]);
    sub.setCaughtUp();

    // Still lower than the watermark ...
    void sub.send([
      '121',
      'begin',
      json(['begin', messages.begin(), {commitWatermark: '123'}]),
    ]);
    void sub.send([
      '123',
      'commit',
      json(['commit', messages.commit(), {watermark: '123'}]),
    ]);

    // These should be sent.
    void sub.send([
      '124',
      'begin',
      json(['begin', messages.begin(), {commitWatermark: '125'}]),
    ]);
    void sub.send([
      '125',
      'commit',
      json(['commit', messages.commit(), {watermark: '125'}]),
    ]);

    // Replays should be ignored.
    void sub.send([
      '124',
      'begin',
      json(['begin', messages.begin(), {commitWatermark: '125'}]),
    ]);
    void sub.send([
      '125',
      'commit',
      json(['commit', messages.commit(), {watermark: '125'}]),
    ]);

    sub.close();
    expect(stream).toMatchInlineSnapshot(`
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
            "commitWatermark": "125",
          },
        ],
        [
          "commit",
          {
            "tag": "commit",
          },
          {
            "watermark": "125",
          },
        ],
      ]
    `);
  });

  test('v7 sends ordered changes as named batches', async () => {
    const [sub, _, receiver] = createSubscriber(
      '00',
      true,
      CHANGE_STREAMER_V7_PROTOCOL_VERSION,
    );

    const pipeline = receiver.pipeline;
    expect(pipeline).toBeDefined();
    const iterator = pipeline![Symbol.asyncIterator]();
    const status = await iterator.next();
    expect(status.done).not.toBe(true);
    expect(JSON.parse(status.value.value as string)).toEqual([
      'status',
      {tag: 'status'},
    ]);
    status.value.consumed();

    const send = sub.sendBatch([
      [
        '11',
        'begin',
        json(['begin', messages.begin(), {commitWatermark: '12'}]),
      ],
      ['12', 'commit', json(['commit', messages.commit(), {watermark: '12'}])],
    ]);

    const next = await iterator.next();
    expect(next.done).not.toBe(true);
    const frame = next.value.value;
    expect(typeof frame).toBe('string');
    expect(JSON.parse(frame as string)).toMatchInlineSnapshot(`
      [
        "change-batch",
        {
          "changes": [
            [
              "begin",
              {
                "tag": "begin",
              },
              {
                "commitWatermark": "12",
              },
            ],
            [
              "commit",
              {
                "tag": "commit",
              },
              {
                "watermark": "12",
              },
            ],
          ],
          "tag": "change-batch",
        },
      ]
    `);

    expect(sub.acked).toBe('00');
    next.value.consumed();
    await send;
    expect(sub.acked).toBe('12');

    sub.close();
  });

  test('acks, pending, processed, stats', async () => {
    const [sub, _, receiver] = createSubscriber('00');

    // Send some messages while it is catching up.
    void sub.send([
      '11',
      'begin',
      json(['begin', messages.begin(), {commitWatermark: '12'}]),
    ]);
    void sub.send([
      '12',
      'commit',
      json(['commit', messages.commit(), {watermark: '12'}]),
    ]);

    // Send catchup messages.
    void sub.catchup([
      '01',
      'begin',
      json(['begin', messages.begin(), {commitWatermark: '02'}]),
    ]);
    void sub.catchup([
      '02',
      'commit',
      json(['commit', messages.commit(), {watermark: '02'}]),
    ]);

    sub.setCaughtUp();

    // Send some messages after catchup.
    void sub.send([
      '21',
      'begin',
      json(['begin', messages.begin(), {commitWatermark: '22'}]),
    ]);
    void sub.send([
      '22',
      'commit',
      json(['commit', messages.commit(), {watermark: '22'}]),
    ]);

    void sub.send([
      '31',
      'begin',
      json(['begin', messages.begin(), {commitWatermark: '31'}]),
    ]);

    expect(sub.acked).toBe('00');

    let processed = 0;
    let pending = 8;
    expect(sub.getStats()).toEqual({processRate: 0, pending: 8});
    expect(sub.numPending).toBe(pending);

    let txNum = 0;
    for await (const payload of receiver) {
      const beforeProcessed = processed;
      const beforePending = pending;
      const messages = expandPayload(payload);
      for (const json of messages) {
        const msg = JSON.parse(json);
        if (msg[0] === 'begin') {
          txNum++;
        }
        switch (txNum) {
          case 1:
            expect(sub.acked).toBe('00');
            break;
          case 2:
            expect(sub.acked).toBe('02');
            break;
          case 3:
            expect(sub.acked).toBe('12');
            break;
          case 4:
            expect(sub.acked).toBe('22');
            sub.close();
            break;
        }
      }
      expect(sub.numProcessed).toBe(beforeProcessed);
      expect(sub.numPending).toBe(beforePending);
      processed += messages.length;
      pending -= messages.length;
    }
    expect(sub.numProcessed).toBe(8);
    expect(
      sub.sampleProcessRate(performance.now()).getStats().processRate,
    ).toBeGreaterThan(0);
  });
});
