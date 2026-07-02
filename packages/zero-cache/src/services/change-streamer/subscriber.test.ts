import {describe, expect, test} from 'vitest';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {createSubscriber} from './test-utils.ts';

const json = JSON.stringify;

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

    void sub.setCaughtUp();

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
    void sub.setCaughtUp();

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

  test('backlog applies backpressure until drained', async () => {
    const [sub, _, receiver] = createSubscriber('00', false, {
      backlogHighWaterBytes: 1,
    });

    let released = false;
    const blocked = sub
      .send([
        '11',
        'begin',
        json(['begin', messages.begin(), {commitWatermark: '12'}]),
      ])
      .then(() => {
        released = true;
      });

    await Promise.resolve();
    expect(released).toBe(false);

    const drained = sub.setCaughtUp();
    await Promise.resolve();
    expect(released).toBe(false);

    receiver.cancel();
    await blocked;
    await drained;
    expect(released).toBe(true);
  });

  test('close releases backlog backpressure', async () => {
    const [sub] = createSubscriber('00', false, {
      backlogHighWaterBytes: 1,
    });

    let released = false;
    const blocked = sub
      .send([
        '11',
        'begin',
        json(['begin', messages.begin(), {commitWatermark: '12'}]),
      ])
      .then(() => {
        released = true;
      });

    await Promise.resolve();
    expect(released).toBe(false);

    sub.close();
    await blocked;
    expect(released).toBe(true);
  });

  test('setCaughtUp drains backlog with bounded in-flight sends', async () => {
    const [sub, _, receiver] = createSubscriber('00', false, {
      backlogHighWaterBytes: 1,
    });

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
    void sub.send([
      '21',
      'begin',
      json(['begin', messages.begin(), {commitWatermark: '22'}]),
    ]);

    const drained = sub.setCaughtUp();

    // Status initialization plus the first backlog entry. The remaining
    // backlog stays buffered until the receiver consumes this window.
    expect(receiver.queued).toBe(2);
    receiver.cancel();
    await drained;
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

    void sub.setCaughtUp();

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
    const initialStats = sub.getStats();
    expect(initialStats.processRate).toBe(0);
    expect(initialStats.pending).toBe(8);
    expect(initialStats.backlog).toBe(0);
    expect(initialStats.backlogBytes).toBeGreaterThan(0);
    expect(sub.numPending).toBe(pending);

    let txNum = 0;
    for await (const json of receiver) {
      const msg = JSON.parse(json);
      expect(sub.numProcessed).toBe(processed++);
      expect(sub.numPending).toBe(pending--);

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
    expect(sub.numProcessed).toBe(8);
    expect(
      sub.sampleProcessRate(performance.now()).getStats().processRate,
    ).toBeGreaterThan(0);
  });
});
