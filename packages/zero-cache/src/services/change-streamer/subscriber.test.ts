import {describe, expect, test} from 'vitest';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import * as ErrorType from './error-type-enum.ts';
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

  test('whenBacklogFull resolves at the same point send() blocks', async () => {
    const [sub] = createSubscriber('00', false, {
      backlogHighWaterBytes: 1_000,
    });

    let full = false;
    const backlogFull = sub.whenBacklogFull();
    void backlogFull.promise.then(() => {
      full = true;
    });
    expect(sub.backlogFull).toBe(false);

    const small = json(['begin', messages.begin(), {commitWatermark: '12'}]);
    expect(small.length).toBeLessThan(1_000);
    let released = false;
    void sub.send(['11', 'begin', small]).then(() => {
      released = true;
    });

    await Promise.resolve();
    expect(full).toBe(false);
    expect(released).toBe(true);

    // Enough to cross the mark, which is exactly where send() stops resolving.
    void sub.send(['12', 'commit', 'x'.repeat(1_000)]);

    await Promise.resolve();
    expect(sub.backlogFull).toBe(true);
    expect(full).toBe(true);
  });

  test('close releases whenBacklogFull waiters', async () => {
    const [sub] = createSubscriber('00', false, {
      backlogHighWaterBytes: 1,
    });

    let full = false;
    const backlogFull = sub.whenBacklogFull();
    const waiting = backlogFull.promise.then(() => {
      full = true;
    });

    sub.close();
    await waiting;
    // Resolved so the waiter is not stranded, but the backlog is gone, so a
    // caller that re-checks does not mistake this for an overflow.
    expect(full).toBe(true);
    expect(sub.backlogFull).toBe(false);
  });

  test('whenBacklogFull waiters can be cancelled', async () => {
    const [sub] = createSubscriber('00', false, {
      backlogHighWaterBytes: 1,
    });

    let full = false;
    const backlogFull = sub.whenBacklogFull();
    void backlogFull.promise.then(() => {
      full = true;
    });
    backlogFull.cancel();

    const blocked = sub.send([
      '11',
      'begin',
      json(['begin', messages.begin(), {commitWatermark: '12'}]),
    ]);
    await Promise.resolve();
    expect(sub.backlogFull).toBe(true);
    expect(full).toBe(false);

    sub.close();
    await blocked;
    await Promise.resolve();
    expect(full).toBe(false);
  });

  test('fail ends the subscription without sending an error', async () => {
    const [sub, , receiver] = createSubscriber();
    const iterator = receiver[Symbol.asyncIterator]();

    sub.fail(new Error('boom'));

    // No ['error', ...] downstream: IncrementalSyncer would treat it as
    // terminal and restore a fresh replica, where these failures only warrant
    // a reconnect.
    expect((await iterator.next()).done).toBe(true);
  });

  test('close with an error type sends it downstream', async () => {
    const [sub, , receiver] = createSubscriber();
    const iterator = receiver[Symbol.asyncIterator]();

    sub.close(ErrorType.WatermarkTooOld, 'too old');

    const error = await iterator.next();
    expect(error.done).toBeFalsy();
    expect(JSON.parse(error.value as string)).toEqual([
      'error',
      {type: ErrorType.WatermarkTooOld, message: 'too old'},
    ]);
    expect((await iterator.next()).done).toBe(true);
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

  test('post-catchup live sends accumulate without downstream consumption', async () => {
    const [sub, _, receiver] = createSubscriber('00', true);

    let completed = 0;
    const sends: Promise<void>[] = [];
    const count = 1000;

    for (let i = 0; i < count; i++) {
      const watermark = String(i + 1).padStart(4, '0');
      sends.push(
        sub
          .send([
            watermark,
            'begin',
            json(['begin', messages.begin(), {commitWatermark: watermark}]),
          ])
          .then(() => {
            completed++;
          }),
      );
    }

    await Promise.resolve();

    // Status initialization plus every live send is retained because nothing
    // is consuming the downstream Subscription.
    expect(receiver.queued).toBe(count + 1);
    expect(sub.getStats()).toMatchObject({
      pending: count + 1,
      backlog: 0,
      backlogBytes: 0,
    });
    expect(completed).toBe(0);

    receiver.cancel();
    await Promise.all(sends);
    expect(completed).toBe(count);
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

  test('onAck reports each advance of the acked watermark', async () => {
    const acks: string[] = [];
    const [sub, _, receiver] = createSubscriber('00', true, {
      onAck: watermark => acks.push(watermark),
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
    void sub.send([
      '22',
      'commit',
      json(['commit', messages.commit(), {watermark: '22'}]),
    ]);
    // Trailing message: a commit is only acked once the consumer moves past it.
    void sub.send([
      '31',
      'begin',
      json(['begin', messages.begin(), {commitWatermark: '32'}]),
    ]);

    let count = 0;
    for await (const _json of receiver) {
      // The status message from setCaughtUp() plus the five sends.
      if (++count === 6) {
        sub.close();
      }
    }

    // Only commits are acked, and only when the subscriber confirms them.
    expect(acks).toEqual(['12', '22']);
    expect(sub.acked).toBe('22');
  });
});
