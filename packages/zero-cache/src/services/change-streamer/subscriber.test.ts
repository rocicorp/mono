import {describe, expect, test, vi} from 'vitest';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {BackfillState} from './backfill-state.ts';
import {preSerializeBatch} from './broadcast.ts';
import type {WatermarkedChange} from './change-streamer.ts';
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
      totalBufferedBytes: 52027, // update this as necessary
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
      const msg =
        typeof json === 'string'
          ? JSON.parse(json)
          : JSON.parse(json.changes[0][2]);
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

  describe('lagging detection', () => {
    test('trackResponseResult sets missedLastTimeout', () => {
      const [sub] = createSubscriber('00', true);
      expect(sub.getStats().missedLastTimeout).toBe(false);

      sub.trackResponseResult('timed-out');
      expect(sub.getStats().missedLastTimeout).toBe(true);

      sub.trackResponseResult('on-time');
      expect(sub.getStats().missedLastTimeout).toBe(false);
    });

    test('reportChangeRate asserts the subscriber missed the last timeout', () => {
      const [sub] = createSubscriber('00', true);
      // Not timed out: reporting a change rate is a programming error.
      expect(() => sub.reportChangeRate(100, 'lagging')).toThrow(
        'reportChangeRate should only be called for slow subscribers',
      );

      sub.trackResponseResult('timed-out');
      expect(() => sub.reportChangeRate(100, 'lagging')).not.toThrow();
    });

    test('lagging duration accumulates continuously from the first report', () => {
      const [sub] = createSubscriber('00', true);
      sub.trackResponseResult('timed-out');

      // First report starts the clock; duration is measured from here.
      expect(sub.reportChangeRate(1000, 'lagging')).toBe(0);
      expect(sub.reportChangeRate(1500, 'lagging')).toBe(500);
      expect(sub.reportChangeRate(2200, 'lagging')).toBe(1200);
    });

    test('catching-up resets the lagging clock', () => {
      const [sub] = createSubscriber('00', true);
      sub.trackResponseResult('timed-out');

      expect(sub.reportChangeRate(1000, 'lagging')).toBe(0);
      expect(sub.reportChangeRate(1500, 'lagging')).toBe(500);

      // A single catching-up sample breaks continuity and resets the clock.
      expect(sub.reportChangeRate(1600, 'catching-up')).toBe(0);

      // Subsequent lagging restarts from the next report.
      expect(sub.reportChangeRate(1700, 'lagging')).toBe(0);
      expect(sub.reportChangeRate(1900, 'lagging')).toBe(200);
    });

    test('an on-time response resets the lagging clock', () => {
      const [sub] = createSubscriber('00', true);
      sub.trackResponseResult('timed-out');
      expect(sub.reportChangeRate(1000, 'lagging')).toBe(0);
      expect(sub.reportChangeRate(1800, 'lagging')).toBe(800);

      // Responding on time breaks continuity, even before another timeout.
      sub.trackResponseResult('on-time');
      sub.trackResponseResult('timed-out');
      expect(sub.reportChangeRate(2000, 'lagging')).toBe(0);
      expect(sub.reportChangeRate(2300, 'lagging')).toBe(300);
    });
  });

  describe('sendBatch with pre-serialized shared buffer', () => {
    test('steady-state subscriber uses single pre-serialized push and advances acked', async () => {
      const onAck = vi.fn();
      const [sub, , receiver] = createSubscriber('00', true, {onAck});

      const changes: WatermarkedChange[] = [
        [
          '01',
          'begin',
          json(['begin', messages.begin(), {commitWatermark: '02'}]),
        ],
        [
          '02',
          'commit',
          json(['commit', messages.commit(), {watermark: '02'}]),
        ],
      ];
      const preSerialized = preSerializeBatch(changes);

      const sendPromise = sub.sendBatch(changes, preSerialized);

      // Status was queued on init (1), plus 1 pre-serialized batch item = 2 total in queue.
      expect(receiver.queued).toBe(2);
      expect(sub.watermark).toBe('02');
      expect(sub.acked).toBe('00'); // not acked until downstream consumes

      // Consume downstream messages
      const pipeline = receiver.pipeline!;
      const it = pipeline[Symbol.asyncIterator]();

      const statusItem = (await it.next()).value!;
      statusItem.consumed();

      const batchItem = (await it.next()).value!;
      expect(batchItem.value).toBe(preSerialized);
      expect(batchItem.value.changes).toEqual(changes);
      batchItem.consumed();

      await sendPromise;

      expect(sub.acked).toBe('02');
      expect(onAck).toHaveBeenCalledWith('02');

      sub.close();
    });

    test('fallback to individual send when subscriber is backlogged', () => {
      const [sub, stream, receiver] = createSubscriber('00', false); // catching up

      const changes: WatermarkedChange[] = [
        [
          '01',
          'begin',
          json(['begin', messages.begin(), {commitWatermark: '02'}]),
        ],
        [
          '02',
          'commit',
          json(['commit', messages.commit(), {watermark: '02'}]),
        ],
      ];
      const preSerialized = preSerializeBatch(changes);

      void sub.sendBatch(changes, preSerialized);

      // Backlog buffers the changes individually; nothing pushed downstream yet
      expect(receiver.queued).toBe(0);

      // Catchup and mark caught up
      void sub.catchup([
        '00',
        'begin',
        json(['begin', messages.begin(), {commitWatermark: '00'}]),
      ]);
      void sub.setCaughtUp();

      sub.close();
      expect(stream.length).toBeGreaterThan(0);
    });

    test('fallback to individual send when wsBatched is false', () => {
      const [sub, stream, receiver] = createSubscriber('00', true, {
        wsBatched: false,
      });

      const changes: WatermarkedChange[] = [
        [
          '01',
          'begin',
          json(['begin', messages.begin(), {commitWatermark: '02'}]),
        ],
        [
          '02',
          'commit',
          json(['commit', messages.commit(), {watermark: '02'}]),
        ],
      ];
      const preSerialized = preSerializeBatch(changes);

      void sub.sendBatch(changes, preSerialized);

      // When wsBatched is false, items are pushed individually: 1 status + 2 changes = 3 queued
      expect(receiver.queued).toBe(3);

      sub.close();
      expect(stream).toHaveLength(3);
    });
  });

  test('tracks backfills and signals alignment', () => {
    const table = {schema: 'public', name: 'issues'};
    const backfills = new BackfillState([
      {
        table: {...table, metadata: null},
        columns: {a: {id: {a: 1}, progress: {progressMark: '05'}}},
      },
    ]);
    const onAligned = vi.fn();
    const onBackfillIgnored = vi.fn();
    const [sub] = createSubscriber('00', false, {
      backfills,
      onAligned,
      onBackfillIgnored,
    });
    const backfill = (
      watermark: string,
      previous: string,
      current: string,
    ): WatermarkedChange => [
      watermark,
      'backfill',
      json([
        'data',
        {
          tag: 'backfill',
          relation: {...table, rowKey: {columns: ['id']}},
          columns: ['a'],
          watermark: '01',
          rowValues: [],
          progressMarks: {
            previous: {progressMark: previous},
            current: {progressMark: current},
          },
        },
      ]),
    ];
    const tx = (watermark: string, ...changes: WatermarkedChange[]) => [
      [
        watermark,
        'begin',
        json(['begin', messages.begin(), {commitWatermark: watermark}]),
      ] satisfies WatermarkedChange,
      ...changes,
      [
        watermark,
        'commit',
        json(['commit', messages.commit(), {watermark}]),
      ] satisfies WatermarkedChange,
    ];

    // Live changes during catchup are backlogged.
    for (const change of tx('03', backfill('03', '07', '09'))) {
      void sub.send(change);
    }
    // Catchup messages are tracked.
    for (const change of tx('02', backfill('02', '05', '07'))) {
      void sub.catchup(change);
    }
    expect(backfills.requests()[0].columns.a.progress).toEqual({
      progressMark: '07',
    });
    expect(onAligned).not.toHaveBeenCalled();

    void sub.setCaughtUp();
    // The backlog is tracked before alignment is signaled.
    expect(backfills.requests()[0].columns.a.progress).toEqual({
      progressMark: '09',
    });
    expect(onAligned).toHaveBeenCalledExactlyOnceWith(sub);
    expect(sub.aligned).toBe(true);
    expect(onBackfillIgnored).not.toHaveBeenCalled();

    // Ignored backfill data after alignment is reported.
    for (const change of tx('04', backfill('04', '10', '12'))) {
      void sub.send(change);
    }
    expect(onBackfillIgnored).toHaveBeenCalledWith(['public.issues.a']);
    expect(backfills.requests()[0].columns.a.progress).toEqual({
      progressMark: '09',
    });
  });
});
