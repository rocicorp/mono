import {describe, expect, test, vi} from 'vitest';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {StreamTooFarBehind} from './error-type-enum.ts';
import {Forwarder} from './forwarder.ts';
import type {SubscriberStats} from './subscriber.ts';
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

  describe('lagging subscriber disconnection', () => {
    // Adds an active subscriber whose sampled stats are controlled directly, so
    // the progress-monitor's classification can be driven deterministically. The
    // real reportChangeRate() accumulation still runs, so its `missedLastTimeout`
    // state is set for real to match the mocked stats.
    function addSubscriber(
      forwarder: Forwarder,
      stats: {processRate: number; missedLastTimeout: boolean},
    ) {
      const [sub] = createSubscriber('00', true);
      vi.spyOn(sub, 'getStats').mockReturnValue({
        processRate: stats.processRate,
        pending: 0,
        backlog: 0,
        backlogBytes: 0,
        totalBufferedBytes: 0,
        missedLastTimeout: stats.missedLastTimeout,
      } satisfies SubscriberStats);
      sub.trackResponseResult(
        stats.missedLastTimeout ? 'timed-out' : 'on-time',
      );
      const close = vi.spyOn(sub, 'close').mockImplementation(() => {});
      forwarder.add(sub);
      return {sub, close};
    }

    test('a lagging subscriber is disconnected only after the grace period', () => {
      const forwarder = new Forwarder(createSilentLogContext(), {
        flowControlConsensusTimeoutProportion: 2,
        flowControlSlowSubscriberGracePeriodMs: 1000,
      });
      // The healthy subscriber's positive rate is the baseline; a `0`-seeded
      // slowestOnTime would never adopt it and detection would silently no-op.
      const healthy = addSubscriber(forwarder, {
        processRate: 10,
        missedLastTimeout: false,
      });
      const laggard = addSubscriber(forwarder, {
        processRate: 1,
        missedLastTimeout: true,
      });

      // Within the grace period the laggard is left alone.
      forwarder.checkSubscriberProgress(1000);
      forwarder.checkSubscriberProgress(1500);
      expect(laggard.close).not.toHaveBeenCalled();

      // Once it has lagged continuously for the grace period, it is disconnected.
      forwarder.checkSubscriberProgress(2000);
      expect(laggard.close).toHaveBeenCalledWith(
        StreamTooFarBehind,
        expect.stringContaining('lagging'),
      );
      expect(healthy.close).not.toHaveBeenCalled();
    });

    test('a catching-up subscriber is never disconnected', () => {
      const forwarder = new Forwarder(createSilentLogContext(), {
        flowControlConsensusTimeoutProportion: 2,
        flowControlSlowSubscriberGracePeriodMs: 1000,
      });
      addSubscriber(forwarder, {processRate: 10, missedLastTimeout: false});
      // Faster than the healthy baseline: timing out is expected while catching
      // up, so it must never be counted as lagging no matter how long it takes.
      const catchingUp = addSubscriber(forwarder, {
        processRate: 100,
        missedLastTimeout: true,
      });

      forwarder.checkSubscriberProgress(1000);
      forwarder.checkSubscriberProgress(5000);
      forwarder.checkSubscriberProgress(60_000);
      expect(catchingUp.close).not.toHaveBeenCalled();
    });

    test('recovering before the grace period elapses avoids disconnection', () => {
      const forwarder = new Forwarder(createSilentLogContext(), {
        flowControlConsensusTimeoutProportion: 2,
        flowControlSlowSubscriberGracePeriodMs: 1000,
      });
      addSubscriber(forwarder, {processRate: 10, missedLastTimeout: false});
      const {sub, close} = addSubscriber(forwarder, {
        processRate: 1,
        missedLastTimeout: true,
      });

      forwarder.checkSubscriberProgress(1000);
      forwarder.checkSubscriberProgress(1500);

      // A subsequent broadcast the subscriber responds to on time resets the
      // lagging clock, so it survives even a later stretch of lagging.
      sub.trackResponseResult('on-time');
      vi.spyOn(sub, 'getStats').mockReturnValue({
        processRate: 1,
        pending: 0,
        backlog: 0,
        backlogBytes: 0,
        totalBufferedBytes: 0,
        missedLastTimeout: true,
      });
      sub.trackResponseResult('timed-out');

      forwarder.checkSubscriberProgress(2400); // clock restarts here
      forwarder.checkSubscriberProgress(3200); // only 800ms of lagging so far
      expect(close).not.toHaveBeenCalled();
    });

    test('detection is disabled when no grace period is configured', () => {
      const forwarder = new Forwarder(createSilentLogContext(), {
        flowControlConsensusTimeoutProportion: 2,
        // flowControlSlowSubscriberGracePeriodMs omitted -> disabled.
      });
      addSubscriber(forwarder, {processRate: 10, missedLastTimeout: false});
      const laggard = addSubscriber(forwarder, {
        processRate: 1,
        missedLastTimeout: true,
      });

      forwarder.checkSubscriberProgress(1000);
      forwarder.checkSubscriberProgress(100_000);
      expect(laggard.close).not.toHaveBeenCalled();
    });

    test('no subscriber is disconnected without an on-time baseline', () => {
      const forwarder = new Forwarder(createSilentLogContext(), {
        flowControlConsensusTimeoutProportion: 2,
        flowControlSlowSubscriberGracePeriodMs: 1000,
      });
      // Every subscriber timed out: there is no healthy peer to judge against,
      // so none can be classified as lagging.
      const laggard1 = addSubscriber(forwarder, {
        processRate: 1,
        missedLastTimeout: true,
      });
      const laggard2 = addSubscriber(forwarder, {
        processRate: 2,
        missedLastTimeout: true,
      });

      forwarder.checkSubscriberProgress(1000);
      forwarder.checkSubscriberProgress(100_000);
      expect(laggard1.close).not.toHaveBeenCalled();
      expect(laggard2.close).not.toHaveBeenCalled();
    });
  });
});
