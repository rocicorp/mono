import {describe, expect, test} from 'vitest';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {Broadcast} from './broadcast.ts';
import type {WatermarkedChange} from './change-streamer.ts';
import {createSubscriber} from './test-utils.ts';

const json = BigIntJSON.stringify;

describe('change-streamer/broadcast', () => {
  const messages = new ReplicationMessages({issues: 'id'});
  const lc = createSilentLogContext();

  test('without tracking', () => {
    const [sub1, stream1] = createSubscriber('00', true);
    const [sub2, stream2] = createSubscriber('00', true);
    const [sub3, stream3] = createSubscriber('00', true);
    const [sub4, stream4] = createSubscriber('00', true);

    Broadcast.withoutTracking(
      [sub1, sub2, sub3, sub4],
      [
        '11',
        'begin',
        json(['begin', messages.begin(), {commitWatermark: '13'}]),
      ],
    );

    for (const sub of [sub1, sub2, sub3, sub4]) {
      sub.close();
    }

    for (const stream of [stream1, stream2, stream3, stream4]) {
      // sub1 gets all of the messages, as it was not added in a transaction.
      expect(stream).toMatchObject([
        ['status', {tag: 'status'}],
        ['begin', {tag: 'begin'}, {commitWatermark: '13'}],
      ]);
    }
  });

  test('with tracking', async () => {
    const [sub1, stream1] = createSubscriber('00', true);
    const [sub2, stream2] = createSubscriber('00', true);
    const [sub3, stream3] = createSubscriber('00', true);
    const [sub4, stream4] = createSubscriber('00', true);

    const broadcast = new Broadcast(
      lc,
      [sub1, sub2, sub3, sub4],
      [
        '11',
        'begin',
        json(['begin', messages.begin(), {commitWatermark: '13'}]),
      ],
    );

    expect(broadcast.isDone).toBe(false);

    for (const sub of [sub1, sub2, sub3]) {
      sub.close();
    }

    expect(broadcast.isDone).toBe(false);
    sub4.close();

    await broadcast.done;
    expect(broadcast.isDone).toBe(true);

    for (const stream of [stream1, stream2, stream3, stream4]) {
      // sub1 gets all of the messages, as it was not added in a transaction.
      expect(stream).toMatchObject([
        ['status', {tag: 'status'}],
        ['begin', {tag: 'begin'}, {commitWatermark: '13'}],
      ]);
    }
  });

  const begin: WatermarkedChange = [
    '11',
    'begin',
    json(['begin', messages.begin(), {commitWatermark: '13'}]),
  ];

  function captureTimers() {
    const scheduled: {
      cb: () => void;
      ms: number | undefined;
      handle: number;
    }[] = [];
    const cleared: number[] = [];
    let nextHandle = 1;
    const setTimeoutFn = ((cb: () => void, ms?: number) => {
      const handle = nextHandle++;
      scheduled.push({cb, ms, handle});
      return handle;
    }) as unknown as typeof setTimeout;
    const clearTimeoutFn = ((handle?: number) => {
      if (handle !== undefined) {
        cleared.push(handle);
      }
    }) as unknown as typeof clearTimeout;
    return {scheduled, cleared, setTimeoutFn, clearTimeoutFn};
  }

  test('early release once a majority acks', async () => {
    const [sub1] = createSubscriber('00', true);
    const [sub2] = createSubscriber('00', true);
    const [sub3] = createSubscriber('00', true);
    const [sub4] = createSubscriber('00', true);
    const {scheduled, setTimeoutFn, clearTimeoutFn} = captureTimers();

    const broadcast = new Broadcast(lc, [sub1, sub2, sub3, sub4], begin, {
      consensusTimeoutProportion: 2,
      setTimeoutFn,
      clearTimeoutFn,
    });

    // Two acks: below the majority of 3, so no early-release timer is armed.
    sub1.close();
    sub2.close();
    await sleep(1);
    expect(scheduled).toHaveLength(0);
    expect(broadcast.isDone).toBe(false);

    // Third ack reaches the majority: the release timer is armed with a delay
    // proportional to how long the majority took to ack (ceil, so >= 1ms).
    sub3.close();
    await sleep(1);
    expect(scheduled).toHaveLength(1);
    expect(Number.isInteger(scheduled[0].ms)).toBe(true);
    expect(scheduled[0].ms).toBeGreaterThanOrEqual(1);
    expect(broadcast.isDone).toBe(false);

    // Firing the timer releases via consensus-timeout, without a 1s tick.
    scheduled[0].cb();
    await broadcast.done;
    expect(broadcast.isDone).toBe(true);
    expect(broadcast.releaseMode).toBe('consensus-timeout');
  });

  test('a zero proportion arms an immediate release timer', async () => {
    const [sub1] = createSubscriber('00', true);
    const [sub2] = createSubscriber('00', true);
    const [sub3] = createSubscriber('00', true);
    const [sub4] = createSubscriber('00', true);
    const {scheduled, setTimeoutFn, clearTimeoutFn} = captureTimers();

    const broadcast = new Broadcast(lc, [sub1, sub2, sub3, sub4], begin, {
      consensusTimeoutProportion: 0,
      setTimeoutFn,
      clearTimeoutFn,
    });

    // Reaching the majority of 3 with a proportion of 0 arms a 0ms timer,
    // regardless of how long the majority took.
    sub1.close();
    sub2.close();
    sub3.close();
    await sleep(1);
    expect(scheduled).toHaveLength(1);
    expect(scheduled[0].ms).toBe(0);

    scheduled[0].cb();
    await broadcast.done;
    expect(broadcast.releaseMode).toBe('consensus-timeout');
  });

  test('early-release timer is armed once at majority and not re-armed by later completions', async () => {
    const [sub1] = createSubscriber('00', true);
    const [sub2] = createSubscriber('00', true);
    const [sub3] = createSubscriber('00', true);
    const [sub4] = createSubscriber('00', true);
    const [sub5] = createSubscriber('00', true);
    const {scheduled, cleared, setTimeoutFn, clearTimeoutFn} = captureTimers();

    const broadcast = new Broadcast(lc, [sub1, sub2, sub3, sub4, sub5], begin, {
      consensusTimeoutProportion: 2,
      setTimeoutFn,
      clearTimeoutFn,
    });

    // Majority of 5 is 3: arms the timer exactly once.
    sub1.close();
    sub2.close();
    sub3.close();
    await sleep(1);
    expect(scheduled).toHaveLength(1);

    // A later completion does NOT re-arm the timer (fixed proportional deadline)
    // and does not cancel the already-armed one.
    sub4.close();
    await sleep(1);
    expect(scheduled).toHaveLength(1);
    expect(cleared).not.toContain(scheduled[0].handle);
    expect(broadcast.isDone).toBe(false);

    // The single armed timer releases the broadcast.
    scheduled[0].cb();
    await broadcast.done;
    expect(broadcast.releaseMode).toBe('consensus-timeout');
  });

  test('all-subscribers release takes precedence over a pending early-release timer', async () => {
    const [sub1] = createSubscriber('00', true);
    const [sub2] = createSubscriber('00', true);
    const [sub3] = createSubscriber('00', true);
    const {scheduled, cleared, setTimeoutFn, clearTimeoutFn} = captureTimers();

    const broadcast = new Broadcast(lc, [sub1, sub2, sub3], begin, {
      consensusTimeoutProportion: 2,
      setTimeoutFn,
      clearTimeoutFn,
    });

    // Majority of 3 is 2: arms the timer.
    sub1.close();
    sub2.close();
    await sleep(1);
    expect(scheduled).toHaveLength(1);

    // All subscribers ack before the timer fires.
    sub3.close();
    await broadcast.done;
    expect(broadcast.releaseMode).toBe('all-subscribers');
    // Resolving cancels the pending early-release timer.
    expect(cleared).toContain(scheduled[0].handle);

    // A late timer callback is a no-op: the broadcast is already done.
    scheduled[0].cb();
    expect(broadcast.releaseMode).toBe('all-subscribers');
  });

  test('without early-release options, only all-subscribers releases', async () => {
    const [sub1] = createSubscriber('00', true);
    const [sub2] = createSubscriber('00', true);
    const [sub3] = createSubscriber('00', true);
    const [sub4] = createSubscriber('00', true);

    const broadcast = new Broadcast(lc, [sub1, sub2, sub3, sub4], begin);

    // Reaching the majority of 3 does not release when early release is off.
    sub1.close();
    sub2.close();
    sub3.close();
    await sleep(1);
    expect(broadcast.isDone).toBe(false);

    sub4.close();
    await broadcast.done;
    expect(broadcast.releaseMode).toBe('all-subscribers');
  });

  test('a consensus-timeout release marks pending subscribers as timed-out', async () => {
    const [sub1] = createSubscriber('00', true);
    const [sub2] = createSubscriber('00', true);
    const [sub3] = createSubscriber('00', true);
    const [sub4] = createSubscriber('00', true);
    const {scheduled, setTimeoutFn, clearTimeoutFn} = captureTimers();

    const broadcast = new Broadcast(lc, [sub1, sub2, sub3, sub4], begin, {
      consensusTimeoutProportion: 2,
      setTimeoutFn,
      clearTimeoutFn,
    });

    // Majority of 3 acks; sub4 remains pending.
    sub1.close();
    sub2.close();
    sub3.close();
    await sleep(1);

    // Firing the timer releases via consensus-timeout: the acked subscribers are
    // recorded on-time and the one still pending is recorded timed-out.
    scheduled[0].cb();
    await broadcast.done;
    expect(sub1.getStats().missedLastTimeout).toBe(false);
    expect(sub2.getStats().missedLastTimeout).toBe(false);
    expect(sub3.getStats().missedLastTimeout).toBe(false);
    expect(sub4.getStats().missedLastTimeout).toBe(true);
  });

  test('an all-subscribers release marks every subscriber on-time', async () => {
    const [sub1] = createSubscriber('00', true);
    const [sub2] = createSubscriber('00', true);
    const [sub3] = createSubscriber('00', true);

    // Pre-existing timed-out state is cleared once the subscriber acks.
    sub3.trackResponseResult('timed-out');

    const broadcast = new Broadcast(lc, [sub1, sub2, sub3], begin, {
      consensusTimeoutProportion: 2,
      setTimeoutFn: captureTimers().setTimeoutFn,
      clearTimeoutFn: captureTimers().clearTimeoutFn,
    });

    sub1.close();
    sub2.close();
    sub3.close();
    await broadcast.done;
    expect(broadcast.releaseMode).toBe('all-subscribers');
    expect(sub1.getStats().missedLastTimeout).toBe(false);
    expect(sub2.getStats().missedLastTimeout).toBe(false);
    expect(sub3.getStats().missedLastTimeout).toBe(false);
  });

  describe('backup-replicator is a required consensus member', () => {
    // Helper: a serving-mode subscriber (the default) and a backup-mode one.
    const serving = () => createSubscriber('00', true);
    const backup = () => createSubscriber('00', true, {}, 'backup');

    test('a majority of serving replicas does NOT early-release while the backup is still pending', async () => {
      const [s1] = serving();
      const [s2] = serving();
      const [s3] = serving();
      const [b] = backup();
      const {scheduled, setTimeoutFn, clearTimeoutFn} = captureTimers();

      // 4 subscribers => majority of 3.
      const broadcast = new Broadcast(lc, [s1, s2, s3, b], begin, {
        consensusTimeoutProportion: 2,
        setTimeoutFn,
        clearTimeoutFn,
      });

      // All 3 serving replicas ack: the majority is met, but the backup has
      // not responded, so no early-release timer is armed.
      s1.close();
      s2.close();
      s3.close();
      await sleep(1);
      expect(scheduled).toHaveLength(0);
      expect(broadcast.isDone).toBe(false);

      // The backup acking completes the set and releases via all-subscribers.
      b.close();
      await broadcast.done;
      expect(broadcast.releaseMode).toBe('all-subscribers');
      // The backup is never marked as a laggard.
      expect(b.getStats().missedLastTimeout).toBe(false);
    });

    test('the backup acking AFTER the majority arms the timer and drops the remaining laggard', async () => {
      const [s1] = serving();
      const [s2] = serving();
      const [s3] = serving();
      const [s4] = serving();
      const [b] = backup();
      const {scheduled, setTimeoutFn, clearTimeoutFn} = captureTimers();

      // 5 subscribers => majority of 3.
      const broadcast = new Broadcast(lc, [s1, s2, s3, s4, b], begin, {
        consensusTimeoutProportion: 2,
        setTimeoutFn,
        clearTimeoutFn,
      });

      // 3 serving acks reach the majority, but the backup is still pending, so
      // no timer is armed (regression guard: `>=` vs `===` on the majority).
      s1.close();
      s2.close();
      s3.close();
      await sleep(1);
      expect(scheduled).toHaveLength(0);
      expect(broadcast.isDone).toBe(false);

      // The backup acks last. Even though `completed` (4) is now past the
      // majority (3), the timer must still arm so the remaining laggard (s4)
      // does not stall the pipeline.
      b.close();
      await sleep(1);
      expect(scheduled).toHaveLength(1);
      expect(broadcast.isDone).toBe(false);

      // Firing the timer drops s4 as a laggard; the backup stays on-time.
      scheduled[0].cb();
      await broadcast.done;
      expect(broadcast.releaseMode).toBe('consensus-timeout');
      expect(s4.getStats().missedLastTimeout).toBe(true);
      expect(b.getStats().missedLastTimeout).toBe(false);
    });

    test('a backup that acks WITHIN the majority arms the timer at the majority', async () => {
      const [b] = backup();
      const [s1] = serving();
      const [s2] = serving();
      const [s3] = serving();
      const {scheduled, setTimeoutFn, clearTimeoutFn} = captureTimers();

      // 4 subscribers => majority of 3.
      const broadcast = new Broadcast(lc, [b, s1, s2, s3], begin, {
        consensusTimeoutProportion: 2,
        setTimeoutFn,
        clearTimeoutFn,
      });

      // Backup acks early, then two serving replicas reach the majority: the
      // timer arms at the majority since the backup has already responded.
      b.close();
      s1.close();
      s2.close();
      await sleep(1);
      expect(scheduled).toHaveLength(1);
      expect(broadcast.isDone).toBe(false);

      // The remaining serving replica is dropped; the backup is never a laggard.
      scheduled[0].cb();
      await broadcast.done;
      expect(broadcast.releaseMode).toBe('consensus-timeout');
      expect(s3.getStats().missedLastTimeout).toBe(true);
      expect(b.getStats().missedLastTimeout).toBe(false);
    });

    test('a consensus-timeout with a still-pending backup never marks the backup timed-out', async () => {
      // Guards the core guarantee directly: even if the timer somehow fires
      // while the backup is pending, the backup must not be flagged as a
      // laggard. With the fix the timer cannot arm until the backup responds,
      // so this asserts the backup is on-time after a normal release.
      const [s1] = serving();
      const [s2] = serving();
      const [s3] = serving();
      const [b] = backup();
      const {scheduled, setTimeoutFn, clearTimeoutFn} = captureTimers();

      const broadcast = new Broadcast(lc, [s1, s2, s3, b], begin, {
        consensusTimeoutProportion: 2,
        setTimeoutFn,
        clearTimeoutFn,
      });

      s1.close();
      s2.close();
      s3.close();
      await sleep(1);
      // No timer while the backup is pending.
      expect(scheduled).toHaveLength(0);

      b.close();
      await broadcast.done;
      expect(b.getStats().missedLastTimeout).toBe(false);
    });

    test('a catching-up (backlogged) backup does NOT gate consensus', async () => {
      const [s1] = serving();
      const [s2] = serving();
      const [s3] = serving();
      // A backup that is still in its initial catchup: not caught up, and with a
      // tiny backlog high-water so its live send blocks (stays pending) instead
      // of resolving. isBacklogged() is true, so it must not be required for
      // consensus.
      const [b] = createSubscriber(
        '00',
        false,
        {backlogHighWaterBytes: 1},
        'backup',
      );
      const {scheduled, setTimeoutFn, clearTimeoutFn} = captureTimers();

      // 4 subscribers => majority of 3.
      const broadcast = new Broadcast(lc, [s1, s2, s3, b], begin, {
        consensusTimeoutProportion: 2,
        setTimeoutFn,
        clearTimeoutFn,
      });

      // The 3 serving replicas reaching the majority arms the timer immediately,
      // even though the backup (still catching up) has not responded.
      s1.close();
      s2.close();
      s3.close();
      await sleep(1);
      expect(scheduled).toHaveLength(1);
      expect(broadcast.isDone).toBe(false);

      // Firing the timer releases via consensus-timeout. The catching-up backup
      // is left pending and recorded timed-out (the forwarder fail-safe, not the
      // Broadcast, is what keeps it from being disconnected).
      scheduled[0].cb();
      await broadcast.done;
      expect(broadcast.releaseMode).toBe('consensus-timeout');
      expect(b.getStats().missedLastTimeout).toBe(true);
    });

    test('no backup present: a majority alone early-releases (unchanged behavior)', async () => {
      const [s1] = serving();
      const [s2] = serving();
      const [s3] = serving();
      const [s4] = serving();
      const {scheduled, setTimeoutFn, clearTimeoutFn} = captureTimers();

      // 4 serving subscribers, no backup => majority of 3 releases on its own.
      const broadcast = new Broadcast(lc, [s1, s2, s3, s4], begin, {
        consensusTimeoutProportion: 2,
        setTimeoutFn,
        clearTimeoutFn,
      });

      s1.close();
      s2.close();
      s3.close();
      await sleep(1);
      expect(scheduled).toHaveLength(1);
      expect(broadcast.isDone).toBe(false);

      scheduled[0].cb();
      await broadcast.done;
      expect(broadcast.releaseMode).toBe('consensus-timeout');
      expect(s4.getStats().missedLastTimeout).toBe(true);
    });
  });
});
