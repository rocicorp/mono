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
});
