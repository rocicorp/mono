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

  test('checkProgress', async () => {
    const [sub1] = createSubscriber('00', true);
    const [sub2] = createSubscriber('00', true);
    const [sub3] = createSubscriber('00', true);
    const [sub4] = createSubscriber('00', true);

    const broadcast = new Broadcast(
      [sub1, sub2, sub3, sub4],
      [
        '11',
        'begin',
        json(['begin', messages.begin(), {commitWatermark: '13'}]),
      ],
    );

    expect(broadcast.isDone).toBe(false);

    sub1.close();
    sub2.close();

    await sleep(1);
    const twoDoneTime = performance.now();

    // 2 is less than majority, so checkProgress should not yet advance.
    expect(broadcast.checkProgress(lc, 2000, twoDoneTime + 2100)).toBe(false);

    sub3.close();
    await sleep(1);
    const threeDoneTime = performance.now();

    // 3 reaches majority, but not enough time has elapsed.
    expect(broadcast.checkProgress(lc, 2000, threeDoneTime + 1100)).toBe(false);

    expect(broadcast.isDone).toBe(false);

    // Once enough time has elapsed, the flow should advance.
    expect(broadcast.checkProgress(lc, 2000, threeDoneTime + 2100)).toBe(true);

    await broadcast.done;
    expect(broadcast.isDone).toBe(true);
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

  test('event-driven early release once a majority acks', async () => {
    const [sub1] = createSubscriber('00', true);
    const [sub2] = createSubscriber('00', true);
    const [sub3] = createSubscriber('00', true);
    const [sub4] = createSubscriber('00', true);
    const {scheduled, setTimeoutFn, clearTimeoutFn} = captureTimers();

    const broadcast = new Broadcast([sub1, sub2, sub3, sub4], begin, {
      consensusTimeoutMs: 2000,
      setTimeoutFn,
      clearTimeoutFn,
    });

    // Two acks: below the majority of 3, so no early-release timer is armed.
    sub1.close();
    sub2.close();
    await sleep(1);
    expect(scheduled).toHaveLength(0);
    expect(broadcast.isDone).toBe(false);

    // Third ack reaches the majority: the release timer is armed.
    sub3.close();
    await sleep(1);
    expect(scheduled).toHaveLength(1);
    expect(scheduled[0].ms).toBe(2000);
    expect(broadcast.isDone).toBe(false);

    // Firing the timer releases via consensus-timeout, without a 1s tick.
    scheduled[0].cb();
    await broadcast.done;
    expect(broadcast.isDone).toBe(true);
    expect(broadcast.releaseMode).toBe('consensus-timeout');
  });

  test('early-release timer re-arms on later completions; stale timers are ignored', async () => {
    const [sub1] = createSubscriber('00', true);
    const [sub2] = createSubscriber('00', true);
    const [sub3] = createSubscriber('00', true);
    const [sub4] = createSubscriber('00', true);
    const [sub5] = createSubscriber('00', true);
    const {scheduled, cleared, setTimeoutFn, clearTimeoutFn} = captureTimers();

    const broadcast = new Broadcast([sub1, sub2, sub3, sub4, sub5], begin, {
      consensusTimeoutMs: 2000,
      setTimeoutFn,
      clearTimeoutFn,
    });

    // Majority of 5 is 3: arms the timer.
    sub1.close();
    sub2.close();
    sub3.close();
    await sleep(1);
    expect(scheduled).toHaveLength(1);

    // A later completion re-arms (new generation) instead of releasing now, and
    // cancels the previously-armed timer so stale timers don't accumulate.
    sub4.close();
    await sleep(1);
    expect(scheduled).toHaveLength(2);
    expect(cleared).toContain(scheduled[0].handle);
    expect(broadcast.isDone).toBe(false);

    // The stale (first-generation) timer must not release the broadcast.
    scheduled[0].cb();
    expect(broadcast.isDone).toBe(false);

    // The latest timer releases it.
    scheduled[1].cb();
    await broadcast.done;
    expect(broadcast.releaseMode).toBe('consensus-timeout');
  });

  test('all-subscribers release takes precedence over a pending early-release timer', async () => {
    const [sub1] = createSubscriber('00', true);
    const [sub2] = createSubscriber('00', true);
    const [sub3] = createSubscriber('00', true);
    const {scheduled, cleared, setTimeoutFn, clearTimeoutFn} = captureTimers();

    const broadcast = new Broadcast([sub1, sub2, sub3], begin, {
      consensusTimeoutMs: 2000,
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
});
