import {afterEach, describe, expect, test, vi} from 'vitest';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import type {Subscription} from '../../types/subscription.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {Broadcast} from './broadcast.ts';
import type {Subscriber} from './subscriber.ts';
import {createSubscriber} from './test-utils.ts';

const json = BigIntJSON.stringify;

describe('change-streamer/broadcast', () => {
  const messages = new ReplicationMessages({issues: 'id'});
  const lc = createSilentLogContext();
  const change: [string, 'begin', string] = [
    '11',
    'begin',
    json(['begin', messages.begin(), {commitWatermark: '13'}]),
  ];

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  async function consumeOne(downstream: Subscription<string>) {
    const pipeline = downstream.pipeline;
    if (pipeline === undefined) {
      throw new Error('Expected pipelined subscription');
    }
    const next = await pipeline[Symbol.asyncIterator]().next();
    if (next.done) {
      throw new Error('Expected subscription message');
    }
    next.value.consumed();
  }

  async function flushCompletions() {
    await Promise.resolve();
    await Promise.resolve();
  }

  async function makeSubscribers(count: number) {
    const subscribers: Subscriber[] = [];
    const downstreams: Subscription<string>[] = [];
    for (let i = 0; i < count; i++) {
      const [subscriber, , downstream] = createSubscriber('00', true);
      subscribers.push(subscriber);
      downstreams.push(downstream);
    }
    await Promise.all(downstreams.map(consumeOne));
    await flushCompletions();
    return {subscribers, downstreams};
  }

  async function consumeChange(downstream: Subscription<string>) {
    await consumeOne(downstream);
    await flushCompletions();
  }

  async function closeAll(subscribers: readonly Subscriber[]) {
    for (const sub of subscribers) {
      sub.close();
    }
    await flushCompletions();
  }

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

  test('flow control releases after majority plus padding', async () => {
    vi.useFakeTimers();
    const {subscribers, downstreams} = await makeSubscribers(4);
    const broadcast = new Broadcast(subscribers, change, {
      lc,
      flowControlConsensusPaddingMs: 50,
    });

    await consumeChange(downstreams[0]);
    await consumeChange(downstreams[1]);
    await vi.advanceTimersByTimeAsync(100);
    expect(broadcast.isDone).toBe(false);

    await consumeChange(downstreams[2]);
    await vi.advanceTimersByTimeAsync(49);
    expect(broadcast.isDone).toBe(false);

    await vi.advanceTimersByTimeAsync(1);
    await broadcast.done;
    expect(broadcast.isDone).toBe(true);

    await closeAll(subscribers);
  });

  test('flow control timer resets after post-majority progress', async () => {
    vi.useFakeTimers();
    const {subscribers, downstreams} = await makeSubscribers(5);
    const broadcast = new Broadcast(subscribers, change, {
      lc,
      flowControlConsensusPaddingMs: 50,
    });

    await consumeChange(downstreams[0]);
    await consumeChange(downstreams[1]);
    await consumeChange(downstreams[2]);
    await vi.advanceTimersByTimeAsync(30);
    await consumeChange(downstreams[3]);

    await vi.advanceTimersByTimeAsync(49);
    expect(broadcast.isDone).toBe(false);

    await vi.advanceTimersByTimeAsync(1);
    await broadcast.done;
    expect(broadcast.isDone).toBe(true);

    await closeAll(subscribers);
  });

  test('flow control timer is cleared when all subscribers complete', async () => {
    vi.useFakeTimers();
    const clearTimeoutSpy = vi.spyOn(globalThis, 'clearTimeout');
    const {subscribers, downstreams} = await makeSubscribers(4);
    const broadcast = new Broadcast(subscribers, change, {
      lc,
      flowControlConsensusPaddingMs: 50,
    });

    await consumeChange(downstreams[0]);
    await consumeChange(downstreams[1]);
    await consumeChange(downstreams[2]);
    expect(broadcast.isDone).toBe(false);

    await consumeChange(downstreams[3]);
    await broadcast.done;
    expect(broadcast.isDone).toBe(true);
    expect(clearTimeoutSpy).toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(50);
    expect(broadcast.isDone).toBe(true);
    await closeAll(subscribers);
  });

  test('negative flow control padding disables early release', async () => {
    vi.useFakeTimers();
    const {subscribers, downstreams} = await makeSubscribers(4);
    const broadcast = new Broadcast(subscribers, change, {
      lc,
      flowControlConsensusPaddingMs: -1,
    });

    await consumeChange(downstreams[0]);
    await consumeChange(downstreams[1]);
    await consumeChange(downstreams[2]);
    expect(broadcast.checkProgress(lc, -1, performance.now() + 1000)).toBe(
      false,
    );
    await vi.advanceTimersByTimeAsync(1000);
    expect(broadcast.isDone).toBe(false);

    await consumeChange(downstreams[3]);
    await broadcast.done;
    expect(broadcast.isDone).toBe(true);

    await closeAll(subscribers);
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
});
