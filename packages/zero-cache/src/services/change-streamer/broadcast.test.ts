import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {Broadcast} from './broadcast.ts';
import {createSubscriber} from './test-utils.ts';

describe('change-streamer/broadcast', () => {
  const messages = new ReplicationMessages({issues: 'id'});
  const lc = new LogContext('debug', {}, consoleLogSink);
  createSilentLogContext();

  test('without tracking', () => {
    const [sub1, stream1] = createSubscriber('00', true);
    const [sub2, stream2] = createSubscriber('00', true);
    const [sub3, stream3] = createSubscriber('00', true);
    const [sub4, stream4] = createSubscriber('00', true);

    Broadcast.withoutTracking(
      [sub1, sub2, sub3, sub4],
      ['11', ['begin', messages.begin(), {commitWatermark: '13'}]],
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
      ['11', ['begin', messages.begin(), {commitWatermark: '13'}]],
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
      ['11', ['begin', messages.begin(), {commitWatermark: '13'}]],
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
