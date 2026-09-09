import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import {Subscription} from '../../types/subscription.ts';
import {
  reserveAndGetSnapshotStatus,
  type ReserveSnapshot,
  type SnapshotMessage,
  type SnapshotStatus,
} from './snapshot.ts';

describe('change-streamer/snapshot', () => {
  const lc = createSilentLogContext();
  const config = {} as NormalizedZeroConfig;
  const status: SnapshotStatus = {
    tag: 'status',
    backupURL: 's3://bucket/backup',
    replicaVersion: '123',
    minWatermark: '0a',
  };

  let sigintBefore: number;
  let sigtermBefore: number;

  beforeEach(() => {
    vi.useFakeTimers();
    sigintBefore = process.listenerCount('SIGINT');
    sigtermBefore = process.listenerCount('SIGTERM');
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  function expectListenersAdded(n: number) {
    expect(process.listenerCount('SIGINT')).toBe(sigintBefore + n);
    expect(process.listenerCount('SIGTERM')).toBe(sigtermBefore + n);
  }

  test('signal listeners are removed once the reservation completes', async () => {
    const stream = Subscription.create<SnapshotMessage>();
    const reserve: ReserveSnapshot = vi
      .fn<ReserveSnapshot>()
      // The first attempt fails (e.g. incompatible replication-manager).
      .mockRejectedValueOnce(new Error('not yet'))
      .mockResolvedValueOnce(stream);

    const result = reserveAndGetSnapshotStatus(lc, config, reserve);
    // The listeners are registered while the reservation is in progress...
    expectListenersAdded(1);

    // ... including across the retry sleep.
    await vi.advanceTimersByTimeAsync(5000);
    expect(reserve).toHaveBeenCalledTimes(2);
    expectListenersAdded(1);

    stream.push(['status', status]);
    expect(await result).toEqual(status);

    // The change-streamer closes the stream when the subscription starts.
    stream.cancel();
    await vi.advanceTimersByTimeAsync(0);

    // ... and are gone once the reservation is over, so that repeated
    // reservations do not accumulate process listeners.
    expectListenersAdded(0);
  });

  test('signal during retry rejects and removes the listeners', async () => {
    const reserve: ReserveSnapshot = vi
      .fn<ReserveSnapshot>()
      .mockRejectedValue(new Error('not yet'));

    const sigtermListeners = new Set(process.listeners('SIGTERM'));
    const result = reserveAndGetSnapshotStatus(lc, config, reserve);
    // Attach a rejection handler immediately to prevent unhandledRejections.
    result.catch(() => {});
    await vi.advanceTimersByTimeAsync(1000);
    expectListenersAdded(1);

    // Invoke the newly added SIGTERM listener directly (emitting a real
    // SIGTERM would also reach the test runner's own handlers).
    const added = process
      .listeners('SIGTERM')
      .find(l => !sigtermListeners.has(l));
    expect(added).toBeDefined();
    (added as () => void)();

    await expect(result).rejects.toBeInstanceOf(AbortError);
    expectListenersAdded(0);
  });
});
