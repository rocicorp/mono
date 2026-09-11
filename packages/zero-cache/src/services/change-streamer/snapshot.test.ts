import {resolver} from '@rocicorp/resolver';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import {
  reserveAndGetSnapshotStatus,
  RESTORE_RETRY_INTERVAL_MS,
  restoreUnderReservation,
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

  // Invokes the SIGTERM listener added since `listenersBefore` directly
  // (emitting a real SIGTERM would also reach the test runner's handlers).
  function sendSigterm(listenersBefore: Set<Function>) {
    const added = process
      .listeners('SIGTERM')
      .find(l => !listenersBefore.has(l));
    expect(added).toBeDefined();
    (added as () => void)();
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

    sendSigterm(sigtermListeners);

    await expect(result).rejects.toBeInstanceOf(AbortError);
    expectListenersAdded(0);
  });

  test('signal while the reservation is pending rejects and removes the listeners', async () => {
    const reservation = resolver<Source<SnapshotMessage>>();
    const reserve: ReserveSnapshot = vi
      .fn<ReserveSnapshot>()
      .mockReturnValue(reservation.promise);

    const sigtermListeners = new Set(process.listeners('SIGTERM'));
    const result = reserveAndGetSnapshotStatus(lc, config, reserve);
    result.catch(() => {});
    await vi.advanceTimersByTimeAsync(0);
    expectListenersAdded(1);

    sendSigterm(sigtermListeners);

    await expect(result).rejects.toBeInstanceOf(AbortError);
    expectListenersAdded(0);

    // A reservation that completes after the abort is not held open.
    const stream = Subscription.create<SnapshotMessage>();
    reservation.resolve(stream);
    await vi.advanceTimersByTimeAsync(0);
    expect((await stream[Symbol.asyncIterator]().next()).done).toBe(true);
  });

  test('signal while the stream is open cancels it and removes the listeners', async () => {
    const stream = Subscription.create<SnapshotMessage>();
    const reserve: ReserveSnapshot = vi
      .fn<ReserveSnapshot>()
      .mockResolvedValue(stream);

    const sigtermListeners = new Set(process.listeners('SIGTERM'));
    const result = reserveAndGetSnapshotStatus(lc, config, reserve);
    stream.push(['status', status]);
    expect(await result).toEqual(status);
    expectListenersAdded(1);

    sendSigterm(sigtermListeners);
    await vi.advanceTimersByTimeAsync(0);

    expectListenersAdded(0);
    await expect(stream[Symbol.asyncIterator]().next()).rejects.toBeInstanceOf(
      AbortError,
    );
  });

  test('restoreUnderReservation reserves again after a restore that produced nothing', async () => {
    const reserve = vi.fn(() => Promise.resolve(status));
    const restore = vi
      .fn<(s: SnapshotStatus) => Promise<{restored: boolean; result: string}>>()
      .mockResolvedValueOnce({restored: false, result: 'invalid_replica'})
      .mockResolvedValueOnce({restored: true, result: 'success'});

    const restored = restoreUnderReservation(lc, reserve, restore);
    await vi.advanceTimersByTimeAsync(0);
    expect(reserve).toHaveBeenCalledTimes(1);
    expect(restore).toHaveBeenCalledTimes(1);

    // Not before the retry interval.
    await vi.advanceTimersByTimeAsync(RESTORE_RETRY_INTERVAL_MS - 1);
    expect(reserve).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(1);
    expect(await restored).toBe('success');
    expect(reserve).toHaveBeenCalledTimes(2);
    expect(restore).toHaveBeenLastCalledWith(status);
  });
});
