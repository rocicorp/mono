import {resolver} from '@rocicorp/resolver';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {
  ReservationFollowup,
  SnapshotReserver,
} from './change-streamer-http.ts';
import {reserveAndGetSnapshotStatus, type SnapshotStatus} from './snapshot.ts';

describe('change-streamer/snapshot', () => {
  const lc = createSilentLogContext();
  const status: SnapshotStatus = {
    tag: 'snapshot',
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

  function fakeFollowup(): ReservationFollowup {
    return {
      subscribe: vi.fn(),
      signal: new AbortController().signal,
      cancel: vi.fn(),
    };
  }

  test('signal listeners are removed once the reservation completes', async () => {
    const followup = fakeFollowup();
    const changeStreamer: SnapshotReserver = {
      reserveSnapshot: vi
        .fn()
        // The first attempt fails (e.g. incompatible replication-manager).
        .mockRejectedValueOnce(new Error('not yet'))
        .mockResolvedValueOnce({reserved: status, followup}),
    };
    const result = reserveAndGetSnapshotStatus(lc, 'task-id', changeStreamer);
    // The listeners are registered while the reservation is in progress...
    expectListenersAdded(1);

    // ... including across the retry sleep. Advancing past the retry also
    // resolves the (immediately-settling, per the mock) second attempt, so by
    // the time this settles the reservation has already fully completed.
    await vi.advanceTimersByTimeAsync(5000);
    expect(changeStreamer.reserveSnapshot).toHaveBeenCalledTimes(2);

    expect(await result).toEqual({reserved: status, followup});

    // ... and are gone as soon as the reservation resolves, so that repeated
    // reservations do not accumulate process listeners. The returned
    // `followup`'s connection lifetime is the caller's responsibility from
    // here (subscribe() or cancel() it); this function is no longer involved.
    expectListenersAdded(0);
    expect(followup.cancel).not.toHaveBeenCalled();
  });

  test('signal during retry rejects and removes the listeners', async () => {
    const changeStreamer: SnapshotReserver = {
      reserveSnapshot: vi
        .fn()
        // The first attempt fails (e.g. incompatible replication-manager).
        .mockRejectedValueOnce(new Error('not yet')),
    };

    const sigtermListeners = new Set(process.listeners('SIGTERM'));
    const result = reserveAndGetSnapshotStatus(lc, 'task-id', changeStreamer);
    // Attach a rejection handler immediately to prevent unhandledRejections.
    result.catch(() => {});
    await vi.advanceTimersByTimeAsync(1000);
    expectListenersAdded(1);

    sendSigterm(sigtermListeners);

    await expect(result).rejects.toBeInstanceOf(AbortError);
    expectListenersAdded(0);
  });

  test('signal while the reservation is pending rejects, removes the listeners, and cancels a late reservation', async () => {
    const reservation = resolver<{
      reserved: SnapshotStatus;
      followup: ReservationFollowup;
    }>();
    const changeStreamer: SnapshotReserver = {
      reserveSnapshot: vi.fn().mockReturnValue(reservation.promise),
    };

    const sigtermListeners = new Set(process.listeners('SIGTERM'));
    const result = reserveAndGetSnapshotStatus(lc, 'task-id', changeStreamer);
    result.catch(() => {});
    await vi.advanceTimersByTimeAsync(0);
    expectListenersAdded(1);

    sendSigterm(sigtermListeners);

    await expect(result).rejects.toBeInstanceOf(AbortError);
    expectListenersAdded(0);

    // promiseOrAbort() doesn't cancel the loser of the race: the reservation
    // can still resolve after this function has already given up on it. When
    // it does, its connection must be closed rather than leaked, since
    // nobody will call followup.subscribe().
    const followup = fakeFollowup();
    reservation.resolve({reserved: status, followup});
    await vi.advanceTimersByTimeAsync(0);
    expect(followup.cancel).toHaveBeenCalledTimes(1);
  });
});
