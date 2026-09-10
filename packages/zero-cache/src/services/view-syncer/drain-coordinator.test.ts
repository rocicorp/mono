import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {DrainCoordinator} from './drain-coordinator.ts';

describe('view-syncer/drain-coordinator', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  test('onDraining fires once draining starts', () => {
    const coordinator = new DrainCoordinator();
    const listener = vi.fn();

    coordinator.onDraining(listener);
    expect(listener).not.toHaveBeenCalled();
    expect(coordinator.drainListenerCount).toBe(1);

    coordinator.drainNextIn(0);
    expect(listener).toHaveBeenCalledTimes(1);
    // Fired listeners are released.
    expect(coordinator.drainListenerCount).toBe(0);

    // A later drain does not fire it again.
    vi.advanceTimersByTime(1);
    coordinator.drainNextIn(0);
    expect(listener).toHaveBeenCalledTimes(1);
  });

  test('onDraining fires immediately when already draining', () => {
    const coordinator = new DrainCoordinator();
    coordinator.drainNextIn(0);

    const listener = vi.fn();
    coordinator.onDraining(listener);
    expect(listener).toHaveBeenCalledTimes(1);
    expect(coordinator.drainListenerCount).toBe(0);
  });

  test('an unsubscribed listener is released and never called', () => {
    const coordinator = new DrainCoordinator();
    const listener = vi.fn();

    const unsubscribe = coordinator.onDraining(listener);
    expect(coordinator.drainListenerCount).toBe(1);

    unsubscribe();
    expect(coordinator.drainListenerCount).toBe(0);

    coordinator.drainNextIn(0);
    expect(listener).not.toHaveBeenCalled();
  });
});
