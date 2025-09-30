import {describe, it, expect, vi, beforeEach, afterEach} from 'vitest';

import {OnlineManager} from './online-manager.ts';
import type {OnlineStatus} from './online-manager.ts';
import type {ZeroLogContext} from './zero-log-context.ts';

type MockLogger = {
  debug: ReturnType<typeof vi.fn>;
  info: ReturnType<typeof vi.fn>;
};

function createTestContext(offlineDelayMs = 100) {
  const lc: MockLogger = {
    debug: vi.fn(),
    info: vi.fn(),
  };

  const manager = new OnlineManager(
    offlineDelayMs,
    lc as unknown as ZeroLogContext,
  );
  const events: OnlineStatus[] = [];
  const unsubscribe = manager.subscribe(status => {
    events.push(status);
  });

  return {manager, lc, events, unsubscribe, offlineDelayMs};
}

describe('OnlineManager', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('starts offline', () => {
    const {manager} = createTestContext();
    expect(manager.status).toBe('offline-pending');
  });

  it('goes online immediately and notifies listeners', () => {
    const {manager, events} = createTestContext();
    manager.setOnline(true);
    expect(manager.status).toBe('online');
    expect(events).toEqual(['online']);
  });

  it('auto-transitions from offline-pending to offline after grace period', () => {
    const {manager, events, lc, offlineDelayMs} = createTestContext(150);
    // Initially in offline-pending
    expect(manager.status).toBe('offline-pending');
    expect(events).toEqual([]);

    // Advance timers to trigger scheduled offline
    vi.advanceTimersByTime(offlineDelayMs - 1);
    // Not yet offline until full delay
    expect(manager.status).toBe('offline-pending');
    expect(lc.info).not.toHaveBeenCalled();

    vi.advanceTimersByTime(1);
    expect(manager.status).toBe('offline');
    expect(events).toEqual(['offline']);
    expect(lc.info).toHaveBeenCalledTimes(1);
  });

  it('schedules offline with a grace period, then flips offline', () => {
    const {manager, events, lc, offlineDelayMs} = createTestContext(123);
    manager.setOnline(true);
    expect(manager.status).toBe('online');

    manager.setOnline(false);
    expect(manager.status).toBe('offline-pending');
    expect(events).toEqual(['online', 'offline-pending']);
    expect(lc.debug).toHaveBeenCalledTimes(1);

    vi.advanceTimersByTime(offlineDelayMs);
    expect(manager.status).toBe('offline');
    expect(events).toEqual(['online', 'offline-pending', 'offline']);
    expect(lc.info).toHaveBeenCalledTimes(1);
  });

  it('cancels pending offline if going back online before delay', () => {
    const {manager, events, lc, offlineDelayMs} = createTestContext(200);
    manager.setOnline(true);
    manager.setOnline(false); // offline-pending
    expect(manager.status).toBe('offline-pending');
    expect(events).toEqual(['online', 'offline-pending']);

    manager.setOnline(true); // should clear timer and set online
    expect(manager.status).toBe('online');
    expect(events).toEqual(['online', 'offline-pending', 'online']);

    vi.advanceTimersByTime(offlineDelayMs);
    // Should not transition to offline since timer was cleared
    expect(manager.status).toBe('online');
    expect(events).toEqual(['online', 'offline-pending', 'online']);
    expect(lc.info).not.toHaveBeenCalled();
  });

  it('does not schedule multiple timers while already pending', () => {
    const {manager, events, lc} = createTestContext(100);
    manager.setOnline(true);
    manager.setOnline(false); // first pending
    expect(events).toEqual(['online', 'offline-pending']);
    const debugCalls = lc.debug.mock.calls.length;

    manager.setOnline(false); // should be a no-op
    expect(lc.debug).toHaveBeenCalledTimes(debugCalls);
    expect(events).toEqual(['online', 'offline-pending']);
  });

  it('is idempotent when already in the target state', () => {
    const {manager, events} = createTestContext(100);

    // Already offline-pending initially
    manager.setOnline(false);
    expect(events).toEqual([]);
    expect(manager.status).toBe('offline-pending');

    // Go online once
    manager.setOnline(true);
    expect(events).toEqual(['online']);
    expect(manager.status).toBe('online');

    // Going online again should not notify
    manager.setOnline(true);
    expect(events).toEqual(['online']);
    expect(manager.status).toBe('online');
  });
});
