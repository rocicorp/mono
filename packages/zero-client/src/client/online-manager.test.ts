import {describe, test, expect, vi, beforeEach, afterEach} from 'vitest';
import {OnlineManager} from './online-manager.ts';
import type {OnlineStatus} from './online-manager.ts';
import {TestLogSink} from '../../../shared/src/logging-test-utils.ts';
import {LogContext} from '@rocicorp/logger';

function createTestContext(offlineDelayMs = 100) {
  const logSink = new TestLogSink();
  const lc = new LogContext('debug', {}, logSink);
  const manager = new OnlineManager(offlineDelayMs, lc);
  const events: OnlineStatus[] = [];
  const unsubscribe = manager.subscribe(status => {
    events.push(status);
  });

  return {manager, logSink, events, unsubscribe, offlineDelayMs};
}

describe('OnlineManager', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  test('starts online', () => {
    const {manager, events, logSink} = createTestContext();
    expect(manager.status).toBe('online');
    expect(events).toEqual([]);
    expect(logSink.messages).toEqual([]);
  });

  test('setOnline(true) is a no-op when already online', () => {
    const {manager, events, logSink} = createTestContext();
    manager.setOnline(true);
    expect(manager.status).toBe('online');
    expect(events).toEqual([]);
    expect(logSink.messages).toEqual([]);
  });

  test('schedules offline mode after the grace period', () => {
    const {manager, events, logSink, offlineDelayMs} = createTestContext(150);

    manager.setOnline(false);
    expect(manager.status).toBe('offline-pending');
    expect(events).toEqual(['offline-pending']);
    expect(logSink.messages).toEqual([
      ['debug', {}, ['Scheduling offline mode in', offlineDelayMs, 'ms']],
    ]);

    vi.advanceTimersByTime(offlineDelayMs - 1);
    expect(manager.status).toBe('offline-pending');
    expect(events).toEqual(['offline-pending']);
    expect(logSink.messages).toEqual([
      ['debug', {}, ['Scheduling offline mode in', offlineDelayMs, 'ms']],
    ]);

    vi.advanceTimersByTime(1);
    expect(manager.status).toBe('offline');
    expect(events).toEqual(['offline-pending', 'offline']);
    expect(logSink.messages).toEqual([
      ['debug', {}, ['Scheduling offline mode in', offlineDelayMs, 'ms']],
      ['info', {}, ['Offline mode enabled']],
    ]);
  });

  test('clears the pending offline timer when going back online', () => {
    const {manager, events, logSink, offlineDelayMs} = createTestContext(200);

    manager.setOnline(false);
    expect(manager.status).toBe('offline-pending');
    expect(events).toEqual(['offline-pending']);

    vi.advanceTimersByTime(offlineDelayMs - 1);

    manager.setOnline(true);
    expect(manager.status).toBe('online');
    expect(events).toEqual(['offline-pending', 'online']);

    vi.advanceTimersByTime(1);
    expect(manager.status).toBe('online');
    expect(events).toEqual(['offline-pending', 'online']);
    expect(logSink.messages).toEqual([
      ['debug', {}, ['Scheduling offline mode in', offlineDelayMs, 'ms']],
    ]);
  });

  test('does not schedule additional timers while offline is pending', () => {
    const {manager, events, logSink, offlineDelayMs} = createTestContext(123);

    manager.setOnline(false);
    expect(manager.status).toBe('offline-pending');
    expect(events).toEqual(['offline-pending']);

    manager.setOnline(false);
    expect(manager.status).toBe('offline-pending');
    expect(events).toEqual(['offline-pending']);
    expect(logSink.messages).toEqual([
      ['debug', {}, ['Scheduling offline mode in', offlineDelayMs, 'ms']],
    ]);
  });

  test('is idempotent when already offline', () => {
    const {manager, events, offlineDelayMs, logSink} = createTestContext(100);

    manager.setOnline(false);
    vi.advanceTimersByTime(offlineDelayMs);
    expect(manager.status).toBe('offline');
    expect(events).toEqual(['offline-pending', 'offline']);

    manager.setOnline(false);
    expect(manager.status).toBe('offline');
    expect(events).toEqual(['offline-pending', 'offline']);
    expect(logSink.messages).toEqual([
      ['debug', {}, ['Scheduling offline mode in', offlineDelayMs, 'ms']],
      ['info', {}, ['Offline mode enabled']],
    ]);

    manager.setOnline(true);
    expect(manager.status).toBe('online');
    expect(events).toEqual(['offline-pending', 'offline', 'online']);
  });
});
