import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../../../shared/src/logging-test-utils.ts';
import {ProgressMonitor} from './storer.ts';

describe('storer/ProgressMonitor', () => {
  function captureFailures() {
    const errors: Error[] = [];
    return {errors, onFailure: (err: Error) => void errors.push(err)};
  }

  // Most of these drive checkTaskProgress() with explicit Date objects
  // (rather than start()'s real setInterval), so they're deterministic and
  // don't depend on wall-clock timing. start()/stop() wiring is covered
  // separately below with fake timers.

  test('a task refreshed within the threshold never fires', () => {
    const {errors, onFailure} = captureFailures();
    const monitor = new ProgressMonitor(
      createSilentLogContext(),
      100,
      onFailure,
    );
    const t0 = new Date('2026-01-01T00:00:00.000Z');

    // Mirrors real usage: call the previous done-fn, then track anew.
    let done = monitor.trackTask({task: 'db-write'}, t0);
    for (let i = 1; i <= 10; i++) {
      const now = new Date(t0.getTime() + i * 50);
      done();
      done = monitor.trackTask({task: 'db-write'}, now);
      monitor.checkTaskProgress(new Date(now.getTime() + 50));
    }

    expect(errors).toHaveLength(0);
  });

  test('re-tracking the same object refreshes its timestamp in place', () => {
    const {errors, onFailure} = captureFailures();
    const monitor = new ProgressMonitor(
      createSilentLogContext(),
      100,
      onFailure,
    );
    const t0 = new Date('2026-01-01T00:00:00.000Z');
    const task = {task: 'queue-entry', type: 'change'};

    monitor.trackTask(task, t0);
    monitor.trackTask(task, new Date(t0.getTime() + 90));

    // 100ms after the *second* track, still under threshold from it.
    monitor.checkTaskProgress(new Date(t0.getTime() + 189));
    expect(errors).toHaveLength(0);

    monitor.checkTaskProgress(new Date(t0.getTime() + 190));
    expect(errors).toHaveLength(1);
  });

  test('fires once a task goes stale for at least the failure threshold', () => {
    const {errors, onFailure} = captureFailures();
    const monitor = new ProgressMonitor(
      createSilentLogContext(),
      100,
      onFailure,
    );
    const t0 = new Date('2026-01-01T00:00:00.000Z');

    monitor.trackTask({task: 'catchup', subscriber: 'sub1'}, t0);

    // Just under the threshold: no failure yet.
    monitor.checkTaskProgress(new Date(t0.getTime() + 99));
    expect(errors).toHaveLength(0);

    // At (>=) the threshold: fires.
    monitor.checkTaskProgress(new Date(t0.getTime() + 100));
    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(Error);
    expect(errors[0].message).toContain('catchup');
    expect(errors[0].message).toContain('sub1');
    expect(errors[0].message).toContain('100ms');
  });

  test('logs the stale task and last-update time at error level', () => {
    const logSink = new TestLogSink();
    const lc = new LogContext('error', undefined, logSink);
    const {onFailure} = captureFailures();
    const monitor = new ProgressMonitor(lc, 50, onFailure);
    const t0 = new Date('2026-01-01T00:00:00.000Z');
    const task = {task: 'queue-entry', type: 'commit'};

    monitor.trackTask(task, t0);
    monitor.checkTaskProgress(new Date(t0.getTime() + 50));

    expect(logSink.messages).toHaveLength(1);
    const [level, , args] = logSink.messages[0];
    expect(level).toBe('error');
    expect(args[0]).toContain('more than 50ms ago');
    expect(args[1]).toMatchObject({task, lastUpdate: t0});
  });

  test('the done callback removes the task so it never fires', () => {
    const {errors, onFailure} = captureFailures();
    const monitor = new ProgressMonitor(
      createSilentLogContext(),
      50,
      onFailure,
    );
    const t0 = new Date('2026-01-01T00:00:00.000Z');

    const done = monitor.trackTask(
      {task: 'start-catchup', subscribers: ['sub1', 'sub2']},
      t0,
    );
    done();

    monitor.checkTaskProgress(new Date(t0.getTime() + 1000));

    expect(errors).toHaveLength(0);
  });

  test('calling the done callback twice is a harmless no-op', () => {
    const {errors, onFailure} = captureFailures();
    const monitor = new ProgressMonitor(
      createSilentLogContext(),
      50,
      onFailure,
    );
    const t0 = new Date('2026-01-01T00:00:00.000Z');

    const done = monitor.trackTask({task: 'db-write'}, t0);
    done();
    expect(() => done()).not.toThrow();

    monitor.checkTaskProgress(new Date(t0.getTime() + 1000));
    expect(errors).toHaveLength(0);
  });

  test('tracks concurrent tasks independently', () => {
    const {errors, onFailure} = captureFailures();
    const monitor = new ProgressMonitor(
      createSilentLogContext(),
      100,
      onFailure,
    );
    const t0 = new Date('2026-01-01T00:00:00.000Z');

    monitor.trackTask({task: 'catchup', subscriber: 'sub1'}, t0);
    let healthyDone = monitor.trackTask(
      {task: 'queue-entry', type: 'change'},
      t0,
    );

    // Keep refreshing only the healthy task, well past when `stuck` goes stale.
    for (let i = 1; i <= 5; i++) {
      const now = new Date(t0.getTime() + i * 40);
      healthyDone();
      healthyDone = monitor.trackTask(
        {task: 'queue-entry', type: 'change'},
        now,
      );
    }

    monitor.checkTaskProgress(new Date(t0.getTime() + 5 * 40));

    expect(errors).toHaveLength(1);
    expect(errors[0].message).toContain('catchup');
    expect(errors[0].message).not.toContain('"type":"change"');
  });

  test('reports only the first stale task found and stops tracking the rest', () => {
    // Current design: one failure is enough to tear the process down, so
    // checkTaskProgress() reports the first stale task (in insertion order)
    // and then stops the monitor (clearing the interval and the map) rather
    // than reporting every stale task.
    const {errors, onFailure} = captureFailures();
    const monitor = new ProgressMonitor(
      createSilentLogContext(),
      50,
      onFailure,
    );
    const t0 = new Date('2026-01-01T00:00:00.000Z');

    monitor.trackTask({task: 'catchup', subscriber: 'a'}, t0);
    monitor.trackTask({task: 'catchup', subscriber: 'b'}, t0);

    monitor.checkTaskProgress(new Date(t0.getTime() + 60));

    expect(errors).toHaveLength(1);
    expect(errors[0].message).toContain('"subscriber":"a"');
  });

  test('does not fire again on a later check after reporting once', () => {
    const {errors, onFailure} = captureFailures();
    const monitor = new ProgressMonitor(
      createSilentLogContext(),
      50,
      onFailure,
    );
    const t0 = new Date('2026-01-01T00:00:00.000Z');

    monitor.trackTask({task: 'db-write'}, t0);

    monitor.checkTaskProgress(new Date(t0.getTime() + 60));
    expect(errors).toHaveLength(1);

    // The map was cleared by the self-stop, so a later check is a no-op even
    // though nothing was ever explicitly marked done.
    monitor.checkTaskProgress(new Date(t0.getTime() + 10_000));
    expect(errors).toHaveLength(1);
  });

  test('stop() clears tracked tasks so a stale one is dropped', () => {
    const {errors, onFailure} = captureFailures();
    const monitor = new ProgressMonitor(
      createSilentLogContext(),
      50,
      onFailure,
    );
    const t0 = new Date('2026-01-01T00:00:00.000Z');

    monitor.trackTask({task: 'db-write'}, t0);
    monitor.stop();

    monitor.checkTaskProgress(new Date(t0.getTime() + 1000));

    expect(errors).toHaveLength(0);
  });

  test('stop() is idempotent', () => {
    const monitor = new ProgressMonitor(createSilentLogContext(), 50, () => {});
    monitor.start();
    expect(() => {
      monitor.stop();
      monitor.stop();
    }).not.toThrow();
  });

  describe('start() / stop() wiring', () => {
    beforeEach(() => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'));
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    test('start() polls on the failure-threshold interval and fires onFailure', async () => {
      const {errors, onFailure} = captureFailures();
      const monitor = new ProgressMonitor(
        createSilentLogContext(),
        100,
        onFailure,
      );
      monitor.start();

      monitor.trackTask({task: 'db-write'});

      await vi.advanceTimersByTimeAsync(99);
      expect(errors).toHaveLength(0);

      await vi.advanceTimersByTimeAsync(1);
      expect(errors).toHaveLength(1);
    });

    test('a failure self-stops the interval so it does not fire again', async () => {
      const {errors, onFailure} = captureFailures();
      const monitor = new ProgressMonitor(
        createSilentLogContext(),
        100,
        onFailure,
      );
      monitor.start();
      monitor.trackTask({task: 'db-write'});

      await vi.advanceTimersByTimeAsync(100);
      expect(errors).toHaveLength(1);

      // If the interval weren't cleared on self-stop, it would keep firing
      // (there's nothing left in the map, but this also proves no timer
      // is still scheduled to run at all).
      await vi.advanceTimersByTimeAsync(1000);
      expect(errors).toHaveLength(1);
    });

    test('stop() halts the interval so no further checks run', async () => {
      const {errors, onFailure} = captureFailures();
      const monitor = new ProgressMonitor(
        createSilentLogContext(),
        100,
        onFailure,
      );
      monitor.start();
      monitor.trackTask({task: 'db-write'});

      monitor.stop();
      await vi.advanceTimersByTimeAsync(10_000);

      expect(errors).toHaveLength(0);
    });

    test('calling start() again resets the interval without duplicating it', async () => {
      const {errors, onFailure} = captureFailures();
      const monitor = new ProgressMonitor(
        createSilentLogContext(),
        100,
        onFailure,
      );
      monitor.start();
      monitor.start(); // simulate a restart; must clear the first interval

      monitor.trackTask({task: 'db-write'});
      await vi.advanceTimersByTimeAsync(100);

      // Exactly one failure, not two (which a leaked first interval running
      // alongside the second would produce).
      expect(errors).toHaveLength(1);
    });

    test('a task tracked after start() does not fire until it too goes stale', async () => {
      // Note: checkTaskProgress() polls on a fixed cadence set by start()
      // (t=100, 200, 300, ...), not one re-synced to when a task is
      // tracked. A task tracked at t=80 is first checked at t=100 (only
      // 20ms old: not stale) and isn't caught as stale until the t=200
      // check (120ms old by then) -- so detection latency for a
      // newly-tracked task can be nearly 2x the threshold in the worst case.
      const {errors, onFailure} = captureFailures();
      const monitor = new ProgressMonitor(
        createSilentLogContext(),
        100,
        onFailure,
      );
      monitor.start();

      await vi.advanceTimersByTimeAsync(80);
      monitor.trackTask({task: 'catchup', subscriber: 'sub1'});

      await vi.advanceTimersByTimeAsync(20); // t=100 check: 20ms old
      expect(errors).toHaveLength(0);

      await vi.advanceTimersByTimeAsync(100); // t=200 check: 120ms old
      expect(errors).toHaveLength(1);
    });
  });
});
