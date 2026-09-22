import {mkdtempSync, renameSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {ViewSyncerStatusEvent} from '../../../../zero-events/src/status.ts';
import {RestoreProgressReporter} from './restore-progress.ts';

describe('litestream/restore-progress', () => {
  let dir: string;
  let replicaFile: string;
  let events: ViewSyncerStatusEvent[];
  let reporter: RestoreProgressReporter;

  beforeEach(() => {
    vi.useFakeTimers();
    dir = mkdtempSync(join(tmpdir(), 'restore-progress-'));
    replicaFile = join(dir, 'replica.db');
    events = [];
    reporter = new RestoreProgressReporter(
      createSilentLogContext(),
      replicaFile,
      (_lc, event) => events.push(event),
    );
  });

  afterEach(() => {
    reporter.stop();
    vi.useRealTimers();
    rmSync(dir, {recursive: true, force: true});
  });

  function restoreStatuses() {
    return events.map(e => [e.stage, e.state?.restoreStatus?.bytes]);
  }

  test('publishes the size of the temporary replica as it grows', () => {
    reporter.start(3000, 1000);
    expect(events[0]).toMatchObject({
      type: 'zero/events/status/view-syncer/v1',
      component: 'view-syncer',
      status: 'OK',
      stage: 'Restoring',
      state: {restoreStatus: {bytes: 0, totalBytes: 3000}},
    });

    writeFileSync(`${replicaFile}.tmp`, Buffer.alloc(1000));
    vi.advanceTimersByTime(1000);
    writeFileSync(`${replicaFile}.tmp`, Buffer.alloc(2000));
    vi.advanceTimersByTime(1000);

    expect(restoreStatuses()).toEqual([
      ['Restoring', 0],
      ['Restoring', 1000],
      ['Restoring', 2000],
    ]);
  });

  test('does not republish unchanged progress', () => {
    reporter.start(3000, 1000);
    vi.advanceTimersByTime(5000);

    // A retried attempt does not repeat the same progress either.
    reporter.start(3000, 1000);
    vi.advanceTimersByTime(5000);

    expect(restoreStatuses()).toEqual([['Restoring', 0]]);
  });

  test('measures elapsed time from the start of the restore', () => {
    // e.g. waiting for the replication-manager's backup.
    vi.advanceTimersByTime(60_000);
    reporter.start(3000, 1000);
    vi.advanceTimersByTime(2000);
    writeFileSync(replicaFile, Buffer.alloc(3000));
    reporter.done();

    expect(events.map(e => e.state?.restoreStatus?.elapsedMs)).toEqual([
      0, 2000,
    ]);
  });

  test('republishes when a retry reports a different total', () => {
    reporter.start(3000, 1000);
    reporter.start(4000, 1000);

    expect(events.map(e => e.state?.restoreStatus?.totalBytes)).toEqual([
      3000, 4000,
    ]);
  });

  test('does not report an existing replica before the restore writes', () => {
    writeFileSync(replicaFile, Buffer.alloc(2500));
    reporter.start(3000, 1000);
    vi.advanceTimersByTime(5000);

    expect(restoreStatuses()).toEqual([['Restoring', 0]]);
  });

  test('ignores a stale temporary replica when starting', () => {
    writeFileSync(`${replicaFile}.tmp`, Buffer.alloc(2000));
    reporter.start(3000, 1000);

    expect(restoreStatuses()).toEqual([['Restoring', 0]]);
  });

  test('keeps the progress once the temporary replica is renamed', () => {
    reporter.start(3000, 1000);
    writeFileSync(`${replicaFile}.tmp`, Buffer.alloc(2000));
    vi.advanceTimersByTime(1000);
    // litestream renames the file before its post-restore integrity check.
    renameSync(`${replicaFile}.tmp`, replicaFile);
    vi.advanceTimersByTime(1000);

    expect(restoreStatuses()).toEqual([
      ['Restoring', 0],
      ['Restoring', 2000],
    ]);
  });

  test('publishes the size of the restored replica when done', () => {
    reporter.start(undefined, 1000);
    writeFileSync(`${replicaFile}.tmp`, Buffer.alloc(2500));
    renameSync(`${replicaFile}.tmp`, replicaFile);
    reporter.done();

    expect(events.at(-1)).toMatchObject({
      stage: 'Restored',
      state: {restoreStatus: {bytes: 2500}},
    });
    expect(events.at(-1)?.state?.restoreStatus?.totalBytes).toBeUndefined();

    // No more progress after done().
    const count = events.length;
    vi.advanceTimersByTime(5000);
    expect(events).toHaveLength(count);
  });
});
