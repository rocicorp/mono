import {existsSync, writeFileSync} from 'node:fs';
import {describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {changeLogFileName} from '../services/replicator/change-log-db.ts';
import type {ReplicaState} from '../services/replicator/replicator.ts';
import {DbFile} from '../test/lite.ts';
import {inProcChannel} from '../types/processes.ts';
import {Subscription} from '../types/subscription.ts';
import {
  createNotifierFrom,
  deleteStaleChangeLog,
  replicaFileName,
  replicatorDeletesStaleChangeLog,
  setUpMessageHandlers,
  subscribeTo,
  type ReplicaFileMode,
} from './replicator.ts';

const lc = createSilentLogContext();

describe('workers/replicator', () => {
  // The change log belongs to the change-streamer, and on POSIX a replicator
  // that unlinked it would fail quietly: the writer would keep appending to its
  // own inode while every reader opening by path saw nothing. So the guard has
  // to hold in both topologies, and its key has to stay the config flag rather
  // than anything derived per replica.
  test('no replicator deletes the live change log when the writer is enabled', () => {
    const REPLICA = '/data/replica.db';
    const liveLog = changeLogFileName(REPLICA);

    // The `backupURL` topology runs 'backup' + 'serving-copy'; the
    // no-`backupURL` one runs 'serving'.
    const topologies: {
      backupURL: string | undefined;
      modes: ReplicaFileMode[];
    }[] = [
      {backupURL: 's3://backup', modes: ['backup', 'serving-copy']},
      {backupURL: undefined, modes: ['serving']},
    ];

    for (const {modes} of topologies) {
      for (const mode of modes) {
        const replicatorLog = changeLogFileName(replicaFileName(REPLICA, mode));
        // Paths coincide for every mode but 'serving-copy', so a delete keyed on
        // anything but the config flag would take the live log with it.
        expect(replicatorLog === liveLog).toBe(mode !== 'serving-copy');

        for (const sqliteChangeLogMode of ['write', 'compare', 'serve']) {
          expect(replicatorDeletesStaleChangeLog(sqliteChangeLogMode)).toBe(
            false,
          );
        }
        // Only `off` -- i.e. nothing in the task writes the log -- deletes it.
        expect(replicatorDeletesStaleChangeLog('off')).toBe(true);
      }
    }
  });

  // The predicate test above pins the decision; this pins the delete the
  // replicator actually performs, on a real file.
  test('deleteStaleChangeLog removes the file only when the writer is off', () => {
    const replica = new DbFile('replicator-stale-change-log');
    const logFile = changeLogFileName(replica.path);
    try {
      for (const sqliteChangeLogMode of ['write', 'compare', 'serve']) {
        writeFileSync(logFile, 'the live log');
        expect(deleteStaleChangeLog(sqliteChangeLogMode, replica.path)).toBe(
          false,
        );
        expect(existsSync(logFile)).toBe(true);
      }
      expect(deleteStaleChangeLog('off', replica.path)).toBe(true);
      expect(existsSync(logFile)).toBe(false);
    } finally {
      deleteStaleChangeLog('off', replica.path);
      replica.delete();
    }
  });

  test('replicator subscription', async () => {
    const originalSub = Subscription.create<ReplicaState>();

    const replicator = {
      status: vi.fn(),
      subscribe: () => originalSub,
    };

    const [parent, child] = inProcChannel();

    setUpMessageHandlers(lc, replicator, parent);

    originalSub.push({state: 'version-ready', testSeqNum: 1});
    originalSub.push({state: 'version-ready', testSeqNum: 2});
    const msg3 = originalSub.push({state: 'version-ready', testSeqNum: 3});

    const notifications = [];
    const notifier = createNotifierFrom(lc, child);
    subscribeTo(lc, child);

    for await (const msg of notifier.subscribe()) {
      notifications.push(msg);
      if (notifications.length === 3) {
        break;
      }
    }

    // When the loop has been exited, msg3 should be ACKed.
    expect(await msg3.result).toBe('consumed');

    expect(notifications).toEqual([
      {state: 'version-ready', testSeqNum: 1},
      {state: 'version-ready', testSeqNum: 2},
      {state: 'version-ready', testSeqNum: 3},
    ]);
  });
});
