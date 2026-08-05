import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import Sqlite3Database from '@rocicorp/zero-sqlite3';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import type {Worker} from '../../types/processes.ts';
import {VfsBackupWatermarkPoller} from './vfs-watermark-poller.ts';

const lc = createSilentLogContext();

let dir: string;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'vfs-poller-'));
});

afterEach(() => {
  rmSync(dir, {recursive: true, force: true});
});

/**
 * A real, file-backed sqlite db shaped like the `_zero.replicationState` table
 * that the poller reads. The returned `writer` handle mutates the single row
 * via `set()`; because the data lives in a file, it survives the poller
 * closing and reopening its own read-only connection (pausing/resuming
 * polling), exactly as an s3 backup would.
 *
 * `backupTime` holds the value the remote's `litestream_time()` returns (see
 * `openReadonly`); the extension normally sources it from the replica, so here
 * it is controlled directly. It defaults to a valid RFC3339Nano timestamp: the
 * poller only reads it after a successful watermark query, which implies LTX
 * files were loaded, so `litestream_time()` never returns its `'latest'`
 * sentinel in that path.
 */
const DEFAULT_BACKUP_TIME = '2026-08-04T00:00:00.123456789Z';
// The millisecond epoch the poller should derive from DEFAULT_BACKUP_TIME,
// computed independently of string parsing (JS `Date` truncates the
// sub-millisecond digits), so assertions actually verify the parse.
const DEFAULT_BACKUP_TIME_MS = Date.UTC(2026, 7, 4, 0, 0, 0, 123);

function fileDb(name: string) {
  const path = join(dir, name);
  const writer = new Database(lc, path);
  writer.exec(`
    CREATE TABLE "_zero.replicationState" (stateVersion NOT NULL, writeTimeMs NOT NULL);
    INSERT INTO "_zero.replicationState" (stateVersion, writeTimeMs) VALUES ('', 'not-a-number');
  `);
  const setStmt = writer.prepare(
    'UPDATE "_zero.replicationState" SET stateVersion = ?, writeTimeMs = ?',
  );
  const backupTime = {value: DEFAULT_BACKUP_TIME};
  return {
    path,
    backupTime,
    set: (watermark: string, writeTimeMs: number | string) =>
      setStmt.run(watermark, writeTimeMs),
    setBackupTime: (time: string) => {
      backupTime.value = time;
    },
    dispose: () => writer.close(),
  };
}

/**
 * Opens a genuine read-only sqlite connection over a `fileDb`'s file, shaped
 * as the `QueryDatabase` the poller consumes. When `litestreamTime` is
 * supplied (the remote db), a connection-scoped `litestream_time()` SQL
 * function is registered to stand in for the VFS extension. `onClose` observes
 * the connection lifecycle before the underlying db is really closed.
 */
function openReadonly(
  path: string,
  litestreamTime: (() => string) | undefined,
  onClose: () => void,
) {
  const db = new Sqlite3Database(path, {readonly: true});
  if (litestreamTime) {
    db.function('litestream_time', litestreamTime);
  }
  return {
    prepare: (sql: string) => {
      const stmt = db.prepare(sql);
      return {get: <T>() => stmt.get() as T};
    },
    close: () => {
      onClose();
      db.close();
    },
  };
}

const update = (
  watermark: string,
  writeTimeMs: number,
  backupTimeMs: number,
) => ['backupWatermarkUpdate', {watermark, writeTimeMs, backupTimeMs}];

/**
 * Wires a poller to two file-backed dbs, handing it genuine read-only sqlite
 * connections. Remote opens are counted at the factory and remote closes via
 * the connection wrapper, so the polling lifecycle (resume = a fresh
 * connection re-reading the file; pause = close) is observable.
 */
function makePoller(
  local: ReturnType<typeof fileDb>,
  remote: ReturnType<typeof fileDb>,
) {
  const sent: unknown[] = [];
  const parent = {
    send: (msg: unknown) => {
      sent.push(msg);
      return true;
    },
  } as unknown as Worker;

  let remoteOpenCount = 0;
  let remoteCloseCount = 0;

  const poller = new VfsBackupWatermarkPoller(
    lc,
    parent,
    {
      localDbFile: local.path,
      backupURL: 's3://bucket/db',
      extensionPath: '/opt/litestream-vfs.so',
      remotePollIntervalMs: 1_000,
    },
    {
      loaderFactory: () => ({loadExtension: vi.fn(), close: vi.fn()}),
      databaseFactory: (_lc, uri) => {
        if (uri === local.path) {
          return openReadonly(local.path, undefined, () => {});
        }
        remoteOpenCount++;
        return openReadonly(
          remote.path,
          () => remote.backupTime.value,
          () => {
            remoteCloseCount++;
          },
        );
      },
    },
  );

  return {
    poller,
    sent,
    remoteOpenCount: () => remoteOpenCount,
    remoteCloseCount: () => remoteCloseCount,
  };
}

describe('litestream/vfs-watermark-poller checkWatermarks', () => {
  test('opens remote once on startup, then stays idle while local matches', () => {
    const local = fileDb('local.db');
    local.set('05', 100);
    const remote = fileDb('remote.db');
    remote.set('05', 90); // backupTime defaults to DEFAULT_BACKUP_TIME
    const {poller, sent, remoteOpenCount, remoteCloseCount} = makePoller(
      local,
      remote,
    );

    // First check reconciles local against the backup, emits the backup's
    // state, then pauses polling since they match.
    poller.checkWatermarks();
    expect(remoteOpenCount()).toBe(1);
    expect(remoteCloseCount()).toBe(1);
    expect(sent).toEqual([update('05', 90, DEFAULT_BACKUP_TIME_MS)]);

    // Nothing changed: the poller must NOT reopen the remote (no s3 access).
    poller.checkWatermarks();
    poller.checkWatermarks();
    expect(remoteOpenCount()).toBe(1);
    expect(remoteCloseCount()).toBe(1);
    expect(sent).toHaveLength(1);

    local.dispose();
    remote.dispose();
  });

  test('resumes polling when local advances, pauses when the backup catches up', () => {
    const backupTime = '2026-08-04T01:00:00.987654321Z';
    const backupTimeMs = Date.UTC(2026, 7, 4, 1, 0, 0, 987);
    const local = fileDb('local.db');
    local.set('05', 100);
    const remote = fileDb('remote.db');
    remote.set('05', 100);
    const {poller, sent, remoteOpenCount, remoteCloseCount} = makePoller(
      local,
      remote,
    );

    poller.checkWatermarks(); // sync at 05, remote paused
    expect(remoteOpenCount()).toBe(1);
    expect(remoteCloseCount()).toBe(1);
    expect(sent).toHaveLength(1);

    // Local moves ahead; the backup (s3) has not caught up yet.
    local.set('07', 200);
    poller.checkWatermarks();
    expect(remoteOpenCount()).toBe(2); // reopened to resume polling
    expect(remoteCloseCount()).toBe(1); // still open
    expect(sent).toHaveLength(1); // backup didn't advance -> no update

    // Repeated checks while the backup is still behind don't spam updates
    // and don't churn the remote connection.
    poller.checkWatermarks();
    expect(remoteOpenCount()).toBe(2);
    expect(remoteCloseCount()).toBe(1);
    expect(sent).toHaveLength(1);

    // Backup catches up to local, now with a concrete snapshot time.
    remote.set('07', 190);
    remote.setBackupTime(backupTime);
    poller.checkWatermarks();
    expect(remoteCloseCount()).toBe(2); // paused again
    expect(sent).toHaveLength(2);
    expect(sent[1]).toEqual(update('07', 190, backupTimeMs));

    local.dispose();
    remote.dispose();
  });

  test('emits an update for each backup advance while local stays ahead', () => {
    const t0 = '2026-08-04T00:00:00.111222333Z';
    const t1 = '2026-08-04T00:05:00.444555666Z';
    const t2 = '2026-08-04T00:10:00.777888999Z';
    const t0Ms = Date.UTC(2026, 7, 4, 0, 0, 0, 111);
    const t1Ms = Date.UTC(2026, 7, 4, 0, 5, 0, 444);
    const t2Ms = Date.UTC(2026, 7, 4, 0, 10, 0, 777);
    const local = fileDb('local.db');
    local.set('09', 300);
    const remote = fileDb('remote.db');
    remote.set('03', 10);
    remote.setBackupTime(t0);
    const {poller, sent, remoteOpenCount, remoteCloseCount} = makePoller(
      local,
      remote,
    );

    // Local is well ahead: open the remote and keep it open across advances.
    poller.checkWatermarks();
    expect(remoteOpenCount()).toBe(1);
    expect(remoteCloseCount()).toBe(0);
    expect(sent).toEqual([update('03', 10, t0Ms)]);

    remote.set('05', 20);
    remote.setBackupTime(t1);
    poller.checkWatermarks();
    expect(remoteOpenCount()).toBe(1); // same connection
    expect(remoteCloseCount()).toBe(0);
    expect(sent).toHaveLength(2);
    expect(sent[1]).toEqual(update('05', 20, t1Ms));

    remote.set('09', 30);
    remote.setBackupTime(t2);
    poller.checkWatermarks();
    expect(remoteCloseCount()).toBe(1); // caught up -> pause
    expect(sent).toHaveLength(3);
    expect(sent[2]).toEqual(update('09', 30, t2Ms));

    local.dispose();
    remote.dispose();
  });

  test('throws on a malformed local watermark row', () => {
    const local = fileDb('local.db'); // writeTimeMs left 'not-a-number'
    const remote = fileDb('remote.db');
    remote.set('05', 100);
    const {poller, sent, remoteOpenCount} = makePoller(local, remote);

    expect(() => poller.checkWatermarks()).toThrow(
      /Expected number at writeTimeMs/,
    );
    // Failed before touching the backup.
    expect(remoteOpenCount()).toBe(0);
    expect(sent).toHaveLength(0);

    local.dispose();
    remote.dispose();
  });
});
