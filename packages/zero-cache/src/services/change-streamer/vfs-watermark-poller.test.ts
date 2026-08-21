import * as childProcess from 'node:child_process';
import {mkdtempSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {afterEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {Source} from '../../types/streams.ts';
import {
  initReplicationState,
  updateReplicationWatermark,
} from '../replicator/schema/replication-state.ts';
import type {BackedUpWatermark} from './backup-monitor.ts';
import {
  buildLitestreamVfsReplicaURL,
  VfsWatermarkPoller,
  type VfsPollerConfig,
} from './vfs-watermark-poller.ts';

// Wrap (don't replace) node:child_process.spawn so tests can assert the args
// it was called with while it still spawns the fake vfs-query executable.
// Node's built-in module exports are non-configurable, so vi.spyOn cannot be
// used here. See litestream/commands.test.ts for the same pattern.
vi.mock('node:child_process', async importOriginal => {
  const actual = await importOriginal<typeof childProcess>();
  return {...actual, spawn: vi.fn(actual.spawn)};
});
const spawnMock = vi.mocked(childProcess.spawn);

function line(stateVersion: string, writeTimeMs: number, litestreamTime = '') {
  const metadata = litestreamTime
    ? `,"litestream_time":"${litestreamTime}"`
    : '';
  return `{"query_result":[{"stateVersion":"${stateVersion}","writeTimeMs":${writeTimeMs}}],"metadata":{${metadata.replace(/^,/, '')}}}`;
}

// Writes a fake `vfs-query` executable (a POSIX shell script) that ignores
// its flags and just does whatever `sh` says: emit lines to stdout, then
// block (so it stays alive until killed, like the real vfs-query watching
// stdin/signals), or exit with a given code to simulate a crash.
function fakeVfsQuery(sh: string): string {
  const dir = mkdtempSync(join(tmpdir(), 'vfs-query-test-'));
  const executable = join(dir, 'fake-vfs-query');
  writeFileSync(executable, `#!/bin/sh\n${sh}\n`, {mode: 0o755});
  return executable;
}

// Blocks forever (until SIGTERM) after emitting its lines, mirroring the
// real vfs-query, which stays running between emissions.
const BLOCK = 'exec cat';

describe('change-streamer/vfs-watermark-poller', () => {
  let dir: string;
  let replicaFile: string;
  let setupDb: Database;
  let source: Source<BackedUpWatermark> | undefined;
  let pushed: BackedUpWatermark[];
  let consumeError: unknown;

  function makePoller(
    executable: string,
    overrides: Partial<Omit<VfsPollerConfig, 'executable'>> = {},
  ) {
    const poller = new VfsWatermarkPoller(
      createSilentLogContext(),
      replicaFile,
      {
        executable,
        remotePollIntervalMs: 50,
        backupURL: 's3://fake-bucket/backup',
        ...overrides,
      },
    );
    source = poller.start();
    pushed = [];
    consumeError = undefined;
    void (async () => {
      try {
        for await (const backedUp of source) {
          pushed.push(backedUp);
        }
      } catch (e) {
        consumeError = e;
      }
    })();
    return poller;
  }

  function setLocalWatermark(stateVersion: string, writeTimeMs = 0) {
    const db = new Database(createSilentLogContext(), replicaFile);
    try {
      updateReplicationWatermark(
        new StatementRunner(db),
        stateVersion,
        writeTimeMs,
      );
    } finally {
      db.close();
    }
  }

  function setUp() {
    dir = mkdtempSync(join(tmpdir(), 'vfs-watermark-poller-test-'));
    replicaFile = join(dir, 'replica.db');
    setupDb = new Database(createSilentLogContext(), replicaFile);
    initReplicationState(setupDb, ['zero_pub'], '00');
    setupDb.close();
  }

  afterEach(() => {
    source?.cancel();
    spawnMock.mockClear();
  });

  test('spawns the remote poller with the expected args when local diverges from remote', () => {
    setUp();
    // start() runs an immediate checkLocalWatermark(); local ('00') differs
    // from remote (undefined at construction), so this alone spawns.
    makePoller(fakeVfsQuery(BLOCK), {
      remotePollIntervalMs: 250,
      logLevel: 'debug',
      logFormat: 'text',
    });

    expect(spawnMock).toHaveBeenCalledTimes(1);
    const [executable, args] = spawnMock.mock.calls[0];
    expect(executable).toEqual(expect.stringContaining('fake-vfs-query'));
    expect(args).toEqual([
      '--replica-url',
      's3://fake-bucket/backup',
      '--query',
      `SELECT stateVersion, writeTimeMs FROM "_zero.replicationState"`,
      '--remote-poll-interval',
      '250ms',
      '--log-level',
      'debug',
      '--log-format',
      'text',
    ]);
  });

  test('pushes watermark updates parsed from vfs-query stdout, deduped by stateVersion', async () => {
    setUp();
    const poller = makePoller(
      fakeVfsQuery(
        `echo '${line('01', 100, '2025-01-01T00:00:00Z')}'\n` +
          `echo '${line('01', 100, '2025-01-01T00:00:01Z')}'\n` + // duplicate watermark
          `echo '${line('02', 200, '2025-01-01T00:00:02Z')}'\n` +
          BLOCK,
      ),
    );
    poller.checkLocalWatermark();

    await vi.waitFor(() =>
      expect(pushed.map(p => p.watermark)).toEqual(['01', '02']),
    );
    expect(pushed[0].backupTimeMs).toEqual(Date.parse('2025-01-01T00:00:00Z'));
    expect(pushed[0].writeTimeMs).toEqual(100);
  });

  test('missing metadata.litestream_time does not drop the watermark update', async () => {
    setUp();
    // No litestream_time in metadata at all (as vfs-query sends on a
    // metadata-read failure, since the Go field is `omitempty`).
    const poller = makePoller(
      fakeVfsQuery(
        `echo '{"query_result":[{"stateVersion":"01","writeTimeMs":100}],"metadata":{}}'\n` +
          BLOCK,
      ),
    );
    const before = Date.now();
    poller.checkLocalWatermark();

    await vi.waitFor(() => expect(pushed).toHaveLength(1));
    expect(pushed[0].watermark).toEqual('01');
    // Falls back to wall-clock time rather than being dropped entirely.
    expect(pushed[0].backupTimeMs).toBeGreaterThanOrEqual(before);
  });

  test('malformed output is logged and ignored, not thrown', async () => {
    setUp();
    const poller = makePoller(
      fakeVfsQuery(
        `echo 'not json'\n` +
          `echo '{"query_result":[],"metadata":{}}'\n` + // empty tuple: fails schema
          `echo '${line('01', 100, '2025-01-01T00:00:00Z')}'\n` +
          BLOCK,
      ),
    );
    poller.checkLocalWatermark();

    await vi.waitFor(() => expect(pushed).toHaveLength(1));
    expect(pushed[0].watermark).toEqual('01');
    expect(consumeError).toBeUndefined();
  });

  test('stops the remote poller once the local watermark catches up', async () => {
    setUp();
    const poller = makePoller(
      fakeVfsQuery(
        `echo '${line('01', 100, '2025-01-01T00:00:00Z')}'\n${BLOCK}`,
      ),
    );
    poller.checkLocalWatermark();
    await vi.waitFor(() => expect(pushed).toHaveLength(1));

    const child = spawnMock.mock.results[0].value as childProcess.ChildProcess;
    expect(child.killed).toBe(false);

    setLocalWatermark('01');
    poller.checkLocalWatermark();

    await vi.waitFor(() => expect(child.killed).toBe(true));
    // No respawn once caught up.
    await new Promise(r => setTimeout(r, 100));
    expect(spawnMock).toHaveBeenCalledTimes(1);
  });

  test('an unexpected exit while still behind schedules a respawn', async () => {
    setUp();
    const poller = makePoller(fakeVfsQuery('exit 1'), {
      remotePollIntervalMs: 10,
    });
    poller.checkLocalWatermark();

    await vi.waitFor(() => expect(spawnMock).toHaveBeenCalledTimes(1));
    // Backoff respawns the poller even though the local watermark never
    // changed (still behind), because it crashed rather than caught up.
    await vi.waitFor(
      () => expect(spawnMock.mock.calls.length).toBeGreaterThan(1),
      {
        timeout: 5_000,
      },
    );
  });
});

describe('buildLitestreamVfsReplicaURL', () => {
  test('returns the URL unchanged when no endpoint/region are given', () => {
    expect(
      buildLitestreamVfsReplicaURL({backupURL: 's3://bucket/path'}),
    ).toEqual('s3://bucket/path');
  });

  test('leaves non-s3 URLs untouched even with endpoint/region set', () => {
    expect(
      buildLitestreamVfsReplicaURL({
        backupURL: 'file:///tmp/backup',
        endpoint: 'http://localhost:9000',
        region: 'us-east-1',
      }),
    ).toEqual('file:///tmp/backup');
  });

  test('adds endpoint and region query params to an s3 URL', () => {
    const url = buildLitestreamVfsReplicaURL({
      backupURL: 's3://bucket/path',
      endpoint: 'http://localhost:9000',
      region: 'us-east-1',
    });
    expect(new URL(url).searchParams.get('endpoint')).toEqual(
      'http://localhost:9000',
    );
    expect(new URL(url).searchParams.get('region')).toEqual('us-east-1');
  });

  test('does not override endpoint/region params already present', () => {
    const url = buildLitestreamVfsReplicaURL({
      backupURL: 's3://bucket/path?endpoint=http://existing:9000',
      endpoint: 'http://localhost:9000',
    });
    expect(new URL(url).searchParams.get('endpoint')).toEqual(
      'http://existing:9000',
    );
  });
});
