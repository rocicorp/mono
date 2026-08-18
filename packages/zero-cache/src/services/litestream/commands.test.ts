import * as childProcess from 'node:child_process';
import {
  existsSync,
  mkdtempSync,
  readFileSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {LogContext} from '@rocicorp/logger';
import {afterEach, describe, expect, test, vi} from 'vitest';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../shared/src/must.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {type NormalizedZeroConfig} from '../../config/normalize.ts';
import {changeLogFileName} from '../replicator/change-log-db.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {
  getLastBackupTime,
  parseBackupCreatedTimes,
  tryRestore,
} from './commands.ts';
import * as litestreamMetrics from './metrics.ts';

// Wrap (don't replace) node:child_process.spawn so tests can assert the args it
// was called with while it still spawns the fake litestream executable. Node's
// built-in module exports are non-configurable, so vi.spyOn cannot be used here.
vi.mock('node:child_process', async importOriginal => {
  const actual = await importOriginal<typeof childProcess>();
  return {...actual, spawn: vi.fn(actual.spawn)};
});

// Writes a fake `litestream` executable that emits `sh` (a POSIX shell
// snippet that can branch on `$1`, the litestream subcommand) and returns the
// config pointing at it.
function configWithFakeLitestream(
  sh: string,
  replicaFile?: string,
  litestreamOverrides?: Record<string, unknown>,
): NormalizedZeroConfig {
  const dir = mkdtempSync(join(tmpdir(), 'litestream-test-'));
  const executable = join(dir, 'fake-litestream');
  writeFileSync(executable, `#!/bin/sh\n${sh}\n`, {mode: 0o755});
  return {
    port: 4848,
    log: {format: 'text'},
    replica: {file: replicaFile ?? join(dir, 'replica.db')},
    litestream: {
      port: 4850,
      executable,
      backupURL: 's3://fake-bucket/backup',
      configPath: './src/services/litestream/config.yml',
      configPathV5: './src/services/litestream/config-v5.yml',
      logLevel: 'warn',
      restoreUsingV5: false,
      checkpointThresholdMB: 40,
      incrementalBackupIntervalMinutes: 15,
      incrementalBackupIntervalSeconds: 15,
      snapshotBackupIntervalHours: 12,
      multipartConcurrency: 48,
      multipartSize: 16 * 1024 * 1024,
      restoreParallelism: 48,
      ...litestreamOverrides,
    },
  } as unknown as NormalizedZeroConfig;
}

function createRestorableReplica(file: string, watermark: string) {
  const db = new Database(createSilentLogContext(), file);
  try {
    initReplicationState(db, ['zero_pub'], watermark);
  } finally {
    db.close();
  }
}

const SQLITE_FAMILY_SUFFIXES = ['', '-wal', '-wal2', '-shm', '-journal'];

function sqliteFamilyFiles(file: string) {
  return SQLITE_FAMILY_SUFFIXES.map(suffix => `${file}${suffix}`);
}

function writeSQLiteFamily(file: string, contents = 'test data') {
  for (const familyFile of sqliteFamilyFiles(file)) {
    writeFileSync(familyFile, contents);
  }
}

describe('litestream/commands parseBackupCreatedTimes', () => {
  const lc = createSilentLogContext();

  test('parses the created column from snapshot output', () => {
    const output =
      `replica  generation        index  size     created\n` +
      `s3       1862f44967b3863f  0      4546445  2026-06-10T01:11:32Z\n`;
    expect(parseBackupCreatedTimes(lc, 'snapshots', output)).toEqual([
      new Date('2026-06-10T01:11:32Z'),
    ]);
  });

  test('parses the created column (last) from wal output with extra columns', () => {
    const output =
      `replica  generation        index  offset  size  created\n` +
      `s3       1862f44967b3863f  0      0       100   2026-06-10T01:11:32Z\n` +
      `s3       1862f44967b3863f  1      4096    200   2026-06-10T01:12:00Z\n`;
    expect(parseBackupCreatedTimes(lc, 'wal', output)).toEqual([
      new Date('2026-06-10T01:11:32Z'),
      new Date('2026-06-10T01:12:00Z'),
    ]);
  });

  test('skips the header, blank lines, and short lines', () => {
    const output =
      `\n` +
      `replica  generation        index  size     created\n` +
      `\n` +
      `   \n` +
      `s3       1862f44967b3863f  0      4546445  2026-06-10T01:11:32Z\n`;
    expect(parseBackupCreatedTimes(lc, 'snapshots', output)).toEqual([
      new Date('2026-06-10T01:11:32Z'),
    ]);
  });

  test('returns empty for empty or header-only output', () => {
    expect(parseBackupCreatedTimes(lc, 'snapshots', '')).toEqual([]);
    expect(
      parseBackupCreatedTimes(
        lc,
        'snapshots',
        `replica  generation  index  size  created\n`,
      ),
    ).toEqual([]);
  });

  test('warns and skips lines with an unparseable created time', () => {
    const sink = new TestLogSink();
    const lc = new LogContext('debug', undefined, sink);
    const output =
      `replica  generation        index  size     created\n` +
      `s3       1862f44967b3863f  0      4546445  not-a-date\n` +
      `s3       1862f44967b3863f  1      4546445  2026-06-10T01:12:00Z\n`;
    expect(parseBackupCreatedTimes(lc, 'snapshots', output)).toEqual([
      new Date('2026-06-10T01:12:00Z'),
    ]);
    expect(
      sink.messages.some(
        ([level, , args]) =>
          level === 'warn' &&
          String(args[0]).includes('unexpected line in litestream snapshots'),
      ),
    ).toBe(true);
  });
});

describe('litestream/commands getLastBackupTime', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  const lc = createSilentLogContext();

  test('returns the most recent created time across snapshots and wal', async () => {
    const {litestream, replica} = configWithFakeLitestream(
      `if [ "$1" = "snapshots" ]; then\n` +
        `  echo "replica generation index size created"\n` +
        `  echo "s3 gen 0 100 2025-04-24T00:00:00Z"\n` +
        `else\n` +
        `  echo "replica generation index offset size created"\n` +
        `  echo "s3 gen 1 0 100 2025-04-24T00:05:00Z"\n` +
        `  echo "s3 gen 2 0 100 2025-04-24T00:10:00Z"\n` +
        `fi`,
    );
    expect(await getLastBackupTime(lc, litestream, replica.file)).toEqual(
      new Date('2025-04-24T00:10:00Z'),
    );
  });

  test('rejects when nothing is listed at the destination', async () => {
    const {litestream, replica} = configWithFakeLitestream(`exit 0`);
    await expect(
      getLastBackupTime(lc, litestream, replica.file),
    ).rejects.toThrow(/no snapshots or WAL segments listed/);
  });

  test('rejects when the litestream process exits non-zero', async () => {
    const {litestream, replica} = configWithFakeLitestream(`exit 1`);
    await expect(
      getLastBackupTime(lc, litestream, replica.file),
    ).rejects.toThrow(/litestream (snapshots|wal) exited with code 1/);
  });

  test('rejects (and kills the process) when listing times out', async () => {
    vi.useFakeTimers();
    const {litestream, replica} = configWithFakeLitestream(`sleep 30`);
    const result = getLastBackupTime(lc, litestream, replica.file);
    // Surface the rejection synchronously so the unhandled-rejection guard
    // does not fire before we await it below.
    const settled = result.then(
      v => ({ok: true as const, v}),
      e => ({ok: false as const, e}),
    );
    await vi.advanceTimersByTimeAsync(30_000);
    const outcome = await settled;
    expect(outcome.ok).toBe(false);
    expect(String(outcome.ok === false && outcome.e)).toMatch(
      /timed out listing backup state/,
    );
  });
});

describe('litestream/commands restoreReplica', () => {
  const lc = createSilentLogContext();

  afterEach(() => {
    vi.restoreAllMocks();
  });

  test('restores and validates a compatible replica', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'litestream-restore-test-'));
    const source = join(dir, 'source.db');
    const replica = join(dir, 'replica.db');
    createRestorableReplica(source, '01');
    const restoredDbBytesAdd = vi.fn();
    vi.spyOn(litestreamMetrics, 'litestreamRestoredDbBytes').mockReturnValue({
      add: restoredDbBytesAdd,
    } as unknown as ReturnType<
      typeof litestreamMetrics.litestreamRestoredDbBytes
    >);
    const {litestream} = configWithFakeLitestream(
      `if [ "$1" = "restore" ]; then\n` +
        `  cp "${source}" "$6"\n` +
        `  exit 0\n` +
        `fi\n` +
        `exit 1`,
      replica,
    );

    await tryRestore(
      lc,
      litestream,
      replica,
      {
        replicaVersion: '01',
        minWatermark: '01',
      },
      'replication_manager',
    );

    expect(existsSync(replica)).toBe(true);
    expect(restoredDbBytesAdd).toHaveBeenCalledWith(
      statSync(replica).size,
      expect.objectContaining({result: 'success'}),
    );
  });

  test('deletes a change log left beside a restored replica', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'litestream-restore-test-'));
    const source = join(dir, 'source.db');
    const replica = join(dir, 'replica.db');
    createRestorableReplica(source, '01');
    // A change log whose replica is gone: the file it was anchored to is not
    // the one this restore materializes.
    const staleLog = changeLogFileName(replica);
    writeFileSync(staleLog, '');
    writeFileSync(`${staleLog}-wal2`, '');
    const {litestream} = configWithFakeLitestream(
      `if [ "$1" = "restore" ]; then\n` +
        `  cp "${source}" "$6"\n` +
        `  exit 0\n` +
        `fi\n` +
        `exit 1`,
      replica,
    );

    expect(
      await tryRestore(
        lc,
        litestream,
        replica,
        {replicaVersion: '01', minWatermark: '01'},
        'replication_manager',
      ),
    ).toMatchObject({restored: true, result: 'success'});

    expect(existsSync(replica)).toBe(true);
    expect(existsSync(staleLog)).toBe(false);
    expect(existsSync(`${staleLog}-wal2`)).toBe(false);
  });

  // The process-restart path: `-if-db-not-exists` makes the restore a no-op,
  // and the change log beside the reused replica is still the one written
  // against it. Deleting it would cost every reconnecting subscriber a
  // `too-old` on every deploy.
  test('keeps the change log when the restore reuses an existing replica', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'litestream-restore-test-'));
    const replica = join(dir, 'replica.db');
    createRestorableReplica(replica, '01');
    const log = changeLogFileName(replica);
    writeFileSync(log, '');
    const {litestream} = configWithFakeLitestream(
      `if [ "$1" = "restore" ]; then\n` + `  exit 0\n` + `fi\n` + `exit 1`,
      replica,
    );

    await tryRestore(
      lc,
      litestream,
      replica,
      {replicaVersion: '01', minWatermark: '01'},
      'replication_manager',
    );

    expect(existsSync(log)).toBe(true);
  });

  test('does not record restored bytes when reusing an existing replica', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'litestream-restore-test-'));
    const replica = join(dir, 'replica.db');
    createRestorableReplica(replica, '01');
    const restoredDbBytesAdd = vi.fn();
    vi.spyOn(litestreamMetrics, 'litestreamRestoredDbBytes').mockReturnValue({
      add: restoredDbBytesAdd,
    } as unknown as ReturnType<
      typeof litestreamMetrics.litestreamRestoredDbBytes
    >);
    const {litestream} = configWithFakeLitestream(
      `if [ "$1" = "restore" ]; then\n` + `  exit 0\n` + `fi\n` + `exit 1`,
      replica,
    );

    await tryRestore(
      lc,
      litestream,
      replica,
      {
        replicaVersion: '01',
        minWatermark: '01',
      },
      'replication_manager',
    );

    expect(restoredDbBytesAdd).not.toHaveBeenCalled();
  });

  test('deletes a stale temporary SQLite family before restore', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'litestream-restore-test-'));
    const source = join(dir, 'source.db');
    const replica = join(dir, 'replica.db');
    const temporaryReplica = `${replica}.tmp`;
    const stagedWAL = `${temporaryReplica}-deadbeef-wal`;
    createRestorableReplica(source, '01');
    writeSQLiteFamily(temporaryReplica);
    writeFileSync(stagedWAL, 'stale WAL data');
    const {litestream} = configWithFakeLitestream(
      `if [ "$1" = "restore" ]; then\n` +
        `  for suffix in "" "-wal" "-wal2" "-shm" "-journal"; do\n` +
        `    if [ -e "$6.tmp$suffix" ]; then exit 9; fi\n` +
        `  done\n` +
        `  if [ -e "$6.tmp-deadbeef-wal" ]; then exit 9; fi\n` +
        `  cp "${source}" "$6"\n` +
        `  exit 0\n` +
        `fi\n` +
        `exit 1`,
      replica,
    );

    await expect(
      tryRestore(
        lc,
        litestream,
        replica,
        {replicaVersion: '01', minWatermark: '01'},
        'replication_manager',
      ),
    ).resolves.toMatchObject({restored: true, result: 'success'});

    expect(sqliteFamilyFiles(temporaryReplica).some(existsSync)).toBe(false);
    expect(existsSync(stagedWAL)).toBe(false);
  });

  test('deletes a temporary SQLite family left by a failed restore', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'litestream-restore-test-'));
    const replica = join(dir, 'replica.db');
    const temporaryReplica = `${replica}.tmp`;
    const stagedWAL = `${temporaryReplica}-deadbeef-wal`;
    const {litestream} = configWithFakeLitestream(
      `if [ "$1" = "restore" ]; then\n` +
        `  for suffix in "" "-wal" "-wal2" "-shm" "-journal"; do\n` +
        `    printf "temporary" > "$6.tmp$suffix"\n` +
        `  done\n` +
        `  printf "temporary WAL" > "${stagedWAL}"\n` +
        `  exit 1\n` +
        `fi\n` +
        `exit 1`,
      replica,
    );

    await expect(
      tryRestore(
        lc,
        litestream,
        replica,
        {replicaVersion: '01', minWatermark: '01'},
        'replication_manager',
      ),
    ).rejects.toThrow('litestream exited with code 1');

    expect(sqliteFamilyFiles(temporaryReplica).some(existsSync)).toBe(false);
    expect(existsSync(stagedWAL)).toBe(false);
  });

  test('temporary restore cleanup preserves the real replica family', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'litestream-restore-test-'));
    const replica = join(dir, 'replica.db');
    const temporaryReplica = `${replica}.tmp`;
    writeSQLiteFamily(replica, 'real replica');
    const {litestream} = configWithFakeLitestream(
      `if [ "$1" = "restore" ]; then\n` +
        `  printf "temporary" > "$6.tmp"\n` +
        `  printf "temporary journal" > "$6.tmp-journal"\n` +
        `  exit 1\n` +
        `fi\n` +
        `exit 1`,
      replica,
    );

    await expect(
      tryRestore(
        lc,
        litestream,
        replica,
        {replicaVersion: '01', minWatermark: '01'},
        'replication_manager',
      ),
    ).rejects.toThrow('litestream exited with code 1');

    for (const familyFile of sqliteFamilyFiles(replica)) {
      expect(readFileSync(familyFile, 'utf8')).toBe('real replica');
    }
    expect(sqliteFamilyFiles(temporaryReplica).some(existsSync)).toBe(false);
  });

  test('logs restore directory contents before and after temporary cleanup', async () => {
    const sink = new TestLogSink();
    const loggingLC = new LogContext('debug', undefined, sink);
    const dir = mkdtempSync(join(tmpdir(), 'litestream-restore-test-'));
    const source = join(dir, 'source.db');
    const replica = join(dir, 'replica.db');
    const temporaryReplica = `${replica}.tmp`;
    const unexpectedFile = join(dir, 'unexpected-restore-file');
    createRestorableReplica(source, '01');
    writeFileSync(temporaryReplica, 'stale temporary data');
    writeFileSync(unexpectedFile, 'unexpected data');
    const {litestream} = configWithFakeLitestream(
      `if [ "$1" = "restore" ]; then\n` +
        `  cp "${source}" "$6"\n` +
        `  exit 0\n` +
        `fi\n` +
        `exit 1`,
      replica,
    );

    await tryRestore(
      loggingLC,
      litestream,
      replica,
      {replicaVersion: '01', minWatermark: '01'},
      'replication_manager',
    );

    const snapshots = sink.messages
      .filter(
        ([, , args]) => args[0] === 'Litestream restore directory contents',
      )
      .map(
        ([, , args]) =>
          args[1] as {
            phase: string;
            entryCount: number;
            entries: {name: string; sizeBytes: number}[];
          },
      );
    expect(snapshots.map(snapshot => snapshot.phase)).toEqual([
      'before-temp-cleanup',
      'after-temp-cleanup',
    ]);
    expect(snapshots[0]?.entries).toContainEqual(
      expect.objectContaining({
        name: 'replica.db.tmp',
        sizeBytes: 20,
      }),
    );
    expect(snapshots[1]?.entries.map(entry => entry.name)).not.toContain(
      'replica.db.tmp',
    );
    for (const snapshot of snapshots) {
      expect(snapshot.entryCount).toBeGreaterThan(0);
      expect(snapshot.entries).toContainEqual(
        expect.objectContaining({
          name: 'unexpected-restore-file',
          sizeBytes: 15,
        }),
      );
    }
  });

  test('includes captured output when restore fails', async () => {
    const {litestream, replica} = configWithFakeLitestream(
      `if [ "$1" = "restore" ]; then\n` +
        `  mkdir "$6.tmp"\n` +
        `  echo "restore stdout"\n` +
        `  echo "restore stderr" >&2\n` +
        `  exit 1\n` +
        `fi\n` +
        `exit 1`,
    );

    const error = await tryRestore(
      lc,
      litestream,
      replica.file,
      {replicaVersion: '01', minWatermark: '01'},
      'replication_manager',
    ).then(
      () => undefined,
      (e: unknown) => e,
    );

    expect(error).toBeInstanceOf(Error);
    expect(String(error)).toContain('litestream exited with code 1');
    expect(String(error)).toContain('restore stdout');
    expect(String(error)).toContain('restore stderr');
  });

  test('deletes an incompatible restored replica', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'litestream-restore-test-'));
    const source = join(dir, 'source.db');
    const replica = join(dir, 'replica.db');
    createRestorableReplica(source, '01');
    const restoreValidationRecordMs = vi.fn();
    vi.spyOn(
      litestreamMetrics,
      'litestreamRestoreValidationDuration',
    ).mockReturnValue({
      recordMs: restoreValidationRecordMs,
    } as unknown as ReturnType<
      typeof litestreamMetrics.litestreamRestoreValidationDuration
    >);
    const {litestream} = configWithFakeLitestream(
      `if [ "$1" = "restore" ]; then\n` +
        `  cp "${source}" "$6"\n` +
        `  exit 0\n` +
        `fi\n` +
        `exit 1`,
      replica,
    );
    writeFileSync(changeLogFileName(replica), '');

    expect(
      await tryRestore(
        lc,
        litestream,
        replica,
        {
          replicaVersion: '02',
          minWatermark: '02',
        },
        'replication_manager',
      ),
    ).toMatchObject({
      restored: false,
      result: 'invalid_replica',
    });

    expect(existsSync(replica)).toBe(false);
    // The change log went with it; the next attempt reseeds against whatever
    // replica it restores.
    expect(existsSync(changeLogFileName(replica))).toBe(false);
    expect(restoreValidationRecordMs).toHaveBeenCalledWith(
      expect.any(Number),
      expect.objectContaining({result: 'invalid_replica'}),
    );
  });

  // INC-961: the mechanism that actually prevents the paging is spawning
  // `litestream restore` with piped (captured) stdio rather than inheriting the
  // pod's streams — with `stdio: 'inherit'` litestream's own ERROR would reach
  // the pod's stdout directly, before our retry logic could downgrade it.
  // Assert the spawn options so a regression back to inheriting is caught.
  test('spawns `litestream restore` with piped stdio (not inherited)', async () => {
    const spawnMock = vi.mocked(childProcess.spawn);
    spawnMock.mockClear();
    const dir = mkdtempSync(join(tmpdir(), 'litestream-restore-test-'));
    const source = join(dir, 'source.db');
    const replica = join(dir, 'replica.db');
    createRestorableReplica(source, '01');
    const {litestream} = configWithFakeLitestream(
      `if [ "$1" = "restore" ]; then\n` +
        `  cp "${source}" "$6"\n` +
        `  exit 0\n` +
        `fi\n` +
        `exit 1`,
      replica,
    );

    await tryRestore(
      lc,
      litestream,
      replica,
      {
        replicaVersion: '01',
        minWatermark: '01',
      },
      'replication_manager',
    );

    const restoreCall = spawnMock.mock.calls.find(
      call => Array.isArray(call[1]) && call[1][0] === 'restore',
    );
    expect(restoreCall).toBeDefined();
    // stdio is [stdin, stdout, stderr]; stdout/stderr must be piped so
    // litestream's output is captured rather than sent to the pod's stdout.
    expect(restoreCall?.[2]?.stdio).toEqual(['ignore', 'pipe', 'pipe']);
  });

  // The v3/v5 fork is a silent one: v5 keys (l0-retention, levels,
  // monitor-interval, ...) live only in config-v5.yml, so selecting the wrong
  // file makes them no-ops with no error. Pin the mapping in both directions
  // so a regression (e.g. inverting the `v5 ? configPathV5 : configPath`
  // ternary) fails loudly.
  async function litestreamConfigForRestore(
    restoreUsingV5: boolean,
  ): Promise<string | undefined> {
    const spawnMock = vi.mocked(childProcess.spawn);
    spawnMock.mockClear();
    const dir = mkdtempSync(join(tmpdir(), 'litestream-restore-test-'));
    const source = join(dir, 'source.db');
    const replica = join(dir, 'replica.db');
    createRestorableReplica(source, '01');
    const {litestream} = configWithFakeLitestream(
      `if [ "$1" = "restore" ]; then\n` +
        `  cp "${source}" "$6"\n` +
        `  exit 0\n` +
        `fi\n` +
        `exit 1`,
      replica,
      {restoreUsingV5},
    );

    await tryRestore(
      lc,
      litestream,
      replica,
      {replicaVersion: '01', minWatermark: '01'},
      'replication_manager',
    );

    const restoreCall = spawnMock.mock.calls.find(
      call => Array.isArray(call[1]) && call[1][0] === 'restore',
    );
    return restoreCall?.[2]?.env?.LITESTREAM_CONFIG;
  }

  test('restore uses the v5 config when restoreUsingV5 is set', async () => {
    expect(await litestreamConfigForRestore(true)).toBe(
      './src/services/litestream/config-v5.yml',
    );
  });

  test('restore uses the v3 config when restoreUsingV5 is not set', async () => {
    expect(await litestreamConfigForRestore(false)).toBe(
      './src/services/litestream/config.yml',
    );
  });

  // The change log is a local cache that is deliberately not backed up: it
  // holds nothing that is not already in the replica's backup or re-derivable
  // from the upstream slot, and shipping it would give back the S3 and
  // follower-download savings the separate file exists to win.
  test('the rendered litestream config never backs up the change-log database', () => {
    const replica = join(
      mkdtempSync(join(tmpdir(), 'litestream-config-test-')),
      'replica.db',
    );
    const {litestream} = configWithFakeLitestream('exit 0', replica);
    // The variables `getLitestream` substitutes into the config it hands
    // litestream; `ZERO_REPLICA_FILE` is always the replica, never a path
    // derived from it.
    const env: Record<string, string> = {
      ZERO_REPLICA_FILE: replica,
      ZERO_LITESTREAM_BACKUP_URL: must(litestream.backupURL),
    };
    const yaml = readFileSync(
      new URL('./config.yml', import.meta.url),
      'utf-8',
    ).replace(/\$\{(\w+)\}/g, (_, name: string) => env[name] ?? '');

    const paths = Array.from(
      yaml.matchAll(/^\s*-?\s*path:\s*(\S+)\s*$/gm),
      ([, path]) => path,
    );
    expect(paths).toEqual([replica]);
    expect(paths).not.toContain(changeLogFileName(replica));
  });

  // Same invariant as above, for the v5 config we now render.
  test('the rendered v5 litestream config never backs up the change-log database', () => {
    const replica = join(
      mkdtempSync(join(tmpdir(), 'litestream-config-test-')),
      'replica.db',
    );
    const {litestream} = configWithFakeLitestream('exit 0', replica);
    const env: Record<string, string> = {
      ZERO_REPLICA_FILE: replica,
      ZERO_LITESTREAM_BACKUP_URL: must(litestream.backupURL),
    };
    const yaml = readFileSync(
      new URL('./config-v5.yml', import.meta.url),
      'utf-8',
    ).replace(/\$\{(\w+)\}/g, (_, name: string) => env[name] ?? '');

    const paths = Array.from(
      yaml.matchAll(/^\s*-?\s*path:\s*(\S+)\s*$/gm),
      ([, path]) => path,
    );
    expect(paths).toEqual([replica]);
    expect(paths).not.toContain(changeLogFileName(replica));
  });

  // Regression guard for a silent misconfiguration: `monitor-interval` is a
  // DB-level key (it governs L0/LTX creation cadence). Nested under `replica:`
  // it is an unknown key that litestream's non-strict YAML parser ignores, so
  // the backup interval silently reverts to the 1s default. Assert it sits at
  // the same indentation as `replica:` (a sibling under the db entry), not
  // deeper.
  test('config-v5.yml places monitor-interval at the db level, not under replica', () => {
    const yaml = readFileSync(
      new URL('./config-v5.yml', import.meta.url),
      'utf-8',
    );
    const indentOf = (line: string) => line.length - line.trimStart().length;

    const replicaLine = yaml
      .split('\n')
      .find(line => /^\s*replica:\s*$/.test(line));
    const monitorLine = yaml
      .split('\n')
      .find(line => /^\s*monitor-interval:/.test(line));

    expect(replicaLine).toBeDefined();
    expect(monitorLine).toBeDefined();
    // Sibling of `replica:` => nested one level under the `- path:` db entry.
    expect(indentOf(must(monitorLine))).toBe(indentOf(must(replicaLine)));
  });
});
