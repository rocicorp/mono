import {describe, expect, test} from 'vitest';
import {assertNormalized} from './normalize.ts';
import type {ZeroConfig} from './zero-config.ts';

function configWith(litestream: Partial<ZeroConfig['litestream']>): ZeroConfig {
  return {
    taskID: 'task-id',
    numSyncWorkers: 1,
    adminPassword: 'admin',
    changeStreamer: {
      port: 4849,
      address: 'localhost',
      sqliteChangeLogMode: 'off',
      sqliteChangeLogReadPercent: 0,
      sqliteChangeLogColdReadPercent: 0,
      sqliteChangeLogComparePercent: 1,
      sqliteChangeLogRetentionMs: 60_000,
      sqliteChangeLogReadBatchRows: 1000,
      sqliteChangeLogPurgeBatchRows: 1000,
      sqliteChangeLogBarrierTimeoutMs: 300_000,
    },
    change: {db: 'postgres:///change'},
    cvr: {db: 'postgres:///cvr'},
    litestream: {
      port: 9090,
      backupUsingV5: false,
      restoreUsingV5: false,
      executable: undefined,
      executableV5: undefined,
      vfsPollIntervalMs: 15_000,
      ...litestream,
    },
  } as unknown as ZeroConfig;
}

describe('config/normalize litestream v5 gating', () => {
  test('backupUsingV5 requires restoreUsingV5', () => {
    expect(() =>
      assertNormalized(
        configWith({
          backupUsingV5: true,
          restoreUsingV5: false,
          executable: '/bin/litestream-v5',
          executableV5: '/bin/litestream-v5',
          vfsQueryExecutable: '/bin/vfs-query',
        }),
      ),
    ).toThrow(
      '--litestream-backup-using-v5 requires --litestream-restore-using-v5',
    );
  });

  test('backupUsingV5 requires executableV5 to actually be configured', () => {
    expect(() =>
      assertNormalized(
        configWith({
          backupURL: 's3://foo/bar',
          backupUsingV5: true,
          restoreUsingV5: true,
          executableV5: undefined,
        }),
      ),
    ).toThrow(
      '--litestream-restore-using-v5 and --litestream-backup-using-v5 require --litestream-executable-v5 to be specified',
    );
  });

  test('backupUsingV5 requires vfs-query-executable to actually be configured', () => {
    expect(() =>
      assertNormalized(
        configWith({
          backupURL: 's3://foo/bar',
          backupUsingV5: true,
          restoreUsingV5: true,
          executableV5: '/bin/litestream-v5',
        }),
      ),
    ).toThrow(
      '--litestream-backup-using-v5 requires --litestream-vfs-query-executable to be specified',
    );
  });

  test('restoreUsingV5 requires executableV5 to actually be configured', () => {
    // Guards against `undefined === undefined` slipping past the equality check
    // when neither executable is set.
    expect(() =>
      assertNormalized(
        configWith({
          backupURL: 's3://foo/bar',
          restoreUsingV5: true,
          executableV5: undefined,
        }),
      ),
    ).toThrow(
      '--litestream-restore-using-v5 and --litestream-backup-using-v5 require --litestream-executable-v5 to be specified',
    );
  });

  test('allows backupUsingV5 when executable !== executableV5', () => {
    expect(() =>
      assertNormalized(
        configWith({
          backupUsingV5: true,
          restoreUsingV5: true,
          executable: '/bin/litestream-v3',
          executableV5: '/bin/litestream-v5',
          vfsQueryExecutable: '/bin/vfs-query',
        }),
      ),
    ).not.toThrow();
  });

  test('does not gate the executable during the restore-only transition', () => {
    // The restore-forward-compat step runs a v3 executable with only
    // restoreUsingV5 enabled (so every replica can restore both WAL and LTX
    // before any backup is flipped). That must remain valid.
    expect(() =>
      assertNormalized(
        configWith({
          backupUsingV5: false,
          restoreUsingV5: true,
          executable: '/bin/litestream-v3',
          executableV5: '/bin/litestream-v5',
        }),
      ),
    ).not.toThrow();
  });
});

describe('config/normalize SQLite change log', () => {
  test('read percentage is only allowed in serve mode', () => {
    const config = configWith({});
    config.changeStreamer.sqliteChangeLogMode = 'compare';
    config.changeStreamer.sqliteChangeLogReadPercent = 1;

    expect(() => assertNormalized(config)).toThrow(
      'must be 0 unless --change-streamer-sqlite-change-log-mode=serve',
    );
  });

  test('read percentage must be an integer from 0 through 100', () => {
    for (const percent of [-1, 1.5, 101]) {
      const config = configWith({});
      config.changeStreamer.sqliteChangeLogMode = 'serve';
      config.changeStreamer.sqliteChangeLogReadPercent = percent;

      expect(() => assertNormalized(config)).toThrow(
        'must be an integer between 0 and 100',
      );
    }
  });

  test('compare percentage must be an integer from 0 through 100, in any mode', () => {
    for (const percent of [-1, 1.5, 101]) {
      const config = configWith({});
      config.changeStreamer.sqliteChangeLogComparePercent = percent;

      expect(() => assertNormalized(config)).toThrow(
        '--change-streamer-sqlite-change-log-compare-percent must be an integer between 0 and 100',
      );
    }
    // A nonzero value is valid in every mode. Sampling starts in `compare` mode.
    const config = configWith({});
    config.changeStreamer.sqliteChangeLogComparePercent = 100;
    expect(() => assertNormalized(config)).not.toThrow();
  });

  test('cold read percentage is only allowed in serve mode', () => {
    const config = configWith({});
    config.changeStreamer.sqliteChangeLogMode = 'compare';
    config.changeStreamer.sqliteChangeLogColdReadPercent = 1;

    expect(() => assertNormalized(config)).toThrow(
      '--change-streamer-sqlite-change-log-cold-read-percent must be 0 unless ' +
        '--change-streamer-sqlite-change-log-mode=serve',
    );
  });

  test('cold read percentage must be an integer from 0 through 100', () => {
    for (const percent of [-1, 1.5, 101]) {
      const config = configWith({});
      config.changeStreamer.sqliteChangeLogMode = 'serve';
      config.changeStreamer.sqliteChangeLogColdReadPercent = percent;

      expect(() => assertNormalized(config)).toThrow(
        '--change-streamer-sqlite-change-log-cold-read-percent must be an ' +
          'integer between 0 and 100',
      );
    }
  });

  test('cold read percentage must be 0 when the read percentage is 0', () => {
    const config = configWith({});
    config.changeStreamer.sqliteChangeLogMode = 'serve';
    config.changeStreamer.sqliteChangeLogReadPercent = 0;
    config.changeStreamer.sqliteChangeLogColdReadPercent = 25;

    expect(() => assertNormalized(config)).toThrow(
      '--change-streamer-sqlite-change-log-cold-read-percent must be 0 when ' +
        '--change-streamer-sqlite-change-log-read-percent is 0',
    );
  });

  test('accepts positive integer tuning values', () => {
    const config = configWith({});
    config.changeStreamer.sqliteChangeLogMode = 'serve';
    config.changeStreamer.sqliteChangeLogReadPercent = 100;
    config.changeStreamer.sqliteChangeLogColdReadPercent = 5;

    expect(() => assertNormalized(config)).not.toThrow();
  });
});
