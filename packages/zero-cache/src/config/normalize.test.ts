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
      sqliteChangeLogRetentionMs: 60_000,
      sqliteChangeLogReadBatchRows: 1000,
      sqliteChangeLogPurgeBatchRows: 1000,
      sqliteChangeLogBarrierTimeoutMs: 30_000,
    },
    change: {db: 'postgres:///change'},
    cvr: {db: 'postgres:///cvr'},
    litestream: {
      port: 9090,
      backupUsingV5: false,
      restoreUsingV5: false,
      executable: undefined,
      executableV5: undefined,
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
        }),
      ),
    ).toThrow(
      '--litestream-backup-using-v5 requires --litestream-restore-using-v5',
    );
  });

  test('backupUsingV5 requires the executable flipped to the v5 binary', () => {
    expect(() =>
      assertNormalized(
        configWith({
          backupUsingV5: true,
          restoreUsingV5: true,
          executable: '/bin/litestream-v3',
          executableV5: '/bin/litestream-v5',
        }),
      ),
    ).toThrow(
      '--litestream-backup-using-v5 requires --litestream-executable to be ' +
        'flipped to the v5 binary',
    );
  });

  test('backupUsingV5 requires executableV5 to actually be configured', () => {
    // Guards against `undefined === undefined` slipping past the equality check
    // when neither executable is set.
    expect(() =>
      assertNormalized(
        configWith({
          backupUsingV5: true,
          restoreUsingV5: true,
          executable: undefined,
          executableV5: undefined,
        }),
      ),
    ).toThrow('--litestream-executable must equal');
  });

  test('allows backupUsingV5 when executable === executableV5', () => {
    expect(() =>
      assertNormalized(
        configWith({
          backupUsingV5: true,
          restoreUsingV5: true,
          executable: '/bin/litestream-v5',
          executableV5: '/bin/litestream-v5',
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

  test('accepts positive integer tuning values', () => {
    const config = configWith({});
    config.changeStreamer.sqliteChangeLogMode = 'serve';
    config.changeStreamer.sqliteChangeLogReadPercent = 100;

    expect(() => assertNormalized(config)).not.toThrow();
  });
});
