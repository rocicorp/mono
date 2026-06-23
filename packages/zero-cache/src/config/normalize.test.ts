import {describe, expect, test} from 'vitest';
import {assertNormalized} from './normalize.ts';
import type {ZeroConfig} from './zero-config.ts';

function configWith(litestream: Partial<ZeroConfig['litestream']>): ZeroConfig {
  return {
    taskID: 'task-id',
    numSyncWorkers: 1,
    adminPassword: 'admin',
    changeStreamer: {port: 4849, address: 'localhost'},
    change: {db: 'postgres:///change'},
    cvr: {db: 'postgres:///cvr'},
    litestream: {
      port: 9090,
      backupUsingV5: false,
      restoreUsingV5: false,
      ackFromBackup: false,
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

  test('ackFromBackup requires backupUsingV5', () => {
    // The backup watermark that gates the slot ACK is produced by the v5 VFS
    // monitor; without it the slot would never advance.
    expect(() =>
      assertNormalized(
        configWith({
          ackFromBackup: true,
          backupUsingV5: false,
        }),
      ),
    ).toThrow(
      '--litestream-ack-from-backup requires --litestream-backup-using-v5',
    );
  });

  test('allows ackFromBackup when backupUsingV5 is enabled', () => {
    expect(() =>
      assertNormalized(
        configWith({
          ackFromBackup: true,
          backupUsingV5: true,
          restoreUsingV5: true,
          executable: '/bin/litestream-v5',
          executableV5: '/bin/litestream-v5',
        }),
      ),
    ).not.toThrow();
  });
});
