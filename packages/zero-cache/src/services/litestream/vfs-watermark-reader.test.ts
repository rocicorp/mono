import {describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {
  buildLitestreamVfsReplicaURL,
  VfsBackupWatermarkReader,
} from './vfs-watermark-reader.ts';

describe('litestream/vfs-watermark-reader buildLitestreamVfsReplicaURL', () => {
  test('returns backup url unchanged when no endpoint or region are configured', () => {
    expect(buildLitestreamVfsReplicaURL({backupURL: 's3://bucket/path'})).toBe(
      's3://bucket/path',
    );
  });

  test('adds endpoint and region query params for s3 backups', () => {
    expect(
      buildLitestreamVfsReplicaURL({
        backupURL: 's3://bucket/path/to/db',
        endpoint: 'https://example.com',
        region: 'us-east-2',
      }),
    ).toBe(
      's3://bucket/path/to/db?endpoint=https%3A%2F%2Fexample.com&region=us-east-2',
    );
  });

  test('does not overwrite endpoint or region already present in the backup url', () => {
    expect(
      buildLitestreamVfsReplicaURL({
        backupURL:
          's3://bucket/path?endpoint=http%3A%2F%2Foriginal&region=us-west-1',
        endpoint: 'https://ignored.example.com',
        region: 'us-east-2',
      }),
    ).toBe('s3://bucket/path?endpoint=http%3A%2F%2Foriginal&region=us-west-1');
  });

  test('does not apply s3 endpoint settings to non-s3 backups', () => {
    expect(
      buildLitestreamVfsReplicaURL({
        backupURL: 'file:///tmp/litestream',
        endpoint: 'https://ignored.example.com',
        region: 'us-east-2',
      }),
    ).toBe('file:///tmp/litestream');
  });
});

describe('litestream/vfs-watermark-reader VfsBackupWatermarkReader', () => {
  test('loads extension, configures env, opens vfs db, and reads watermark', () => {
    const env: NodeJS.ProcessEnv = {};
    const loader = {
      loadExtension: vi.fn(),
      close: vi.fn(),
    };
    const stmt = {
      get: <T>() =>
        ({
          watermark: '02',
          writeTimeMs: 123,
          txid: '0000000000000007',
          lagSeconds: 4,
        }) as T,
    };
    const db = {
      prepare: vi.fn(() => stmt),
      close: vi.fn(),
    };

    const reader = new VfsBackupWatermarkReader(
      createSilentLogContext(),
      {
        replicaURL: 's3://bucket/path',
        extensionPath: '/opt/litestream-vfs.so',
        logLevel: 'debug',
        logFile: '/tmp/litestream-vfs.log',
        env,
      },
      {
        loaderFactory: () => loader,
        databaseFactory: (_lc, uri) => {
          expect(uri).toBe('file:zero-backup.db?vfs=litestream&mode=ro');
          return db;
        },
      },
    );

    const before = Date.now();
    const watermark = reader.readWatermark();
    const after = Date.now();

    expect(env).toEqual({
      LITESTREAM_REPLICA_URL: 's3://bucket/path',
      LITESTREAM_LOG_LEVEL: 'DEBUG',
      LITESTREAM_LOG_FILE: '/tmp/litestream-vfs.log',
    });
    expect(loader.loadExtension).toHaveBeenCalledWith('/opt/litestream-vfs.so');
    expect(db.prepare).toHaveBeenCalledWith(
      expect.stringContaining('FROM "_zero.replicationState"'),
    );
    expect(watermark).toMatchObject({
      watermark: '02',
      writeTimeMs: 123,
      txid: '0000000000000007',
      lagSeconds: 4,
    });
    expect(watermark.observedAtMs).toBeGreaterThanOrEqual(before);
    expect(watermark.observedAtMs).toBeLessThanOrEqual(after);

    reader.close();
    expect(db.close).toHaveBeenCalledTimes(1);
    expect(loader.close).toHaveBeenCalledTimes(1);
  });

  test('clears unset optional vfs log file env', () => {
    const env: NodeJS.ProcessEnv = {
      LITESTREAM_LOG_FILE: '/tmp/old.log',
    };
    new VfsBackupWatermarkReader(
      createSilentLogContext(),
      {
        replicaURL: 's3://bucket/path',
        extensionPath: '/opt/litestream-vfs.so',
        env,
      },
      {
        loaderFactory: () => ({
          loadExtension: vi.fn(),
          close: vi.fn(),
        }),
        databaseFactory: () => ({
          prepare: () => ({
            get: <T>() =>
              ({
                watermark: '02',
                writeTimeMs: null,
                txid: '0000000000000007',
                lagSeconds: 4,
              }) as T,
          }),
          close: vi.fn(),
        }),
      },
    ).close();

    expect(env['LITESTREAM_LOG_FILE']).toBeUndefined();
    expect(env['LITESTREAM_LOG_LEVEL']).toBe('INFO');
  });

  test('rejects malformed watermark rows', () => {
    const reader = new VfsBackupWatermarkReader(
      createSilentLogContext(),
      {
        replicaURL: 's3://bucket/path',
        extensionPath: '/opt/litestream-vfs.so',
        env: {},
      },
      {
        loaderFactory: () => ({
          loadExtension: vi.fn(),
          close: vi.fn(),
        }),
        databaseFactory: () => ({
          prepare: () => ({
            get: <T>() =>
              ({
                watermark: 2,
                writeTimeMs: null,
                txid: '0000000000000007',
                lagSeconds: 4,
              }) as T,
          }),
          close: vi.fn(),
        }),
      },
    );

    expect(() => reader.readWatermark()).toThrow();
    reader.close();
  });
});
