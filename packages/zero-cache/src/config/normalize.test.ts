import {LogContext} from '@rocicorp/logger';
import {nanoid} from 'nanoid';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {TestLogSink} from '../../../shared/src/logging-test-utils.ts';
import {parseOptionsAdvanced} from '../../../shared/src/options.ts';
import {normalizeZeroConfig} from './normalize.ts';
import type {ZeroConfig} from './zero-config.ts';
import {zeroOptions} from './zero-config.ts';

// Mock nanoid to return predictable values for testing
vi.mock('nanoid', () => ({
  nanoid: vi.fn(() => 'mock-nanoid-value'),
}));

// Mock getHostIp to return predictable value
vi.mock('./network.ts', () => ({
  getHostIp: vi.fn(() => '192.168.1.100'),
}));

// Mock availableParallelism to return predictable value
vi.mock('node:os', () => ({
  availableParallelism: vi.fn(() => 4),
}));

describe('normalizeZeroConfig', () => {
  beforeEach(() => {
    vi.mocked(nanoid).mockClear();
    vi.mocked(nanoid).mockReturnValue('mock-nanoid-value');
  });

  describe('adminPassword behavior', () => {
    const createBaseConfig = (): ZeroConfig => {
      const {config} = parseOptionsAdvanced(zeroOptions, {
        argv: [
          '--upstream-db',
          'postgresql://user:pass@localhost/db',
          '--port',
          '3000',
          '--replica-file',
          './test-replica.db',
        ],
        envNamePrefix: 'ZERO_',
        allowUnknown: false,
        allowPartial: false,
        env: {},
        logger: {info: vi.fn()},
      });
      return config;
    };

    test('generates adminPassword when not provided', () => {
      const logSink = new TestLogSink();
      const lc = new LogContext('debug', {}, logSink);
      const config = createBaseConfig();
      const env: NodeJS.ProcessEnv = {};

      const result = normalizeZeroConfig(lc, config, env);

      expect(result.adminPassword).toBe('mock-nanoid-value');
      expect(env['ZERO_ADMIN_PASSWORD']).toBe('mock-nanoid-value');
      expect(logSink.messages).toContainEqual([
        'info',
        expect.anything(),
        [
          'Admin password was not set. Using a new random password: mock-nanoid-value',
        ],
      ]);
    });

    test('preserves existing adminPassword when provided', () => {
      const logSink = new TestLogSink();
      const lc = new LogContext('debug', {}, logSink);
      const config = createBaseConfig();
      config.adminPassword = 'existing-password';
      const env: NodeJS.ProcessEnv = {};

      const result = normalizeZeroConfig(lc, config, env);

      expect(result.adminPassword).toBe('existing-password');
      expect(env['ZERO_ADMIN_PASSWORD']).toBeUndefined();
      // Should not log about generating a new password
      expect(logSink.messages).not.toContainEqual([
        'info',
        expect.anything(),
        expect.arrayContaining([
          expect.stringContaining('Admin password was not set'),
        ]),
      ]);
    });

    test('generates different passwords for multiple calls', () => {
      // Clear the default mock behavior and set up specific return values
      // Note: normalizeZeroConfig calls nanoid() twice per call - once for taskID and once for adminPassword
      vi.mocked(nanoid).mockClear();
      vi.mocked(nanoid)
        .mockReturnValueOnce('task-id-1')
        .mockReturnValueOnce('first-password')
        .mockReturnValueOnce('task-id-2')
        .mockReturnValueOnce('second-password');

      const logSink1 = new TestLogSink();
      const lc1 = new LogContext('debug', {}, logSink1);
      const config1 = createBaseConfig();
      const env1: NodeJS.ProcessEnv = {};

      const logSink2 = new TestLogSink();
      const lc2 = new LogContext('debug', {}, logSink2);
      const config2 = createBaseConfig();
      const env2: NodeJS.ProcessEnv = {};

      const result1 = normalizeZeroConfig(lc1, config1, env1);
      const result2 = normalizeZeroConfig(lc2, config2, env2);

      expect(result1.adminPassword).toBe('first-password');
      expect(result2.adminPassword).toBe('second-password');
      expect(env1['ZERO_ADMIN_PASSWORD']).toBe('first-password');
      expect(env2['ZERO_ADMIN_PASSWORD']).toBe('second-password');
    });

    test('handles empty string adminPassword as falsy', () => {
      const logSink = new TestLogSink();
      const lc = new LogContext('debug', {}, logSink);
      const config = createBaseConfig();
      config.adminPassword = '';
      const env: NodeJS.ProcessEnv = {};

      const result = normalizeZeroConfig(lc, config, env);

      expect(result.adminPassword).toBe('mock-nanoid-value');
      expect(env['ZERO_ADMIN_PASSWORD']).toBe('mock-nanoid-value');
      expect(logSink.messages).toContainEqual([
        'info',
        expect.anything(),
        [
          'Admin password was not set. Using a new random password: mock-nanoid-value',
        ],
      ]);
    });
  });
});
