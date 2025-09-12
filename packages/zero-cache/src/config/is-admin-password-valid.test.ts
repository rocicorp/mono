import {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {TestLogSink} from '../../../shared/src/logging-test-utils.ts';
import type {NormalizedZeroConfig} from './normalize.ts';
import {isAdminPasswordValid, resetWarnOnceState} from './zero-config.ts';

describe('isAdminPasswordValid', () => {
  let testLogSink: TestLogSink;
  let lc: LogContext;

  beforeEach(() => {
    // Create a test log sink to capture log messages
    testLogSink = new TestLogSink();
    lc = new LogContext('debug', undefined, testLogSink);

    // Reset the warning state for each test
    resetWarnOnceState();
  });

  describe('development mode (NODE_ENV=development)', () => {
    beforeEach(() => {
      vi.stubEnv('NODE_ENV', 'development');
    });

    test('allows access when no password is provided and no admin password is configured', () => {
      const config: Pick<NormalizedZeroConfig, 'adminPassword'> = {
        adminPassword: undefined,
      };

      const result = isAdminPasswordValid(lc, config, undefined);

      expect(result).toBe(true);
      expect(testLogSink.messages).toContainEqual([
        'warn',
        undefined,
        ['No admin password set; allowing access in development mode only'],
      ]);
    });

    test('denies access when admin password is configured but no password is provided', () => {
      const config: Pick<NormalizedZeroConfig, 'adminPassword'> = {
        adminPassword: 'secret123',
      };

      const result = isAdminPasswordValid(lc, config, undefined);

      expect(result).toBe(false);
      expect(testLogSink.messages).toContainEqual([
        'warn',
        undefined,
        ['Invalid admin password'],
      ]);
    });

    test('allows access when provided password matches configured admin password', () => {
      const config: Pick<NormalizedZeroConfig, 'adminPassword'> = {
        adminPassword: 'secret123',
      };

      const result = isAdminPasswordValid(lc, config, 'secret123');

      expect(result).toBe(true);
      expect(testLogSink.messages).toContainEqual([
        'debug',
        undefined,
        ['Admin password accepted'],
      ]);
    });

    test('denies access when provided password does not match configured admin password', () => {
      const config: Pick<NormalizedZeroConfig, 'adminPassword'> = {
        adminPassword: 'secret123',
      };

      const result = isAdminPasswordValid(lc, config, 'wrong-password');

      expect(result).toBe(false);
      expect(testLogSink.messages).toContainEqual([
        'warn',
        undefined,
        ['Invalid admin password'],
      ]);
    });

    test('denies access when password is provided but admin password is empty string', () => {
      const config: Pick<NormalizedZeroConfig, 'adminPassword'> = {
        adminPassword: '',
      };

      const result = isAdminPasswordValid(lc, config, 'some-password');

      // Empty string adminPassword is treated as "not set"
      // Since user provided a password but no admin password is configured, deny access
      expect(result).toBe(false);
      expect(testLogSink.messages).toContainEqual([
        'warn',
        undefined,
        ['No admin password set; denying access'],
      ]);
    });
  });

  describe('production mode (NODE_ENV=production)', () => {
    beforeEach(() => {
      vi.stubEnv('NODE_ENV', 'production');
    });

    test('denies access when no admin password is configured', () => {
      const config: Pick<NormalizedZeroConfig, 'adminPassword'> = {
        adminPassword: undefined,
      };

      const result = isAdminPasswordValid(lc, config, undefined);

      expect(result).toBe(false);
      expect(testLogSink.messages).toContainEqual([
        'warn',
        undefined,
        ['No admin password set; denying access'],
      ]);
    });

    test('denies access when admin password is configured but no password is provided', () => {
      const config: Pick<NormalizedZeroConfig, 'adminPassword'> = {
        adminPassword: 'secret123',
      };

      const result = isAdminPasswordValid(lc, config, undefined);

      expect(result).toBe(false);
      expect(testLogSink.messages).toContainEqual([
        'warn',
        undefined,
        ['Invalid admin password'],
      ]);
    });

    test('allows access when provided password matches configured admin password', () => {
      const config: Pick<NormalizedZeroConfig, 'adminPassword'> = {
        adminPassword: 'secret123',
      };

      const result = isAdminPasswordValid(lc, config, 'secret123');

      expect(result).toBe(true);
      expect(testLogSink.messages).toContainEqual([
        'debug',
        undefined,
        ['Admin password accepted'],
      ]);
    });

    test('denies access when provided password does not match configured admin password', () => {
      const config: Pick<NormalizedZeroConfig, 'adminPassword'> = {
        adminPassword: 'secret123',
      };

      const result = isAdminPasswordValid(lc, config, 'wrong-password');

      expect(result).toBe(false);
      expect(testLogSink.messages).toContainEqual([
        'warn',
        undefined,
        ['Invalid admin password'],
      ]);
    });

    test('denies access when admin password is empty string', () => {
      const config: Pick<NormalizedZeroConfig, 'adminPassword'> = {
        adminPassword: '',
      };

      const result = isAdminPasswordValid(lc, config, 'some-password');

      // Empty string adminPassword is treated as "not set"
      expect(result).toBe(false);
      expect(testLogSink.messages).toContainEqual([
        'warn',
        undefined,
        ['No admin password set; denying access'],
      ]);
    });
  });

  describe('no NODE_ENV set (defaults to production mode)', () => {
    beforeEach(() => {
      vi.stubEnv('NODE_ENV', '');
    });

    test('denies access when no admin password is configured (default production behavior)', () => {
      const config: Pick<NormalizedZeroConfig, 'adminPassword'> = {
        adminPassword: undefined,
      };

      const result = isAdminPasswordValid(lc, config, undefined);

      expect(result).toBe(false);
      expect(testLogSink.messages).toContainEqual([
        'warn',
        undefined,
        ['No admin password set; denying access'],
      ]);
    });

    test('allows access when provided password matches configured admin password', () => {
      const config: Pick<NormalizedZeroConfig, 'adminPassword'> = {
        adminPassword: 'secret123',
      };

      const result = isAdminPasswordValid(lc, config, 'secret123');

      expect(result).toBe(true);
      expect(testLogSink.messages).toContainEqual([
        'debug',
        undefined,
        ['Admin password accepted'],
      ]);
    });
  });

  describe('edge cases', () => {
    beforeEach(() => {
      vi.stubEnv('NODE_ENV', 'development');
    });

    test('handles empty password string correctly', () => {
      const config: Pick<NormalizedZeroConfig, 'adminPassword'> = {
        adminPassword: '',
      };

      const result = isAdminPasswordValid(lc, config, '');

      // Empty string adminPassword is treated as "not set"
      // In dev mode with no admin password, access is allowed
      expect(result).toBe(true);
      expect(testLogSink.messages).toContainEqual([
        'warn',
        undefined,
        ['No admin password set; allowing access in development mode only'],
      ]);
    });
  });
});
