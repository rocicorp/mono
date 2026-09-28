import {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {TestLogSink} from '../../../shared/src/logging-test-utils.ts';
import type {NormalizedZeroConfig} from './normalize.ts';
import {
  getOperatorAccess,
  isAdminPasswordValid,
  resetWarnOnceState,
} from './zero-config.ts';

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

describe('operator password', () => {
  let testLogSink: TestLogSink;
  let lc: LogContext;

  const config: Pick<
    NormalizedZeroConfig,
    'adminPassword' | 'operatorPassword'
  > = {
    adminPassword: 'admin-secret',
    operatorPassword: 'operator-secret',
  };

  beforeEach(() => {
    testLogSink = new TestLogSink();
    lc = new LogContext('debug', undefined, testLogSink);
    resetWarnOnceState();
  });

  describe.each(['production', 'development'])('NODE_ENV=%s', nodeEnv => {
    beforeEach(() => {
      vi.stubEnv('NODE_ENV', nodeEnv);
    });

    test('isAdminPasswordValid never accepts the operator password', () => {
      expect(isAdminPasswordValid(lc, config, 'operator-secret')).toBe(false);
      expect(testLogSink.messages).toContainEqual([
        'warn',
        undefined,
        ['Invalid admin password'],
      ]);
    });

    test('getOperatorAccess grants operator access for the operator password', () => {
      expect(getOperatorAccess(lc, config, 'operator-secret')).toBe('operator');
      expect(testLogSink.messages).toContainEqual([
        'debug',
        undefined,
        ['Operator password accepted'],
      ]);
    });

    test('getOperatorAccess grants admin access for the admin password', () => {
      expect(getOperatorAccess(lc, config, 'admin-secret')).toBe('admin');
      expect(testLogSink.messages).toContainEqual([
        'debug',
        undefined,
        ['Admin password accepted'],
      ]);
    });

    test.each([undefined, '', 'wrong', 'operator-secretX', 'operator'])(
      'getOperatorAccess denies access for password %o',
      password => {
        expect(getOperatorAccess(lc, config, password)).toBeUndefined();
        expect(testLogSink.messages).toContainEqual([
          'warn',
          undefined,
          ['Invalid admin or operator password'],
        ]);
      },
    );

    test('getOperatorAccess without an operator password configured is admin-only', () => {
      const adminOnly = {
        adminPassword: 'admin-secret',
        operatorPassword: undefined,
      };
      expect(getOperatorAccess(lc, adminOnly, 'admin-secret')).toBe('admin');
      expect(
        getOperatorAccess(lc, adminOnly, 'operator-secret'),
      ).toBeUndefined();
      expect(getOperatorAccess(lc, adminOnly, '')).toBeUndefined();
    });
  });

  test('getOperatorAccess in development mode with only an operator password configured', () => {
    vi.stubEnv('NODE_ENV', 'development');
    const operatorOnly = {
      adminPassword: undefined,
      operatorPassword: 'operator-secret',
    };
    expect(getOperatorAccess(lc, operatorOnly, 'operator-secret')).toBe(
      'operator',
    );
    expect(getOperatorAccess(lc, operatorOnly, 'wrong')).toBeUndefined();
    // Unchanged from before the operator password existed: in development
    // mode with no admin password, a request without a password is allowed.
    expect(getOperatorAccess(lc, operatorOnly, undefined)).toBe('admin');
  });

  test('getOperatorAccess grants admin access when both passwords are the same', () => {
    vi.stubEnv('NODE_ENV', 'production');
    const same = {
      adminPassword: 'same-secret',
      operatorPassword: 'same-secret',
    };
    expect(getOperatorAccess(lc, same, 'same-secret')).toBe('admin');
    expect(isAdminPasswordValid(lc, same, 'same-secret')).toBe(true);
    expect(getOperatorAccess(lc, same, 'wrong')).toBeUndefined();
  });

  test('getOperatorAccess in production mode with no passwords configured', () => {
    vi.stubEnv('NODE_ENV', 'production');
    const none = {adminPassword: undefined, operatorPassword: undefined};
    expect(getOperatorAccess(lc, none, undefined)).toBeUndefined();
    expect(getOperatorAccess(lc, none, '')).toBeUndefined();
    expect(testLogSink.messages).toContainEqual([
      'warn',
      undefined,
      ['No admin password set; denying access'],
    ]);
  });
});
