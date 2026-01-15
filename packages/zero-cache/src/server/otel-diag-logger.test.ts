import {beforeEach, describe, expect, test, vi} from 'vitest';
import {type DiagLogger} from '@opentelemetry/api';
import type {LogContext} from '@rocicorp/logger';
import {
  resetOtelDiagnosticLogger,
  setupOtelDiagnosticLogger,
} from './otel-diag-logger.js';

// Mock the diag.setLogger function
vi.mock('@opentelemetry/api', async () => {
  const actual = await vi.importActual('@opentelemetry/api');
  return {
    ...actual,
    diag: {
      setLogger: vi.fn(),
    },
  };
});

// Get reference to the mocked function
const {diag} = await import('@opentelemetry/api');
const mockSetLogger = vi.mocked(diag.setLogger);

describe('otel-diag-logger', () => {
  let mockLogContext: LogContext;
  let mockLog: {
    debug: ReturnType<typeof vi.fn>;
    info: ReturnType<typeof vi.fn>;
    warn: ReturnType<typeof vi.fn>;
    error: ReturnType<typeof vi.fn>;
  };

  beforeEach(() => {
    vi.clearAllMocks();
    resetOtelDiagnosticLogger();

    // Create mock log functions
    mockLog = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };

    // Create mock LogContext
    mockLogContext = {
      withContext: vi.fn().mockReturnValue(mockLog),
      debug: mockLog.debug,
      info: mockLog.info,
      warn: mockLog.warn,
      error: mockLog.error,
      flush: vi.fn(),
    } as unknown as LogContext;
  });

  test('setupOtelDiagnosticLogger configures diag logger with proper options', () => {
    const result = setupOtelDiagnosticLogger(mockLogContext);

    expect(result).toBe(true);
    expect(mockSetLogger).toHaveBeenCalledTimes(1);

    const [logger, options] = mockSetLogger.mock.calls[0];
    expect(options).toEqual({
      logLevel: 30, // DiagLogLevel.ERROR
      suppressOverrideMessage: true,
    });

    // Verify logger has all required methods
    expect(logger).toHaveProperty('verbose');
    expect(logger).toHaveProperty('debug');
    expect(logger).toHaveProperty('info');
    expect(logger).toHaveProperty('warn');
    expect(logger).toHaveProperty('error');
  });

  test('setupOtelDiagnosticLogger returns false when no LogContext provided', () => {
    const result = setupOtelDiagnosticLogger();

    expect(result).toBe(false);
    expect(mockSetLogger).not.toHaveBeenCalled();
  });

  test('setupOtelDiagnosticLogger returns false when called multiple times', () => {
    const result1 = setupOtelDiagnosticLogger(mockLogContext);
    const result2 = setupOtelDiagnosticLogger(mockLogContext);

    expect(result1).toBe(true);
    expect(result2).toBe(false);
    expect(mockSetLogger).toHaveBeenCalledTimes(1);
  });

  test('setupOtelDiagnosticLogger can be forced to reconfigure', () => {
    const result1 = setupOtelDiagnosticLogger(mockLogContext);
    const result2 = setupOtelDiagnosticLogger(mockLogContext, true); // force = true

    expect(result1).toBe(true);
    expect(result2).toBe(true);
    expect(mockSetLogger).toHaveBeenCalledTimes(2); // Should be called twice
  });

  test('diagnostic logger routes messages to LogContext correctly', () => {
    setupOtelDiagnosticLogger(mockLogContext);

    const [logger] = mockSetLogger.mock.calls[0] as [DiagLogger, unknown];

    // Test all log levels
    logger.verbose('verbose message', 'arg1');
    logger.debug('debug message', 'arg2');
    logger.info('info message', 'arg3');
    logger.warn('warn message', 'arg4');
    logger.error('error message', 'arg5');

    expect(mockLog.debug).toHaveBeenCalledWith('verbose message', 'arg1');
    expect(mockLog.debug).toHaveBeenCalledWith('debug message', 'arg2');
    expect(mockLog.info).toHaveBeenCalledWith('info message', 'arg3');
    expect(mockLog.warn).toHaveBeenCalledWith('warn message', 'arg4');
    // All OTEL errors are logged as warnings since they don't affect app functionality
    expect(mockLog.warn).toHaveBeenCalledWith('error message', 'arg5');
  });

  test('diagnostic logger logs all errors as warnings', () => {
    setupOtelDiagnosticLogger(mockLogContext);

    const [logger] = mockSetLogger.mock.calls[0] as [DiagLogger, unknown];

    logger.error('Request Timeout occurred', 'extra-arg');
    logger.error('Some other error', 'extra-arg');
    logger.error('Real error message', 'arg1');

    // All OTEL errors are logged as warnings since they don't affect app functionality
    expect(mockLog.warn).toHaveBeenCalledTimes(3);
    expect(mockLog.error).not.toHaveBeenCalled();
  });

  test('respects OTEL_LOG_LEVEL environment variable', () => {
    const originalLogLevel = process.env.OTEL_LOG_LEVEL;

    try {
      process.env.OTEL_LOG_LEVEL = 'debug';
      setupOtelDiagnosticLogger(mockLogContext);

      const [, options] = mockSetLogger.mock.calls[0];
      expect(options).toEqual({
        logLevel: 70, // DiagLogLevel.DEBUG
        suppressOverrideMessage: true,
      });
    } finally {
      if (originalLogLevel !== undefined) {
        process.env.OTEL_LOG_LEVEL = originalLogLevel;
      } else {
        delete process.env.OTEL_LOG_LEVEL;
      }
    }
  });

  test('handles various OTEL_LOG_LEVEL values correctly', () => {
    const testCases = [
      {env: 'none', expected: 0},
      {env: 'error', expected: 30},
      {env: 'warn', expected: 50},
      {env: 'warning', expected: 50},
      {env: 'info', expected: 60},
      {env: 'debug', expected: 70},
      {env: 'verbose', expected: 80},
      {env: 'all', expected: 9999},
      {env: 'invalid', expected: 30}, // Falls back to ERROR
      {env: undefined, expected: 30}, // Falls back to ERROR
    ];

    const originalLogLevel = process.env.OTEL_LOG_LEVEL;

    testCases.forEach(({env, expected}) => {
      try {
        // Reset for each test
        resetOtelDiagnosticLogger();
        mockSetLogger.mockClear();

        if (env !== undefined) {
          process.env.OTEL_LOG_LEVEL = env;
        } else {
          delete process.env.OTEL_LOG_LEVEL;
        }

        setupOtelDiagnosticLogger(mockLogContext);

        const [, options] = mockSetLogger.mock.calls[0];
        expect(options).toBeDefined();
        expect(options).toHaveProperty('logLevel', expected);
      } finally {
        if (originalLogLevel !== undefined) {
          process.env.OTEL_LOG_LEVEL = originalLogLevel;
        } else {
          delete process.env.OTEL_LOG_LEVEL;
        }
      }
    });
  });

  test('resetOtelDiagnosticLogger allows reconfiguration', () => {
    // First setup
    const result1 = setupOtelDiagnosticLogger(mockLogContext);
    expect(result1).toBe(true);

    // Second setup should fail
    const result2 = setupOtelDiagnosticLogger(mockLogContext);
    expect(result2).toBe(false);

    // Reset and try again
    resetOtelDiagnosticLogger();
    const result3 = setupOtelDiagnosticLogger(mockLogContext);
    expect(result3).toBe(true);

    expect(mockSetLogger).toHaveBeenCalledTimes(2);
  });
});
