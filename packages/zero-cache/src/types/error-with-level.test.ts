import {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, test} from 'vitest';
import {TestLogSink} from '../../../shared/src/logging-test-utils.ts';
import {ProtocolError} from '../../../zero-protocol/src/error.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {
  ErrorWithLevel,
  getLogLevel,
  logError,
  ProtocolErrorWithLevel,
  UrlConfigurationError,
} from './error-with-level.ts';

describe('ErrorWithLevel', () => {
  test('creates error with specified log level', () => {
    const error = new ErrorWithLevel('test message', 'warn');
    expect(error.message).toBe('test message');
    expect(error.logLevel).toBe('warn');
  });

  test('defaults to error log level', () => {
    const error = new ErrorWithLevel('test message');
    expect(error.logLevel).toBe('error');
  });
});

describe('UrlConfigurationError', () => {
  test('creates error with warn log level', () => {
    const error = new UrlConfigurationError('https://example.com');
    expect(error.message).toContain('https://example.com');
    expect(error.message).toContain(
      'not allowed by the ZERO_MUTATE/GET_QUERIES_URL configuration',
    );
    expect(error.logLevel).toBe('warn');
  });
});

describe('getLogLevel', () => {
  test('returns the explicit level from ErrorWithLevel', () => {
    const error = new ErrorWithLevel('test', 'info');
    expect(getLogLevel(error)).toBe('info');
  });

  test('returns the explicit level from ProtocolErrorWithLevel', () => {
    const error = new ProtocolErrorWithLevel(
      {
        kind: ErrorKind.Internal,
        message: 'explicit',
        origin: ErrorOrigin.ZeroCache,
      },
      'info',
    );

    expect(getLogLevel(error)).toBe('info');
  });

  test('returns warn when given a ProtocolError', () => {
    const error = new ProtocolError({
      kind: ErrorKind.Internal,
      message: 'protocol',
      origin: ErrorOrigin.Server,
    });

    expect(getLogLevel(error)).toBe('warn');
  });

  test('defaults to error for other values', () => {
    expect(getLogLevel(new Error('boom'))).toBe('error');
  });
});

describe('logError', () => {
  let sink: TestLogSink;
  let lc: LogContext;

  beforeEach(() => {
    sink = new TestLogSink();
    lc = new LogContext('debug', {}, sink);
  });

  test('uses logLevel from ErrorWithLevel', () => {
    const error = new ErrorWithLevel('test error', 'warn');
    logError(lc, error);

    expect(sink.messages).toHaveLength(1);
    expect(sink.messages[0]![0]).toBe('warn');
    expect(sink.messages[0]![2]).toEqual(['test error', error]);
  });

  test('uses logLevel from ProtocolErrorWithLevel', () => {
    const error = new ProtocolErrorWithLevel(
      {
        kind: ErrorKind.Internal,
        message: 'protocol error',
        origin: ErrorOrigin.ZeroCache,
      },
      'info',
    );
    logError(lc, error);

    expect(sink.messages).toHaveLength(1);
    expect(sink.messages[0]![0]).toBe('info');
  });

  test('uses classify function when error has no logLevel', () => {
    const error = new Error('some error');
    logError(lc, error, undefined, () => 'warn');

    expect(sink.messages).toHaveLength(1);
    expect(sink.messages[0]![0]).toBe('warn');
  });

  test('prefers ErrorWithLevel logLevel over classify', () => {
    const error = new ErrorWithLevel('test', 'info');
    logError(lc, error, undefined, () => 'error');

    expect(sink.messages).toHaveLength(1);
    expect(sink.messages[0]![0]).toBe('info');
  });

  test('defaults to error level when no classify function', () => {
    const error = new Error('some error');
    logError(lc, error);

    expect(sink.messages).toHaveLength(1);
    expect(sink.messages[0]![0]).toBe('error');
  });

  test('uses custom message when provided', () => {
    const error = new Error('original message');
    logError(lc, error, 'Custom message');

    expect(sink.messages).toHaveLength(1);
    expect(sink.messages[0]![2]).toEqual(['Custom message', error]);
  });

  test('uses error message when no custom message provided', () => {
    const error = new Error('error message');
    logError(lc, error);

    expect(sink.messages).toHaveLength(1);
    expect(sink.messages[0]![2]).toEqual(['error message', error]);
  });

  test('stringifies non-Error values', () => {
    logError(lc, 'string error');

    expect(sink.messages).toHaveLength(1);
    expect(sink.messages[0]![2]).toEqual(['string error', 'string error']);
  });

  test('classify function receives the error', () => {
    const error = {details: 'test details'};
    const classify = (e: unknown) => {
      const details = (e as {details?: unknown}).details;
      return typeof details === 'string' && details === 'test details'
        ? 'warn'
        : 'error';
    };
    logError(lc, error, undefined, classify);

    expect(sink.messages).toHaveLength(1);
    expect(sink.messages[0]![0]).toBe('warn');
  });
});
