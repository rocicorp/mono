import {describe, expect, test} from 'vitest';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ProtocolError} from '../../../zero-protocol/src/error.ts';
import {
  getLogLevel,
  ProtocolErrorWithLevel,
  wrapWithProtocolErrorWithLevel,
} from './error-with-level.ts';

describe('ProtocolErrorWithLevel', () => {
  test('creates error with specified log level', () => {
    const error = new ProtocolErrorWithLevel(
      {
        kind: ErrorKind.Internal,
        message: 'test message',
        origin: ErrorOrigin.ZeroCache,
      },
      'warn',
    );
    expect(error.message).toBe('test message');
    expect(error.logLevel).toBe('warn');
  });
});

describe('getLogLevel', () => {
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

describe('wrapWithProtocolErrorWithLevel', () => {
  test('wraps non-protocol errors with the specified log level', () => {
    const error = wrapWithProtocolErrorWithLevel(new Error('boom'), 'warn');

    expect(error).toBeInstanceOf(ProtocolErrorWithLevel);
    expect(error.message).toBe('boom');
    expect(error.errorBody).toEqual({
      kind: ErrorKind.Internal,
      message: 'boom',
      origin: ErrorOrigin.ZeroCache,
    });
    expect(getLogLevel(error)).toBe('warn');
  });

  test('wraps protocol errors with the specified log level', () => {
    const error = wrapWithProtocolErrorWithLevel(
      new ProtocolError({
        kind: ErrorKind.Internal,
        message: 'protocol',
        origin: ErrorOrigin.ZeroCache,
      }),
      'warn',
    );

    expect(error).toBeInstanceOf(ProtocolErrorWithLevel);
    expect(error.message).toBe('protocol');
    expect(getLogLevel(error)).toBe('warn');
  });
});
