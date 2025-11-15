import {describe, expect, test} from 'vitest';
import {ProtocolError} from '../../../zero-protocol/src/error.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {getLogLevel, ProtocolErrorWithLevel} from './error-with-level.ts';

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

  test('defaults to warn log level', () => {
    const error = new ProtocolErrorWithLevel({
      kind: ErrorKind.Internal,
      message: 'test message',
      origin: ErrorOrigin.ZeroCache,
    });
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
