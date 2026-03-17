import {describe, expect, test} from 'vitest';

import {ErrorKind} from './error-kind.ts';
import {ErrorOrigin} from './error-origin.ts';
import {ProtocolError, isProtocolError, type ErrorBody} from './error.ts';

describe('ProtocolError', () => {
  test('exposes error body and metadata', () => {
    const body: ErrorBody = {
      kind: ErrorKind.InvalidPush,
      message: 'invalid push',
      origin: ErrorOrigin.Server,
    };

    const error = new ProtocolError(body);

    expect(error).toBeInstanceOf(Error);
    expect(error.errorBody).toBe(body);
    expect(error.kind).toBe(ErrorKind.InvalidPush);
    expect(error.name).toBe('ProtocolError');
    expect(error.message).toBe('invalid push');
    expect(isProtocolError(error)).toBe(true);
  });

  test('preserves error cause when provided', () => {
    const cause = new Error('network failure');
    const body: ErrorBody = {
      kind: ErrorKind.Unauthorized,
      message: 'unauthorized',
      origin: ErrorOrigin.Server,
    };

    const error = new ProtocolError(body, {cause});

    expect(error.cause).toBe(cause);
    expect(error.errorBody).toBe(body);
    expect(error.kind).toBe(ErrorKind.Unauthorized);
  });

  test('has useful stack trace', () => {
    const error = new ProtocolError({
      kind: ErrorKind.InvalidPush,
      message: 'invalid push',
    });

    expect(error.stack).toBeDefined();
    // Stack formatting differs by engine: V8/Spidermonkey includes error class names,
    // while WebKit often only includes callsite URLs and line numbers.
    expect(error.stack).toMatch(/ProtocolError|error\.test\.ts:\d+:\d+/);
  });
});
