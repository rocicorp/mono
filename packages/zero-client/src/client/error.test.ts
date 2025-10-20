import {describe, expect, test} from 'vitest';

import {
  ClientError,
  ServerError,
  isAuthError,
  isBackoffError,
  isClientError,
  isServerError,
} from './error.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import type {BackoffBody, ErrorBody} from '../../../zero-protocol/src/error.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';

describe('ClientError', () => {
  test('exposes error body and metadata', () => {
    const body = {
      kind: ClientErrorKind.ConnectTimeout,
      message: 'connect timeout',
    } as const;

    const error = new ClientError(body);

    expect(error).toBeInstanceOf(Error);
    expect(error.errorBody).toBe(body);
    expect(error.kind).toBe(ClientErrorKind.ConnectTimeout);
    expect(error.name).toBe('ClientError');
    expect(error.message).toBe('ConnectTimeout: connect timeout');
    expect(isClientError(error)).toBe(true);
    expect(isServerError(error)).toBe(false);
  });

  test('preserves error cause when provided', () => {
    const cause = new Error('underlying error');
    const body = {
      kind: ClientErrorKind.AbruptClose,
      message: 'connection closed',
    } as const;

    const error = new ClientError(body, {cause});

    expect(error.cause).toBe(cause);
    expect(error.errorBody).toBe(body);
    expect(error.kind).toBe(ClientErrorKind.AbruptClose);
  });

  test('has useful stack trace', () => {
    const error = new ClientError({
      kind: ClientErrorKind.PingTimeout,
      message: 'ping timeout',
    });

    expect(error.stack).toBeDefined();
    expect(error.stack).toContain('ClientError');
  });
});

describe('ServerError', () => {
  test('exposes error body and metadata', () => {
    const body: ErrorBody = {
      kind: ErrorKind.InvalidPush,
      message: 'invalid push',
    };

    const error = new ServerError(body);

    expect(error).toBeInstanceOf(Error);
    expect(error.errorBody).toBe(body);
    expect(error.kind).toBe(ErrorKind.InvalidPush);
    expect(error.name).toBe('ServerError');
    expect(error.message).toBe('InvalidPush: invalid push');
    expect(isServerError(error)).toBe(true);
    expect(isClientError(error)).toBe(false);
  });

  test('preserves error cause when provided', () => {
    const cause = new Error('network failure');
    const body: ErrorBody = {
      kind: ErrorKind.Unauthorized,
      message: 'unauthorized',
    };

    const error = new ServerError(body, {cause});

    expect(error.cause).toBe(cause);
    expect(error.errorBody).toBe(body);
    expect(error.kind).toBe(ErrorKind.Unauthorized);
  });

  test('has useful stack trace', () => {
    const error = new ServerError({
      kind: ErrorKind.InvalidPush,
      message: 'invalid push',
    });

    expect(error.stack).toBeDefined();
    expect(error.stack).toContain('ServerError');
  });
});

describe('isAuthError', () => {
  test.each([ErrorKind.AuthInvalidated, ErrorKind.Unauthorized] as const)(
    'returns true for %s server errors',
    kind => {
      const error = new ServerError({kind, message: 'auth'});
      expect(isAuthError(error)).toBe(true);
    },
  );

  test('returns false for non-auth errors and non-server errors', () => {
    const serverError = new ServerError({
      kind: ErrorKind.InvalidPush,
      message: 'not auth',
    });
    const clientError = new ClientError({
      kind: ClientErrorKind.Hidden,
      message: 'client',
    });

    expect(isAuthError(serverError)).toBe(false);
    expect(isAuthError(clientError)).toBe(false);
    expect(isAuthError(new Error('boom'))).toBe(false);
    expect(isAuthError(undefined)).toBe(false);
  });
});

describe('isBackoffError', () => {
  const backoffKinds: ReadonlyArray<BackoffBody['kind']> = [
    ErrorKind.Rebalance,
    ErrorKind.Rehome,
    ErrorKind.ServerOverloaded,
  ];

  test.each(backoffKinds)('returns backoff body for %s errors', kind => {
    const body: BackoffBody = {
      kind,
      message: 'backoff',
      minBackoffMs: 100,
    };
    const error = new ServerError(body);

    expect(isBackoffError(error)).toBe(body);
  });

  test('returns undefined for non-backoff errors', () => {
    const serverError = new ServerError({
      kind: ErrorKind.InvalidPush,
      message: 'not backoff',
    });
    const clientError = new ClientError({
      kind: ClientErrorKind.ClientClosed,
      message: 'client closed',
    });

    expect(isBackoffError(serverError)).toBeUndefined();
    expect(isBackoffError(clientError)).toBeUndefined();
    expect(isBackoffError(new Error('boom'))).toBeUndefined();
    expect(isBackoffError(undefined)).toBeUndefined();
  });
});
