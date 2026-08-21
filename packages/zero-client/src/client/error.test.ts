import {describe, expect, test} from 'vitest';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import {
  ProtocolError,
  type BackoffBody,
  type ErrorBody,
} from '../../../zero-protocol/src/error.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import {ConnectionStatus} from './connection-status.ts';
import {
  ClientError,
  getBackoffParams,
  getErrorConnectionTransition,
  isAuthError,
  isClientError,
  isOfflineError,
  isServerError,
  isZeroError,
  MAX_AMBIGUOUS_ERROR_RETRIES,
  NO_STATUS_TRANSITION,
} from './error.ts';

describe('ClientError', () => {
  test('exposes error body and metadata', () => {
    const body = {
      kind: ClientErrorKind.ConnectTimeout,
      message: 'connect timeout',
    } as const;

    const error = new ClientError(body);

    expect(error).toBeInstanceOf(Error);
    expect(error.errorBody).toStrictEqual({
      ...body,
      origin: ErrorOrigin.Client,
    });
    expect(error.kind).toBe(ClientErrorKind.ConnectTimeout);
    expect(error.name).toBe('ClientError');
    expect(error.message).toBe('connect timeout');
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
    expect(error.errorBody).toStrictEqual({
      ...body,
      origin: ErrorOrigin.Client,
    });
    expect(error.kind).toBe(ClientErrorKind.AbruptClose);
  });

  test('has useful stack trace', () => {
    const error = new ClientError({
      kind: ClientErrorKind.PingTimeout,
      message: 'ping timeout',
    });

    expect(error.stack).toBeDefined();
    // Stack formatting differs by engine: V8/Spidermonkey includes error class
    // names, while WebKit often only includes callsite URLs and line numbers.
    expect(error.stack).toMatch(/ClientError|error\.test\.ts:\d+:\d+/);
  });
});

describe('isZeroError', () => {
  test('returns true for client errors', () => {
    const error = new ClientError({
      kind: ClientErrorKind.Hidden,
      message: 'hidden',
    });

    expect(isZeroError(error)).toBe(true);
  });

  test('returns true for server errors', () => {
    const error = new ProtocolError({
      kind: ErrorKind.Internal,
      message: 'internal',
      origin: ErrorOrigin.Server,
    });

    expect(isZeroError(error)).toBe(true);
  });

  test('returns false for non-zero errors', () => {
    expect(isZeroError(new Error('oops'))).toBe(false);
  });
});

describe('ServerError', () => {
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
    expect(isServerError(error)).toBe(true);
    expect(isClientError(error)).toBe(false);
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

describe('isOfflineError', () => {
  test('returns true for offline client errors', () => {
    const error = new ClientError({
      kind: ClientErrorKind.Offline,
      message: 'offline',
    });

    expect(isOfflineError(error)).toBe(true);
  });

  test('returns false for other errors', () => {
    const clientError = new ClientError({
      kind: ClientErrorKind.ConnectTimeout,
      message: 'connect timeout',
    });
    const genericError = new Error('boom');

    expect(isOfflineError(clientError)).toBe(false);
    expect(isOfflineError(genericError)).toBe(false);
  });
});

describe('isAuthError', () => {
  test.each([ErrorKind.AuthInvalidated, ErrorKind.Unauthorized] as const)(
    'returns true for %s server errors',
    kind => {
      const error = new ProtocolError({
        kind,
        message: 'auth',
        origin: ErrorOrigin.Server,
      });
      expect(isAuthError(error)).toBe(true);
    },
  );

  test('returns false for non-auth errors and non-server errors', () => {
    const serverError = new ProtocolError({
      kind: ErrorKind.InvalidPush,
      message: 'not auth',
      origin: ErrorOrigin.Server,
    });
    const clientError = new ClientError({
      kind: ClientErrorKind.Hidden,
      message: 'client',
    });

    expect(isAuthError(serverError)).toBe(false);
    expect(isAuthError(clientError)).toBe(false);
  });

  test('returns true for PushFailed with HTTP 401 status', () => {
    const error = new ProtocolError({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.HTTP,
      status: 401,
      message: 'Unauthorized',
      mutationIDs: [],
    });

    expect(isAuthError(error)).toBe(true);
  });

  test('returns true for PushFailed with HTTP 403 status', () => {
    const error = new ProtocolError({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.HTTP,
      status: 403,
      message: 'Forbidden',
      mutationIDs: [],
    });

    expect(isAuthError(error)).toBe(true);
  });

  test('returns true for TransformFailed with HTTP 401 status', () => {
    const error = new ProtocolError({
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.HTTP,
      status: 401,
      message: 'Unauthorized',
      queryIDs: [],
    });

    expect(isAuthError(error)).toBe(true);
  });

  test('returns true for TransformFailed with HTTP 403 status', () => {
    const error = new ProtocolError({
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.HTTP,
      status: 403,
      message: 'Forbidden',
      queryIDs: [],
    });

    expect(isAuthError(error)).toBe(true);
  });

  test('returns false for PushFailed with non-auth HTTP status', () => {
    const error = new ProtocolError({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.HTTP,
      status: 500,
      message: 'Internal Server Error',
      mutationIDs: [],
    });

    expect(isAuthError(error)).toBe(false);
  });

  test('returns false for PushFailed with non-http type', () => {
    const error = new ProtocolError({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.Internal,
      message: 'Internal error',
      mutationIDs: [],
    });

    expect(isAuthError(error)).toBe(false);
  });
});

describe('getBackoffParams', () => {
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
      origin: ErrorOrigin.ZeroCache,
    };
    const error = new ProtocolError(body);

    expect(getBackoffParams(error)).toBe(body);
  });

  test('returns undefined for non-backoff errors', () => {
    const serverError = new ProtocolError({
      kind: ErrorKind.InvalidPush,
      message: 'not backoff',
      origin: ErrorOrigin.Server,
    });
    const clientError = new ClientError({
      kind: ClientErrorKind.ClientClosed,
      message: 'client closed',
    });

    expect(getBackoffParams(serverError)).toBeUndefined();
    expect(getBackoffParams(clientError)).toBeUndefined();
  });
});

describe('getErrorConnectionTransition', () => {
  test.each([
    ClientErrorKind.AbruptClose,
    ClientErrorKind.CleanClose,
    ClientErrorKind.ConnectTimeout,
    ClientErrorKind.PingTimeout,
    ClientErrorKind.PullTimeout,
    ClientErrorKind.UnexpectedBaseCookie,
  ] as const)(
    'returns no status transition for retryable client error %s',
    kind => {
      const error = new ClientError({
        kind,
        message: 'retry',
      });

      expect(getErrorConnectionTransition(error)).toEqual({
        status: NO_STATUS_TRANSITION,
        reason: error,
      });
    },
  );

  test('bounds retries for an internal client error', () => {
    const error = new ClientError({
      kind: ClientErrorKind.Internal,
      message: 'internal',
    });

    expect(getErrorConnectionTransition(error)).toEqual({
      status: NO_STATUS_TRANSITION,
      reason: error,
      maxRetries: MAX_AMBIGUOUS_ERROR_RETRIES,
    });
  });

  test.each([
    // The client sent a message the server refused (e.g. an oversized
    // mutation). Reconnecting replays it, so this must not retry.
    ClientErrorKind.InvalidMessage,
    ClientErrorKind.UserDisconnect,
  ] as const)('returns error status for fatal client error %s', kind => {
    const error = new ClientError({
      kind,
      message: 'fatal',
    });

    expect(getErrorConnectionTransition(error)).toEqual({
      status: ConnectionStatus.Error,
      reason: error,
    });
  });

  test('returns disconnected status for disconnect timeout', () => {
    const error = new ClientError({
      kind: ClientErrorKind.Offline,
      message: 'disconnect',
    });

    expect(getErrorConnectionTransition(error)).toEqual({
      status: ConnectionStatus.Disconnected,
      reason: error,
    });
  });

  test('returns disconnected status for missing socket origin', () => {
    const error = new ClientError({
      kind: ClientErrorKind.NoSocketOrigin,
      message: 'no socket origin',
    });

    expect(getErrorConnectionTransition(error)).toEqual({
      status: ConnectionStatus.Disconnected,
      reason: error,
    });
  });

  test('returns disconnected status for hidden tab', () => {
    const error = new ClientError({
      kind: ClientErrorKind.Hidden,
      message: 'hidden',
    });

    expect(getErrorConnectionTransition(error)).toEqual({
      status: ConnectionStatus.Disconnected,
      reason: error,
    });
  });

  test('returns closed status for client closed', () => {
    const error = new ClientError({
      kind: ClientErrorKind.ClientClosed,
      message: 'closed',
    });

    expect(getErrorConnectionTransition(error)).toEqual({
      status: ConnectionStatus.Closed,
      reason: error,
    });
  });

  test('returns error status for fatal server errors', () => {
    const error = new ProtocolError({
      kind: ErrorKind.InvalidPush,
      message: 'invalid push',
      origin: ErrorOrigin.Server,
    });

    expect(getErrorConnectionTransition(error)).toEqual({
      status: ConnectionStatus.Error,
      reason: error,
    });
  });

  test.each([
    ErrorKind.Rebalance,
    ErrorKind.Rehome,
    ErrorKind.ServerOverloaded,
    ErrorKind.MutationRateLimited,
    ErrorKind.MutationFailed,
  ] as const)(
    'returns no status transition for non-fatal server error %s',
    kind => {
      const error = new ProtocolError({
        kind,
        message: 'non-fatal',
        origin: ErrorOrigin.Server,
      } as ErrorBody);

      expect(getErrorConnectionTransition(error)).toEqual({
        status: NO_STATUS_TRANSITION,
        reason: error,
      });
    },
  );

  test('bounds retries for an unmarked server internal error', () => {
    const error = new ProtocolError({
      kind: ErrorKind.Internal,
      message: 'internal',
      origin: ErrorOrigin.Server,
    });

    expect(getErrorConnectionTransition(error)).toEqual({
      status: NO_STATUS_TRANSITION,
      reason: error,
      maxRetries: MAX_AMBIGUOUS_ERROR_RETRIES,
    });
  });

  test.each([true, false] as const)(
    'honors retryable=%s on a server internal error',
    retryable => {
      const error = new ProtocolError({
        kind: ErrorKind.Internal,
        message: 'internal',
        origin: ErrorOrigin.Server,
        retryable,
      });

      expect(getErrorConnectionTransition(error)).toEqual({
        status: retryable ? NO_STATUS_TRANSITION : ConnectionStatus.Error,
        reason: error,
      });
    },
  );

  test.each([ErrorKind.AuthInvalidated, ErrorKind.Unauthorized] as const)(
    'returns needs auth status for auth error %s',
    kind => {
      const error = new ProtocolError({
        kind,
        message: 'non-fatal',
        origin: ErrorOrigin.Server,
      } as ErrorBody);

      expect(getErrorConnectionTransition(error)).toEqual({
        status: ConnectionStatus.NeedsAuth,
        reason: error,
      });
    },
  );

  test('wraps unknown errors as internal client error', () => {
    const result = getErrorConnectionTransition(new Error('boom'));

    expect(result.status).toBe(NO_STATUS_TRANSITION);
    expect(result.reason).toBeInstanceOf(ClientError);
    expect(result.reason?.kind).toBe(ClientErrorKind.Internal);
    expect(result.reason?.message).toBe('Unexpected internal error: boom');
    expect(result.reason?.errorBody.message).toBe(
      'Unexpected internal error: boom',
    );
    expect(result).toHaveProperty('maxRetries', MAX_AMBIGUOUS_ERROR_RETRIES);
  });

  test('wraps string errors as internal client error', () => {
    const result = getErrorConnectionTransition('string error');

    expect(result.status).toBe(NO_STATUS_TRANSITION);
    expect(result.reason).toBeInstanceOf(ClientError);
    expect(result.reason?.kind).toBe(ClientErrorKind.Internal);
    expect(result.reason?.message).toBe(
      'Unexpected internal error: string error',
    );
    expect(result).toHaveProperty('maxRetries', MAX_AMBIGUOUS_ERROR_RETRIES);
  });

  test.each([
    {reason: ErrorReason.HTTP, status: 500},
    {reason: ErrorReason.HTTP, status: 408},
    {reason: ErrorReason.HTTP, status: 425},
    {reason: ErrorReason.HTTP, status: 429},
    {reason: ErrorReason.Timeout},
  ] as const)('PushFailed with %o keeps retrying', body => {
    const error = new ProtocolError({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.ZeroCache,
      message: 'push failed',
      mutationIDs: [],
      ...body,
    } as ErrorBody);

    expect(getErrorConnectionTransition(error)).toEqual({
      status: NO_STATUS_TRANSITION,
      reason: error,
    });
  });

  test.each([
    {reason: ErrorReason.HTTP, status: 500},
    {reason: ErrorReason.Timeout},
  ] as const)('TransformFailed with %o keeps retrying', body => {
    const error = new ProtocolError({
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.ZeroCache,
      message: 'transform failed',
      queryIDs: [],
      ...body,
    } as ErrorBody);

    expect(getErrorConnectionTransition(error)).toEqual({
      status: NO_STATUS_TRANSITION,
      reason: error,
    });
  });

  test.each([
    {reason: ErrorReason.HTTP, status: 400},
    {reason: ErrorReason.Parse},
  ] as const)('PushFailed with %o is fatal', body => {
    const error = new ProtocolError({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.ZeroCache,
      message: 'push failed',
      mutationIDs: [],
      ...body,
    } as ErrorBody);

    expect(getErrorConnectionTransition(error)).toEqual({
      status: ConnectionStatus.Error,
      reason: error,
    });
  });

  test.each([ErrorKind.PushFailed, ErrorKind.TransformFailed] as const)(
    'bounds retries for an unmarked %s internal error',
    kind => {
      const error = new ProtocolError({
        kind,
        origin: ErrorOrigin.ZeroCache,
        reason: ErrorReason.Internal,
        message: 'internal',
        ...(kind === ErrorKind.PushFailed ? {mutationIDs: []} : {queryIDs: []}),
      } as ErrorBody);

      expect(getErrorConnectionTransition(error)).toEqual({
        status: NO_STATUS_TRANSITION,
        reason: error,
        maxRetries: MAX_AMBIGUOUS_ERROR_RETRIES,
      });
    },
  );

  test('honors an explicit retryable marker over inferred classification', () => {
    const retryableParse = new ProtocolError({
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.Parse,
      retryable: true,
      message: 'temporary parse failure',
      queryIDs: [],
    });
    const nonRetryable500 = new ProtocolError({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.HTTP,
      retryable: false,
      status: 500,
      message: 'do not retry',
      mutationIDs: [],
    });

    expect(getErrorConnectionTransition(retryableParse)).toEqual({
      status: NO_STATUS_TRANSITION,
      reason: retryableParse,
    });
    expect(getErrorConnectionTransition(nonRetryable500)).toEqual({
      status: ConnectionStatus.Error,
      reason: nonRetryable500,
    });
  });

  // Retrying would replay the same unsupported push forever.
  test('PushFailed with an unsupported push version is fatal', () => {
    const error = new ProtocolError({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.UnsupportedPushVersion,
      message: 'unsupported push version',
      mutationIDs: [],
    });

    expect(getErrorConnectionTransition(error)).toEqual({
      status: ConnectionStatus.Error,
      reason: error,
    });
  });

  test('returns needs auth status for auth errors via HTTP status codes', () => {
    const error = new ProtocolError({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.HTTP,
      status: 401,
      message: 'Unauthorized',
      mutationIDs: [],
    });

    expect(getErrorConnectionTransition(error)).toEqual({
      status: ConnectionStatus.NeedsAuth,
      reason: error,
    });
  });

  test('returns needs auth status for TransformFailed with 401 status', () => {
    const error = new ProtocolError({
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.HTTP,
      status: 401,
      message: 'Unauthorized',
      queryIDs: ['query1'],
    });

    expect(getErrorConnectionTransition(error)).toEqual({
      status: ConnectionStatus.NeedsAuth,
      reason: error,
    });
  });

  test('returns needs auth status for TransformFailed with 403 status', () => {
    const error = new ProtocolError({
      kind: ErrorKind.TransformFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.HTTP,
      status: 403,
      message: 'Forbidden',
      queryIDs: ['query1'],
    });

    expect(getErrorConnectionTransition(error)).toEqual({
      status: ConnectionStatus.NeedsAuth,
      reason: error,
    });
  });

  test('returns needs auth status for PushFailed with 403 status', () => {
    const error = new ProtocolError({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.HTTP,
      status: 403,
      message: 'Forbidden',
      mutationIDs: [],
    });

    expect(getErrorConnectionTransition(error)).toEqual({
      status: ConnectionStatus.NeedsAuth,
      reason: error,
    });
  });
});
