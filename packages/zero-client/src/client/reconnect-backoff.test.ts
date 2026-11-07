import {expect, test} from 'vitest';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ProtocolError} from '../../../zero-protocol/src/error.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import {ClientError, isServerError} from './error.ts';
import {
  FAST_RECONNECT_INITIAL_MS,
  isTransportFailure,
  reconnectBackoffMs,
} from './reconnect-backoff.ts';

const MAX_MS = 5_000;

test.each([
  [1, 250, 500],
  [2, 500, 1000],
  [3, 1000, 2000],
  [4, 2000, 4000],
  [5, MAX_MS, MAX_MS],
  [50, MAX_MS, MAX_MS],
])('failure %i waits between %i and %i ms', (failedAttempts, minMs, maxMs) => {
  expect(reconnectBackoffMs(failedAttempts, MAX_MS, () => 0)).toBe(minMs);
  expect(reconnectBackoffMs(failedAttempts, MAX_MS, () => 0.999999)).toBe(
    maxMs,
  );
});

test('the first wait starts from FAST_RECONNECT_INITIAL_MS', () => {
  expect(reconnectBackoffMs(1, MAX_MS, () => 1)).toBe(
    FAST_RECONNECT_INITIAL_MS,
  );
});

test('a maximum below the first ceiling is used as is', () => {
  expect(reconnectBackoffMs(1, 400, () => 0)).toBe(400);
});

test('the wait varies within its range', () => {
  const waits = new Set(
    Array.from({length: 100}, () => reconnectBackoffMs(3, MAX_MS)),
  );
  expect(Math.min(...waits)).toBeGreaterThanOrEqual(1000);
  expect(Math.max(...waits)).toBeLessThanOrEqual(2000);
  expect(waits.size).toBeGreaterThan(1);
});

test.each([
  [ClientErrorKind.AbruptClose, true],
  [ClientErrorKind.CleanClose, true],
  [ClientErrorKind.ConnectTimeout, true],
  [ClientErrorKind.PingTimeout, true],
  [ClientErrorKind.PullTimeout, false],
  [ClientErrorKind.UnexpectedBaseCookie, false],
  [ClientErrorKind.Hidden, false],
] as const)('%s is a transport failure: %s', (kind, expected) => {
  expect(isTransportFailure(new ClientError({kind, message: 'x'}))).toBe(
    expected,
  );
});

test('errors sent by the server are not transport failures', () => {
  const rehome = new ProtocolError({
    kind: ErrorKind.Rehome,
    message: 'Server is draining',
    origin: ErrorOrigin.ZeroCache,
  });
  expect(isServerError(rehome)).toBe(true);
  expect(isTransportFailure(rehome)).toBe(false);
});
