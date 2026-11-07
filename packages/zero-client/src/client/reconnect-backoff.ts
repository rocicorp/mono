import {ClientErrorKind} from './client-error-kind.ts';
import {isClientError} from './error.ts';

/**
 * The longest wait before the first retry after a failed connection attempt.
 * Each further consecutive failure doubles it, up to the run loop interval.
 */
export const FAST_RECONNECT_INITIAL_MS = 500;

/**
 * How long a connection must stay up before a later failure starts the
 * schedule from the beginning again. Without this, a server that accepts
 * connections and then fails each one would keep clients on the fast end of
 * the schedule.
 */
export const STABLE_CONNECTION_MS = 5_000;

/**
 * Whether `ex` is a failure of the transport rather than a reply from the
 * server: the socket closed or never answered. These carry no backoff
 * instructions, and the usual cause (a server that has just gone away, or a
 * load balancer that has not yet stopped routing to it) clears within a few
 * seconds, so they are retried on the fast schedule. Errors sent by the server
 * keep their own backoff rules.
 */
export function isTransportFailure(ex: unknown): boolean {
  if (!isClientError(ex)) {
    return false;
  }
  switch (ex.kind) {
    case ClientErrorKind.AbruptClose:
    case ClientErrorKind.CleanClose:
    case ClientErrorKind.ConnectTimeout:
    case ClientErrorKind.PingTimeout:
      return true;
    default:
      return false;
  }
}

/**
 * The wait before retrying after `failedAttempts` consecutive transport
 * failures (1 for the first). The ceiling starts at
 * {@link FAST_RECONNECT_INITIAL_MS} and doubles with each failure, and the
 * wait is chosen between half the ceiling and the ceiling, so that clients
 * that lost the same server don't all retry at once. Once the ceiling reaches
 * `maxMs`, the wait is exactly `maxMs`.
 */
export function reconnectBackoffMs(
  failedAttempts: number,
  maxMs: number,
  random: () => number = Math.random,
): number {
  const ceiling = FAST_RECONNECT_INITIAL_MS * 2 ** (failedAttempts - 1);
  if (ceiling >= maxMs) {
    return maxMs;
  }
  return Math.round(ceiling / 2 + (random() * ceiling) / 2);
}
