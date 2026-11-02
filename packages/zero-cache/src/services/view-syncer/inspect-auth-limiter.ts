/**
 * Limits how often the inspector's admin password can be tried.
 *
 * Failed attempts are counted per connection, per client group and per
 * worker, each with an {@link AuthAttemptCounter}: a few failures are allowed
 * and then further attempts are blocked for a period that doubles with every
 * failure. An attempt made while any of its counters is blocked is not checked
 * against the password at all: it counts as another failure and extends the
 * block. A counter is cleared by a successful authentication or by a quiet
 * period without failures. A connection that keeps failing is reported for
 * closing.
 */

import {AuthAttemptCounter} from '../auth-attempt-counter.ts';

/** Failures a single connection may make before it is blocked. */
const CONNECTION_FREE_ATTEMPTS = 3;
/** Failures after which the connection is closed. */
const CONNECTION_MAX_FAILURES = 10;
/**
 * Failures a client group may make before it is blocked. The client group
 * outlives the connection, so reconnecting does not reset this counter.
 */
const CLIENT_GROUP_FREE_ATTEMPTS = 5;
/**
 * Failures across all connections served by this worker before every attempt
 * is blocked. This bounds the rate at which attempts can be spread over many
 * client groups, at the cost of a flood of wrong passwords also blocking the
 * right one for a while.
 */
const WORKER_FREE_ATTEMPTS = 100;
/**
 * Bound on the connection and client group counters kept in memory. When it
 * is reached, the counter that failed least recently is dropped.
 */
const MAX_ENTRIES = 10_000;

/** The ids that, with the client group, name one connection. */
type Connection = {
  readonly clientID: string;
  readonly wsID: string;
};

export class InspectorAuthLimiter {
  readonly #entries = new Map<string, AuthAttemptCounter>();
  readonly #worker: AuthAttemptCounter;
  readonly #now: () => number;

  constructor(now: () => number = Date.now) {
    this.#now = now;
    this.#worker = new AuthAttemptCounter(WORKER_FREE_ATTEMPTS, now);
  }

  /**
   * Whether an attempt from the connection must be refused without checking
   * the password.
   */
  isBlocked(clientGroupID: string, connection: Connection): boolean {
    return [
      this.#worker,
      this.#entries.get(connectionKey(clientGroupID, connection)),
      this.#entries.get(clientGroupKey(clientGroupID)),
    ].some(counter => counter !== undefined && counter.blockedFor() > 0);
  }

  /**
   * Records a failed (or blocked) attempt. Returns `true` when the connection
   * has failed so often that it should be closed.
   */
  recordFailure(clientGroupID: string, connection: Connection): boolean {
    const connectionFailures = this.#counter(
      connectionKey(clientGroupID, connection),
      CONNECTION_FREE_ATTEMPTS,
    ).recordFailure();
    this.#counter(
      clientGroupKey(clientGroupID),
      CLIENT_GROUP_FREE_ATTEMPTS,
    ).recordFailure();
    this.#worker.recordFailure();
    return connectionFailures >= CONNECTION_MAX_FAILURES;
  }

  /** Clears the connection's and client group's counters. */
  recordSuccess(clientGroupID: string, connection: Connection): void {
    this.#entries.delete(connectionKey(clientGroupID, connection));
    this.#entries.delete(clientGroupKey(clientGroupID));
  }

  /**
   * Returns the counter for `key`, moving it to the end of the map so that
   * the map stays ordered by last failure (a Map iterates in insertion
   * order). When the map is full, the least recently failed counter is
   * dropped, in constant time.
   */
  #counter(key: string, freeAttempts: number): AuthAttemptCounter {
    let counter = this.#entries.get(key);
    if (counter === undefined) {
      if (this.#entries.size >= MAX_ENTRIES) {
        for (const oldest of this.#entries.keys()) {
          this.#entries.delete(oldest);
          break;
        }
      }
      counter = new AuthAttemptCounter(freeAttempts, this.#now);
    } else {
      this.#entries.delete(key);
    }
    this.#entries.set(key, counter);
    return counter;
  }
}

/**
 * All three ids are chosen by the client, but together they name a single
 * connection: the view-syncer closes the previous connection when a new one
 * arrives with the same ids, so unrelated sockets cannot share this counter.
 */
function connectionKey(
  clientGroupID: string,
  {clientID, wsID}: Connection,
): string {
  // Encoded as a tuple so that ids containing a separator cannot collide.
  return `connection:${JSON.stringify([clientGroupID, clientID, wsID])}`;
}

function clientGroupKey(clientGroupID: string): string {
  return `clientGroup:${clientGroupID}`;
}
