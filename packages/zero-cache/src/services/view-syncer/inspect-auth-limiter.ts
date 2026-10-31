/**
 * Limits how often the inspector's admin password can be tried.
 *
 * Failed attempts are counted per connection, per client group and per
 * worker. Each counter allows a few failures and then blocks further attempts
 * for a period that doubles with every failure, up to a cap. An attempt made
 * while any of its counters is blocked is not checked against the password at
 * all: it counts as another failure and extends the block, so a correct
 * password submitted during the block is refused too. A counter is cleared by
 * a successful authentication or by a quiet period without failures. A
 * connection that keeps failing is reported for closing.
 *
 * A blocked attempt is refused immediately rather than delayed, so that no
 * time is spent inside the view-syncer's lock, where a wait would stall the
 * client group's other traffic.
 */

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
const BASE_BLOCK_MS = 1_000;
const MAX_BLOCK_MS = 60_000;
/** A counter with no failures for this long starts over. */
const RESET_AFTER_MS = 15 * 60_000;
/**
 * Bound on the connection and client group counters kept in memory. When it
 * is reached, the counter that failed least recently is dropped.
 */
const MAX_ENTRIES = 10_000;

type Entry = {
  failures: number;
  lastFailure: number;
  blockedUntil: number;
};

function newEntry(): Entry {
  return {failures: 0, lastFailure: 0, blockedUntil: 0};
}

/** The ids that, with the client group, name one connection. */
type Connection = {
  readonly clientID: string;
  readonly wsID: string;
};

export class InspectorAuthLimiter {
  readonly #entries = new Map<string, Entry>();
  readonly #worker = newEntry();
  readonly #now: () => number;

  constructor(now: () => number = Date.now) {
    this.#now = now;
  }

  /**
   * Whether an attempt from the connection must be refused without checking
   * the password.
   */
  isBlocked(clientGroupID: string, connection: Connection): boolean {
    const now = this.#now();
    return [
      this.#worker,
      this.#entries.get(connectionKey(clientGroupID, connection)),
      this.#entries.get(clientGroupKey(clientGroupID)),
    ].some(entry => entry !== undefined && entry.blockedUntil > now);
  }

  /**
   * Records a failed (or blocked) attempt. Returns `true` when the connection
   * has failed so often that it should be closed.
   */
  recordFailure(clientGroupID: string, connection: Connection): boolean {
    const now = this.#now();
    const connectionFailures = this.#fail(
      this.#entry(connectionKey(clientGroupID, connection)),
      CONNECTION_FREE_ATTEMPTS,
      now,
    );
    this.#fail(
      this.#entry(clientGroupKey(clientGroupID)),
      CLIENT_GROUP_FREE_ATTEMPTS,
      now,
    );
    this.#fail(this.#worker, WORKER_FREE_ATTEMPTS, now);
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
  #entry(key: string): Entry {
    let entry = this.#entries.get(key);
    if (entry === undefined) {
      if (this.#entries.size >= MAX_ENTRIES) {
        for (const oldest of this.#entries.keys()) {
          this.#entries.delete(oldest);
          break;
        }
      }
      entry = newEntry();
    } else {
      this.#entries.delete(key);
    }
    this.#entries.set(key, entry);
    return entry;
  }

  #fail(entry: Entry, freeAttempts: number, now: number): number {
    if (now - entry.lastFailure > RESET_AFTER_MS) {
      entry.failures = 0;
    }
    entry.failures++;
    entry.lastFailure = now;
    if (entry.failures >= freeAttempts) {
      entry.blockedUntil =
        now +
        Math.min(
          BASE_BLOCK_MS * 2 ** (entry.failures - freeAttempts),
          MAX_BLOCK_MS,
        );
    }
    return entry.failures;
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
