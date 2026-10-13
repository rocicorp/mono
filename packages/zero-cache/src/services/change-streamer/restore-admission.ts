import type {LogContext} from '@rocicorp/logger';
import {getOrCreateGauge} from '../../observability/metrics.ts';
import type {LocalReplica} from './subscribe.ts';

/**
 * The default time to wait for a subscriber that was granted permission to
 * restore to reconnect (i.e. to open a replacement reservation) after its
 * held connection closes, or after its replacement reservation closes
 * without subscribing (e.g. to retry a failed restore). This covers the
 * subscriber's process exit and restart (e.g. a container restart, the
 * backoff of which starts at 10 seconds in Kubernetes), but not the restore
 * itself, which is tracked by the liveness of its snapshot reservation.
 */
export const DEFAULT_PENDING_GRANT_TTL_MS = 90_000;

export type RestoreAdmissionOptions = {
  /** The replica version of the replication-manager. */
  replicaVersion: string;
  /**
   * The maximum number of subscribers that may be restoring (or granted
   * permission to restore) at the same time.
   */
  maxConcurrentRestores: number;
  pendingGrantTTLMs?: number | undefined;
  // Injectable timers, for deterministic testing.
  setTimeoutFn?: typeof setTimeout | undefined;
  clearTimeoutFn?: typeof clearTimeout | undefined;
};

export type AdmissionRequest = {
  /**
   * Called when the request is queued, and again whenever its (1-based)
   * position in the queue changes.
   */
  onQueued: (position: number) => void;

  /** Called (once) when the request is granted. */
  onGranted: () => void;
};

export type AdmissionTicket = {
  /**
   * Must be called when the requesting connection closes. A queued request
   * is removed from the queue. For a granted request, this starts the
   * timer within which the subscriber is expected to reconnect and open a
   * replacement reservation.
   */
  release(): void;
};

type Entry = {
  readonly request: AdmissionRequest;
  state: 'queued' | 'granted' | 'released';
  position: number;
  grant?: Grant | undefined;
};

type Grant = {
  /** Whether the granted subscriber's connection has closed. */
  released: boolean;
  timer: ReturnType<typeof setTimeout> | undefined;
};

/**
 * Limits the number of view-syncers that replace their replica at the same
 * time, so that view-syncers switching over from an incompatible
 * replication-manager (e.g. after a resync) do not all stop serving at once.
 *
 * Capacity is consumed by:
 * * Replacement restores: snapshot reservations of subscribers that are
 *   replacing an (incompatible or outdated) local replica, i.e. subscribers
 *   that were serving. Restores onto an empty disk (e.g. a new pod) do not
 *   take a serving subscriber out of service, and are not counted. The
 *   reservation's connection is held open for the duration of the restore,
 *   and the subscription that follows it is held open until the restored
 *   subscriber has caught up, so their liveness tracks that of the
 *   replacement without a timeout.
 * * Outstanding grants: permission to restore that has not yet been
 *   followed by a replacement reservation (e.g. while the subscriber drains
 *   and restarts). Grants are not matched to reservations by identity:
 *   any replacement reservation consumes an outstanding grant, and a grant
 *   that is not consumed expires a short time after the granted connection
 *   closes (e.g. if the subscriber reconnected to a different
 *   replication-manager or was deleted).
 *
 * ## Life cycle of a switchover restore
 *
 * The following traces a view-syncer (VS) with a replica at version `R1`,
 * served by an old replication-manager (RM) at `R1`, while a new RM (e.g.
 * after a resync) is at `R2 > R1`. The RestoreAdmission is that of the new
 * RM. Terms in `code` are those used in the code.
 *
 * 1. **Drain.** The old RM receives a SIGTERM, and its `DrainTracker` sends
 *    `status {draining: true}` to its `drainable` (i.e. serving) subscribers.
 *    It continues to stream changes to them, and exits once they have all
 *    disconnected.
 *
 * 2. **Switchover.** The VS's `DrainHandler` opens a second `/subscribe`
 *    with `switchover: true` at the watermark of its last committed
 *    transaction, which is routed to some RM. The VS continues to consume
 *    the old RM's stream. If the RM is compatible (i.e. another old RM),
 *    the subscription is accepted and the VS simply moves to it. If the RM
 *    is the new one, the subscriber's replica version is older, and the
 *    subscription proceeds to step 3.
 *
 * 3. **Queued.** `ChangeStreamerImpl.subscribe()` holds the subscription
 *    (it is not added to the forwarder) and calls {@link request}. The
 *    request is `queued` and `onQueued(position)` sends
 *    `status {restore: {state: 'queued', position}}`, again whenever the
 *    position changes. If the held connection closes while queued (e.g.
 *    the VS's old stream ended and it gave up on the switchover), the
 *    ticket is `release()`d and the request leaves the queue.
 *
 * 4. **Granted.** When the number in flight (`replacing` + `granted`) is
 *    below `maxConcurrentRestores`, the head of the queue is `granted`: a
 *    {@link Grant} is added (counting toward capacity), and `onGranted()`
 *    sends `status {restore: {state: 'granted'}}`. The VS keeps the granted
 *    connection open, and initiates a self-drain (`drainRequested`): it
 *    fails its health checks, drains its clients, and keeps replicating
 *    from the old RM in the meantime.
 *
 * 5. **Restart.** The VS process exits, closing the granted connection.
 *    The ticket is `release()`d, marking the grant as `released` and
 *    starting the pending grant TTL ({@link DEFAULT_PENDING_GRANT_TTL_MS}),
 *    within which a replacement reservation is expected. The drain itself
 *    does not count against the TTL.
 *
 * 6. **Reservation opened.** The restarted VS sends
 *    `reserve-snapshot {taskID, localReplica}`, and
 *    `ChangeStreamerImpl.startSnapshotReservation()` calls
 *    {@link reservationOpened}:
 *    * A `localReplica` of a different replica version (e.g. `R1`) is
 *      `replacing`: the reservation is counted, and consumes an outstanding
 *      grant (preferring a `released` one), so the number in flight is
 *      unchanged.
 *    * A `localReplica` of the same replica version is `unclassified` until
 *      the reservation is confirmed ({@link reservationConfirmed}) with the
 *      backup's minimum watermark. It is `replacing` if its watermark is
 *      older (i.e. it cannot catch up from the change log).
 *    * No `localReplica` (an empty disk) is not counted.
 *
 *    If the VS reached a different RM instead, the grant here expires after
 *    the TTL and frees its capacity, while the replacement is counted (as
 *    an ungranted one) by the RM that it reached.
 *
 * 7. **Restoring.** The VS restores the backup while holding the
 *    reservation's connection open. A retried reservation from the same
 *    task that arrives while the previous one is still open replaces it
 *    (`SnapshotReservations` `onClose`) but does not end it (`onEnd`), so it
 *    remains `replacing`. If the reservation instead closes without
 *    subscribing (e.g. a failed or disconnected restore, after which the VS
 *    retries), {@link reservationClosed} converts its capacity into a
 *    `released` grant, which the retry's replacement reservation consumes
 *    (step 6), or which expires after the pending grant TTL (e.g. if the VS
 *    was deleted).
 *
 * 8. **Subscribed.** Once restored, the VS subscribes on the same
 *    connection at `R2`, which is accepted. `ChangeStreamerImpl.subscribe()`
 *    calls {@link reservationSubscribed}, which moves the replacement from
 *    its reservation (the key leaves `replacing`) to the subscription
 *    (`catchingUp`), and then closes the task's reservation. Its `onEnd`
 *    calls {@link reservationClosed}, which is a no-op for the moved key.
 *
 * 9. **Caught up.** The VS catches up on the change stream from the
 *    backup's watermark. Once it has acked all changes up to the head of
 *    the stream (`Subscriber.caughtUp`), or if it disconnects first, the
 *    replacement is released and the next queued request is granted.
 *
 * A VS that restarts outside of a switchover with an incompatible replica
 * (e.g. after a crash) enters at step 6 without a grant, and its restore
 * consumes capacity all the same, delaying queued requests.
 *
 * ## States
 *
 * | Term          | Where                    | Meaning                          |
 * | ------------- | ------------------------ | -------------------------------- |
 * | queued        | `Entry.state`            | Held, waiting for capacity       |
 * | granted       | `Entry.state`, `#grants` | Permitted to restore, pending a  |
 * |               |                          | replacement reservation          |
 * | released      | `Entry.state`            | The held connection closed       |
 * | released      | `Grant.released`         | The granted connection, or an    |
 * |               |                          | unsubscribed replacement         |
 * |               |                          | reservation, closed; the TTL is  |
 * |               |                          | running                          |
 * | unclassified  | `#unclassified`          | Same-version reservation, not    |
 * |               |                          | yet confirmed                    |
 * | replacing     | `#replacing`             | Open replacement reservation     |
 * | catching up   | `#catchingUp`            | Subscribed replacement, not yet  |
 * |               |                          | caught up                        |
 * | in flight     | `#inFlight`              | `replacing` + `catchingUp` +     |
 * |               |                          | `granted`, bounded by            |
 * |               |                          | `maxConcurrentRestores`          |
 */
export class RestoreAdmission {
  readonly #lc: LogContext;
  readonly #maxConcurrentRestores: number;
  readonly #pendingGrantTTLMs: number;
  readonly #setTimeout: typeof setTimeout;
  readonly #clearTimeout: typeof clearTimeout;

  readonly #replicaVersion: string;
  /** Keys (task IDs) of open replacement reservations. */
  readonly #replacing = new Set<string>();
  /** Replacements whose subscribers have subscribed but not caught up. */
  readonly #catchingUp = new Set<object>();
  /**
   * The local replicas of open reservations with this replica version, which
   * are classified upon confirmation of the reservation's minimum watermark.
   */
  readonly #unclassified = new Map<string, LocalReplica>();
  readonly #grants = new Set<Grant>();
  readonly #queue: Entry[] = [];

  constructor(lc: LogContext, opts: RestoreAdmissionOptions) {
    this.#lc = lc.withContext('component', 'restore-admission');
    this.#replicaVersion = opts.replicaVersion;
    this.#maxConcurrentRestores = Math.max(1, opts.maxConcurrentRestores);
    this.#pendingGrantTTLMs =
      opts.pendingGrantTTLMs ?? DEFAULT_PENDING_GRANT_TTL_MS;
    this.#setTimeout = opts.setTimeoutFn ?? setTimeout;
    this.#clearTimeout = opts.clearTimeoutFn ?? clearTimeout;

    getOrCreateGauge(
      'replication',
      'restore_admission.queued',
      'Switchover subscribers waiting for permission to restore a new replica.',
    ).addCallback(result => result.observe(this.#queue.length));
    getOrCreateGauge(
      'replication',
      'restore_admission.in_flight',
      'Subscribers replacing, or granted permission to replace, their replica.',
    ).addCallback(result => result.observe(this.#inFlight));
  }

  get #inFlight() {
    return this.#replacing.size + this.#catchingUp.size + this.#grants.size;
  }

  /** For testing and logging. */
  get stats() {
    return {
      replacing: this.#replacing.size + this.#catchingUp.size,
      granted: this.#grants.size,
      queued: this.#queue.length,
    };
  }

  /**
   * Called when a snapshot reservation is opened by a subscriber with the
   * given `localReplica` (if any). A subscriber with a local replica of a
   * different replica version is replacing it. One with the same replica
   * version is replacing it if it is outdated, which is determined by
   * {@link reservationConfirmed}. Idempotent for the same `key` (e.g. a
   * retried reservation).
   */
  reservationOpened(key: string, localReplica: LocalReplica | undefined) {
    this.#unclassified.delete(key);
    if (!localReplica) {
      return; // Restoring onto an empty disk.
    }
    if (localReplica.replicaVersion !== this.#replicaVersion) {
      this.#replacementOpened(key);
    } else {
      this.#unclassified.set(key, localReplica);
    }
  }

  /**
   * Called when a reservation is confirmed with the minimum watermark of the
   * replica to restore.
   */
  reservationConfirmed(key: string, minWatermark: string) {
    const localReplica = this.#unclassified.get(key);
    this.#unclassified.delete(key);
    if (localReplica && localReplica.watermark < minWatermark) {
      this.#replacementOpened(key); // The local replica is outdated.
    }
  }

  /**
   * Called when a reservation ends. A replacement reservation normally ends
   * after its task subscribes ({@link reservationSubscribed}), in which case
   * this is a no-op. A replacement reservation that ends without subscribing
   * (e.g. a failed or disconnected restore) is most likely followed by a
   * retry, so its capacity is retained as a released grant (which the retry
   * consumes) rather than released immediately.
   */
  reservationClosed(key: string) {
    this.#unclassified.delete(key);
    if (this.#replacing.delete(key)) {
      const grant: Grant = {released: true, timer: undefined};
      this.#grants.add(grant);
      this.#startGrantTTL(grant);
    }
  }

  /**
   * Called when the task of a reservation subscribes (before the reservation
   * is closed). If the reservation is a replacement, it continues to be
   * counted until the returned function is called, i.e. when the subscriber
   * has caught up or disconnected.
   */
  reservationSubscribed(key: string): (() => void) | undefined {
    this.#unclassified.delete(key);
    if (!this.#replacing.delete(key)) {
      return undefined;
    }
    const replacement = {};
    this.#catchingUp.add(replacement);
    return () => {
      if (this.#catchingUp.delete(replacement)) {
        this.#processQueue();
      }
    };
  }

  #replacementOpened(key: string) {
    if (this.#replacing.has(key)) {
      return;
    }
    this.#replacing.add(key);
    // The restore is now tracked by its reservation. Prefer consuming a grant
    // whose subscriber has already disconnected, as it is the most likely
    // origin of the reservation.
    const grants = [...this.#grants];
    const grant = grants.find(g => g.released) ?? grants[0];
    if (grant) {
      this.#deleteGrant(grant);
    }
  }

  request(request: AdmissionRequest): AdmissionTicket {
    const entry: Entry = {request, state: 'queued', position: 0};
    this.#queue.push(entry);
    this.#processQueue();
    return {release: () => this.#release(entry)};
  }

  #release(entry: Entry) {
    const {state, grant} = entry;
    entry.state = 'released';
    if (state === 'queued') {
      const i = this.#queue.indexOf(entry);
      if (i >= 0) {
        this.#queue.splice(i, 1);
      }
      this.#processQueue();
    } else if (state === 'granted' && grant && this.#grants.has(grant)) {
      // Wait for the subscriber to reconnect with a replacement reservation.
      grant.released = true;
      this.#startGrantTTL(grant);
    }
  }

  #startGrantTTL(grant: Grant) {
    grant.timer = this.#setTimeout(() => {
      if (this.#grants.has(grant)) {
        this.#lc.info?.(`grant expired without a replacement reservation`);
        this.#deleteGrant(grant);
        this.#processQueue();
      }
    }, this.#pendingGrantTTLMs);
  }

  #deleteGrant(grant: Grant) {
    if (grant.timer) {
      this.#clearTimeout(grant.timer);
    }
    this.#grants.delete(grant);
  }

  #processQueue() {
    while (this.#inFlight < this.#maxConcurrentRestores && this.#queue.length) {
      const entry = this.#queue.shift()!;
      entry.state = 'granted';
      entry.grant = {released: false, timer: undefined};
      this.#grants.add(entry.grant);
      this.#lc.info?.(`granting permission to restore`, {...this.stats});
      entry.request.onGranted();
    }
    this.#queue.forEach((entry, i) => {
      const position = i + 1;
      if (entry.position !== position) {
        entry.position = position;
        entry.request.onQueued(position);
      }
    });
  }
}
