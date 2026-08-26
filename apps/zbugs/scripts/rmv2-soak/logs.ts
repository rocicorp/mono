/**
 * The soak's view of a zero-cache JSON log stream.
 *
 * `ZERO_LOG_FORMAT=json` writes one object per line: `{level, ...context,
 * ...lastArgObject, message}`. The structured payloads the change log already
 * attaches -- `sqliteChangeLogReconcile`, `sqliteChangeLogPurge`,
 * `sqliteChangeLogCoverage`, `backedUp` -- carry most of what this harness
 * needs; the rest is matched off the message text.
 *
 * Counters and distributions come from OTel (see `otlp.ts`). Logs are for
 * *events with a time*: when the log reseeded, when a reservation opened and
 * when it was confirmed, which task was demoted.
 */

export type LogRecord = {
  readonly node: string;
  readonly tsMs: number;
  readonly level: string;
  readonly message: string;
  readonly fields: Readonly<Record<string, unknown>>;
  readonly raw: string;
};

export type SoakEventKind =
  | 'change-log-startup'
  | 'change-log-reconcile'
  | 'change-log-reseed'
  | 'purge-pass'
  | 'floor-probe-violation'
  | 'compare-mismatch'
  | 'reservation-opened'
  | 'reservation-confirmed'
  | 'reservation-delayed'
  | 'reservation-demoted'
  | 'backup-watermark'
  | 'served-from-sqlite'
  | 'served-from-pg'
  | 'subscriber-rejected'
  | 'restore-started'
  | 'restore-finished'
  // A view-syncer discarded its own replica because the snapshot
  // reservation's `minWatermark` was above it, and restored instead.
  | 'replica-discarded'
  | 'barrier-timeout'
  | 'registration-failed';

export type SoakEvent = {
  readonly kind: SoakEventKind;
  readonly node: string;
  readonly tsMs: number;
  readonly detail: Readonly<Record<string, unknown>>;
  readonly record: LogRecord;
};

export type Tripwire = {
  readonly name: string;
  readonly node: string;
  readonly tsMs: number;
  readonly message: string;
  readonly detail: Readonly<Record<string, unknown>>;
};

type Waiter<T> = {
  predicate: (value: T) => boolean;
  resolve: (value: T) => void;
  timer: NodeJS.Timeout;
};

const DEMOTION =
  /^demoting (\S+) to PG catchup: SQLite change-log minimum (\S+) is later than backupWatermark (\S+)/;
const RESERVATION_OPENED = /^created snasphot reservation for (\S+)$/;
const RESERVATION_CONFIRMED =
  /^reserving change-log entries since (\S+) for (\S+)$/;
const RESERVATION_DELAYED =
  /^pg change-log minWatermark (\S+) is later than backupWatermark (\S+)/;
const SERVED_FROM_SQLITE = /^serving (\S+) from SQLite catchup$/;
const SERVED_FROM_PG = /^serving (\S+) from PG catchup: (.*)$/;
const SQLITE_ROUTE = /^SQLite route (\S+)/;

/**
 * The reasons `serving ... from PG catchup` is emitted with, mapped back onto
 * `ChangeLogReadRouteReason`. The route counter in OTel is the census of
 * record; this is what gives each one a timestamp and a subscriber.
 */
function pgRouteReason(rest: string): string {
  if (rest.startsWith('SQLite change log is still warming')) {
    return 'cold-log';
  }
  if (rest.includes('is below the SQLite change-log minimum')) {
    return 'watermark-uncovered';
  }
  if (rest.startsWith('SQLite catchup registration failed')) {
    return 'registration-failed';
  }
  const routed = SQLITE_ROUTE.exec(rest);
  return routed ? routed[1] : 'unknown';
}

export class SoakLog {
  readonly events: SoakEvent[] = [];
  readonly tripwires: Tripwire[] = [];
  readonly #waiters = new Set<Waiter<SoakEvent>>();
  readonly #recordWaiters = new Set<Waiter<LogRecord>>();
  readonly #listeners = new Set<(event: SoakEvent) => void>();
  #onTripwire: ((t: Tripwire) => void) | undefined;

  onTripwire(handler: (t: Tripwire) => void): void {
    this.#onTripwire = handler;
  }

  onEvent(listener: (event: SoakEvent) => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  /** Feeds one raw stdout/stderr line from `node` into the stream. */
  push(node: string, line: string): void {
    const trimmed = line.trim();
    if (trimmed.length === 0) {
      return;
    }
    let parsed: Record<string, unknown>;
    if (trimmed.startsWith('{')) {
      try {
        parsed = JSON.parse(trimmed) as Record<string, unknown>;
      } catch {
        return;
      }
    } else {
      // litestream's own stdout, node warnings, etc.
      return;
    }
    const {level, message, ...fields} = parsed;
    const record: LogRecord = {
      node,
      tsMs: Date.now(),
      level: typeof level === 'string' ? level : 'INFO',
      message: typeof message === 'string' ? message : '',
      fields,
      raw: trimmed,
    };
    // Iterating the set directly is safe: `resolve` deletes only the waiter
    // currently being visited, which is well defined for Set iteration.
    for (const waiter of this.#recordWaiters) {
      if (waiter.predicate(record)) {
        waiter.resolve(record);
      }
    }
    this.#classify(record);
  }

  since(tsMs: number, kind?: SoakEventKind): SoakEvent[] {
    return this.events.filter(
      e => e.tsMs >= tsMs && (kind === undefined || e.kind === kind),
    );
  }

  /**
   * Resolves with the first matching event, including events that already
   * arrived at or after `sinceMs`.
   */
  waitFor(
    description: string,
    predicate: (event: SoakEvent) => boolean,
    timeoutMs: number,
    sinceMs = 0,
  ): Promise<SoakEvent> {
    const already = this.events.find(e => e.tsMs >= sinceMs && predicate(e));
    if (already) {
      return Promise.resolve(already);
    }
    return new Promise<SoakEvent>((resolve, reject) => {
      const waiter: Waiter<SoakEvent> = {
        predicate,
        resolve: event => {
          clearTimeout(waiter.timer);
          this.#waiters.delete(waiter);
          resolve(event);
        },
        timer: setTimeout(() => {
          this.#waiters.delete(waiter);
          reject(
            new Error(
              `timed out after ${timeoutMs}ms waiting for ${description}`,
            ),
          );
        }, timeoutMs),
      };
      this.#waiters.add(waiter);
    });
  }

  /**
   * Resolves with the first raw log line matching `predicate`. Readiness and
   * shutdown are announced in prose rather than in a structured payload, so
   * they are matched here rather than promoted to {@link SoakEvent}s.
   */
  waitForRecord(
    description: string,
    predicate: (record: LogRecord) => boolean,
    timeoutMs: number,
  ): Promise<LogRecord> {
    return new Promise<LogRecord>((resolve, reject) => {
      const waiter: Waiter<LogRecord> = {
        predicate,
        resolve: record => {
          clearTimeout(waiter.timer);
          this.#recordWaiters.delete(waiter);
          resolve(record);
        },
        timer: setTimeout(() => {
          this.#recordWaiters.delete(waiter);
          reject(
            new Error(
              `timed out after ${timeoutMs}ms waiting for ${description}`,
            ),
          );
        }, timeoutMs),
      };
      this.#recordWaiters.add(waiter);
    });
  }

  #emit(
    kind: SoakEventKind,
    record: LogRecord,
    detail: Record<string, unknown>,
  ): void {
    const event: SoakEvent = {
      kind,
      node: record.node,
      tsMs: record.tsMs,
      detail,
      record,
    };
    this.events.push(event);
    for (const listener of this.#listeners) {
      listener(event);
    }
    // See the note in `push`: each `resolve` deletes only its own waiter.
    for (const waiter of this.#waiters) {
      if (waiter.predicate(event)) {
        waiter.resolve(event);
      }
    }
  }

  #trip(
    name: string,
    record: LogRecord,
    detail: Record<string, unknown> = {},
  ): void {
    const tripwire: Tripwire = {
      name,
      node: record.node,
      tsMs: record.tsMs,
      message: record.message,
      detail,
    };
    this.tripwires.push(tripwire);
    this.#onTripwire?.(tripwire);
  }

  #classify(record: LogRecord): void {
    const {message, fields} = record;

    // Matched on the message, not just the field: `reconcileChangeLog` also
    // attaches `sqliteChangeLogReconcile` to its "at the resume watermark"
    // trace, which carries a head but no action.
    const reconcile =
      message === 'SQLite change-log reconciliation'
        ? (fields.sqliteChangeLogReconcile as
            | {action: string; head: string; reason?: string; rows?: number}
            | undefined)
        : undefined;
    if (reconcile) {
      this.#emit('change-log-reconcile', record, {...reconcile});
      if (reconcile.action === 'reseeded') {
        this.#emit('change-log-reseed', record, {...reconcile});
      }
      return;
    }

    if (message === 'SQLite change-log startup') {
      this.#emit('change-log-startup', record, {
        ...(fields.sqliteChangeLog as Record<string, unknown>),
      });
      return;
    }

    const purge = fields.sqliteChangeLogPurge as
      | Record<string, unknown>
      | undefined;
    if (purge) {
      this.#emit('purge-pass', record, {...purge});
      return;
    }

    const floorProbe = fields.sqliteChangeLogFloorProbe as
      | Record<string, unknown>
      | undefined;
    if (floorProbe) {
      this.#emit('floor-probe-violation', record, {...floorProbe});
      this.#trip('purge-floor-violation', record, {...floorProbe});
      return;
    }

    if (message === 'SQLite change-log catchup output diverged from Postgres') {
      const detail = (fields.sqliteChangeLogCompare ?? {}) as Record<
        string,
        unknown
      >;
      this.#emit('compare-mismatch', record, {...detail});
      this.#trip('compare-mismatch', record, {...detail});
      return;
    }

    if (message === 'received backup watermark') {
      const backedUp = (fields.backedUp ?? {}) as {
        watermark?: string;
        writeTimeMs?: number;
        backupTimeMs?: number;
      };
      this.#emit('backup-watermark', record, {
        ...backedUp,
        lagMs:
          backedUp.backupTimeMs !== undefined &&
          backedUp.writeTimeMs !== undefined
            ? backedUp.backupTimeMs - backedUp.writeTimeMs
            : undefined,
      });
      return;
    }

    let m = DEMOTION.exec(message);
    if (m) {
      this.#emit('reservation-demoted', record, {
        taskID: m[1],
        minWatermark: m[2],
        backupWatermark: m[3],
      });
      return;
    }

    m = RESERVATION_OPENED.exec(message);
    if (m) {
      this.#emit('reservation-opened', record, {taskID: m[1]});
      return;
    }

    m = RESERVATION_CONFIRMED.exec(message);
    if (m) {
      this.#emit('reservation-confirmed', record, {
        minWatermark: m[1],
        taskID: m[2],
      });
      return;
    }

    m = RESERVATION_DELAYED.exec(message);
    if (m) {
      this.#emit('reservation-delayed', record, {
        minWatermark: m[1],
        backupWatermark: m[2],
      });
      return;
    }

    m = SERVED_FROM_SQLITE.exec(message);
    if (m) {
      this.#emit('served-from-sqlite', record, {
        subscriber: m[1],
        coverage: fields.sqliteChangeLogCoverage,
      });
      return;
    }

    m = SERVED_FROM_PG.exec(message);
    if (m) {
      const reason = pgRouteReason(m[2]);
      this.#emit('served-from-pg', record, {
        subscriber: m[1],
        reason,
        coverage: fields.sqliteChangeLogCoverage,
      });
      if (reason === 'registration-failed') {
        this.#emit('registration-failed', record, {subscriber: m[1]});
        this.#trip('sqlite-registration-failed', record, {subscriber: m[1]});
      }
      return;
    }

    if (message.startsWith('rejecting subscriber at replica version')) {
      this.#emit('subscriber-rejected', record, {});
      this.#trip('wrong-replica-version', record, {});
      return;
    }

    if (message.startsWith('Deleting local replica and retrying restore')) {
      this.#emit('replica-discarded', record, {});
      return;
    }

    if (message.startsWith('starting litestream restore')) {
      this.#emit('restore-started', record, {...fields});
      return;
    }
    if (
      message.startsWith('litestream restore complete') ||
      message.startsWith('finished litestream restore')
    ) {
      this.#emit('restore-finished', record, {...fields});
      return;
    }

    if (
      message.includes('WatermarkTooOld') ||
      message.includes('WrongReplicaVersion')
    ) {
      this.#trip('watermark-too-old', record, {});
      return;
    }

    if (record.level === 'ERROR') {
      // Everything else at ERROR is worth surfacing but is not a named
      // tripwire; the report lists them separately.
      this.#trip('error-log', record, {
        errorMsg: fields.errorMsg,
        name: fields.name,
      });
    }
  }
}
