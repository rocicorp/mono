import type {LogContext} from '@rocicorp/logger';
import {Database} from '../../../../zqlite/src/db.ts';
import {
  CHANGE_LOG_DB_SCHEMA_VERSION,
  readChangeLogBounds,
  readChangeLogMeta,
  type ChangeLogIdentity,
} from '../replicator/change-log-db.ts';

export type SQLiteChangeLogCoverage = {
  readonly seededAtMs: number;
  readonly seedWatermark: string;
  readonly minWatermark: string;
  readonly headWatermark: string;
};

export type ChangeLogReadRoute =
  | {
      readonly available: true;
      // 'selected-cold' is a log still inside its warm-up window, i.e. one
      // seeded less than a retention window ago. It is kept distinct from
      // 'selected' so that bootstrap serving is separable in metrics from
      // steady-state serving.
      readonly reason: 'selected' | 'selected-cold';
      readonly coverage: SQLiteChangeLogCoverage;
      readonly pinned?: boolean | undefined;
    }
  | {
      readonly available: false;
      readonly reason: 'log-unavailable' | 'breaker-open';
      readonly pinned?: boolean | undefined;
    };

export type SQLiteChangeLogReadRouterOptions = {
  /** The warm-up window that a reseeded log ages through. */
  readonly retentionMs: number;
  readonly inspect: () => SQLiteChangeLogCoverage | undefined;
  readonly failureCooldownMs?: number | undefined;
  readonly now?: (() => number) | undefined;
};

const DEFAULT_FAILURE_COOLDOWN_MS = 30_000;

/**
 * Determines, and pins, whether the SQLite change log can serve the catchup
 * shared by `/snapshot` and `/changes`. Pinning keeps the bounds a snapshot
 * reservation advertises in agreement with the coverage that its task's
 * subsequent `/changes` request is served from.
 *
 * A post-registration SQLite failure opens a process-local breaker. Requests
 * are declined during the cooldown; the first request after it expires is the
 * bounded probe. A successful readiness inspection closes the breaker, while
 * another unavailable result rearms it. A writer failure opens it permanently
 * because the writer remains disabled and its file stays absent for the life
 * of the process.
 */
export class SQLiteChangeLogReadRouter {
  readonly #retentionMs: number;
  readonly #inspect: () => SQLiteChangeLogCoverage | undefined;
  readonly #failureCooldownMs: number;
  readonly #now: () => number;
  readonly #pins = new Map<string, ChangeLogReadRoute>();

  #breakerUntilMs: number | undefined;
  #permanentlyDisabled = false;

  constructor(opts: SQLiteChangeLogReadRouterOptions) {
    this.#retentionMs = opts.retentionMs;
    this.#inspect = opts.inspect;
    this.#failureCooldownMs =
      opts.failureCooldownMs ?? DEFAULT_FAILURE_COOLDOWN_MS;
    this.#now = opts.now ?? Date.now;
  }

  /** Determines and pins the route for a snapshot reservation. */
  pin(taskID: string): ChangeLogReadRoute {
    const route = this.#choose();
    this.#pins.set(taskID, route);
    return route;
  }

  /** Returns the route pinned by an active snapshot reservation. */
  peek(taskID: string): ChangeLogReadRoute | undefined {
    const route = this.#pins.get(taskID);
    return route && this.#asPinned(route);
  }

  /** Consumes a snapshot pin, or determines the route for a direct retry. */
  consume(taskID: string): ChangeLogReadRoute {
    const route = this.#pins.get(taskID);
    if (route === undefined) {
      return this.#choose();
    }
    this.#pins.delete(taskID);
    return this.#asPinned(route);
  }

  /** Releases a pin whose snapshot reservation ended before subscribing. */
  release(taskID: string): void {
    this.#pins.delete(taskID);
  }

  /** Opens the post-registration failure breaker. */
  trip(permanent = false): void {
    if (permanent) {
      this.#permanentlyDisabled = true;
      this.#breakerUntilMs = undefined;
      return;
    }
    if (!this.#permanentlyDisabled) {
      this.#breakerUntilMs = this.#now() + this.#failureCooldownMs;
    }
  }

  #choose(): ChangeLogReadRoute {
    const now = this.#now();
    if (
      this.#permanentlyDisabled ||
      (this.#breakerUntilMs !== undefined && now < this.#breakerUntilMs)
    ) {
      return {available: false, reason: 'breaker-open'};
    }

    let coverage: SQLiteChangeLogCoverage | undefined;
    try {
      coverage = this.#inspect();
    } catch {
      coverage = undefined;
    }
    if (coverage === undefined) {
      // This was a breaker probe rather than ordinary startup unavailability.
      // Rearm it so a busy retry loop does not turn into an open/read loop.
      if (this.#breakerUntilMs !== undefined) {
        this.#breakerUntilMs = now + this.#failureCooldownMs;
      }
      return {available: false, reason: 'log-unavailable'};
    }
    // A successful probe restores eligibility.
    this.#breakerUntilMs = undefined;
    const cold = now - coverage.seededAtMs < this.#retentionMs;
    return {
      available: true,
      reason: cold ? 'selected-cold' : 'selected',
      coverage,
    };
  }

  #asPinned(route: ChangeLogReadRoute): ChangeLogReadRoute {
    return {...route, pinned: true};
  }
}

/**
 * Reads the coverage and validates the v2 schema and RMv2 identity in one
 * short-lived readonly connection. It never creates the log.
 */
export function inspectSQLiteChangeLog(
  lc: LogContext,
  changeLogFile: string,
  identity: ChangeLogIdentity,
): SQLiteChangeLogCoverage | undefined {
  let db: Database | undefined;
  try {
    db = new Database(
      lc.withContext('component', 'sqlite-change-log-read-router'),
      changeLogFile,
      {readonly: true},
    );
    const meta = readChangeLogMeta(db);
    if (
      meta.schemaVersion !== CHANGE_LOG_DB_SCHEMA_VERSION ||
      meta.epoch !== identity.epoch ||
      meta.generation !== identity.generation ||
      meta.replicaID !== identity.replicaID
    ) {
      return undefined;
    }
    const bounds = readChangeLogBounds(db);
    if (bounds.minWatermark === null || bounds.headWatermark === null) {
      return undefined;
    }
    return {
      seededAtMs: meta.seededAtMs,
      seedWatermark: meta.seedWatermark,
      minWatermark: bounds.minWatermark,
      headWatermark: bounds.headWatermark,
    };
  } catch {
    return undefined;
  } finally {
    db?.close();
  }
}
