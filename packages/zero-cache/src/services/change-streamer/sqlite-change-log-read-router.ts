import type {LogContext} from '@rocicorp/logger';
import {Database} from '../../../../zqlite/src/db.ts';
import type {ShardID} from '../../types/shards.ts';
import {
  CHANGE_LOG_DB_SCHEMA_VERSION,
  readChangeLogBounds,
  readChangeLogMeta,
  type ChangeLogIdentity,
} from '../replicator/change-log-db.ts';
import {isSampledForShard} from './shard-sampling.ts';

export type ChangeLogReadSource = 'pg' | 'sqlite';

export type SQLiteChangeLogCoverage = {
  readonly seededAtMs: number;
  readonly seedWatermark: string;
  readonly minWatermark: string;
  readonly headWatermark: string;
};

export type ChangeLogReadRouteReason =
  | 'selected'
  // Selected while the log was still inside its warm-up window. Kept
  // distinct from 'selected' so bootstrap serving is separable in metrics
  // from steady-state serving.
  | 'selected-cold'
  | 'percentage'
  | 'cold-log'
  | 'log-unavailable'
  | 'breaker-open'
  // Pinned to SQLite, then demoted because the log could not cover the
  // backup the reservation is restoring from. See {@link
  // SQLiteChangeLogReadRouter.demote}.
  | 'backup-uncovered';

export type ChangeLogReadRoute = {
  readonly source: ChangeLogReadSource;
  readonly reason: ChangeLogReadRouteReason;
  readonly pinned?: boolean | undefined;
  readonly coverage?: SQLiteChangeLogCoverage | undefined;
};

export type SQLiteChangeLogReadRouterOptions = {
  readonly shard: ShardID;
  readonly readPercent: number;
  /**
   * Percentage of eligible tasks served from a log that has not yet aged
   * through {@link retentionMs}. Defaults to zero, which keeps every such
   * task on PG.
   */
  readonly coldReadPercent?: number | undefined;
  readonly retentionMs: number;
  readonly inspect: () => SQLiteChangeLogCoverage | undefined;
  readonly failureCooldownMs?: number | undefined;
  readonly now?: (() => number) | undefined;
};

const DEFAULT_FAILURE_COOLDOWN_MS = 30_000;

/**
 * Selects and pins the catchup source shared by `/snapshot` and `/changes`.
 *
 * A post-registration SQLite failure opens a process-local breaker. Requests
 * use PG during the cooldown; the first request after it expires is the
 * bounded probe. A successful readiness inspection closes the breaker, while
 * another unavailable result rearms it. A writer failure opens it permanently
 * because the writer remains disabled and its file stays absent for the life
 * of the process.
 */
export class SQLiteChangeLogReadRouter {
  readonly #shard: ShardID;
  readonly #readPercent: number;
  readonly #coldReadPercent: number;
  readonly #retentionMs: number;
  readonly #inspect: () => SQLiteChangeLogCoverage | undefined;
  readonly #failureCooldownMs: number;
  readonly #now: () => number;
  readonly #pins = new Map<string, ChangeLogReadRoute>();

  #breakerUntilMs: number | undefined;
  #permanentlyDisabled = false;

  constructor(opts: SQLiteChangeLogReadRouterOptions) {
    this.#shard = opts.shard;
    this.#readPercent = opts.readPercent;
    this.#coldReadPercent = opts.coldReadPercent ?? 0;
    this.#retentionMs = opts.retentionMs;
    this.#inspect = opts.inspect;
    this.#failureCooldownMs =
      opts.failureCooldownMs ?? DEFAULT_FAILURE_COOLDOWN_MS;
    this.#now = opts.now ?? Date.now;
  }

  /** Chooses and pins a source for a snapshot reservation. */
  pin(taskID: string): ChangeLogReadRoute {
    const route = this.#choose(taskID);
    this.#pins.set(taskID, route);
    return route;
  }

  /** Returns the source pinned by an active snapshot reservation. */
  peek(taskID: string): ChangeLogReadRoute | undefined {
    const route = this.#pins.get(taskID);
    return route && this.#asPinned(route);
  }

  /** Consumes a snapshot pin, or makes the stable choice for a direct retry. */
  consume(taskID: string): ChangeLogReadRoute {
    const route = this.#pins.get(taskID);
    if (route === undefined) {
      return this.#choose(taskID);
    }
    this.#pins.delete(taskID);
    return this.#asPinned(route);
  }

  /** Releases a pin whose snapshot reservation ended before subscribing. */
  release(taskID: string): void {
    this.#pins.delete(taskID);
  }

  /**
   * Replaces a pinned SQLite route with PG, for a reservation whose backup
   * the log cannot cover -- most often a log seeded after that backup.
   *
   * The pin moves, not just the reservation's advertised bounds. Confirming
   * with PG bounds while the task's `/changes` request still resolves to
   * SQLite is exactly the mismatch the reservation path exists to prevent,
   * so the two have to change together.
   *
   * A task with no pin is left alone: `consume` makes it a fresh choice.
   */
  demote(taskID: string): ChangeLogReadRoute {
    const route: ChangeLogReadRoute = {
      source: 'pg',
      reason: 'backup-uncovered',
    };
    if (this.#pins.has(taskID)) {
      this.#pins.set(taskID, route);
    }
    return route;
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

  #choose(taskID: string): ChangeLogReadRoute {
    const now = this.#now();
    if (
      this.#permanentlyDisabled ||
      (this.#breakerUntilMs !== undefined && now < this.#breakerUntilMs)
    ) {
      return {source: 'pg', reason: 'breaker-open'};
    }

    // Inspect eligibility even at readPercent=0. Slice 11 starts its rollout
    // at zero specifically so warm/cold/unavailable rates are visible before
    // any subscriber is routed to SQLite.
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
      return {source: 'pg', reason: 'log-unavailable'};
    }
    // A successful probe restores eligibility. This happens before the gates
    // below so that declining a task -- for a cold log or for the percentage
    // -- does not leave the cooldown armed against the next ordinary
    // unavailability.
    this.#breakerUntilMs = undefined;
    // A log seeded less than a retention window ago holds less history than
    // the purger promises to keep, so it is the case that most often cannot
    // cover a follower. It is also the only path that has no PG to fall back
    // to once PG is retired, which is the argument for exercising it now
    // rather than meeting it at the cutover: `coldReadPercent` is that dial.
    const cold = now - coverage.seededAtMs < this.#retentionMs;
    if (
      cold &&
      !isSampledForShard(this.#shard, taskID, this.#coldReadPercent)
    ) {
      return {source: 'pg', reason: 'cold-log', coverage};
    }

    // Keeping the percentage decision after readiness makes zero-percent
    // rollouts emit eligibility metrics without serving reads.
    if (!isSampledForShard(this.#shard, taskID, this.#readPercent)) {
      return {source: 'pg', reason: 'percentage', coverage};
    }
    return {
      source: 'sqlite',
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
