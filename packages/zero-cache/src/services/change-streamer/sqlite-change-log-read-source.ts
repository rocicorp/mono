import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import {h32} from '../../../../shared/src/hash.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import type {ShardID} from '../../types/shards.ts';
import {CHANGE_LOG_STREAM_TABLE} from '../replicator/schema/change-log-stream.ts';
import type {SubscriberContext} from './change-streamer.ts';
import {SQLITE_CHANGE_LOG_SCHEMA_VERSION} from './sqlite-change-log-comparator.ts';
import {
  SQLiteChangeLogReader,
  type CatchupPlan,
} from './sqlite-change-log-reader.ts';

export type ChangeLogReadSource = 'pg' | 'sqlite';

export type ChangeLogState = {
  readonly replicaVersion: string;
  readonly minWatermark: string;
};

export type SQLiteChangeLogReadState = {
  readonly schemaVersion: number;
  readonly replicaVersion: string;
  readonly stateWatermark: string;
  readonly minWatermark: string | null;
  readonly headWatermark: string | null;
};

export interface SQLiteChangeLogReadSource {
  inspect(): SQLiteChangeLogReadState;
  plan(fromWatermark: string): CatchupPlan;
  close(): void;
}

export type SQLiteChangeLogReadRequestClassification =
  | 'snapshot'
  | 'reserved'
  | 'unreserved';

export type SQLiteChangeLogReadDecisionReason =
  | 'eligible'
  | 'reserved'
  | 'not-sampled'
  | 'circuit-open'
  | 'schema-version'
  | 'replica-version'
  | 'warming-up'
  | 'head-mismatch'
  | 'range-unavailable'
  | 'reader-error';

export type SQLiteChangeLogReadDecision = {
  readonly source: ChangeLogReadSource;
  readonly classification: SQLiteChangeLogReadRequestClassification;
  readonly reason: SQLiteChangeLogReadDecisionReason;
  readonly pinned: boolean;
};

export type SQLiteChangeLogReadSourceRouterOptions = {
  readonly replicaFile: string;
  readonly readPercent: number;
  readonly retentionMs: number;
  readonly warmupStartedAtMs?: number | undefined;
  readonly healthProbeIntervalMs?: number | undefined;
  readonly now?: (() => number) | undefined;
  readonly setTimeoutFn?: typeof setTimeout | undefined;
  readonly clearTimeoutFn?: typeof clearTimeout | undefined;
  readonly createSource?: (() => SQLiteChangeLogReadSource) | undefined;
  readonly onDecision?:
    | ((decision: SQLiteChangeLogReadDecision) => void)
    | undefined;
};

type FreshDecision = SQLiteChangeLogReadDecision & {
  readonly state?: SQLiteChangeLogReadState | undefined;
};

type Reservation = {
  readonly source: ChangeLogReadSource;
  readonly state: ChangeLogState;
};

const SAMPLE_BUCKETS = 10_000;
const DEFAULT_HEALTH_PROBE_INTERVAL_MS = 30_000;

/** Stable canary selection for snapshot retries and reconnecting subscribers. */
export function isSQLiteChangeLogReadSelected(
  shard: ShardID,
  replicaVersion: string,
  subscriberIdentity: string,
  readPercent: number,
): boolean {
  assertPercent(readPercent);
  if (readPercent === 0) {
    return false;
  }
  if (readPercent === 100) {
    return true;
  }
  const key =
    `${shard.appID}\0${shard.shardNum}\0${replicaVersion}\0` +
    subscriberIdentity;
  return h32(key) % SAMPLE_BUCKETS < readPercent * 100;
}

/**
 * Pins `/snapshot` and the matching initial `/changes` request to one change
 * log while keeping unreserved reconnects eligible for immediate PG fallback.
 */
export class SQLiteChangeLogReadSourceRouter implements Disposable {
  readonly #lc: LogContext;
  readonly #shard: ShardID;
  readonly #replicaVersion: string;
  readonly #readPercent: number;
  readonly #retentionMs: number;
  readonly #warmupStartedAtMs: number;
  readonly #healthProbeIntervalMs: number;
  readonly #now: () => number;
  readonly #setTimeout: typeof setTimeout;
  readonly #clearTimeout: typeof clearTimeout;
  readonly #createSource: () => SQLiteChangeLogReadSource;
  readonly #onDecision:
    | ((decision: SQLiteChangeLogReadDecision) => void)
    | undefined;
  readonly #requests = getOrCreateCounter(
    'replication',
    'sqlite_change_log.catchup_source',
    'Selected catchup source and pre-registration eligibility result.',
  );
  readonly #reservations = new Map<string, Reservation>();

  #source: SQLiteChangeLogReadSource | undefined;
  #circuitOpen = false;
  #probeTimer: ReturnType<typeof setTimeout> | undefined;
  #closed = false;

  constructor(
    lc: LogContext,
    shard: ShardID,
    replicaVersion: string,
    opts: SQLiteChangeLogReadSourceRouterOptions,
  ) {
    assertPercent(opts.readPercent);
    assertPositiveSafeInteger(opts.retentionMs, 'retention');
    const healthProbeIntervalMs =
      opts.healthProbeIntervalMs ?? DEFAULT_HEALTH_PROBE_INTERVAL_MS;
    assertPositiveSafeInteger(healthProbeIntervalMs, 'health probe interval');

    this.#lc = lc.withContext('component', 'sqlite-change-log-read-source');
    this.#shard = shard;
    this.#replicaVersion = replicaVersion;
    this.#readPercent = opts.readPercent;
    this.#retentionMs = opts.retentionMs;
    this.#now = opts.now ?? Date.now;
    this.#warmupStartedAtMs = opts.warmupStartedAtMs ?? this.#now();
    this.#healthProbeIntervalMs = healthProbeIntervalMs;
    this.#setTimeout = opts.setTimeoutFn ?? setTimeout;
    this.#clearTimeout = opts.clearTimeoutFn ?? clearTimeout;
    this.#createSource =
      opts.createSource ??
      (() => new SQLiteReplicaChangeLogReadSource(this.#lc, opts.replicaFile));
    this.#onDecision = opts.onDecision;
  }

  async reserveSnapshot(
    taskID: string,
    getPGState: () => Promise<ChangeLogState>,
  ): Promise<ChangeLogState> {
    const existing = this.#reservations.get(taskID);
    if (existing) {
      this.#record({
        source: existing.source,
        classification: 'snapshot',
        reason: 'reserved',
        pinned: true,
      });
      return existing.state;
    }

    const selected = this.#selectFresh(taskID, undefined, 'snapshot');
    const state =
      selected.source === 'sqlite'
        ? sqliteSnapshotState(selected.state)
        : await getPGState();
    this.#reservations.set(taskID, {source: selected.source, state});
    return state;
  }

  selectForSubscriber(ctx: SubscriberContext): SQLiteChangeLogReadDecision {
    if (ctx.initial && ctx.taskID) {
      const reservation = this.#reservations.get(ctx.taskID);
      if (reservation) {
        this.#reservations.delete(ctx.taskID);
        return this.#record({
          source: reservation.source,
          classification: 'reserved',
          reason: 'reserved',
          pinned: true,
        });
      }
    }

    return this.#selectFresh(ctx.taskID ?? ctx.id, ctx.watermark, 'unreserved');
  }

  releaseReservation(taskID: string): void {
    this.#reservations.delete(taskID);
  }

  /** Opens the circuit after a selected catchup's barrier or reader fails. */
  reportFailure(error: unknown): void {
    if (this.#closed) {
      return;
    }
    const wasOpen = this.#circuitOpen;
    this.#circuitOpen = true;
    this.#discardSource();
    if (!wasOpen) {
      this.#lc.warn?.(
        'temporarily disabling SQLite change-log catchup after a local failure',
        {errorName: error instanceof Error ? error.name : typeof error},
      );
    }
    this.#scheduleProbe();
  }

  close(): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#reservations.clear();
    if (this.#probeTimer !== undefined) {
      this.#clearTimeout(this.#probeTimer);
      this.#probeTimer = undefined;
    }
    this.#discardSource();
  }

  [Symbol.dispose](): void {
    this.close();
  }

  #selectFresh(
    identity: string,
    fromWatermark: string | undefined,
    classification: 'snapshot' | 'unreserved',
  ): FreshDecision {
    const sampled = isSQLiteChangeLogReadSelected(
      this.#shard,
      this.#replicaVersion,
      identity,
      this.#readPercent,
    );
    // At zero percent, continue evaluating replica-level eligibility so the
    // first rollout step has useful readiness metrics before serving traffic.
    if (!sampled && this.#readPercent !== 0) {
      return this.#record({
        source: 'pg',
        classification,
        reason: 'not-sampled',
        pinned: false,
      });
    }
    if (this.#circuitOpen) {
      return this.#record({
        source: 'pg',
        classification,
        reason: 'circuit-open',
        pinned: false,
      });
    }

    let state: SQLiteChangeLogReadState;
    try {
      state = this.#readSource().inspect();
    } catch (error) {
      this.reportFailure(error);
      return this.#record({
        source: 'pg',
        classification,
        reason: 'reader-error',
        pinned: false,
      });
    }

    const ineligible = this.#ineligibleReason(state);
    if (ineligible !== undefined) {
      if (ineligible === 'head-mismatch') {
        this.reportFailure(new Error('SQLite change-log head mismatch'));
      }
      return this.#record({
        source: 'pg',
        classification,
        reason: ineligible,
        pinned: false,
      });
    }

    if (!sampled) {
      return this.#record({
        source: 'pg',
        classification,
        reason: 'not-sampled',
        pinned: false,
      });
    }

    if (fromWatermark !== undefined) {
      try {
        if (this.#readSource().plan(fromWatermark).kind === 'too-old') {
          return this.#record({
            source: 'pg',
            classification,
            reason: 'range-unavailable',
            pinned: false,
          });
        }
      } catch (error) {
        this.reportFailure(error);
        return this.#record({
          source: 'pg',
          classification,
          reason: 'reader-error',
          pinned: false,
        });
      }
    }

    return this.#record({
      source: 'sqlite',
      classification,
      reason: 'eligible',
      pinned: false,
      state,
    });
  }

  #ineligibleReason(
    state: SQLiteChangeLogReadState,
  ): SQLiteChangeLogReadDecisionReason | undefined {
    if (state.schemaVersion < SQLITE_CHANGE_LOG_SCHEMA_VERSION) {
      return 'schema-version';
    }
    if (state.replicaVersion !== this.#replicaVersion) {
      return 'replica-version';
    }
    if (
      state.minWatermark === null ||
      state.headWatermark === null ||
      state.headWatermark !== state.stateWatermark
    ) {
      return 'head-mismatch';
    }
    if (this.#now() - this.#warmupStartedAtMs < this.#retentionMs) {
      return 'warming-up';
    }
    return undefined;
  }

  #readSource(): SQLiteChangeLogReadSource {
    assert(!this.#closed, 'SQLite change-log read-source router is closed');
    return (this.#source ??= this.#createSource());
  }

  #discardSource(): void {
    this.#source?.close();
    this.#source = undefined;
  }

  #scheduleProbe(): void {
    if (this.#closed || !this.#circuitOpen || this.#probeTimer !== undefined) {
      return;
    }
    this.#probeTimer = this.#setTimeout(() => {
      this.#probeTimer = undefined;
      this.#probe();
    }, this.#healthProbeIntervalMs);
  }

  #probe(): void {
    if (this.#closed || !this.#circuitOpen) {
      return;
    }
    try {
      this.#discardSource();
      const source = this.#readSource();
      const state = source.inspect();
      if (
        this.#ineligibleReason(state) === undefined &&
        state.minWatermark !== null &&
        state.headWatermark !== null &&
        source.plan(state.minWatermark).kind === 'range' &&
        source.plan(state.headWatermark).kind === 'range'
      ) {
        this.#circuitOpen = false;
        this.#lc.info?.('SQLite change-log catchup health probe succeeded');
        return;
      }
    } catch (error) {
      this.#lc.debug?.('SQLite change-log catchup health probe failed', {
        errorName: error instanceof Error ? error.name : typeof error,
      });
      this.#discardSource();
    }
    this.#scheduleProbe();
  }

  #record<T extends FreshDecision>(decision: T): T {
    const {source, classification, reason, pinned} = decision;
    this.#requests.add(1, {source, classification, reason});
    try {
      this.#onDecision?.({source, classification, reason, pinned});
    } catch (error) {
      this.#lc.warn?.('SQLite change-log read-source observer failed', {
        errorName: error instanceof Error ? error.name : typeof error,
      });
    }
    return decision;
  }
}

class SQLiteReplicaChangeLogReadSource implements SQLiteChangeLogReadSource {
  readonly #lc: LogContext;
  readonly #replicaFile: string;
  #db: Database | undefined;
  #reader: SQLiteChangeLogReader | undefined;

  constructor(lc: LogContext, replicaFile: string) {
    this.#lc = lc;
    this.#replicaFile = replicaFile;
  }

  inspect(): SQLiteChangeLogReadState {
    const db = this.#database();
    const version = db
      .prepare(/*sql*/ `
        SELECT "schemaVersion"
          FROM "_zero.versionHistory"
      `)
      .get<{schemaVersion: number} | undefined>();
    const state = db
      .prepare(/*sql*/ `
        SELECT config."replicaVersion",
               state."stateVersion" AS "stateWatermark"
          FROM "_zero.replicationConfig" AS config,
               "_zero.replicationState" AS state
      `)
      .get<{replicaVersion: string; stateWatermark: string} | undefined>();
    assert(version !== undefined, 'SQLite schema version must be initialized');
    assert(state !== undefined, 'SQLite replication state must be initialized');
    if (version.schemaVersion < SQLITE_CHANGE_LOG_SCHEMA_VERSION) {
      return {
        ...version,
        ...state,
        minWatermark: null,
        headWatermark: null,
      };
    }
    const bounds = db
      .prepare(/*sql*/ `
        SELECT min("watermark") AS "minWatermark",
               max("watermark") AS "headWatermark"
          FROM "${CHANGE_LOG_STREAM_TABLE}"
      `)
      .get<{
        minWatermark: string | null;
        headWatermark: string | null;
      }>();
    return {...version, ...state, ...bounds};
  }

  plan(fromWatermark: string): CatchupPlan {
    return (this.#reader ??= new SQLiteChangeLogReader(
      this.#lc,
      this.#replicaFile,
    )).plan(fromWatermark);
  }

  close(): void {
    this.#reader?.close();
    this.#reader = undefined;
    this.#db?.close();
    this.#db = undefined;
  }

  #database(): Database {
    return (this.#db ??= new Database(this.#lc, this.#replicaFile, {
      readonly: true,
    }));
  }
}

function sqliteSnapshotState(state: SQLiteChangeLogReadState | undefined) {
  assert(
    state?.minWatermark !== null && state?.minWatermark !== undefined,
    'eligible SQLite change log must have a minimum watermark',
  );
  return {
    replicaVersion: state.replicaVersion,
    minWatermark: state.minWatermark,
  };
}

function assertPercent(value: number): void {
  assert(
    Number.isSafeInteger(value) && value >= 0 && value <= 100,
    'SQLite change-log read percentage must be an integer from 0 through 100',
  );
}

function assertPositiveSafeInteger(value: number, name: string): void {
  assert(
    Number.isSafeInteger(value) && value > 0,
    `SQLite change-log ${name} must be a positive safe integer`,
  );
}
