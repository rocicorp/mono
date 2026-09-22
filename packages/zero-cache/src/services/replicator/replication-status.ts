import type {LogContext} from '@rocicorp/logger';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {JSONObject} from '../../../../zero-events/src/json.ts';
import type {
  IndexingStatus,
  ReplicatedIndex,
  ReplicatedTable,
  ReplicationStage,
  ReplicationState,
  ReplicationStatusEvent,
  Status,
} from '../../../../zero-events/src/status.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {computeZqlSpecs, listIndexes} from '../../db/lite-tables.ts';
import type {LiteIndexSpec, LiteTableSpec} from '../../db/specs.ts';
import {
  makeErrorDetails,
  publishCriticalEvent,
} from '../../observability/events.ts';

const byKeys = (a: [string, unknown], b: [string, unknown]) =>
  a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0;

export class ReplicationStatusPublisher {
  readonly #dbRunner: <T>(lc: LogContext, fn: (db: Database) => T) => T;
  readonly #publishFn: typeof publishCriticalEvent;
  #timer: NodeJS.Timeout | undefined;

  static forTesting() {
    return ReplicationStatusPublisher.forReplicaFile(':memory:');
  }

  static forRunningTransaction(tx: Database, publishFn = publishCriticalEvent) {
    return new ReplicationStatusPublisher((_lc, fn) => fn(tx), publishFn);
  }

  static forReplicaFile(file: string, publishFn = publishCriticalEvent) {
    return new ReplicationStatusPublisher((lc, fn) => {
      const db = new Database(lc, file, {readonly: true});
      try {
        return fn(db);
      } finally {
        db.close();
      }
    }, publishFn);
  }

  constructor(
    dbRunner: <T>(lc: LogContext, fn: (db: Database) => T) => T,
    publishFn = publishCriticalEvent,
  ) {
    this.#dbRunner = dbRunner;
    this.#publishFn = publishFn;
  }

  publish(
    lc: LogContext,
    stage: ReplicationStage,
    description?: string,
    interval = 0,
    extraState?: () => Partial<ReplicationState>,
    now = new Date(),
  ): this {
    void this.#publish(lc, stage, description, interval, extraState, now);
    return this;
  }

  /**
   * Like {@link publish}, but waits (for at most `maxWaitMs`) for the event
   * to be sent. Use this before a long synchronous operation (e.g. creating
   * an index) that blocks the event loop and would otherwise prevent the
   * event from being sent until the operation completes.
   */
  async publishAndFlush(
    lc: LogContext,
    stage: ReplicationStage,
    description?: string,
    interval = 0,
    extraState?: () => Partial<ReplicationState>,
    maxWaitMs = 1000,
  ): Promise<void> {
    const published = this.#publish(
      lc,
      stage,
      description,
      interval,
      extraState,
    );
    let timer: NodeJS.Timeout | undefined;
    try {
      await Promise.race([
        published,
        new Promise<void>(resolve => {
          timer = setTimeout(resolve, maxWaitMs);
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  }

  #publish(
    lc: LogContext,
    stage: ReplicationStage,
    description: string | undefined,
    interval: number,
    extraState: (() => Partial<ReplicationState>) | undefined,
    now = new Date(),
  ): Promise<void> {
    this.stop();

    const event = this.#dbRunner(lc, db =>
      replicationStatusEvent(lc, db, stage, 'OK', description, now),
    );
    if (event.state) {
      event.state = {
        ...event.state,
        ...extraState?.(),
      };
    }
    const published = this.#publishFn(lc, event);

    if (interval) {
      this.#timer = setInterval(
        () => this.publish(lc, stage, description, interval, extraState),
        interval,
      );
    }
    return published;
  }

  async publishAndThrowError(
    lc: LogContext,
    stage: ReplicationStage,
    e: unknown,
  ): Promise<never> {
    this.stop();
    const event = this.#dbRunner(lc, db =>
      replicationStatusError(lc, stage, e, db),
    );
    await this.#publishFn(lc, event);
    throw e;
  }

  stop(): this {
    clearInterval(this.#timer);
    return this;
  }
}

/**
 * Tracks the progress of a sequence of index creations for reporting in
 * the `indexingStatus` field of {@link ReplicationState}.
 */
export class IndexingProgress {
  readonly #totalIndexes: number | undefined;
  readonly #now: () => number;
  #count = 0;
  #completedMs = 0;
  #current:
    | {index: LiteIndexSpec; start: number; elapsedMs: number | undefined}
    | undefined;

  /**
   * @param totalIndexes The total number of indexes to be created, if known.
   */
  constructor(totalIndexes?: number, now = () => performance.now()) {
    this.#totalIndexes = totalIndexes;
    this.#now = now;
  }

  /** Marks the start of creating the `index`. */
  start(index: LiteIndexSpec) {
    this.#count++;
    this.#current = {index, start: this.#now(), elapsedMs: undefined};
  }

  /**
   * Restarts the timer of the current index, e.g. to exclude time spent
   * reporting its start from its creation time.
   */
  restartTimer() {
    if (this.#current) {
      this.#current.start = this.#now();
    }
  }

  /**
   * Marks the completion of the index passed to the last call to
   * {@link start}, returning the milliseconds it took to create.
   */
  finish(): number {
    const current = this.#current;
    if (!current) {
      return 0;
    }
    if (current.elapsedMs === undefined) {
      current.elapsedMs = this.#now() - current.start;
      this.#completedMs += current.elapsedMs;
    }
    return current.elapsedMs;
  }

  /** The `extraState` for {@link ReplicationStatusPublisher.publish}. */
  readonly state = (): Partial<ReplicationState> => {
    const status = this.status();
    return status ? {indexingStatus: status} : {};
  };

  status(): IndexingStatus | undefined {
    const current = this.#current;
    if (!current) {
      return undefined;
    }
    const {index, start, elapsedMs} = current;
    const done = elapsedMs !== undefined;
    return {
      name: index.name,
      table: index.tableName,
      columns: Object.keys(index.columns),
      unique: index.unique,
      index: this.#count,
      totalIndexes: this.#totalIndexes,
      elapsedMs: Math.round(done ? elapsedMs : this.#now() - start),
      completedMs: Math.round(
        done ? this.#completedMs - elapsedMs : this.#completedMs,
      ),
      done,
    };
  }
}

export async function publishReplicationError(
  lc: LogContext,
  stage: ReplicationStage,
  description: string,
  errorDetails?: JSONObject,
  now = new Date(),
) {
  const event: ReplicationStatusEvent = {
    type: 'zero/events/status/replication/v1',
    component: 'replication',
    status: 'ERROR',
    stage,
    description,
    errorDetails,
    time: now.toISOString(),
  };
  await publishCriticalEvent(lc, event);
}

export function replicationStatusError(
  lc: LogContext,
  stage: ReplicationStage,
  e: unknown,
  db?: Database,
  now = new Date(),
) {
  const event = replicationStatusEvent(lc, db, stage, 'ERROR', String(e), now);
  event.errorDetails = makeErrorDetails(e);
  return event;
}

// Exported for testing.
export function replicationStatusEvent(
  lc: LogContext,
  db: Database | undefined,
  stage: ReplicationStage,
  status: Status,
  description?: string,
  now = new Date(),
): ReplicationStatusEvent {
  const start = performance.now();
  try {
    return {
      type: 'zero/events/status/replication/v1',
      component: 'replication',
      status,
      stage,
      description,
      time: now.toISOString(),
      state: {
        tables: db ? getReplicatedTables(db) : [],
        indexes: db ? getReplicatedIndexes(db) : [],
        replicaSize: db ? getReplicaSize(db) : undefined,
      },
    };
  } catch (e) {
    lc.warn?.(`Unable to create full ReplicationStatusEvent`, e);
    return {
      type: 'zero/events/status/replication/v1',
      component: 'replication',
      status,
      stage,
      description,
      time: now.toISOString(),
      state: {
        tables: [],
        indexes: [],
        replicaSize: 0,
      },
    };
  } finally {
    const elapsed = (performance.now() - start).toFixed(3);
    lc.debug?.(`computed schema for replication event (${elapsed} ms)`);
  }
}

function getReplicatedTables(db: Database): ReplicatedTable[] {
  const fullTables = new Map<string, LiteTableSpec>();
  const clientSchema = computeZqlSpecs(
    createSilentLogContext(), // avoid logging warnings about indexes
    db,
    {includeBackfillingColumns: false},
    new Map(),
    fullTables,
  );

  // oxlint-disable-next-line e18e/prefer-array-to-sorted
  return [...fullTables.entries()].sort(byKeys).map(([table, spec]) => ({
    table,
    columns: Object.entries(spec.columns)
      .sort(byKeys)
      .map(([column, spec]) => ({
        column,
        upstreamType: spec.dataType.split('|')[0],
        clientType: clientSchema.get(table)?.zqlSpec[column]?.type ?? null,
      })),
  }));
}

function getReplicatedIndexes(db: Database): ReplicatedIndex[] {
  return listIndexes(db).map(({tableName: table, columns, unique}) => ({
    table,
    unique,
    columns: Object.entries(columns)
      .sort(byKeys)
      .map(([column, dir]) => ({column, dir})),
  }));
}

function getReplicaSize(db: Database) {
  const [{page_count: pageCount}] = db.pragma<{page_count: number}>(
    'page_count',
  );
  const [{page_size: pageSize}] = db.pragma<{page_size: number}>('page_size');
  return pageCount * pageSize;
}
