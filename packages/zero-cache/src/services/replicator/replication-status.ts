import type {LogContext} from '@rocicorp/logger';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {JSONObject} from '../../../../zero-events/src/json.ts';
import type {
  ReplicatedIndex,
  ReplicatedTable,
  ReplicationStage,
  ReplicationState,
  ReplicationStatusEvent,
  Status,
} from '../../../../zero-events/src/status.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {computeZqlSpecs, listIndexes} from '../../db/lite-tables.ts';
import type {LiteTableSpec} from '../../db/specs.ts';
import {
  makeErrorDetails,
  publishCriticalEvent,
} from '../../observability/events.ts';

const byKeys = (a: [string, unknown], b: [string, unknown]) =>
  a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0;

export class ReplicationStatusPublisher {
  readonly #db: Database;
  readonly #publish: typeof publishCriticalEvent;
  #timer: NodeJS.Timeout | undefined;

  constructor(db: Database, publishFn = publishCriticalEvent) {
    this.#db = db;
    this.#publish = publishFn;
  }

  publish(
    lc: LogContext,
    stage: ReplicationStage,
    description?: string,
    interval = 0,
    extraState?: () => Partial<ReplicationState>,
    now = new Date(),
  ): this {
    this.stop();
    const event = replicationStatusEvent(
      lc,
      this.#db,
      stage,
      'OK',
      description,
      now,
    );
    if (event.state) {
      event.state = {
        ...event.state,
        ...extraState?.(),
      };
    }
    void this.#publish(lc, event);

    if (interval) {
      this.#timer = setInterval(
        () => this.publish(lc, stage, description, interval, extraState),
        interval,
      );
    }
    return this;
  }

  async publishAndThrowError(
    lc: LogContext,
    stage: ReplicationStage,
    e: unknown,
  ): Promise<never> {
    this.stop();
    const event = replicationStatusError(lc, stage, 'ERROR', this.#db);
    await this.#publish(lc, event);
    throw e;
  }

  stop(): this {
    clearInterval(this.#timer);
    return this;
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
