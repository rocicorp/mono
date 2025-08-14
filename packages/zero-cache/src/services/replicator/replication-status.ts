import type {LogContext} from '@rocicorp/logger';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {
  ReplicatedIndex,
  ReplicatedTable,
  ReplicationStage,
  ReplicationStatusEvent,
  Status,
} from '../../../../zero-events/src/status.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {computeZqlSpecs, listIndexes} from '../../db/lite-tables.ts';
import type {LiteTableSpec} from '../../db/specs.ts';
import {
  makeErrorDetails,
  publishCriticalEvent,
  publishEvent,
} from '../../observability/events.ts';

const byKeys = ([a]: [string, unknown], [b]: [string, unknown]) =>
  a < b ? -1 : a > b ? 1 : 0;

export class ReplicationStatusPublisher {
  readonly #db: Database;
  #timer: NodeJS.Timeout | undefined;

  constructor(db: Database) {
    this.#db = db;
  }

  publish(
    lc: LogContext,
    stage: ReplicationStage,
    description?: string,
    interval = 0,
  ): this {
    this.stop();
    publishEvent(
      lc,
      replicationStatusEvent(lc, this.#db, stage, 'OK', description),
    );

    if (interval) {
      this.#timer = setInterval(
        () => this.publish(lc, stage, description, interval),
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
    const event = replicationStatusEvent(
      lc,
      this.#db,
      stage,
      'ERROR',
      String(e),
    );
    event.errorDetails = makeErrorDetails(e);
    await publishCriticalEvent(lc, event);
    throw e;
  }

  stop(): this {
    clearInterval(this.#timer);
    return this;
  }
}

// Exported for testing.
export function replicationStatusEvent(
  lc: LogContext,
  db: Database,
  stage: ReplicationStage,
  status: Status,
  description?: string,
  now = new Date(),
): ReplicationStatusEvent {
  try {
    return {
      type: 'zero/events/status/replication/v1',
      component: 'replication',
      status,
      stage,
      description,
      time: now.toISOString(),
      tables: getReplicatedTables(db),
      indexes: getReplicatedIndexes(db),
      replicaSize: getReplicaSize(db),
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
      tables: [],
      indexes: [],
      replicaSize: 0,
    };
  }
}

function getReplicatedTables(db: Database): ReplicatedTable[] {
  const fullTables = new Map<string, LiteTableSpec>();
  const clientSchema = computeZqlSpecs(
    createSilentLogContext(), // avoid logging warnings about indexes
    db,
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
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const [{page_count: pageCount}] = db.pragma<{page_count: number}>(
    'page_count',
  );
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const [{page_size: pageSize}] = db.pragma<{page_size: number}>('page_size');
  return pageCount * pageSize;
}
