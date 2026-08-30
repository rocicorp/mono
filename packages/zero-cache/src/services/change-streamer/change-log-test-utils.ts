/**
 * The fixture the change-log parity tests share.
 *
 * Several tests drive one change stream into more than one store and require
 * the stores to agree: `change-log-cookies.pg.test.ts` (the Postgres change log
 * against the SQLite one), `change-log-initializer{,.pg}.test.ts` (the change
 * log against the replica, through the comparator), and
 * `replicator/schema/backfilling.pg.test.ts` (Postgres against the replica).
 * They differ only in which stores they attach and in what they assert.
 *
 * The driver is shared so that "the same stream" stays literally the same. Two
 * copies of it that drifted would show up as a difference between the stores,
 * which is indistinguishable from the difference these tests exist to catch.
 */

import type {LogContext} from '@rocicorp/logger';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {TestDBs} from '../../test/db.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {cdcSchema} from '../../types/shards.ts';
import type {SchemaChange} from '../change-source/protocol/current/data.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {
  CREATE_CHANGE_LOG_COOKIE_SCHEMA,
  type BackfillCookie,
  type CookieSet,
  type TableMetadataCookie,
} from '../replicator/change-log-cookies.ts';
import {CREATE_CHANGE_LOG_STREAM_SCHEMA} from '../replicator/change-log-db.ts';
import {ChangeLogStreamWriter} from '../replicator/change-log-stream-writer.ts';
import {ChangeProcessor} from '../replicator/change-processor.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {serializeChangeStreamData} from './change-log-codec.ts';
import {ensureReplicationConfig, setupCDCTables} from './schema/tables.ts';
import {Storer, type TuningOptions} from './storer.ts';

export const REPLICA_VERSION = '00';

const TUNING_OPTIONS: TuningOptions = {
  backPressureLimitHeapProportion: 0.04,
  statementTimeoutMs: 20_000,
  changeLogBatchSize: 2000,
};

/** An in-memory SQLite change log, holding both the stream and the cookies. */
export function createChangeLog(lc: LogContext): Database {
  const db = new Database(lc, ':memory:');
  db.exec(CREATE_CHANGE_LOG_STREAM_SCHEMA);
  db.exec(CREATE_CHANGE_LOG_COOKIE_SCHEMA);
  return db;
}

/**
 * An in-memory replica and the change-processor that writes it, in the
 * replication-manager's `backup` mode -- i.e. the replica that a change log
 * would eventually be initialized from.
 */
export function createReplica(lc: LogContext): {
  replica: Database;
  processor: ChangeProcessor;
} {
  const replica = new Database(lc, ':memory:');
  initReplicationState(replica, ['zero_data'], REPLICA_VERSION);
  const processor = new ChangeProcessor(
    new StatementRunner(replica),
    'backup',
    (_, err: unknown) => {
      throw err;
    },
  );
  return {replica, processor};
}

export type ShardStorer = {
  readonly db: PostgresDB;
  readonly storer: Storer;
  /** The shard's `cdc` schema, for tests that read the tables directly. */
  readonly cdcSchema: string;
  /** Stops the storer and drops the database. For the `beforeEach` teardown. */
  close(): Promise<void>;
};

/** A shard's `cdc` schema, with a {@link Storer} running over it. */
export async function createShardStorer(
  lc: LogContext,
  testDBs: TestDBs,
  dbName: string,
  appID: string,
  shardNum = 1,
): Promise<ShardStorer> {
  const db = await testDBs.create(dbName, {
    typeOpts: {sendStringAsJson: true},
  });
  const shard = {appID, shardNum};
  await db.begin(tx => setupCDCTables(lc, tx, shard));
  await ensureReplicationConfig(
    lc,
    db,
    {
      replicaVersion: REPLICA_VERSION,
      publications: [],
      watermark: REPLICA_VERSION,
    },
    shard,
    true,
  );

  const storer = new Storer(
    lc,
    shard,
    'task-id',
    'change-streamer:12345',
    'ws',
    () => db,
    REPLICA_VERSION,
    () => {},
    () => {},
    TUNING_OPTIONS,
  );
  await storer.assumeOwnership();
  const done = storer.run();

  return {
    db,
    storer,
    cdcSchema: cdcSchema(shard),
    close: async () => {
      await testDBs.drop(db);
      void storer.stop();
      await done;
    },
  };
}

/** The `ChangeStreamData` messages of one transaction, in stream order. */
export function transactionMessages(
  watermark: string,
  changes: SchemaChange[],
): ChangeStreamData[] {
  return [
    ['begin', {tag: 'begin'}, {commitWatermark: watermark}],
    ...changes.map((change): ChangeStreamData => ['data', change]),
    ['commit', {tag: 'commit'}, {watermark}],
  ];
}

/**
 * The subset of {@link Storer} the driver needs, so that a test which attaches
 * no shard does not pull one in.
 */
export type UpstreamStore = {
  store(watermark: string, msg: ChangeStreamData): void;
  allProcessed(): Promise<unknown>;
};

export type ChangeStreamSinks = {
  /** The Postgres change log, i.e. a {@link Storer}. */
  readonly upstream?: UpstreamStore | undefined;
  /** The SQLite change log. */
  readonly changeLog?: Database | undefined;
  /** The replica, via the change-processor that writes it. */
  readonly replica?: ChangeProcessor | undefined;
};

/** Drives whole transactions into every attached store, as a stream loop does. */
export class ChangeStreamDriver {
  readonly #lc: LogContext;
  readonly #sinks: ChangeStreamSinks;
  #watermark = 1;

  constructor(lc: LogContext, sinks: ChangeStreamSinks) {
    this.#lc = lc;
    this.#sinks = sinks;
  }

  /**
   * Takes the next watermark without driving anything, for a test that drives
   * a partial transaction itself.
   */
  claimWatermark(): string {
    return String(++this.#watermark).padStart(2, '0');
  }

  /**
   * Drives one transaction into every attached store and returns the watermark
   * it committed at. `toReplica: false` holds the replica back, which is how a
   * replicator trailing the log's head is simulated.
   */
  async transaction(
    changes: SchemaChange[],
    {toReplica = true}: {toReplica?: boolean} = {},
  ): Promise<string> {
    const wm = this.claimWatermark();
    const messages = transactionMessages(wm, changes);
    const {upstream, changeLog, replica} = this.#sinks;

    messages.forEach(message => upstream?.store(wm, message));

    if (changeLog) {
      const writer = new ChangeLogStreamWriter(new StatementRunner(changeLog));
      writer.begin(wm, serializeChangeStreamData(messages[0]));
      changes.forEach((change, i) =>
        writer.append(serializeChangeStreamData(messages[i + 1]), change),
      );
      writer.commit(wm, serializeChangeStreamData(messages.at(-1)!), 1_000);
    }

    if (replica && toReplica) {
      messages.forEach(message => replica.processMessage(this.#lc, message));
    }

    await upstream?.allProcessed();
    return wm;
  }
}

/** The cookie set the Postgres change log holds, in the SQLite log's shape. */
export async function readPgCookies(
  db: PostgresDB,
  cdcSchema: string,
): Promise<CookieSet> {
  const [tableMetadata, backfilling] = await Promise.all([
    db<TableMetadataCookie[]>`
      SELECT "schema", "table", "metadata" FROM ${db(cdcSchema)}."tableMetadata"
        ORDER BY "schema", "table"`,
    db<BackfillCookie[]>`
      SELECT "schema", "table", "column", "backfill" FROM ${db(cdcSchema)}."backfilling"
        ORDER BY "schema", "table", "column"`,
  ]);
  return {
    tableMetadata: [...tableMetadata],
    backfilling: [...backfilling],
  };
}
