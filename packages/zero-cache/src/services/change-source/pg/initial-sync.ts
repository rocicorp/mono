import {PG_INSUFFICIENT_PRIVILEGE} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import postgres from 'postgres';
import {promiseVoid} from '../../../../../shared/src/resolved-promises.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {
  createIndexStatement,
  createTableStatement,
} from '../../../db/create.ts';
import * as Mode from '../../../db/mode-enum.ts';
import {
  mapPostgresToLite,
  mapPostgresToLiteIndex,
} from '../../../db/pg-to-lite.ts';
import type {IndexSpec, PublishedTableSpec} from '../../../db/specs.ts';
import {importSnapshot, TransactionPool} from '../../../db/transaction-pool.ts';
import type {LexiVersion} from '../../../types/lexi-version.ts';
import {liteValues, type LiteValueType} from '../../../types/lite.ts';
import {liteTableName} from '../../../types/names.ts';
import {pgClient, type PostgresDB} from '../../../types/pg.ts';
import type {ShardConfig, ShardID} from '../../../types/shards.ts';
import {ALLOWED_APP_ID_CHARACTERS} from '../../../types/shards.ts';
import {id} from '../../../types/sql.ts';
import {initChangeLog} from '../../replicator/schema/change-log.ts';
import {
  initReplicationState,
  ZERO_VERSION_COLUMN_NAME,
} from '../../replicator/schema/replication-state.ts';
import {toLexiVersion} from './lsn.ts';
import {initShardSchema} from './schema/init.ts';
import {getPublicationInfo, type PublicationInfo} from './schema/published.ts';
import {
  getInternalShardConfig,
  setInitialSchema,
  validatePublications,
} from './schema/shard.ts';

export type InitialSyncOptions = {
  tableCopyWorkers: number;
  rowBatchSize: number;
};

export function replicationSlot({appID, shardNum}: ShardID): string {
  return `${appID}_${shardNum}`;
}

export async function initialSync(
  lc: LogContext,
  shard: ShardConfig,
  tx: Database,
  upstreamURI: string,
  syncOptions: InitialSyncOptions,
) {
  if (!ALLOWED_APP_ID_CHARACTERS.test(shard.appID)) {
    throw new Error(
      'The App ID may only consist of lower-case letters, numbers, and the underscore character',
    );
  }
  const {tableCopyWorkers: numWorkers, rowBatchSize} = syncOptions;
  const upstreamDB = pgClient(lc, upstreamURI);
  const copyPool = pgClient(
    lc,
    upstreamURI,
    {max: numWorkers},
    'json-as-string',
  );
  const replicationSession = pgClient(lc, upstreamURI, {
    ['fetch_types']: false, // Necessary for the streaming protocol
    connection: {replication: 'database'}, // https://www.postgresql.org/docs/current/protocol-replication.html
  });
  try {
    await checkUpstreamConfig(upstreamDB);

    // Kill the active_pid on the existing slot before altering publications,
    // as deleting a publication associated with an existing subscriber causes
    // weirdness; the active_pid becomes null and thus unable to be terminated.
    const slotName = replicationSlot(shard);
    const slots = await upstreamDB<{pid: string | null}[]>`
    SELECT pg_terminate_backend(active_pid), active_pid as pid
      FROM pg_replication_slots WHERE slot_name = ${slotName}`;
    if (slots.length > 0 && slots[0].pid !== null) {
      lc.info?.(`signaled subscriber ${slots[0].pid} to shut down`);
    }

    const {publications} = await ensurePublishedTables(lc, upstreamDB, shard);
    lc.info?.(`Upstream is setup with publications [${publications}]`);

    const {database, host} = upstreamDB.options;
    lc.info?.(`opening replication session to ${database}@${host}`);

    let slot: ReplicationSlot;
    for (let first = true; ; first = false) {
      try {
        slot = await createReplicationSlot(
          lc,
          replicationSession,
          slotName,
          slots.length > 0,
        );
        break;
      } catch (e) {
        if (
          first &&
          e instanceof postgres.PostgresError &&
          e.code === PG_INSUFFICIENT_PRIVILEGE
        ) {
          // Some Postgres variants (e.g. Google Cloud SQL) require that
          // the user have the REPLICATION role in order to create a slot.
          // Note that this must be done by the upstreamDB connection, and
          // does not work in the replicationSession itself.
          await upstreamDB`ALTER ROLE current_user WITH REPLICATION`;
          lc.info?.(`Added the REPLICATION role to database user`);
          continue;
        }
        throw e;
      }
    }
    const {snapshot_name: snapshot, consistent_point: lsn} = slot;
    const initialVersion = toLexiVersion(lsn);

    // Run up to MAX_WORKERS to copy of tables at the replication slot's snapshot.
    const start = Date.now();
    let numTables: number;
    let numRows: number;
    const copiers = startTableCopyWorkers(lc, copyPool, snapshot, numWorkers);
    let published: PublicationInfo;
    try {
      // Retrieve the published schema at the consistent_point.
      published = await upstreamDB.begin(Mode.READONLY, async db => {
        await db.unsafe(`SET TRANSACTION SNAPSHOT '${snapshot}'`);
        return getPublicationInfo(db, publications);
      });
      // Note: If this throws, initial-sync is aborted.
      validatePublications(lc, published);

      // Now that tables have been validated, kick off the copiers.
      const {tables, indexes} = published;
      numTables = tables.length;
      createLiteTables(tx, tables);

      const rowCounts = await Promise.all(
        tables.map(table =>
          copiers.processReadTask(db =>
            copy(lc, table, db, tx, initialVersion, rowBatchSize),
          ),
        ),
      );
      numRows = rowCounts.reduce((sum, count) => sum + count, 0);

      const indexStart = Date.now();
      createLiteIndices(tx, indexes);
      lc.info?.(`Created indexes (${Date.now() - indexStart} ms)`);
    } finally {
      copiers.setDone();
      await copiers.done();
    }

    await setInitialSchema(upstreamDB, shard, initialVersion, published);

    initReplicationState(tx, publications, initialVersion);
    initChangeLog(tx);
    lc.info?.(
      `Synced ${numRows.toLocaleString()} rows of ${numTables} tables in ${publications} up to ${lsn} (${
        Date.now() - start
      } ms)`,
    );
  } finally {
    await replicationSession.end();
    await upstreamDB.end();
    await copyPool.end();
  }
}

async function checkUpstreamConfig(upstreamDB: PostgresDB) {
  const {walLevel, version} = (
    await upstreamDB<{walLevel: string; version: number}[]>`
      SELECT current_setting('wal_level') as "walLevel", 
             current_setting('server_version_num') as "version";
  `
  )[0];

  if (walLevel !== 'logical') {
    throw new Error(
      `Postgres must be configured with "wal_level = logical" (currently: "${walLevel})`,
    );
  }
  if (version < 150000) {
    throw new Error(
      `Must be running Postgres 15 or higher (currently: "${version}")`,
    );
  }
}

async function ensurePublishedTables(
  lc: LogContext,
  upstreamDB: PostgresDB,
  shard: ShardConfig,
): Promise<{publications: string[]}> {
  const {database, host} = upstreamDB.options;
  lc.info?.(`Ensuring upstream PUBLICATION on ${database}@${host}`);

  await initShardSchema(lc, upstreamDB, shard);

  return getInternalShardConfig(upstreamDB, shard);
}

/* eslint-disable @typescript-eslint/naming-convention */
// Row returned by `CREATE_REPLICATION_SLOT`
type ReplicationSlot = {
  slot_name: string;
  consistent_point: string;
  snapshot_name: string;
  output_plugin: string;
};
/* eslint-enable @typescript-eslint/naming-convention */

// Note: The replication connection does not support the extended query protocol,
//       so all commands must be sent using sql.unsafe(). This is technically safe
//       because all placeholder values are under our control (i.e. "slotName").
async function createReplicationSlot(
  lc: LogContext,
  session: postgres.Sql,
  slotName: string,
  dropExisting: boolean,
): Promise<ReplicationSlot> {
  // Because a snapshot created by CREATE_REPLICATION_SLOT only lasts for the lifetime
  // of the replication session, if there is an existing slot, it must be deleted so that
  // the slot (and corresponding snapshot) can be created anew.
  //
  // This means that in order for initial data sync to succeed, it must fully complete
  // within the lifetime of a replication session. Note that this is same requirement
  // (and behavior) for Postgres-to-Postgres initial sync:
  // https://github.com/postgres/postgres/blob/5304fec4d8a141abe6f8f6f2a6862822ec1f3598/src/backend/replication/logical/tablesync.c#L1358
  if (dropExisting) {
    lc.info?.(`Dropping existing replication slot ${slotName}`);
    await session.unsafe(`DROP_REPLICATION_SLOT "${slotName}" WAIT`);
  }
  const slot = (
    await session.unsafe<ReplicationSlot[]>(
      `CREATE_REPLICATION_SLOT "${slotName}" LOGICAL pgoutput`,
    )
  )[0];
  lc.info?.(`Created replication slot ${slotName}`, slot);
  return slot;
}

function startTableCopyWorkers(
  lc: LogContext,
  db: PostgresDB,
  snapshot: string,
  numWorkers: number,
): TransactionPool {
  const {init} = importSnapshot(snapshot);
  const tableCopiers = new TransactionPool(
    lc,
    Mode.READONLY,
    init,
    undefined,
    numWorkers,
  );
  tableCopiers.run(db);

  lc.info?.(`Started ${numWorkers} workers to copy tables`);
  return tableCopiers;
}

function createLiteTables(tx: Database, tables: PublishedTableSpec[]) {
  for (const t of tables) {
    tx.exec(createTableStatement(mapPostgresToLite(t)));
  }
}

function createLiteIndices(tx: Database, indices: IndexSpec[]) {
  for (const index of indices) {
    tx.exec(createIndexStatement(mapPostgresToLiteIndex(index)));
  }
}

// Verified empirically that batches of 50 seem to be the sweet spot,
// similar to the report in https://sqlite.org/forum/forumpost/8878a512d3652655
//
// Exported for testing.
export const INSERT_BATCH_SIZE = 50;

async function copy(
  lc: LogContext,
  table: PublishedTableSpec,
  from: PostgresDB,
  to: Database,
  initialVersion: LexiVersion,
  rowBatchSize: number,
) {
  let totalRows = 0;
  const tableName = liteTableName(table);
  const selectColumns = Object.keys(table.columns)
    .map(c => id(c))
    .join(',');
  const insertColumns = [
    ...Object.keys(table.columns),
    ZERO_VERSION_COLUMN_NAME,
  ];
  const insertColumnList = insertColumns.map(c => id(c)).join(',');

  // (?,?,?,?,?)
  const valuesSql = `(${new Array(insertColumns.length).fill('?').join(',')})`;
  const insertSql = `INSERT INTO "${tableName}" (${insertColumnList}) VALUES ${valuesSql}`;
  const insertStmt = to.prepare(insertSql);
  // INSERT VALUES (?,?,?,?,?),... x INSERT_BATCH_SIZE
  const insertBatchStmt = to.prepare(
    insertSql + `,${valuesSql}`.repeat(INSERT_BATCH_SIZE - 1),
  );

  const filterConditions = Object.values(table.publications)
    .map(({rowFilter}) => rowFilter)
    .filter(f => !!f); // remove nulls
  const selectStmt =
    `SELECT ${selectColumns} FROM ${id(table.schema)}.${id(table.name)}` +
    (filterConditions.length === 0
      ? ''
      : ` WHERE ${filterConditions.join(' OR ')}`);

  lc.info?.(`Starting copy of ${tableName}:`, selectStmt);

  const cursor = from.unsafe(selectStmt).cursor(rowBatchSize);
  let prevBatch = promiseVoid;

  for await (const rows of cursor) {
    await prevBatch;

    // Parallelize the reading from postgres (`cursor`) with the processing
    // of the results (`prevBatch`) by running the latter after I/O events.
    // This allows the cursor to query the next batch from postgres before
    // the CPU is consumed by the previous batch (of inserts).
    prevBatch = runAfterIO(() => {
      let i = 0;
      for (; i + INSERT_BATCH_SIZE < rows.length; i += INSERT_BATCH_SIZE) {
        const values: LiteValueType[] = [];
        for (let j = i; j < i + INSERT_BATCH_SIZE; j++) {
          values.push(
            ...liteValues(rows[j], table, 'json-as-string'),
            initialVersion,
          );
        }
        insertBatchStmt.run(values);
      }
      // Remaining set of rows is < INSERT_BATCH_SIZE
      for (; i < rows.length; i++) {
        insertStmt.run([
          ...liteValues(rows[i], table, 'json-as-string'),
          initialVersion,
        ]);
      }
      totalRows += rows.length;
      lc.debug?.(`Copied ${totalRows} rows from ${table.schema}.${table.name}`);
    });
  }
  await prevBatch;

  lc.info?.(`Finished copying ${totalRows} rows into ${tableName}`);
  return totalRows;
}

function runAfterIO(fn: () => void): Promise<void> {
  const {promise, resolve, reject} = resolver();
  setTimeout(() => {
    try {
      fn();
      resolve();
    } catch (e) {
      reject(e);
    }
  }, 0);
  return promise;
}
