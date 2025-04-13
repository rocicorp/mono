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
import {
  JSON_STRINGIFIED,
  liteValues,
  type LiteValueType,
} from '../../../types/lite.ts';
import {liteTableName} from '../../../types/names.ts';
import {pgClient, type PostgresDB} from '../../../types/pg.ts';
import type {ShardConfig} from '../../../types/shards.ts';
import {ALLOWED_APP_ID_CHARACTERS} from '../../../types/shards.ts';
import {id} from '../../../types/sql.ts';
import {initChangeLog} from '../../replicator/schema/change-log.ts';
import {
  initReplicationState,
  ZERO_VERSION_COLUMN_NAME,
} from '../../replicator/schema/replication-state.ts';
import {toLexiVersion} from './lsn.ts';
import {ensureShardSchema} from './schema/init.ts';
import {getPublicationInfo, type PublicationInfo} from './schema/published.ts';
import {
  addReplica,
  dropShard,
  getInternalShardConfig,
  newReplicationSlot,
  validatePublications,
} from './schema/shard.ts';

export type InitialSyncOptions = {
  tableCopyWorkers: number;
  rowBatchSize: number;
};

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
  const sql = pgClient(lc, upstreamURI);
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
  const slotName = newReplicationSlot(shard);
  try {
    await checkUpstreamConfig(sql);

    const {publications} = await ensurePublishedTables(lc, sql, shard);
    lc.info?.(`Upstream is setup with publications [${publications}]`);

    const {database, host} = sql.options;
    lc.info?.(`opening replication session to ${database}@${host}`);

    let slot: ReplicationSlot;
    for (let first = true; ; first = false) {
      try {
        slot = await createReplicationSlot(lc, replicationSession, slotName);
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
          await sql`ALTER ROLE current_user WITH REPLICATION`;
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
      published = await sql.begin(Mode.READONLY, async tx => {
        await tx.unsafe(/* sql*/ `SET TRANSACTION SNAPSHOT '${snapshot}'`);
        return getPublicationInfo(tx, publications);
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

    await addReplica(sql, shard, slotName, initialVersion, published);

    initReplicationState(tx, publications, initialVersion);
    initChangeLog(tx);
    lc.info?.(
      `Synced ${numRows.toLocaleString()} rows of ${numTables} tables in ${publications} up to ${lsn} (${
        Date.now() - start
      } ms)`,
    );
  } catch (e) {
    // If initial-sync did not succeed, make a best effort to drop the
    // orphaned replication slot to avoid running out of slots in
    // pathological cases that result in repeated failures.
    lc.warn?.(`dropping replication slot ${slotName}`, e);
    await sql`
      SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
        WHERE slot_name = ${slotName};
    `;
    throw e;
  } finally {
    await replicationSession.end();
    await sql.end();
    await copyPool.end();
  }
}

async function checkUpstreamConfig(sql: PostgresDB) {
  const {walLevel, version} = (
    await sql<{walLevel: string; version: number}[]>`
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
  sql: PostgresDB,
  shard: ShardConfig,
  validate = true,
): Promise<{publications: string[]}> {
  const {database, host} = sql.options;
  lc.info?.(`Ensuring upstream PUBLICATION on ${database}@${host}`);

  await ensureShardSchema(lc, sql, shard);
  const {publications} = await getInternalShardConfig(sql, shard);

  if (validate) {
    const exists = await sql`
      SELECT pubname FROM pg_publication WHERE pubname IN ${sql(publications)}
      `.values();
    if (exists.length !== publications.length) {
      lc.warn?.(
        `some configured publications [${publications}] are missing: ` +
          `[${exists.flat()}]. resyncing`,
      );
      await sql.unsafe(dropShard(shard.appID, shard.shardNum));
      return ensurePublishedTables(lc, sql, shard, false);
    }
  }
  return {publications};
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
): Promise<ReplicationSlot> {
  const slot = (
    await session.unsafe<ReplicationSlot[]>(
      /*sql*/ `CREATE_REPLICATION_SLOT "${slotName}" LOGICAL pgoutput`,
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
  const insertSql = /*sql*/ `
    INSERT INTO "${tableName}" (${insertColumnList}) VALUES ${valuesSql}`;
  const insertStmt = to.prepare(insertSql);
  // INSERT VALUES (?,?,?,?,?),... x INSERT_BATCH_SIZE
  const insertBatchStmt = to.prepare(
    insertSql + `,${valuesSql}`.repeat(INSERT_BATCH_SIZE - 1),
  );

  const filterConditions = Object.values(table.publications)
    .map(({rowFilter}) => rowFilter)
    .filter(f => !!f); // remove nulls
  const selectStmt =
    /*sql*/ `
    SELECT ${selectColumns} FROM ${id(table.schema)}.${id(table.name)}` +
    (filterConditions.length === 0
      ? ''
      : /*sql*/ ` WHERE ${filterConditions.join(' OR ')}`);

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
            ...liteValues(rows[j], table, JSON_STRINGIFIED),
            initialVersion,
          );
        }
        insertBatchStmt.run(values);
      }
      // Remaining set of rows is < INSERT_BATCH_SIZE
      for (; i < rows.length; i++) {
        insertStmt.run([
          ...liteValues(rows[i], table, JSON_STRINGIFIED),
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
