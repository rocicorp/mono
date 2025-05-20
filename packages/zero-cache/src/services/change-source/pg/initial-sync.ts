import {PG_INSUFFICIENT_PRIVILEGE} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import {setDefaultHighWaterMark, Writable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import postgres from 'postgres';
import {promiseVoid} from '../../../../../shared/src/resolved-promises.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {
  createIndexStatement,
  createTableStatement,
} from '../../../db/create.ts';
import * as Mode from '../../../db/mode-enum.ts';
import {
  RowTransform,
  TextTransform,
  type RowTransformOutput,
} from '../../../db/pg-copy.ts';
import {
  mapPostgresToLite,
  mapPostgresToLiteIndex,
} from '../../../db/pg-to-lite.ts';
import type {IndexSpec, PublishedTableSpec} from '../../../db/specs.ts';
import type {LexiVersion} from '../../../types/lexi-version.ts';
import {
  JSON_STRINGIFIED,
  liteValues,
  type LiteValueType,
} from '../../../types/lite.ts';
import {liteTableName} from '../../../types/names.ts';
import {
  pgClient,
  type PostgresDB,
  type PostgresTransaction,
} from '../../../types/pg.ts';
import type {ShardConfig} from '../../../types/shards.ts';
import {ALLOWED_APP_ID_CHARACTERS} from '../../../types/shards.ts';
import {id} from '../../../types/sql.ts';
import {initChangeLog} from '../../replicator/schema/change-log.ts';
import {
  initReplicationState,
  ZERO_VERSION_COLUMN_NAME,
} from '../../replicator/schema/replication-state.ts';
import {CopyRunner} from './copy-runner.ts';
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
  const {tableCopyWorkers: numWorkers} = syncOptions;
  const sql = pgClient(lc, upstreamURI);
  // The typeClient's reason for existence is to configure the type
  // parsing for the copy workers, which skip JSON parsing for efficiency.
  const typeClient = pgClient(lc, upstreamURI, {}, 'json-as-string');
  // Fire off an innocuous request to initialize a connection and thus fetch
  // the array types that will be used to parse the COPY stream.
  void typeClient`SELECT 1`.execute();
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

    initReplicationState(tx, publications, initialVersion);
    initChangeLog(tx);

    // Run up to MAX_WORKERS to copy of tables at the replication slot's snapshot.
    const start = performance.now();
    let numTables: number;
    let numRows: number;
    const copyRunner = new CopyRunner(
      lc,
      () =>
        pgClient(lc, upstreamURI, {
          // No need to fetch array types for these connections, as pgClient
          // streams the COPY data as text, and type parsing is done in the
          // the RowTransform, which gets its types from the typeClient.
          // This eliminates one round trip when each db
          // connection is established.
          ['fetch_types']: false,
          connection: {['application_name']: 'initial-sync-copy-worker'},
        }),
      numWorkers,
      snapshot,
    );
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

      setDefaultHighWaterMark(false, 8 * MB);
      setDefaultHighWaterMark(true, MAX_BUFFERED_ROWS);
      const rowCounts = await Promise.all(
        tables.map(table =>
          copyRunner.run((db, lc) =>
            copy(lc, table, typeClient, db, tx, initialVersion),
          ),
        ),
      );
      numRows = rowCounts.reduce((sum, count) => sum + count, 0);

      const indexStart = performance.now();
      createLiteIndices(tx, indexes);
      lc.info?.(
        `Created indexes (${(performance.now() - indexStart).toFixed(3)} ms)`,
      );
    } finally {
      copyRunner.close();
    }

    await addReplica(sql, shard, slotName, initialVersion, published);

    lc.info?.(
      `Synced ${numRows.toLocaleString()} rows of ${numTables} tables in ${publications} up to ${lsn} (${(
        performance.now() - start
      ).toFixed(3)} ms)`,
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
    await typeClient.end();
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

const MB = 1024 * 1024;
const MAX_BUFFERED_ROWS = 10_000;
const BUFFERED_SIZE_THRESHOLD = 8 * MB;

async function copy(
  lc: LogContext,
  table: PublishedTableSpec,
  dbClient: PostgresDB,
  from: PostgresTransaction,
  to: Database,
  initialVersion: LexiVersion,
) {
  const start = performance.now();
  let totalRows = 0;
  const tableName = liteTableName(table);
  const orderedColumns = Object.entries(table.columns);

  const columnSpecs = orderedColumns.map(([_name, spec]) => spec);
  const selectColumns = orderedColumns.map(([c]) => id(c)).join(',');
  const insertColumns = [
    ...orderedColumns.map(([c]) => c),
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

  const valuesPerRow = columnSpecs.length + 1; // includes _0_version column
  const valuesPerBatch = valuesPerRow * INSERT_BATCH_SIZE;

  // Preallocate the buffer of values to reduce memory allocation churn.
  let pendingValues: LiteValueType[] = Array.from({length: MAX_BUFFERED_ROWS});
  let pendingRows = 0;
  let pendingSize = 0;

  function flush(values: LiteValueType[], rows: number, size: number) {
    const start = performance.now();
    const total = rows;

    for (; rows > INSERT_BATCH_SIZE; rows -= INSERT_BATCH_SIZE) {
      insertBatchStmt.run(values.slice(0, valuesPerBatch));
      values = values.slice(valuesPerBatch); // Allow earlier values to be GC'ed
      totalRows += INSERT_BATCH_SIZE;
    }
    // Insert the remaining rows individually.
    for (let l = 0; rows > 0; rows--) {
      insertStmt.run(values.slice(l, (l += valuesPerRow)));
      totalRows++;
    }
    lc.debug?.(
      `flushed ${total} ${tableName} rows (${size} bytes) in ${(performance.now() - start).toFixed(3)} ms`,
    );
  }

  // Each flush is scheduled to runAfterIO(), or specifically, after the
  // stream `callback` is invoked, to signal Postgres to send more data
  // *before* blocking the CPU to flush the rows to the replica. This
  // essentially allows the CPU latency of the SQLite operation to overlap
  // with the I/O latency of receiving data from upstream.
  let lastFlush = promiseVoid;

  lc.info?.(`Starting copy stream of ${tableName}:`, selectStmt);

  const write = async (
    row: RowTransformOutput,
    _encoding: string,
    callback: (error?: Error) => void,
  ) => {
    try {
      const vals = liteValues(row.values, columnSpecs, JSON_STRINGIFIED);
      let i = 0;
      for (; i < vals.length; i++) {
        pendingValues[pendingRows * valuesPerRow + i] = vals[i];
      }
      pendingValues[pendingRows * valuesPerRow + i] = initialVersion;

      pendingRows++;
      pendingSize += row.size;
      if (
        pendingRows >= MAX_BUFFERED_ROWS - valuesPerRow ||
        pendingSize >= BUFFERED_SIZE_THRESHOLD
      ) {
        const values = pendingValues;
        const rows = pendingRows;
        const size = pendingSize;

        // Allocate a new array for the next batch.
        pendingValues = Array.from({length: MAX_BUFFERED_ROWS});
        pendingRows = 0;
        pendingSize = 0;

        // Wait for the last flush to finish.
        await lastFlush;
        // Then schedule the next to start in the next tick.
        lastFlush = runAfterIO(() => flush(values, rows, size));
      }
      callback();
    } catch (e) {
      callback(e instanceof Error ? e : new Error(String(e)));
    }
  };

  await pipeline(
    await from.unsafe(`COPY (${selectStmt}) TO STDOUT`).readable(),
    new TextTransform(),
    await RowTransform.create(dbClient, columnSpecs),
    new Writable({
      objectMode: true,

      writev: async (
        chunks: {chunk: RowTransformOutput; encoding: BufferEncoding}[],
        callback: (error?: Error) => void,
      ) => {
        lc.debug?.(`received ${chunks.length} rows to write`);
        for (const {chunk, encoding} of chunks) {
          await write(chunk, encoding, () => {});
        }
        callback();
      },

      final: async (callback: (error?: Error) => void) => {
        try {
          await lastFlush;
          await flush(pendingValues, pendingRows, pendingSize);
          callback();
        } catch (e) {
          callback(e instanceof Error ? e : new Error(String(e)));
        }
      },
    }),
  );

  lc.info?.(
    `Finished copying ${totalRows} rows into ${tableName} (${(
      performance.now() - start
    ).toFixed(3)} ms)`,
  );
  return totalRows;
}

function runAfterIO(fn: () => void): Promise<void> {
  return new Promise((resolve, reject) => {
    setTimeout(
      () =>
        setImmediate(() => {
          try {
            fn();
            resolve();
          } catch (e) {
            reject(e);
          }
        }),
      0,
    );
  });
}
