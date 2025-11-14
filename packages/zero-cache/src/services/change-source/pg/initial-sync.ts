import {
  PG_CONFIGURATION_LIMIT_EXCEEDED,
  PG_INSUFFICIENT_PRIVILEGE,
} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import {platform} from 'node:os';
import {Writable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import postgres from 'postgres';
import type {Database} from '../../../../../zqlite/src/db.ts';
import {
  createLiteIndexStatement,
  createLiteTableStatement,
} from '../../../db/create.ts';
import * as Mode from '../../../db/mode-enum.ts';
import {TsvParser} from '../../../db/pg-copy.ts';
import {
  mapPostgresToLite,
  mapPostgresToLiteIndex,
} from '../../../db/pg-to-lite.ts';
import {getTypeParsers} from '../../../db/pg-type-parser.ts';
import type {IndexSpec, PublishedTableSpec} from '../../../db/specs.ts';
import {importSnapshot, TransactionPool} from '../../../db/transaction-pool.ts';
import {
  JSON_STRINGIFIED,
  liteValue,
  type LiteValueType,
} from '../../../types/lite.ts';
import {liteTableName} from '../../../types/names.ts';
import {
  pgClient,
  type PostgresDB,
  type PostgresTransaction,
  type PostgresValueType,
} from '../../../types/pg.ts';
import {CpuProfiler} from '../../../types/profiler.ts';
import type {ShardConfig} from '../../../types/shards.ts';
import {ALLOWED_APP_ID_CHARACTERS} from '../../../types/shards.ts';
import {id} from '../../../types/sql.ts';
import {ReplicationStatusPublisher} from '../../replicator/replication-status.ts';
import {initChangeLog} from '../../replicator/schema/change-log.ts';
import {initReplicationState} from '../../replicator/schema/replication-state.ts';
import {toLexiVersion} from './lsn.ts';
import {ensureShardSchema} from './schema/init.ts';
import {getPublicationInfo} from './schema/published.ts';
import {
  addReplica,
  dropShard,
  getInternalShardConfig,
  newReplicationSlot,
  replicationSlotExpression,
  validatePublications,
} from './schema/shard.ts';

export type InitialSyncOptions = {
  tableCopyWorkers: number;
  profileCopy?: boolean | undefined;
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
  const {tableCopyWorkers, profileCopy} = syncOptions;
  const copyProfiler = profileCopy ? await CpuProfiler.connect() : null;
  const sql = pgClient(lc, upstreamURI);
  const replicationSession = pgClient(lc, upstreamURI, {
    ['fetch_types']: false, // Necessary for the streaming protocol
    connection: {replication: 'database'}, // https://www.postgresql.org/docs/current/protocol-replication.html
  });
  const slotName = newReplicationSlot(shard);
  const statusPublisher = new ReplicationStatusPublisher(tx).publish(
    lc,
    'Initializing',
  );
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
        if (first && e instanceof postgres.PostgresError) {
          if (e.code === PG_INSUFFICIENT_PRIVILEGE) {
            // Some Postgres variants (e.g. Google Cloud SQL) require that
            // the user have the REPLICATION role in order to create a slot.
            // Note that this must be done by the upstreamDB connection, and
            // does not work in the replicationSession itself.
            await sql`ALTER ROLE current_user WITH REPLICATION`;
            lc.info?.(`Added the REPLICATION role to database user`);
            continue;
          }
          if (e.code === PG_CONFIGURATION_LIMIT_EXCEEDED) {
            const slotExpression = replicationSlotExpression(shard);

            const dropped = await sql<{slot: string}[]>`
              SELECT slot_name as slot, pg_drop_replication_slot(slot_name) 
                FROM pg_replication_slots
                WHERE slot_name LIKE ${slotExpression} AND NOT active`;
            if (dropped.length) {
              lc.warn?.(
                `Dropped inactive replication slots: ${dropped.map(({slot}) => slot)}`,
                e,
              );
              continue;
            }
            lc.error?.(`Unable to drop replication slots`, e);
          }
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
    // Retrieve the published schema at the consistent_point.
    const published = await sql.begin(Mode.READONLY, async tx => {
      await tx.unsafe(/* sql*/ `SET TRANSACTION SNAPSHOT '${snapshot}'`);
      return getPublicationInfo(tx, publications);
    });
    // Note: If this throws, initial-sync is aborted.
    validatePublications(lc, published);

    // Now that tables have been validated, kick off the copiers.
    const {tables, indexes} = published;
    const numTables = tables.length;
    if (platform() === 'win32' && tableCopyWorkers < numTables) {
      lc.warn?.(
        `Increasing the number of copy workers from ${tableCopyWorkers} to ` +
          `${numTables} to work around a Node/Postgres connection bug`,
      );
    }
    const numWorkers =
      platform() === 'win32'
        ? numTables
        : Math.min(tableCopyWorkers, numTables);

    const copyPool = pgClient(
      lc,
      upstreamURI,
      {
        max: numWorkers,
        connection: {['application_name']: 'initial-sync-copy-worker'},
      },
      'json-as-string',
    );
    const copiers = startTableCopyWorkers(
      lc,
      copyPool,
      snapshot,
      numWorkers,
      numTables,
    );
    try {
      createLiteTables(tx, tables, initialVersion);
      statusPublisher.publish(
        lc,
        'Initializing',
        `Copying ${numTables} upstream tables at version ${initialVersion}`,
        5000,
      );

      void copyProfiler?.start();
      const rowCounts = await Promise.all(
        tables.map(table =>
          copiers.processReadTask((db, lc) =>
            copy(lc, table, copyPool, db, tx),
          ),
        ),
      );
      void copyProfiler?.stopAndDispose(lc, 'initial-copy');

      const total = rowCounts.reduce(
        (acc, curr) => ({
          rows: acc.rows + curr.rows,
          flushTime: acc.flushTime + curr.flushTime,
        }),
        {rows: 0, flushTime: 0},
      );

      statusPublisher.publish(
        lc,
        'Indexing',
        `Creating ${indexes.length} indexes`,
        5000,
      );
      const indexStart = performance.now();
      createLiteIndices(tx, indexes);
      const index = performance.now() - indexStart;
      lc.info?.(`Created indexes (${index.toFixed(3)} ms)`);

      await addReplica(sql, shard, slotName, initialVersion, published);

      const elapsed = performance.now() - start;
      lc.info?.(
        `Synced ${total.rows.toLocaleString()} rows of ${numTables} tables in ${publications} up to ${lsn} ` +
          `(flush: ${total.flushTime.toFixed(3)}, index: ${index.toFixed(3)}, total: ${elapsed.toFixed(3)} ms)`,
      );
    } finally {
      copiers.setDone();
      if (platform() === 'win32') {
        // Workaround a Node bug in Windows in which certain COPY streams result
        // in hanging the connection, which causes this await to never resolve.
        void copyPool.end().catch(e => lc.warn?.(`Error closing copyPool`, e));
      } else {
        await copyPool.end();
      }
    }
  } catch (e) {
    // If initial-sync did not succeed, make a best effort to drop the
    // orphaned replication slot to avoid running out of slots in
    // pathological cases that result in repeated failures.
    lc.warn?.(`dropping replication slot ${slotName}`, e);
    await sql`
      SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
        WHERE slot_name = ${slotName};
    `.catch(e => lc.warn?.(`Unable to drop replication slot ${slotName}`, e));
    await statusPublisher.publishAndThrowError(lc, 'Initializing', e);
  } finally {
    statusPublisher.stop();
    await replicationSession.end();
    await sql.end();
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

function startTableCopyWorkers(
  lc: LogContext,
  db: PostgresDB,
  snapshot: string,
  numWorkers: number,
  numTables: number,
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

  lc.info?.(`Started ${numWorkers} workers to copy ${numTables} tables`);

  if (parseInt(process.versions.node) < 22) {
    lc.warn?.(
      `\n\n\n` +
        `Older versions of Node have a bug that results in an unresponsive\n` +
        `Postgres connection after running certain combinations of COPY commands.\n` +
        `If initial sync hangs, run zero-cache with Node v22+. This has the additional\n` +
        `benefit of being consistent with the Node version run in the production container image.` +
        `\n\n\n`,
    );
  }
  return tableCopiers;
}

// Row returned by `CREATE_REPLICATION_SLOT`
type ReplicationSlot = {
  slot_name: string;
  consistent_point: string;
  snapshot_name: string;
  output_plugin: string;
};

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

function createLiteTables(
  tx: Database,
  tables: PublishedTableSpec[],
  initialVersion: string,
) {
  for (const t of tables) {
    tx.exec(createLiteTableStatement(mapPostgresToLite(t, initialVersion)));
  }
}

function createLiteIndices(tx: Database, indices: IndexSpec[]) {
  for (const index of indices) {
    tx.exec(createLiteIndexStatement(mapPostgresToLiteIndex(index)));
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
) {
  const start = performance.now();
  let rows = 0;
  let flushTime = 0;

  const tableName = liteTableName(table);
  const orderedColumns = Object.entries(table.columns);

  const columnSpecs = orderedColumns.map(([_name, spec]) => spec);
  const selectColumns = orderedColumns.map(([c]) => id(c)).join(',');
  const insertColumns = orderedColumns.map(([c]) => c);
  const insertColumnList = insertColumns.map(c => id(c)).join(',');

  // (?,?,?,?,?)
  const valuesSql =
    insertColumns.length > 0
      ? `(${'?,'.repeat(insertColumns.length - 1)}?)`
      : '()';
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

  const valuesPerRow = columnSpecs.length;
  const valuesPerBatch = valuesPerRow * INSERT_BATCH_SIZE;

  // Preallocate the buffer of values to reduce memory allocation churn.
  const pendingValues: LiteValueType[] = Array.from({
    length: MAX_BUFFERED_ROWS * valuesPerRow,
  });
  let pendingRows = 0;
  let pendingSize = 0;

  function flush() {
    const start = performance.now();
    const flushedRows = pendingRows;
    const flushedSize = pendingSize;

    let l = 0;
    for (; pendingRows > INSERT_BATCH_SIZE; pendingRows -= INSERT_BATCH_SIZE) {
      insertBatchStmt.run(pendingValues.slice(l, (l += valuesPerBatch)));
    }
    // Insert the remaining rows individually.
    for (; pendingRows > 0; pendingRows--) {
      insertStmt.run(pendingValues.slice(l, (l += valuesPerRow)));
    }
    for (let i = 0; i < flushedRows; i++) {
      // Reuse the array and unreference the values to allow GC.
      // This is faster than allocating a new array every time.
      pendingValues[i] = undefined as unknown as LiteValueType;
    }
    pendingSize = 0;
    rows += flushedRows;

    const elapsed = performance.now() - start;
    flushTime += elapsed;
    lc.debug?.(
      `flushed ${flushedRows} ${tableName} rows (${flushedSize} bytes) in ${elapsed.toFixed(3)} ms`,
    );
  }

  lc.info?.(`Starting copy stream of ${tableName}:`, selectStmt);
  const pgParsers = await getTypeParsers(dbClient);
  const parsers = columnSpecs.map(c => {
    const pgParse = pgParsers.getTypeParser(c.typeOID);
    return (val: string) =>
      liteValue(
        pgParse(val) as PostgresValueType,
        c.dataType,
        JSON_STRINGIFIED,
      );
  });

  const tsvParser = new TsvParser();
  let col = 0;

  await pipeline(
    await from.unsafe(`COPY (${selectStmt}) TO STDOUT`).readable(),
    new Writable({
      highWaterMark: BUFFERED_SIZE_THRESHOLD,

      write(
        chunk: Buffer,
        _encoding: string,
        callback: (error?: Error) => void,
      ) {
        try {
          for (const text of tsvParser.parse(chunk)) {
            pendingSize += text === null ? 4 : text.length;
            pendingValues[pendingRows * valuesPerRow + col] =
              text === null ? null : parsers[col](text);

            if (++col === parsers.length) {
              col = 0;
              if (
                ++pendingRows >= MAX_BUFFERED_ROWS - valuesPerRow ||
                pendingSize >= BUFFERED_SIZE_THRESHOLD
              ) {
                flush();
              }
            }
          }
          callback();
        } catch (e) {
          callback(e instanceof Error ? e : new Error(String(e)));
        }
      },

      final: (callback: (error?: Error) => void) => {
        try {
          flush();
          callback();
        } catch (e) {
          callback(e instanceof Error ? e : new Error(String(e)));
        }
      },
    }),
  );

  const elapsed = performance.now() - start;
  lc.info?.(
    `Finished copying ${rows} rows into ${tableName} ` +
      `(flush: ${flushTime.toFixed(3)} ms) (total: ${elapsed.toFixed(3)} ms) `,
  );
  return {rows, flushTime};
}
