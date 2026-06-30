import {mkdtemp, rm} from 'node:fs/promises';
import {platform, tmpdir} from 'node:os';
import {join} from 'node:path';
import {Writable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {assert} from '../../../../../shared/src/asserts.ts';
import type {JSONObject} from '../../../../../shared/src/bigint-json.ts';
import {must} from '../../../../../shared/src/must.ts';
import {equals} from '../../../../../shared/src/set-utils.ts';
import type {DownloadStatus} from '../../../../../zero-events/src/status.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {
  createLiteIndexStatement,
  createLiteTableStatement,
} from '../../../db/create.ts';
import {listIndexes, listTables} from '../../../db/lite-tables.ts';
import * as Mode from '../../../db/mode-enum.ts';
import {
  BinaryCopyParser,
  hasBinaryDecoder,
  makeBinaryDecoder,
  textCastDecoder,
} from '../../../db/pg-copy-binary.ts';
import {TsvParser} from '../../../db/pg-copy.ts';
import {
  mapPostgresToLite,
  mapPostgresToLiteIndex,
} from '../../../db/pg-to-lite.ts';
import {getTypeParsers} from '../../../db/pg-type-parser.ts';
import {runTx} from '../../../db/run-transaction.ts';
import type {IndexSpec, PublishedTableSpec} from '../../../db/specs.ts';
import {importSnapshot, TransactionPool} from '../../../db/transaction-pool.ts';
import {
  JSON_STRINGIFIED,
  liteValue,
  type LiteValueType,
} from '../../../types/lite.ts';
import {liteTableName} from '../../../types/names.ts';
import {PG_15, PG_17} from '../../../types/pg-versions.ts';
import {
  connectPgClient,
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
import {ColumnMetadataStore} from '../../replicator/schema/column-metadata.ts';
import {initReplicationState} from '../../replicator/schema/replication-state.ts';
import {toStateVersionString} from './lsn.ts';
import {createReplicaAndSlot} from './replication-slots.ts';
import {ensureShardSchema} from './schema/init.ts';
import {getPublicationInfo} from './schema/published.ts';
import {
  dropShard,
  getInternalShardConfig,
  initReplica,
  validatePublications,
} from './schema/shard.ts';

export type InitialSyncOptions = {
  tableCopyWorkers: number;
  chunkTargetBytes?: number | undefined;
  maxChunksPerTable?: number | undefined;
  indexThreads?: number | undefined;
  experimentalIndexMode?: 'all' | 'required' | 'none' | 'dedupe' | undefined;
  sampleRate?: number | undefined;
  maxRowsPerTable?: number | undefined;
  profileCopy?: boolean | undefined;
  textCopy?: boolean | undefined;
  replicationSlotFailover?: boolean | undefined;
  /**
   * When set, run initial sync in "shadow" mode for verification: skip all
   * upstream mutations (no replication slot, no addReplica, no dropShard, no
   * slot drop on failure), suppress status events, and optionally sample
   * rows from each table via TABLESAMPLE BERNOULLI + LIMIT. The caller is
   * responsible for providing (and discarding) a throwaway SQLite `tx`.
   */
  shadow?:
    | {
        /** 0 < rate <= 1. When 1, no TABLESAMPLE clause is added. */
        sampleRate: number;
        /**
         * LIMIT N cap appended after TABLESAMPLE. Required: shadow sync is
         * for verification only, so every run must commit to a row budget.
         */
        maxRowsPerTable: number;
      }
    | undefined;
};

/** Server context to store with the initial sync metadata for debugging. */
export type ServerContext = JSONObject;

export async function initialSync(
  lc: LogContext,
  shard: ShardConfig,
  tx: Database,
  upstreamURI: string,
  syncOptions: InitialSyncOptions,
  context: ServerContext,
) {
  if (!ALLOWED_APP_ID_CHARACTERS.test(shard.appID)) {
    throw new Error(
      'The App ID may only consist of lower-case letters, numbers, and the underscore character',
    );
  }
  const {
    tableCopyWorkers,
    chunkTargetBytes = 0,
    maxChunksPerTable = DEFAULT_MAX_CHUNKS_PER_TABLE,
    indexThreads,
    experimentalIndexMode = 'all',
    sampleRate: optionSampleRate,
    maxRowsPerTable: optionMaxRowsPerTable,
    profileCopy,
    textCopy = false,
    replicationSlotFailover = false,
    shadow,
  } = syncOptions;
  const shouldChunk = chunkTargetBytes > 0 && shadow === undefined;
  const copyProfiler = profileCopy ? await CpuProfiler.connect() : null;
  const sql = await connectPgClient(lc, upstreamURI, 'initial-sync');
  // Replication session is only needed to create a replication slot in the
  // real path. In shadow mode we export a snapshot on a normal connection
  // instead, so no replication session is opened.
  const replicationSession = shadow
    ? undefined
    : pgClient(lc, upstreamURI, 'initial-sync-replication-session', {
        ['fetch_types']: false, // Necessary for the streaming protocol
        connection: {replication: 'database'}, // https://www.postgresql.org/docs/current/protocol-replication.html
      });

  const replicaID = Date.now().toString();
  let slotName: string | undefined; // undefined === shadow
  const statusPublisher = ReplicationStatusPublisher.forRunningTransaction(
    tx,
    shadow ? async () => {} : undefined,
  ).publish(lc, 'Initializing');
  let releaseShadowSnapshot: (() => Promise<void>) | undefined;
  try {
    const pgVersion = await checkUpstreamConfig(sql);

    // In shadow mode we assume the shard is already initialized and just
    // read back the existing publications. `ensurePublishedTables` would
    // otherwise run DDL and potentially call `dropShard`, which must never
    // happen during a shadow run.
    const {publications} = shadow
      ? await getInternalShardConfig(sql, shard)
      : await ensurePublishedTables(lc, sql, shard);
    lc.info?.(`Upstream is setup with publications [${publications}]`);

    const {database, host} = sql.options;
    lc.info?.(
      shadow
        ? `acquiring exported snapshot on ${database}@${host} (shadow mode)`
        : `opening replication session to ${database}@${host}`,
    );

    let snapshot: string;
    let lsn: string;

    if (shadow) {
      const acquired = await acquireExportedSnapshotForShadowSync(
        lc,
        upstreamURI,
      );
      snapshot = acquired.snapshot;
      lsn = acquired.lsn;
      releaseShadowSnapshot = acquired.release;
    } else {
      const slot = await createReplicaAndSlot(
        lc,
        sql,
        must(replicationSession),
        shard,
        replicaID,
        replicationSlotFailover && pgVersion >= PG_17,
      );
      snapshot = slot.snapshot_name;
      lsn = slot.consistent_point;
      slotName = slot.slot_name;
    }

    const initialVersion = toStateVersionString(lsn);

    initReplicationState(tx, publications, initialVersion, context);

    // Run up to MAX_WORKERS to copy of tables at the replication slot's snapshot.
    const start = performance.now();
    // Retrieve the published schema at the consistent_point.
    const published = await runTx(
      sql,
      async tx => {
        await tx.unsafe(/* sql*/ `SET TRANSACTION SNAPSHOT '${snapshot}'`);
        return getPublicationInfo(tx, publications);
      },
      {mode: Mode.READONLY},
    );
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
        ? shouldChunk
          ? Math.max(tableCopyWorkers, numTables)
          : numTables
        : shouldChunk
          ? tableCopyWorkers
          : Math.min(tableCopyWorkers, numTables);

    const copyPool = await connectPgClient(
      lc,
      upstreamURI,
      'initial-sync-copy-worker',
      {
        max: numWorkers,
        ['max_lifetime']: 120 * 60, // set a long (2h) limit for COPY streaming
      },
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
      const sampleRate = shadow?.sampleRate ?? optionSampleRate;
      const maxRowsPerTable = shadow?.maxRowsPerTable ?? optionMaxRowsPerTable;
      const downloads = await Promise.all(
        tables.map(spec =>
          copiers.processReadTask((db, lc) =>
            getInitialDownloadState(
              lc,
              db,
              spec,
              shadow !== undefined,
              shouldChunk,
            ),
          ),
        ),
      );
      statusPublisher.publish(
        lc,
        'Initializing',
        `Copying ${numTables} upstream tables at version ${initialVersion}`,
        5000,
        () => ({downloadStatus: downloads.map(({status}) => status)}),
      );

      void copyProfiler?.start();
      const copyTasks = sortCopyTasksForInitialCopy(
        buildCopyTasks(
          lc,
          downloads,
          shouldChunk ? chunkTargetBytes : 0,
          maxChunksPerTable,
          shadow !== undefined,
        ),
      );
      lc.info?.(
        `initial-sync copy plan: tables=${numTables} tasks=${copyTasks.length} ` +
          `workers=${numWorkers} chunkTargetBytes=${shouldChunk ? chunkTargetBytes : 0} ` +
          `maxChunksPerTable=${maxChunksPerTable}`,
      );

      const rowCounts = await Promise.all(
        copyTasks.map(task =>
          copiers.processReadTask((db, lc) =>
            copy(
              lc,
              task,
              copyPool,
              db,
              tx,
              textCopy,
              sampleRate,
              maxRowsPerTable,
            ),
          ),
        ),
      );
      void copyProfiler?.stopAndDispose(lc, 'initial-copy');
      copiers.setDone();

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
      if (indexThreads !== undefined) {
        lc.info?.(`Setting SQLite PRAGMA threads=${indexThreads}`);
        tx.pragma(`threads = ${indexThreads}`);
      }
      createLiteIndices(
        lc,
        tx,
        filterInitialSyncIndexes(lc, indexes, experimentalIndexMode),
      );
      const index = performance.now() - indexStart;
      lc.info?.(`Created indexes (${index.toFixed(3)} ms)`);

      if (slotName && replicaID) {
        await initReplica(sql, shard, replicaID, published, context);
      } else {
        assert(shadow, 'expected to be in shadow sync if there is no slotName');
        const rowsByTable = new Map<string, number>();
        for (const {table, rows} of rowCounts) {
          rowsByTable.set(table, (rowsByTable.get(table) ?? 0) + rows);
        }
        verifyShadowReplica(lc, tx, published, rowsByTable);
      }

      const elapsed = performance.now() - start;
      lc.info?.(
        `Synced ${total.rows.toLocaleString()} rows of ${numTables} tables in ${publications} up to ${lsn} ` +
          `(flush: ${total.flushTime.toFixed(3)}, index: ${index.toFixed(3)}, total: ${elapsed.toFixed(3)} ms)`,
      );
    } finally {
      // All meaningful errors are handled at the processReadTask() call site.
      void copyPool.end().catch(e => lc.warn?.(`Error closing copyPool`, e));
    }
  } catch (e) {
    if (slotName) {
      // If initial-sync did not succeed, make a best effort to drop the
      // orphaned replication slot to avoid running out of slots in
      // pathological cases that result in repeated failures.
      lc.warn?.(`dropping replication slot ${slotName}`, e);
      await sql`
        SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
          WHERE slot_name = ${slotName};
      `.catch(e => lc.warn?.(`Unable to drop replication slot ${slotName}`, e));
    }
    await statusPublisher.publishAndThrowError(lc, 'Initializing', e);
  } finally {
    statusPublisher.stop();
    if (releaseShadowSnapshot) {
      await releaseShadowSnapshot().catch(e =>
        lc.warn?.(`Error releasing shadow snapshot`, e),
      );
    }
    if (replicationSession) {
      await replicationSession.end();
    }
    await sql.end();
  }
}

export type ShadowSyncOptions = {
  sampleRate: number;
  maxRowsPerTable: number;
  /**
   * Parent directory for the throwaway SQLite replica. Defaults to the OS
   * tmpdir. Primarily for tests that need to isolate the scratch directory.
   */
  parentDir?: string | undefined;
};

/**
 * Exercises the initial-sync code path against a sample of rows from every
 * published table, writing into a throwaway SQLite database that is deleted
 * when the run ends. Produces zero upstream mutations: no replication slot,
 * no `addReplica`, no `dropShard`, no status events.
 *
 * Intended to be invoked periodically so that if a customer ever needs a
 * full reset, we have recent confidence that `initialSync` still works.
 * The shard must already be initialized upstream.
 */
export async function shadowInitialSync(
  lc: LogContext,
  shard: ShardConfig,
  upstreamURI: string,
  shadow: ShadowSyncOptions,
  context: ServerContext,
  syncOptions?: Pick<InitialSyncOptions, 'textCopy'>,
): Promise<void> {
  const dir = await mkdtemp(
    join(shadow.parentDir ?? tmpdir(), 'zero-shadow-sync-'),
  );
  const dbPath = join(dir, 'shadow-replica.db');
  const db = new Database(lc, dbPath);
  try {
    await initialSync(
      lc,
      shard,
      db,
      upstreamURI,
      {
        // Shadow sync copies small samples, so one worker is plenty —
        // no reason to burn additional upstream connections.
        tableCopyWorkers: 1,
        textCopy: syncOptions?.textCopy,
        shadow,
      },
      context,
    );
  } finally {
    try {
      db.close();
    } catch (e) {
      lc.warn?.(`Error closing shadow replica db`, e);
    }
    await rm(dir, {recursive: true, force: true}).catch(e =>
      lc.warn?.(`Error cleaning up shadow replica dir ${dir}`, e),
    );
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
  if (version < PG_15) {
    throw new Error(
      `Must be running Postgres 15 or higher (currently: "${version}")`,
    );
  }
  return version;
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
    let valid = false;
    const nonInternalPublications = publications.filter(
      p => !p.startsWith('_'),
    );
    const exists = await sql`
      SELECT pubname FROM pg_publication WHERE pubname IN ${sql(publications)}
      `.values();
    if (exists.length !== publications.length) {
      lc.warn?.(
        `some configured publications [${publications}] are missing: ` +
          `[${exists.flat()}]. resyncing`,
      );
    } else if (
      !equals(new Set(shard.publications), new Set(nonInternalPublications))
    ) {
      lc.warn?.(
        `requested publications [${shard.publications}] differ from previous` +
          `publications [${nonInternalPublications}]. resyncing`,
      );
    } else {
      valid = true;
    }
    if (!valid) {
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
  const tableCopiers = new TransactionPool(lc, {
    mode: Mode.READONLY,
    init,
    initialWorkers: numWorkers,
  });
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

/**
 * Shadow-mode alternative to `createReplicationSlot`: opens a dedicated
 * READ ONLY REPEATABLE READ transaction on a normal connection, exports the
 * snapshot and captures the current WAL LSN, then holds the transaction
 * open until `release()` is called. The held transaction keeps the snapshot
 * importable by the table-copy workers for the duration of the COPY phase.
 *
 * Idle-in-transaction timeout is disabled locally so the exporter doesn't
 * get killed while workers are still importing.
 */
async function acquireExportedSnapshotForShadowSync(
  lc: LogContext,
  upstreamURI: string,
): Promise<{
  snapshot: string;
  lsn: string;
  release: () => Promise<void>;
}> {
  const holder = await connectPgClient(
    lc,
    upstreamURI,
    'shadow-initial-sync-snapshot',
    {
      max: 1,
    },
  );
  const ready = resolver<{snapshot: string; lsn: string}>();
  const release = resolver<void>();
  const held = holder
    .begin(Mode.READONLY, async tx => {
      await tx`SET LOCAL idle_in_transaction_session_timeout = 0`.execute();
      const [row] = await tx<{snapshot: string; lsn: string}[]>`
        SELECT pg_export_snapshot() AS snapshot,
               pg_current_wal_lsn()::text AS lsn`;
      ready.resolve(row);
      await release.promise;
    })
    .catch(e => ready.reject(e));

  let snapshot: string;
  let lsn: string;
  try {
    ({snapshot, lsn} = await ready.promise);
  } catch (e) {
    await holder
      .end()
      .catch(err =>
        lc.warn?.(`Error ending shadow snapshot holder after failure`, err),
      );
    throw e;
  }
  lc.info?.(
    `Exported snapshot ${snapshot} at LSN ${lsn} (shadow initial sync)`,
  );
  return {
    snapshot,
    lsn,
    release: async () => {
      release.resolve();
      try {
        await held;
      } catch (e) {
        lc.warn?.(`snapshot holder transaction ended with error`, e);
      }
      await holder.end();
    },
  };
}

function createLiteTables(
  tx: Database,
  tables: PublishedTableSpec[],
  initialVersion: string,
) {
  // TODO: Figure out how to reuse the ChangeProcessor here to avoid
  //       duplicating the ColumnMetadata logic.
  const columnMetadata = must(ColumnMetadataStore.getInstance(tx));
  for (const t of tables) {
    tx.exec(createLiteTableStatement(mapPostgresToLite(t, initialVersion)));
    const tableName = liteTableName(t);
    for (const [colName, colSpec] of Object.entries(t.columns)) {
      columnMetadata.insert(tableName, colName, colSpec);
    }
  }
}

function filterInitialSyncIndexes<
  T extends IndexSpec & {isPrimaryKey?: boolean | undefined},
>(
  lc: LogContext,
  indices: T[],
  mode: InitialSyncOptions['experimentalIndexMode'],
): T[] {
  switch (mode) {
    case 'all':
      return indices;
    case 'none':
      lc.warn?.(`Skipping all ${indices.length} initial-sync indexes`);
      return [];
    case 'required': {
      const filtered = indices.filter(index => index.unique || index.isPrimaryKey);
      lc.warn?.(
        `Using required-only initial-sync indexes: ${filtered.length}/${indices.length}`,
      );
      return filtered;
    }
    case 'dedupe': {
      const seen = new Set<string>();
      const filtered: T[] = [];
      for (const index of indices) {
        const key = `${index.tableName}:${Object.entries(index.columns)
          .map(([col, dir]) => `${col}:${dir}`)
          .join(',')}`;
        if (!seen.has(key) || index.unique || index.isPrimaryKey) {
          seen.add(key);
          filtered.push(index);
        } else {
          lc.warn?.(`Skipping duplicate-equivalent index ${index.name}`);
        }
      }
      lc.warn?.(
        `Using deduped initial-sync indexes: ${filtered.length}/${indices.length}`,
      );
      return filtered;
    }
    case undefined:
      return indices;
  }
}

function createLiteIndices(lc: LogContext, tx: Database, indices: IndexSpec[]) {
  for (const [i, index] of indices.entries()) {
    const stmt = createLiteIndexStatement(mapPostgresToLiteIndex(index));
    lc.info?.(`Creating index ${i + 1}/${indices.length}: ${stmt}`);
    const start = performance.now();
    tx.exec(stmt);
    lc.info?.(
      `Created index ${i + 1}/${indices.length} ` +
        `(${(performance.now() - start).toFixed(3)} ms): ${stmt}`,
    );
  }
}

/**
 * Runs structural assertions over a just-synced replica and throws if any
 * fail. Only called in shadow mode — a successful return means the replica
 * is schema-complete, row-count consistent, and its column metadata is in
 * sync with its lite schema.
 *
 * Note: this intentionally does NOT verify ZQL-queryability. Tables that
 * `computeZqlSpecs` drops (no PK / no all-NOT-NULL unique index, unsupported
 * column types, etc.) are silently skipped in production too — there's
 * nothing shadow-specific about them, so failing here would diverge from
 * prod over an upstream-schema condition prod accepts.
 *
 * Exported for testing.
 */
export function verifyShadowReplica(
  lc: LogContext,
  db: Database,
  published: {tables: PublishedTableSpec[]; indexes: IndexSpec[]},
  rowsByTable: ReadonlyMap<string, number>,
): void {
  const issues: string[] = [];
  let columnsChecked = 0;
  let rowsChecked = 0;

  // 1. Schema completeness: every published table exists in the replica
  //    with at least the expected column set.
  const liteTables = listTables(db);
  const liteTableByName = new Map(liteTables.map(t => [t.name, t]));
  for (const pt of published.tables) {
    const name = liteTableName(pt);
    const lite = liteTableByName.get(name);
    if (!lite) {
      issues.push(`missing table in replica: ${name}`);
      continue;
    }
    for (const col of Object.keys(pt.columns)) {
      columnsChecked++;
      if (!(col in lite.columns)) {
        issues.push(`column missing in replica table ${name}: ${col}`);
      }
    }
  }

  //    Every published index exists in the replica.
  const liteIndexNames = new Set(listIndexes(db).map(i => i.name));
  for (const ix of published.indexes) {
    const mapped = mapPostgresToLiteIndex(ix);
    if (!liteIndexNames.has(mapped.name)) {
      issues.push(
        `missing index in replica: ${mapped.name} on ${mapped.tableName}`,
      );
    }
  }

  // 2. Row counts: SQLite COUNT(*) matches the in-memory copy counter.
  for (const [table, expected] of rowsByTable) {
    try {
      const [row] = db
        .prepare(`SELECT COUNT(*) as count FROM "${table}"`)
        .all<{count: number}>();
      if (row.count !== expected) {
        issues.push(
          `row count mismatch for table ${table}: ` +
            `copy counter reported ${expected}, replica has ${row.count}`,
        );
      } else {
        rowsChecked += row.count;
      }
    } catch (e) {
      issues.push(`could not count rows in table ${table}: ${String(e)}`);
    }
  }

  // 3. Column metadata: every published column has a _zero.column_metadata row.
  const meta = must(ColumnMetadataStore.getInstance(db));
  for (const pt of published.tables) {
    const name = liteTableName(pt);
    const rows = meta.getTable(name);
    for (const col of Object.keys(pt.columns)) {
      if (!rows.has(col)) {
        issues.push(`missing column_metadata row for ${name}.${col}`);
      }
    }
  }

  if (issues.length) {
    throw new Error(
      `Shadow replica verification failed (${issues.length} issue(s)):\n` +
        issues.map(i => `  - ${i}`).join('\n'),
    );
  }

  lc.info?.(
    `Shadow replica verification passed: ` +
      `${published.tables.length} tables, ` +
      `${published.indexes.length} indexes, ` +
      `${columnsChecked} columns, ` +
      `${rowsChecked.toLocaleString()} rows`,
  );
}

// Verified empirically that batches of 50 seem to be the sweet spot,
// similar to the report in https://sqlite.org/forum/forumpost/8878a512d3652655
//
// Exported for testing.
export const INSERT_BATCH_SIZE = 50;

// Bound CTID chunk fanout so one very large table cannot create an unbounded
// number of COPY tasks.
export const DEFAULT_MAX_CHUNKS_PER_TABLE = 64;
const COPY_BYTES_ESTIMATE_SAMPLE_PAGES = 16;

const MB = 1024 * 1024;
const MAX_BUFFERED_ROWS = 10_000;
const BUFFERED_SIZE_THRESHOLD = 8 * MB;

export type DownloadStatements = {
  select: string;
  getTotalRows: string;
  getTotalBytes: string;
};

/**
 * Produces ` TABLESAMPLE BERNOULLI(n)` when `sampleRate` is < 1, else `''`.
 * Row-level Bernoulli sampling is used (rather than SYSTEM) because it
 * produces a more uniform sample and, unlike SYSTEM, still returns rows
 * for small tables at low rates.
 */
function tableSampleClause(sampleRate: number | undefined): string {
  if (sampleRate === undefined || sampleRate >= 1) {
    return '';
  }
  // Round away float noise (e.g. 0.3 * 100 = 30.000000000000004) while still
  // preserving sub-integer rates like 0.001 (= 0.1%).
  const pct = parseFloat((sampleRate * 100).toFixed(6));
  return /*sql*/ ` TABLESAMPLE BERNOULLI(${pct})`;
}

function limitClause(maxRowsPerTable: number | undefined): string {
  return maxRowsPerTable !== undefined
    ? /*sql*/ ` LIMIT ${maxRowsPerTable}`
    : '';
}

/**
 * Returns the SELECT column expressions for binary COPY, casting columns
 * without a known binary decoder to `::text`.
 */
export function makeBinarySelectExprs(
  table: PublishedTableSpec,
  cols: string[],
): string[] {
  return cols.map(col => {
    const spec = table.columns[col];
    return hasBinaryDecoder(spec) ? id(col) : `${id(col)}::text`;
  });
}

export function makeDownloadStatements(
  table: PublishedTableSpec,
  cols: string[],
  sampleRate?: number | undefined,
  maxRowsPerTable?: number | undefined,
  selectExprs?: string[] | undefined,
  extraPredicate?: string | undefined,
): DownloadStatements {
  const filterConditions = Object.values(table.publications)
    .map(({rowFilter}) => rowFilter)
    .filter((f): f is string => !!f); // remove nulls
  const where = whereClause(filterConditions, extraPredicate);
  const sample = tableSampleClause(sampleRate);
  const limit = limitClause(maxRowsPerTable);
  const fromTable = /*sql*/ `FROM ${id(table.schema)}.${id(table.name)}${sample} ${where}`;
  const select = /*sql*/ `SELECT ${(selectExprs ?? cols.map(id)).join(',')} ${fromTable}${limit}`;
  if (limit) {
    // With LIMIT, wrap counts/sums in a subquery so they reflect the
    // capped rowset rather than the full (sampled) table.
    const bytesExpr = cols
      .map(col => `COALESCE(pg_column_size(${id(col)}), 0)`)
      .join(' + ');
    return {
      select,
      getTotalRows: /*sql*/ `SELECT COUNT(*)::bigint AS "totalRows" FROM (SELECT 1 AS _ ${fromTable}${limit}) s`,
      getTotalBytes: /*sql*/ `SELECT COALESCE(SUM(b), 0)::bigint AS "totalBytes" FROM (SELECT (${bytesExpr}) AS b ${fromTable}${limit}) s`,
    };
  }
  const totalBytes = `(${cols.map(col => `SUM(COALESCE(pg_column_size(${id(col)}), 0))`).join(' + ')})`;
  return {
    select,
    getTotalRows: /*sql*/ `SELECT COUNT(*) AS "totalRows" ${fromTable}`,
    getTotalBytes: /*sql*/ `SELECT ${totalBytes} AS "totalBytes" ${fromTable}`,
  };
}

function whereClause(
  filterConditions: string[],
  extraPredicate: string | undefined,
): string {
  if (!extraPredicate) {
    return filterConditions.length === 0
      ? ''
      : /*sql*/ `WHERE ${filterConditions.join(' OR ')}`;
  }

  const predicates: string[] = [];
  if (filterConditions.length > 0) {
    const rowFilter = filterConditions.join(' OR ');
    predicates.push(
      filterConditions.length > 1 || extraPredicate
        ? `(${rowFilter})`
        : rowFilter,
    );
  }
  if (extraPredicate) {
    predicates.push(`(${extraPredicate})`);
  }
  return predicates.length > 0
    ? /*sql*/ `WHERE ${predicates.join(' AND ')}`
    : '';
}

export type DownloadState = {
  spec: PublishedTableSpec;
  status: DownloadStatus;
  copyBytesEstimate: number | undefined;
  heapPages: number;
};

type CtidChunk = {
  index: number;
  total: number;
  startBlock: number;
  endBlock: number;
};

export type CopyTask = {
  download: DownloadState;
  estimatedBytes: number;
  chunk?: CtidChunk | undefined;
};

export function sortDownloadsForInitialCopy<
  T extends {
    copyBytesEstimate?: number | undefined;
    status: {totalBytes?: number | undefined};
  },
>(downloads: readonly T[]): T[] {
  return downloads.toSorted(
    (a, b) => copyPlanningBytes(b) - copyPlanningBytes(a),
  );
}

function copyPlanningBytes(download: {
  copyBytesEstimate?: number | undefined;
  status: {totalBytes?: number | undefined};
}): number {
  return download.copyBytesEstimate ?? download.status.totalBytes ?? 0;
}

export function sortCopyTasksForInitialCopy<T extends {estimatedBytes: number}>(
  copyTasks: readonly T[],
): T[] {
  return copyTasks.toSorted((a, b) => b.estimatedBytes - a.estimatedBytes);
}

export function buildCopyTasks(
  lc: LogContext,
  downloads: readonly DownloadState[],
  chunkTargetBytes: number,
  maxChunksPerTable: number,
  shadow: boolean,
): CopyTask[] {
  const tasks: CopyTask[] = [];
  for (const download of downloads) {
    const estimatedCopyBytes = copyPlanningBytes(download);
    const heapPages = Math.floor(download.heapPages);
    if (
      shadow ||
      chunkTargetBytes <= 0 ||
      estimatedCopyBytes <= chunkTargetBytes ||
      heapPages <= 1
    ) {
      tasks.push({download, estimatedBytes: estimatedCopyBytes});
      continue;
    }

    const chunkCount = Math.min(
      Math.ceil(estimatedCopyBytes / chunkTargetBytes),
      maxChunksPerTable,
      heapPages,
    );
    if (chunkCount <= 1) {
      tasks.push({download, estimatedBytes: estimatedCopyBytes});
      continue;
    }

    lc.info?.(
      `chunking table ${download.status.table}: estimatedCopyBytes=${estimatedCopyBytes} ` +
        `totalBytes=${download.status.totalBytes ?? 'unknown'} ` +
        `heapPages=${heapPages} chunks=${chunkCount}`,
    );
    for (let i = 0; i < chunkCount; i++) {
      const startBlock = Math.floor((heapPages * i) / chunkCount);
      const endBlock = Math.floor((heapPages * (i + 1)) / chunkCount);
      tasks.push({
        download,
        estimatedBytes:
          (estimatedCopyBytes * (endBlock - startBlock)) / heapPages,
        chunk: {
          index: i + 1,
          total: chunkCount,
          startBlock,
          endBlock,
        },
      });
    }
  }
  return tasks;
}

// Exported for testing.
export async function getInitialDownloadState(
  lc: LogContext,
  sql: PostgresDB,
  spec: PublishedTableSpec,
  skipTotals: boolean,
  estimateCopyBytes = true,
): Promise<DownloadState> {
  const start = performance.now();
  const table = liteTableName(spec);
  const columns = Object.keys(spec.columns);
  if (skipTotals) {
    // Shadow sync suppresses status events, so the pg_class
    // estimates would be computed and thrown away.
    return {
      spec,
      status: {table, columns, rows: 0, totalRows: 0, totalBytes: 0},
      copyBytesEstimate: 0,
      heapPages: 0,
    };
  }
  // Use pg_class plus a bounded logical-width sample instead of expensive
  // COUNT(*) and SUM(pg_column_size(...)) full table scans. `totalBytes`
  // remains a physical progress estimate, while `copyBytesEstimate` is a
  // logical COPY-work proxy used only for CTID chunk planning.
  const qualifiedName = `${id(spec.schema)}.${id(spec.name)}`;
  const estimateResult = await sql<
    {totalRows: number; totalBytes: number; heapPages: number}[]
  >`SELECT GREATEST(reltuples, 0)::float8 AS "totalRows",
         pg_table_size(oid)::float8 AS "totalBytes",
         CEIL(pg_relation_size(oid)::float8 / current_setting('block_size')::int)::float8 AS "heapPages"
    FROM pg_class
    WHERE oid = ${qualifiedName}::regclass`;

  const {totalRows, totalBytes, heapPages} = estimateResult[0] ?? {
    totalRows: 0,
    totalBytes: 0,
    heapPages: 0,
  };
  const copyBytesEstimate = estimateCopyBytes
    ? await estimateLogicalCopyBytes(sql, spec, columns, totalBytes, heapPages)
    : undefined;

  const state: DownloadState = {
    spec,
    status: {
      table,
      columns,
      rows: 0,
      totalRows,
      totalBytes,
    },
    copyBytesEstimate,
    heapPages,
  };
  const elapsed = (performance.now() - start).toFixed(3);
  lc.info?.(`Computed initial download state for ${table} (${elapsed} ms)`, {
    state: state.status,
    copyBytesEstimate,
    heapPages,
  });
  return state;
}

async function estimateLogicalCopyBytes(
  sql: PostgresDB,
  spec: PublishedTableSpec,
  columns: string[],
  totalBytes: number,
  heapPages: number,
): Promise<number> {
  const physicalBytes = totalBytes ?? 0;
  if (columns.length === 0 || heapPages <= 0) {
    return physicalBytes;
  }

  const sampledBlocks = sampledHeapBlocks(heapPages);
  if (sampledBlocks.length === 0) {
    return physicalBytes;
  }

  const rowFilters = Object.values(spec.publications)
    .map(({rowFilter}) => rowFilter)
    .filter((f): f is string => !!f);
  const sample = await sql.unsafe<{sampleBytes: number}[]>(/*sql*/ `
    SELECT COALESCE(SUM(${logicalRowBytesExpression(columns)}), 0)::float8 AS "sampleBytes"
    FROM ${id(spec.schema)}.${id(spec.name)}
    ${whereClause(rowFilters, ctidBlocksPredicate(sampledBlocks))}`);
  const sampleBytes = sample[0]?.sampleBytes ?? 0;
  return Math.max(
    physicalBytes,
    (sampleBytes / sampledBlocks.length) * heapPages,
  );
}

export function sampledHeapBlocks(heapPages: number): number[] {
  const samplePages = Math.min(
    Math.floor(heapPages),
    COPY_BYTES_ESTIMATE_SAMPLE_PAGES,
  );
  if (samplePages <= 0) {
    return [];
  }
  return Array.from({length: samplePages}, (_, i) =>
    Math.min(Math.floor((heapPages * (i + 0.5)) / samplePages), heapPages - 1),
  );
}

function logicalRowBytesExpression(columns: string[]): string {
  return columns
    .map(col => `COALESCE(octet_length(${id(col)}::text), 0)`)
    .join(' + ');
}

function ctidBlocksPredicate(blocks: readonly number[]): string {
  return blocks
    .map(
      block =>
        /*sql*/ `(ctid >= '(${block},0)'::tid AND ctid < '(${block + 1},0)'::tid)`,
    )
    .join(' OR ');
}

function copy(
  lc: LogContext,
  {download: {spec: table, status}, chunk}: CopyTask,
  dbClient: PostgresDB,
  from: PostgresTransaction,
  to: Database,
  textCopy: boolean,
  sampleRate?: number | undefined,
  maxRowsPerTable?: number | undefined,
) {
  if (textCopy) {
    return copyText(
      lc,
      table,
      status,
      dbClient,
      from,
      to,
      sampleRate,
      maxRowsPerTable,
      chunk,
    );
  }
  return copyBinary(
    lc,
    table,
    status,
    from,
    to,
    sampleRate,
    maxRowsPerTable,
    chunk,
  );
}

async function copyBinary(
  lc: LogContext,
  table: PublishedTableSpec,
  status: DownloadStatus,
  from: PostgresTransaction,
  to: Database,
  sampleRate?: number | undefined,
  maxRowsPerTable?: number | undefined,
  chunk?: CtidChunk | undefined,
) {
  const start = performance.now();
  let flushTime = 0;
  let copiedRows = 0;

  const tableName = liteTableName(table);
  const copyName = copyTaskName(tableName, chunk);
  const orderedColumns = Object.entries(table.columns);

  const columnNames = orderedColumns.map(([c]) => c);
  const columnSpecs = orderedColumns.map(([_name, spec]) => spec);
  const insertColumnList = columnNames.map(c => id(c)).join(',');

  const valuesSql =
    columnNames.length > 0 ? `(${'?,'.repeat(columnNames.length - 1)}?)` : '()';
  const insertSql = /*sql*/ `
    INSERT INTO "${tableName}" (${insertColumnList}) VALUES ${valuesSql}`;
  const insertStmt = to.prepare(insertSql);
  const insertBatchStmt = to.prepare(
    insertSql + `,${valuesSql}`.repeat(INSERT_BATCH_SIZE - 1),
  );

  // Build SELECT with ::text casts for columns without a known binary decoder.
  const select = makeDownloadStatements(
    table,
    columnNames,
    sampleRate,
    maxRowsPerTable,
    makeBinarySelectExprs(table, columnNames),
    ctidRangePredicate(chunk),
  ).select;

  const decoders = orderedColumns.map(([, spec]) =>
    hasBinaryDecoder(spec) ? makeBinaryDecoder(spec) : textCastDecoder,
  );

  const valuesPerRow = columnSpecs.length;
  const valuesPerBatch = valuesPerRow * INSERT_BATCH_SIZE;

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
    for (; pendingRows > 0; pendingRows--) {
      insertStmt.run(pendingValues.slice(l, (l += valuesPerRow)));
    }
    const flushedValues = flushedRows * valuesPerRow;
    for (let i = 0; i < flushedValues; i++) {
      pendingValues[i] = undefined as unknown as LiteValueType;
    }
    pendingSize = 0;
    status.rows += flushedRows;
    copiedRows += flushedRows;

    const elapsed = performance.now() - start;
    flushTime += elapsed;
    lc.debug?.(
      `flushed ${flushedRows} ${tableName} rows (${flushedSize} bytes) in ${elapsed.toFixed(3)} ms`,
    );
  }

  const binaryParser = new BinaryCopyParser();
  let col = 0;

  lc.info?.(`Starting binary copy stream of ${copyName}:`, select);

  await pipeline(
    await from
      .unsafe(`COPY (${select}) TO STDOUT WITH (FORMAT binary)`)
      .readable(),
    new Writable({
      highWaterMark: BUFFERED_SIZE_THRESHOLD,

      write(
        chunk: Buffer,
        _encoding: string,
        callback: (error?: Error) => void,
      ) {
        try {
          for (const fieldBuf of binaryParser.parse(chunk)) {
            pendingSize += fieldBuf === null ? 4 : fieldBuf.length;
            pendingValues[pendingRows * valuesPerRow + col] =
              fieldBuf === null ? null : decoders[col](fieldBuf);

            if (++col === decoders.length) {
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
    `Finished copying ${copiedRows} rows into ${copyName} ` +
      `(flush: ${flushTime.toFixed(3)} ms) (total: ${elapsed.toFixed(3)} ms) `,
  );
  return {table: tableName, rows: copiedRows, flushTime};
}

async function copyText(
  lc: LogContext,
  table: PublishedTableSpec,
  status: DownloadStatus,
  dbClient: PostgresDB,
  from: PostgresTransaction,
  to: Database,
  sampleRate?: number | undefined,
  maxRowsPerTable?: number | undefined,
  chunk?: CtidChunk | undefined,
) {
  const start = performance.now();
  let flushTime = 0;
  let copiedRows = 0;

  const tableName = liteTableName(table);
  const copyName = copyTaskName(tableName, chunk);
  const orderedColumns = Object.entries(table.columns);

  const columnNames = orderedColumns.map(([c]) => c);
  const columnSpecs = orderedColumns.map(([_name, spec]) => spec);
  const insertColumnList = columnNames.map(c => id(c)).join(',');

  const valuesSql =
    columnNames.length > 0 ? `(${'?,'.repeat(columnNames.length - 1)}?)` : '()';
  const insertSql = /*sql*/ `
    INSERT INTO "${tableName}" (${insertColumnList}) VALUES ${valuesSql}`;
  const insertStmt = to.prepare(insertSql);
  const insertBatchStmt = to.prepare(
    insertSql + `,${valuesSql}`.repeat(INSERT_BATCH_SIZE - 1),
  );

  const {select} = makeDownloadStatements(
    table,
    columnNames,
    sampleRate,
    maxRowsPerTable,
    undefined,
    ctidRangePredicate(chunk),
  );
  const valuesPerRow = columnSpecs.length;
  const valuesPerBatch = valuesPerRow * INSERT_BATCH_SIZE;

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
    for (; pendingRows > 0; pendingRows--) {
      insertStmt.run(pendingValues.slice(l, (l += valuesPerRow)));
    }
    const flushedValues = flushedRows * valuesPerRow;
    for (let i = 0; i < flushedValues; i++) {
      pendingValues[i] = undefined as unknown as LiteValueType;
    }
    pendingSize = 0;
    status.rows += flushedRows;
    copiedRows += flushedRows;

    const elapsed = performance.now() - start;
    flushTime += elapsed;
    lc.debug?.(
      `flushed ${flushedRows} ${tableName} rows (${flushedSize} bytes) in ${elapsed.toFixed(3)} ms`,
    );
  }

  lc.info?.(`Starting text copy stream of ${copyName}:`, select);
  const pgParsers = await getTypeParsers(dbClient, {returnJsonAsString: true});
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
    await from.unsafe(`COPY (${select}) TO STDOUT`).readable(),
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
    `Finished copying ${copiedRows} rows into ${copyName} ` +
      `(flush: ${flushTime.toFixed(3)} ms) (total: ${elapsed.toFixed(3)} ms) `,
  );
  return {table: tableName, rows: copiedRows, flushTime};
}

function ctidRangePredicate(chunk: CtidChunk | undefined): string | undefined {
  return chunk
    ? `ctid >= '(${chunk.startBlock},0)'::tid AND ctid < '(${chunk.endBlock},0)'::tid`
    : undefined;
}

function copyTaskName(tableName: string, chunk: CtidChunk | undefined): string {
  return chunk
    ? `${tableName} chunk ${chunk.index}/${chunk.total} blocks=${chunk.startBlock}-${chunk.endBlock}`
    : tableName;
}
