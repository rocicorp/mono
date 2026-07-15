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
  getOrCreateCounter,
  getOrCreateHistogram,
} from '../../../observability/metrics.ts';
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
    profileCopy,
    textCopy = false,
    replicationSlotFailover = false,
    shadow,
  } = syncOptions;
  const syncMode: InitialSyncMode = shadow ? 'shadow' : 'initial';
  const copyFormat: CopyFormat = textCopy ? 'text' : 'binary';
  const start = performance.now();
  let sql: PostgresDB | undefined;
  let replicationSession: PostgresDB | undefined;
  const replicaID = Date.now().toString();
  let slotName: string | undefined; // undefined === shadow
  const statusPublisher = ReplicationStatusPublisher.forRunningTransaction(
    tx,
    shadow ? async () => {} : undefined,
  ).publish(lc, 'Initializing');
  let releaseShadowSnapshot: (() => Promise<void>) | undefined;
  try {
    const copyProfiler = profileCopy ? await CpuProfiler.connect() : null;
    sql = await connectPgClient(lc, upstreamURI, 'initial-sync');
    // Replication session is only needed to create a replication slot in the
    // real path. In shadow mode we export a snapshot on a normal connection
    // instead, so no replication session is opened.
    replicationSession = shadow
      ? undefined
      : pgClient(lc, upstreamURI, 'initial-sync-replication-session', {
          ['fetch_types']: false, // Necessary for the streaming protocol
          connection: {replication: 'database'}, // https://www.postgresql.org/docs/current/protocol-replication.html
        });

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
        ? numTables
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
      const sampleRate = shadow?.sampleRate;
      const maxRowsPerTable = shadow?.maxRowsPerTable;
      const downloads = await Promise.all(
        tables.map(spec =>
          copiers.processReadTask((db, lc) =>
            getInitialDownloadState(lc, db, spec, shadow !== undefined),
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
      const copyStart = performance.now();
      const copyResults = await Promise.all(
        downloads.map(table =>
          copiers.processReadTask((db, lc) =>
            copy(
              lc,
              table,
              copyPool,
              db,
              tx,
              textCopy,
              syncMode,
              sampleRate,
              maxRowsPerTable,
            ),
          ),
        ),
      );
      const copyElapsed = performance.now() - copyStart;
      void copyProfiler?.stopAndDispose(lc, 'initial-copy');
      copiers.setDone();

      const copySummary = initialSyncCopySummary(copyResults, copyElapsed);

      statusPublisher.publish(
        lc,
        'Indexing',
        `Creating ${indexes.length} indexes`,
        5000,
      );
      const indexStart = performance.now();
      createLiteIndices(lc, tx, indexes);
      const index = performance.now() - indexStart;
      lc.info?.(`Created indexes (${index.toFixed(3)} ms)`);

      if (slotName && replicaID) {
        await initReplica(sql, shard, replicaID, published, context);
      } else {
        assert(shadow, 'expected to be in shadow sync if there is no slotName');
        const rowsByTable = new Map<string, number>();
        for (let i = 0; i < downloads.length; i++) {
          rowsByTable.set(downloads[i].status.table, copyResults[i].rows);
        }
        verifyShadowReplica(lc, tx, published, rowsByTable);
      }

      const elapsed = performance.now() - start;
      const copyOtherMs = Math.max(0, elapsed - copySummary.flushMs - index);
      recordInitialSyncRunMetrics(
        {
          durationMs: elapsed,
          rows: copySummary.rows,
          copyBytes: copySummary.copyBytes,
          copyMs: copySummary.copyMs,
          copyOtherMs,
          flushMs: copySummary.flushMs,
          indexMs: index,
        },
        {
          result: 'success',
          syncMode,
          copyFormat,
        },
      );
      lc.info?.(
        `Synced ${copySummary.rows.toLocaleString()} rows of ${numTables} tables in ${publications} up to ${lsn} ` +
          `(flush: ${copySummary.flushMs.toFixed(3)}, index: ${index.toFixed(3)}, total: ${elapsed.toFixed(3)} ms)`,
        {
          syncMode,
          copyFormat,
          publications,
          lsn,
          ...copySummary,
          indexes: indexes.length,
          indexMs: index,
          copyOtherMs,
          totalMs: elapsed,
        },
      );
    } finally {
      // All meaningful errors are handled at the processReadTask() call site.
      void copyPool.end().catch(e => lc.warn?.(`Error closing copyPool`, e));
    }
  } catch (e) {
    recordInitialSyncRunMetrics(
      {durationMs: performance.now() - start},
      {
        result: 'error',
        syncMode,
        copyFormat,
      },
    );
    if (slotName && sql) {
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
    if (sql) {
      await sql.end();
    }
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
): DownloadStatements {
  const filterConditions = Object.values(table.publications)
    .map(({rowFilter}) => rowFilter)
    .filter(f => !!f); // remove nulls
  const where =
    filterConditions.length === 0
      ? ''
      : /*sql*/ `WHERE ${filterConditions.join(' OR ')}`;
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

type DownloadState = {
  spec: PublishedTableSpec;
  status: DownloadStatus;
};

type CopyFormat = 'binary' | 'text';
type InitialSyncMode = 'initial' | 'shadow';

type InitialSyncMetricAttrs = {
  syncMode: InitialSyncMode;
  copyFormat: CopyFormat;
};

type InitialSyncRunMetricAttrs = {
  syncMode: InitialSyncMode;
  copyFormat: CopyFormat;
  result: 'success' | 'error';
};

type CopyResult = {
  schema: string;
  table: string;
  replicaTable: string;
  syncMode: InitialSyncMode;
  copyFormat: CopyFormat;
  columnCount: number;
  rows: number;
  estimatedRows: number;
  estimatedBytes: number | undefined;
  flushMs: number;
  elapsedMs: number;
  sourceWaitMs: number;
  processingMs: number;
  copyBytes: number;
};

const INITIAL_SYNC_DURATION_HISTOGRAM_BOUNDARIES_S = [
  1, 2, 5, 10, 30, 60, 120, 300, 600, 1200, 2400, 3600, 7200,
];
export const COPY_METRIC_BATCH_BYTES = 8 * 1024 * 1024;
const SLOW_COPY_FLUSH_MS = 10_000;

// change-streamer imports this module before startOtelAuto() runs, so create
// instruments lazily to avoid binding them to OTel's no-op meter provider.
function initialSyncRuns() {
  return getOrCreateCounter(
    'replication',
    'initial_sync_runs',
    'Initial sync runs, labeled by result.',
  );
}

function initialSyncDuration() {
  return initialSyncDurationHistogram(
    'initial_sync_duration',
    'Wall-clock duration of an initial sync run, labeled by result.',
  );
}

function initialSyncCopyDuration() {
  return initialSyncDurationHistogram(
    'initial_sync_copy_duration',
    'Wall-clock duration of the COPY phase for a successful initial sync run.',
  );
}

function initialSyncCopyOtherDuration() {
  return initialSyncDurationHistogram(
    'initial_sync_copy_other_duration',
    'Initial sync total duration excluding SQLite flush and index time for a successful run.',
  );
}

function initialSyncFlushDuration() {
  return initialSyncDurationHistogram(
    'initial_sync_flush_duration',
    'Total SQLite flush time for a successful initial sync run.',
  );
}

function initialSyncIndexDuration() {
  return initialSyncDurationHistogram(
    'initial_sync_index_duration',
    'SQLite index creation time for a successful initial sync run.',
  );
}

function initialSyncRows() {
  return getOrCreateCounter(
    'replication',
    'initial_sync_rows',
    'Rows copied during successful initial sync runs.',
  );
}

function initialSyncCopyStream() {
  return getOrCreateCounter('replication', 'initial_sync_copy_stream', {
    description:
      'PostgreSQL COPY stream bytes processed during initial sync, including in-progress and failed runs.',
    unit: 'bytes',
  });
}

function initialSyncCompletedCopyStream() {
  return getOrCreateCounter(
    'replication',
    'initial_sync_completed_copy_stream',
    {
      description:
        'PostgreSQL COPY stream bytes processed during successful initial sync runs.',
      unit: 'bytes',
    },
  );
}

function initialSyncCopyChunks() {
  return getOrCreateCounter(
    'replication',
    'initial_sync_copy_chunks',
    'PostgreSQL COPY stream chunks processed during initial sync.',
  );
}

export function createCopyMetricBatcher(
  record: (bytes: number, chunks: number) => void,
) {
  let bytes = 0;
  let chunks = 0;

  function flush() {
    if (chunks === 0) {
      return;
    }
    record(bytes, chunks);
    bytes = 0;
    chunks = 0;
  }

  return {
    add(chunkBytes: number) {
      bytes += chunkBytes;
      chunks++;
      if (bytes >= COPY_METRIC_BATCH_BYTES) {
        flush();
      }
    },
    flush,
  };
}

export function initialSyncCopyMetrics(attrs: InitialSyncMetricAttrs) {
  const labels = initialSyncMetricAttrs(attrs);
  const copyStreamMetric = initialSyncCopyStream();
  const copyChunksMetric = initialSyncCopyChunks();
  return createCopyMetricBatcher((bytes, chunks) => {
    copyStreamMetric.add(bytes, labels);
    copyChunksMetric.add(chunks, labels);
  });
}

function initialSyncDurationHistogram(name: string, description: string) {
  return getOrCreateHistogram('replication', name, {
    description,
    unit: 's',
    bucketBoundaries: INITIAL_SYNC_DURATION_HISTOGRAM_BOUNDARIES_S,
  });
}

function initialSyncMetricAttrs(attrs: InitialSyncMetricAttrs) {
  return {
    sync_mode: attrs.syncMode,
    copy_format: attrs.copyFormat,
  };
}

function initialSyncRunMetricAttrs(attrs: InitialSyncRunMetricAttrs) {
  return {
    ...initialSyncMetricAttrs(attrs),
    result: attrs.result,
  };
}

function recordInitialSyncRunMetrics(
  stats: {
    durationMs: number;
    rows?: number | undefined;
    copyBytes?: number | undefined;
    copyMs?: number | undefined;
    copyOtherMs?: number | undefined;
    flushMs?: number | undefined;
    indexMs?: number | undefined;
  },
  attrs: InitialSyncRunMetricAttrs,
) {
  const labels = initialSyncRunMetricAttrs(attrs);
  initialSyncRuns().add(1, labels);
  initialSyncDuration().recordMs(stats.durationMs, labels);
  if (attrs.result === 'success') {
    if (stats.copyMs !== undefined) {
      initialSyncCopyDuration().recordMs(stats.copyMs, labels);
    }
    if (stats.copyOtherMs !== undefined) {
      initialSyncCopyOtherDuration().recordMs(stats.copyOtherMs, labels);
    }
    if (stats.flushMs !== undefined) {
      initialSyncFlushDuration().recordMs(stats.flushMs, labels);
    }
    if (stats.indexMs !== undefined) {
      initialSyncIndexDuration().recordMs(stats.indexMs, labels);
    }
    if (stats.rows !== undefined && stats.rows > 0) {
      initialSyncRows().add(stats.rows, labels);
    }
    if (stats.copyBytes !== undefined && stats.copyBytes > 0) {
      initialSyncCompletedCopyStream().add(stats.copyBytes, labels);
    }
  }
}

function initialSyncCopySummary(
  results: readonly CopyResult[],
  copyMs: number,
) {
  const totals = results.reduce(
    (acc, curr) => {
      acc.rows += curr.rows;
      acc.flushMs += curr.flushMs;
      acc.copyBytes += curr.copyBytes;
      return acc;
    },
    {
      rows: 0,
      flushMs: 0,
      copyBytes: 0,
    },
  );
  return {
    tables: results.length,
    rows: totals.rows,
    copyMs,
    flushMs: totals.flushMs,
    copyBytes: totals.copyBytes,
  };
}

function logSlowCopyFlush(
  lc: LogContext,
  details: {
    schema: string;
    table: string;
    replicaTable: string;
    syncMode: InitialSyncMode;
    copyFormat: CopyFormat;
    elapsedMs: number;
    flushedRows: number;
    flushedBytes: number;
    rows: number;
    copyBytes: number;
  },
) {
  if (details.elapsedMs < SLOW_COPY_FLUSH_MS || details.flushedRows === 0) {
    return;
  }
  lc.info?.('initial-sync table copy slow flush', details);
}

// Exported for testing.
export async function getInitialDownloadState(
  lc: LogContext,
  sql: PostgresDB,
  spec: PublishedTableSpec,
  skipTotals: boolean,
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
    };
  }
  // Use pg_class estimates instead of expensive COUNT(*) and
  // SUM(pg_column_size(...)) full table scans. The exact values are only
  // used for progress reporting, so estimates are sufficient.
  const qualifiedName = `${id(spec.schema)}.${id(spec.name)}`;
  const estimateResult = await sql<
    {totalRows: number; totalBytes: number}[]
  >`SELECT GREATEST(reltuples, 0)::float8 AS "totalRows",
         pg_table_size(oid)::float8 AS "totalBytes"
    FROM pg_class
    WHERE oid = ${qualifiedName}::regclass`;

  const {totalRows, totalBytes} = estimateResult[0] ?? {
    totalRows: 0,
    totalBytes: 0,
  };

  const state: DownloadState = {
    spec,
    status: {
      table,
      columns,
      rows: 0,
      totalRows,
      totalBytes,
    },
  };
  const elapsed = (performance.now() - start).toFixed(3);
  lc.info?.(`Computed initial download state for ${table} (${elapsed} ms)`, {
    state: state.status,
  });
  return state;
}

function copy(
  lc: LogContext,
  {spec: table, status}: DownloadState,
  dbClient: PostgresDB,
  from: PostgresTransaction,
  to: Database,
  textCopy: boolean,
  syncMode: InitialSyncMode,
  sampleRate?: number | undefined,
  maxRowsPerTable?: number | undefined,
): Promise<CopyResult> {
  if (textCopy) {
    return copyText(
      lc,
      table,
      status,
      dbClient,
      from,
      to,
      syncMode,
      sampleRate,
      maxRowsPerTable,
    );
  }
  return copyBinary(
    lc,
    table,
    status,
    from,
    to,
    syncMode,
    sampleRate,
    maxRowsPerTable,
  );
}

async function copyBinary(
  lc: LogContext,
  table: PublishedTableSpec,
  status: DownloadStatus,
  from: PostgresTransaction,
  to: Database,
  syncMode: InitialSyncMode,
  sampleRate?: number | undefined,
  maxRowsPerTable?: number | undefined,
): Promise<CopyResult> {
  const start = performance.now();
  const copyFormat: CopyFormat = 'binary';
  const copyMetrics = initialSyncCopyMetrics({syncMode, copyFormat});
  let flushMs = 0;
  let copyBytes = 0;
  let processingMs = 0;

  const tableName = liteTableName(table);
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
    const flushStart = performance.now();
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

    const elapsed = performance.now() - flushStart;
    flushMs += elapsed;
    lc.debug?.(
      `flushed ${flushedRows} ${tableName} rows (${flushedSize} bytes) in ${elapsed.toFixed(3)} ms`,
    );
    logSlowCopyFlush(lc, {
      schema: table.schema,
      table: table.name,
      replicaTable: tableName,
      syncMode,
      copyFormat,
      elapsedMs: elapsed,
      flushedRows,
      flushedBytes: flushedSize,
      rows: status.rows,
      copyBytes,
    });
  }

  const binaryParser = new BinaryCopyParser();
  let col = 0;

  lc.info?.(`Starting binary copy stream of ${tableName}:`, select);

  const copyStreamStart = performance.now();
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
        const processingStart = performance.now();
        try {
          copyBytes += chunk.length;
          copyMetrics.add(chunk.length);
          for (const fieldBuf of binaryParser.parse(chunk)) {
            const fieldSize = fieldBuf === null ? 4 : fieldBuf.length;
            pendingSize += fieldSize;
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
          processingMs += performance.now() - processingStart;
          callback();
        } catch (e) {
          processingMs += performance.now() - processingStart;
          callback(e instanceof Error ? e : new Error(String(e)));
        }
      },

      final: (callback: (error?: Error) => void) => {
        const processingStart = performance.now();
        try {
          copyMetrics.flush();
          flush();
          processingMs += performance.now() - processingStart;
          callback();
        } catch (e) {
          processingMs += performance.now() - processingStart;
          callback(e instanceof Error ? e : new Error(String(e)));
        }
      },

      destroy(error, callback) {
        copyMetrics.flush();
        callback(error);
      },
    }),
  );

  const sourceWaitMs = Math.max(
    0,
    performance.now() - copyStreamStart - processingMs,
  );
  const elapsed = performance.now() - start;
  const result = {
    schema: table.schema,
    table: table.name,
    replicaTable: tableName,
    syncMode,
    copyFormat,
    columnCount: columnNames.length,
    rows: status.rows,
    estimatedRows: status.totalRows,
    estimatedBytes: status.totalBytes,
    flushMs,
    elapsedMs: elapsed,
    sourceWaitMs,
    processingMs,
    copyBytes,
  } satisfies CopyResult;

  lc.info?.(
    `Finished copying ${status.rows} rows into ${tableName} ` +
      `(flush: ${flushMs.toFixed(3)} ms) (total: ${elapsed.toFixed(3)} ms) `,
    result,
  );
  return result;
}

async function copyText(
  lc: LogContext,
  table: PublishedTableSpec,
  status: DownloadStatus,
  dbClient: PostgresDB,
  from: PostgresTransaction,
  to: Database,
  syncMode: InitialSyncMode,
  sampleRate?: number | undefined,
  maxRowsPerTable?: number | undefined,
): Promise<CopyResult> {
  const start = performance.now();
  const copyFormat: CopyFormat = 'text';
  const copyMetrics = initialSyncCopyMetrics({syncMode, copyFormat});
  let flushMs = 0;
  let copyBytes = 0;
  let processingMs = 0;

  const tableName = liteTableName(table);
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
  );
  const valuesPerRow = columnSpecs.length;
  const valuesPerBatch = valuesPerRow * INSERT_BATCH_SIZE;

  const pendingValues: LiteValueType[] = Array.from({
    length: MAX_BUFFERED_ROWS * valuesPerRow,
  });
  let pendingRows = 0;
  let pendingSize = 0;

  function flush() {
    const flushStart = performance.now();
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

    const elapsed = performance.now() - flushStart;
    flushMs += elapsed;
    lc.debug?.(
      `flushed ${flushedRows} ${tableName} rows (${flushedSize} bytes) in ${elapsed.toFixed(3)} ms`,
    );
    logSlowCopyFlush(lc, {
      schema: table.schema,
      table: table.name,
      replicaTable: tableName,
      syncMode,
      copyFormat,
      elapsedMs: elapsed,
      flushedRows,
      flushedBytes: flushedSize,
      rows: status.rows,
      copyBytes,
    });
  }

  lc.info?.(`Starting text copy stream of ${tableName}:`, select);
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

  const copyStreamStart = performance.now();
  await pipeline(
    await from.unsafe(`COPY (${select}) TO STDOUT`).readable(),
    new Writable({
      highWaterMark: BUFFERED_SIZE_THRESHOLD,

      write(
        chunk: Buffer,
        _encoding: string,
        callback: (error?: Error) => void,
      ) {
        const processingStart = performance.now();
        try {
          copyBytes += chunk.length;
          copyMetrics.add(chunk.length);
          for (const text of tsvParser.parse(chunk)) {
            const fieldSize = text === null ? 4 : text.length;
            pendingSize += fieldSize;
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
          processingMs += performance.now() - processingStart;
          callback();
        } catch (e) {
          processingMs += performance.now() - processingStart;
          callback(e instanceof Error ? e : new Error(String(e)));
        }
      },

      final: (callback: (error?: Error) => void) => {
        const processingStart = performance.now();
        try {
          copyMetrics.flush();
          flush();
          processingMs += performance.now() - processingStart;
          callback();
        } catch (e) {
          processingMs += performance.now() - processingStart;
          callback(e instanceof Error ? e : new Error(String(e)));
        }
      },

      destroy(error, callback) {
        copyMetrics.flush();
        callback(error);
      },
    }),
  );

  const sourceWaitMs = Math.max(
    0,
    performance.now() - copyStreamStart - processingMs,
  );
  const elapsed = performance.now() - start;
  const result = {
    schema: table.schema,
    table: table.name,
    replicaTable: tableName,
    syncMode,
    copyFormat,
    columnCount: columnNames.length,
    rows: status.rows,
    estimatedRows: status.totalRows,
    estimatedBytes: status.totalBytes,
    flushMs,
    elapsedMs: elapsed,
    sourceWaitMs,
    processingMs,
    copyBytes,
  } satisfies CopyResult;
  lc.info?.(
    `Finished copying ${status.rows} rows into ${tableName} ` +
      `(flush: ${flushMs.toFixed(3)} ms) (total: ${elapsed.toFixed(3)} ms) `,
    result,
  );
  return result;
}
