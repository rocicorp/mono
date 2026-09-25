import {
  PG_UNDEFINED_COLUMN,
  PG_UNDEFINED_TABLE,
} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import postgres from 'postgres';
import {assert} from '../../../../../shared/src/asserts.ts';
import {must} from '../../../../../shared/src/must.ts';
import {Queue} from '../../../../../shared/src/queue.ts';
import {randomCharacters} from '../../../../../shared/src/random-values.ts';
import {equals} from '../../../../../shared/src/set-utils.ts';
import * as v from '../../../../../shared/src/valita.ts';
import {READONLY} from '../../../db/mode-enum.ts';
import {
  BinaryCopyParser,
  hasBinaryDecoder,
  makeBinaryDecoder,
  textCastDecoder,
} from '../../../db/pg-copy-binary.ts';
import {TsvParser} from '../../../db/pg-copy.ts';
import {getTypeParsers} from '../../../db/pg-type-parser.ts';
import type {PublishedTableSpec} from '../../../db/specs.ts';
import {importSnapshot, TransactionPool} from '../../../db/transaction-pool.ts';
import {connectPgClient, pgClient, type PostgresDB} from '../../../types/pg.ts';
import {id} from '../../../types/sql.ts';
import {
  SchemaIncompatibilityError,
  type BackfillMessage,
} from '../common/backfill-manager.ts';
import {resumePoint} from '../protocol/backfill-progress.ts';
import type {
  BackfillCompleted,
  BackfillProgressMark,
  BackfillRequest,
  DownloadStatus,
  JSONValue,
} from '../protocol/current.ts';
import {
  columnMetadataSchema,
  tableMetadataSchema,
} from './backfill-metadata.ts';
import {
  makeBinarySelectExprs,
  makeDownloadStatements,
  type DownloadStatements,
} from './initial-sync.ts';
import {toStateVersionString} from './lsn.ts';
import {createReplicationSlot} from './replication-slots.ts';
import {getPublicationInfo} from './schema/published.ts';
import type {Replica} from './schema/shard.ts';

type BackfillParams = Omit<BackfillCompleted, 'tag' | 'progressMarks'>;

/**
 * Determines how the progress of a backfill is marked, and from where it is
 * resumed.
 */
type Progress = {
  /**
   * The timeline of the progress marks. For plain tables, this is the table's
   * `relfilenode`, which identifies the physical "generation" of the table's
   * data (changed by heap rewrites like `VACUUM FULL`, `CLUSTER`, or
   * `TRUNCATE`), within which `ctid`s are comparable.
   */
  timeline: string;

  /**
   * Whether the progress marks are resumable `ctid`s. Tables without
   * (their own) storage, such as partitioned tables, are not resumable and
   * are instead marked with the number of rows streamed, on a timeline that
   * is unique to the run.
   */
  resumable: boolean;

  /** The mark from which the backfill resumes, if any. */
  start: BackfillProgressMark | undefined;
};

/**
 * Encodes a `ctid` text representation, e.g. `(123,4)`, as a
 * fixed-width string that sorts lexicographically in `ctid` order.
 */
const CTID_RE = /^\((\d+),(\d+)\)$/;
const CTID_MARK_RE = /^(\d{10})\.(\d{5})$/;

export function ctidToProgressMark(ctid: string): string {
  const match = CTID_RE.exec(ctid);
  assert(match, () => `invalid ctid: ${ctid}`);
  return `${match[1].padStart(10, '0')}.${match[2].padStart(5, '0')}`;
}

/** The inverse of {@link ctidToProgressMark}, as a `tid` literal. */
export function progressMarkToCtid(mark: string): string {
  const match = CTID_MARK_RE.exec(mark);
  assert(match, () => `invalid ctid progress mark: ${mark}`);
  return `(${Number(match[1])},${Number(match[2])})`;
}

function progressMarks(
  previous: BackfillProgressMark | undefined,
  current: BackfillProgressMark,
) {
  return previous ? {previous, current} : {current};
}

/** The `ctid` of each row is appended as the last column of the download. */
const CTID_SELECT = 'ctid::text';

/**
 * Settings that ensure that the COPY scans the table in physical (i.e.
 * `ctid`) order, which is necessary for the progress marks of a resumable
 * backfill to be meaningful. (Monotonicity is nonetheless verified as rows
 * are streamed.)
 *
 * * Synchronized seqscans can start in the middle of the table.
 * * Parallel scans interleave the rows of different workers.
 * * Index scans follow index order. (A publication row filter could
 *   otherwise be satisfied with an index.)
 */
const PHYSICAL_ORDER_SETTINGS = [
  'synchronize_seqscans = off',
  'max_parallel_workers_per_gather = 0',
  'enable_indexscan = off',
  'enable_indexonlyscan = off',
  'enable_bitmapscan = off',
];

function rowCountMark(rows: number): string {
  return String(rows).padStart(16, '0');
}

type StreamOptions = {
  /**
   * The number of bytes at which to flush a batch of rows in a
   * backfill message. Defaults to Node's getDefaultHighWatermark().
   */
  flushThresholdBytes?: number | undefined;

  /**
   * Use text-format COPY instead of binary COPY.
   * Binary is faster and handles all types (unknown types are cast to
   * `::text` in the SELECT). This flag exists as an escape hatch to
   * revert to the old code path if needed.
   */
  textCopy?: boolean | undefined;

  /**
   * @visibleForTesting
   * Called after the snapshot transaction is opened, before the table is
   * locked, e.g. to run a concurrent heap rewrite in between.
   */
  afterSnapshotForTesting?: (() => Promise<void>) | undefined;
};

// The size of chunks that Postgres sends on COPY stream.
// This happens to match NodeJS's getDefaultHighWatermark()
// (for Node v20+).
const POSTGRES_COPY_CHUNK_SIZE = 64 * 1024;

// Matches the exact clauses emitted by makeDownloadStatements; quoted
// identifiers like "limit" won't match because they lack the surrounding
// whitespace.
const SAMPLE_OR_LIMIT_RE = /\sTABLESAMPLE\s+BERNOULLI\b|\sLIMIT\s+\d/i;

/**
 * Streams a series of `backfill` messages (ending with `backfill-complete`)
 * at a set watermark (i.e. LSN). The data is retrieved via a COPY stream
 * made at a transaction snapshot corresponding to specific LSN, obtained by
 * creating a short-lived replication slot.
 */
export async function* streamBackfill(
  lc: LogContext,
  upstreamURI: string,
  {slot, publications}: Pick<Replica, 'slot' | 'publications'>,
  bf: BackfillRequest,
  opts: StreamOptions = {},
): AsyncGenerator<BackfillMessage> {
  lc = lc
    .withContext('component', 'backfill')
    .withContext('table', bf.table.name);

  const {
    flushThresholdBytes = POSTGRES_COPY_CHUNK_SIZE,
    textCopy = false,
    afterSnapshotForTesting,
  } = opts;
  const db = await connectPgClient(lc, upstreamURI, 'backfill-stream', {
    // The COPY is a single stream that must outlive the entire table download,
    // so allow a very long (24h) connection lifetime.
    ['max_lifetime']: 24 * 60 * 60,
    // A backfill COPY is a background process that can be preempted by
    // upstream replication changes, potentially requiring it to sit idle for
    // long stretches. Use TCP keepalive to detect dead connections instead
    // of the inactivity watchdog, which could kill connnections that are
    // taking a back seat for upstream replication changes.
    liveness: 'keepalive',
  });
  let tx: TransactionPool | undefined;
  let watermark: string;
  try {
    ({tx, watermark} = await createSnapshotTransaction(
      lc,
      upstreamURI,
      db,
      slot,
    ));
    await afterSnapshotForTesting?.();
    const {tableSpec, backfill, progress} = await validateSchema(
      lc,
      tx,
      publications,
      bf,
      watermark,
    );

    // Note: validateSchema ensures that the rowKey and columns are disjoint
    const {relation, columns} = backfill;
    const cols = [...relation.rowKey.columns, ...columns];
    // Resumable backfills are streamed in physical (i.e. ctid) order, which
    // is the natural order of a sequential or TID range scan (as enforced by
    // the settings in `stream()`). An ORDER BY is not used, as Postgres does
    // not consider either scan to be ordered, and would unnecessarily sort
    // the table.
    const order =
      progress.resumable && progress.start
        ? {
            after: /*sql*/ `ctid > '${progressMarkToCtid(progress.start.progressMark)}'::tid`,
          }
        : undefined;
    const stmts = makeDownloadStatements(
      tableSpec,
      cols,
      undefined,
      undefined,
      [...cols.map(col => id(col)), CTID_SELECT],
      order,
    );
    if (progress.start) {
      lc.info?.(
        `resuming backfill from ${progress.start.progressMark} ` +
          `(timeline ${progress.timeline})`,
      );
    }

    if (textCopy) {
      const types = await getTypeParsers(db, {returnJsonAsString: true});
      yield* stream(
        lc,
        tx,
        backfill,
        progress,
        stmts,
        `COPY (${stmts.select}) TO STDOUT`,
        new TsvParser(),
        cols.map(col => {
          const parser = types.getTypeParser(tableSpec.columns[col].typeOID);
          return (text: string) => parser(text) as JSONValue;
        }),
        (text: string) => text,
        flushThresholdBytes,
      );
    } else {
      const binaryStmts = makeDownloadStatements(
        tableSpec,
        cols,
        undefined,
        undefined,
        [...makeBinarySelectExprs(tableSpec, cols), CTID_SELECT],
        order,
      );

      yield* stream(
        lc,
        tx,
        backfill,
        progress,
        stmts,
        `COPY (${binaryStmts.select}) TO STDOUT WITH (FORMAT binary)`,
        new BinaryCopyParser(),
        cols.map(col => {
          const spec = tableSpec.columns[col];
          const decoder = hasBinaryDecoder(spec)
            ? makeBinaryDecoder(spec)
            : textCastDecoder;
          return (buf: Buffer) => decoder(buf) as unknown as JSONValue;
        }),
        (buf: Buffer) => buf.toString('utf8'), // ctid::text
        flushThresholdBytes,
      );
    }
  } catch (e) {
    // Although we make the best effort to validate the schema at the
    // transaction snapshot, certain forms of `ALTER TABLE` are not
    // MVCC safe and not "frozen" in the snapshot:
    //
    // https://www.postgresql.org/docs/current/mvcc-caveats.html
    //
    // Handle these errors as schema incompatibility errors rather than
    // unknown runtime errors.
    if (
      e instanceof postgres.PostgresError &&
      (e.code === PG_UNDEFINED_TABLE || e.code === PG_UNDEFINED_COLUMN)
    ) {
      throw new SchemaIncompatibilityError(bf, String(e), {cause: e});
    }
    throw e;
  } finally {
    tx?.setDone();
    // Workaround postgres.js hanging at the end of some COPY commands:
    // https://github.com/porsager/postgres/issues/499
    void db.end().catch(e => lc.warn?.(`error closing backfill connection`, e));
  }
}

async function* stream<T>(
  lc: LogContext,
  tx: TransactionPool,
  backfill: BackfillParams,
  {timeline, resumable, start: startMark}: Progress,
  {
    getTotalRows,
    getTotalBytes,
  }: Pick<DownloadStatements, 'getTotalRows' | 'getTotalBytes'>,
  copyCommand: string,
  parser: {parse(chunk: Buffer): Iterable<T | null>},
  decoders: ((field: T) => JSONValue)[],
  ctidDecoder: (field: T) => string,
  flushThresholdBytes: number,
): AsyncGenerator<BackfillMessage> {
  // Backfill must read every row: TABLESAMPLE / LIMIT are reserved for shadow
  // sync and must never appear in a backfill COPY.
  assert(
    !SAMPLE_OR_LIMIT_RE.test(copyCommand),
    `backfill COPY must not sample or limit: ${copyCommand}`,
  );
  const start = performance.now();
  const [rows, bytes] = await tx.processReadTask(sql =>
    Promise.all([
      sql.unsafe<{totalRows: bigint}[]>(getTotalRows),
      sql.unsafe<{totalBytes: bigint}[]>(getTotalBytes),
    ]),
  );
  const status: DownloadStatus = {
    rows: 0,
    totalRows: Number(rows[0].totalRows),
    totalBytes: Number(bytes[0].totalBytes),
  };

  let elapsed = (performance.now() - start).toFixed(3);
  lc.info?.(
    `Computed total rows and bytes for: ${copyCommand} (${elapsed} ms)`,
    {
      status,
    },
  );

  // Drain the COPY stream from *within* a single read task so that the
  // TransactionPool worker holds the transaction for the entire duration of
  // the COPY, rather than incorrectly considering the worker "idle" and
  // attempting to send keepalives on it.
  //
  // Chunks are bridged out to this generator via a Queue, with a corresponding
  // acks Queue providing strict one-chunk-at-a-time backpressure. This is
  // necessary because it is not possible to "yield" from within the read task.
  const chunks = new Queue<Buffer | 'done'>();
  const acks = new Queue<void>();

  const copyDone = tx.processReadTask(async sql => {
    // SET LOCAL applies to the (snapshot) transaction of this read task.
    for (const setting of PHYSICAL_ORDER_SETTINGS) {
      await sql.unsafe(`SET LOCAL ${setting}`);
    }
    const readable = await sql.unsafe(copyCommand).readable();
    for await (const chunk of readable) {
      chunks.enqueue(chunk as Buffer);
      await acks.dequeue(); // wait for this generator to consume the chunk
    }
    chunks.enqueue('done');
  });
  copyDone.catch(e => chunks.enqueueRejection(e));

  let totalBytes = 0;
  let totalMsgs = 0;
  let rowValues: JSONValue[][] = [];
  let bufferedBytes = 0;

  const logFlushed = () => {
    lc.debug?.(
      `Flushed ${rowValues.length} rows, ${bufferedBytes} bytes ` +
        `(total: rows=${status.rows}, msgs=${totalMsgs}, bytes=${totalBytes})`,
    );
  };

  // The mark of the last row that was streamed, and that of the last row of
  // the previous message.
  let current: BackfillProgressMark | undefined = startMark;
  let previous: BackfillProgressMark | undefined = startMark;
  const markOf = (progressMark: string) => ({progressMark, timeline});

  // The number of columns of each row, followed by the ctid.
  const numCols = decoders.length + 1;

  // Tracks the row being parsed.
  let row: JSONValue[] = Array.from({length: decoders.length});
  let col = 0;

  for (;;) {
    const chunk = await chunks.dequeue();
    if (chunk === 'done') {
      break;
    }
    for (const field of parser.parse(chunk)) {
      if (col < decoders.length) {
        row[col] = field === null ? null : decoders[col](field);
      } else {
        const mark = resumable
          ? ctidToProgressMark(ctidDecoder(field as T))
          : rowCountMark(status.rows + 1);
        if (current !== undefined && mark <= current.progressMark) {
          // This should not happen given the PHYSICAL_ORDER_SETTINGS, but
          // the correctness of the progress marks depends on it.
          throw new Error(
            `backfill rows are not in ctid order ` +
              `(${mark} after ${current.progressMark})`,
          );
        }
        current = markOf(mark);
      }

      if (++col === numCols) {
        rowValues.push(row);
        status.rows++;
        row = Array.from({length: decoders.length});
        col = 0;
      }
    }
    bufferedBytes += chunk.byteLength;
    totalBytes += chunk.byteLength;

    if (bufferedBytes >= flushThresholdBytes && rowValues.length > 0) {
      yield {
        message: {
          tag: 'backfill',
          ...backfill,
          rowValues,
          status,
          progressMarks: progressMarks(previous, must(current)),
        },
        byteSize: bufferedBytes,
      };
      previous = current;
      totalMsgs++;
      logFlushed();
      rowValues = [];
      bufferedBytes = 0;
    }

    // Signal the read task to pull the next chunk (one-chunk backpressure).
    acks.enqueue();
  }

  // Surface any error from the COPY read task (and confirm clean completion).
  await copyDone;

  // Flush the last batch of rows.
  if (rowValues.length > 0) {
    yield {
      message: {
        tag: 'backfill',
        ...backfill,
        rowValues,
        status,
        progressMarks: progressMarks(previous, must(current)),
      },
      byteSize: bufferedBytes,
    };
    previous = current;
    totalMsgs++;
    logFlushed();
  }

  yield {
    message: {
      tag: 'backfill-completed',
      ...backfill,
      status,
      // `previous` is only unset for a backfill from scratch with no rows.
      progressMarks: previous ? {previous} : {},
    },
    byteSize: 0,
  };
  elapsed = (performance.now() - start).toFixed(3);
  lc.info?.(
    `Finished streaming ${status.rows} rows, ${totalMsgs} msgs, ${totalBytes} bytes ` +
      `(${elapsed} ms)`,
  );
}

/**
 * Creates (and drops) a replication slot in order to obtain a snapshot
 * that corresponds with a specific LSN. Sets the snapshot on the
 * TransactionPool and returns the watermark corresponding to the LSN.
 *
 * (Note that PG's other LSN-related functions are not scoped to a
 *  transaction; this is the only way to get set a transaction at a specific
 *  LSN.)
 */
async function createSnapshotTransaction(
  lc: LogContext,
  upstreamURI: string,
  db: PostgresDB,
  slotNamePrefix: string,
) {
  const replicationSession = pgClient(
    lc,
    upstreamURI,
    'backfill-replication-session',
    {
      ['fetch_types']: false, // Necessary for the streaming protocol
      connection: {replication: 'database'}, // https://www.postgresql.org/docs/current/protocol-replication.html
    },
  );
  const slotName = `${slotNamePrefix}_bf_${Date.now()}`;
  try {
    const {snapshot_name: snapshot, consistent_point: lsn} =
      await createReplicationSlot(lc, replicationSession, {
        slotName,
        temporary: true, // deletes the slot when the replicationSession ends
      });

    const {init, imported} = importSnapshot(snapshot);
    const tx = new TransactionPool(lc, {mode: READONLY, init}).run(db);
    await imported.dequeue();

    const watermark = toStateVersionString(lsn);
    lc.info?.(`Opened snapshot transaction at LSN ${lsn} (${watermark})`);
    return {tx, watermark};
  } finally {
    await replicationSession.end();
  }
}

function validateSchema(
  lc: LogContext,
  tx: TransactionPool,
  publications: string[],
  bf: BackfillRequest,
  watermark: string,
): Promise<{
  tableSpec: PublishedTableSpec;
  backfill: BackfillParams;
  progress: Progress;
}> {
  return tx.processReadTask(async sql => {
    const {tables} = await getPublicationInfo(sql, publications);
    const spec = tables.find(
      spec => spec.schema === bf.table.schema && spec.name === bf.table.name,
    );
    if (!spec) {
      throw new SchemaIncompatibilityError(
        bf,
        `Table has been renamed or dropped`,
      );
    }
    const tableMeta = v.parse(bf.table.metadata, tableMetadataSchema);
    if (spec.schemaOID !== tableMeta.schemaOID) {
      throw new SchemaIncompatibilityError(
        bf,
        `Schema no longer corresponds to the original schema`,
      );
    }
    if (spec.oid !== tableMeta.relationOID) {
      throw new SchemaIncompatibilityError(
        bf,
        `Table no longer corresponds to the original table`,
      );
    }
    if (
      !equals(
        new Set(Object.keys(tableMeta.rowKey)),
        new Set(spec.replicaIdentityColumns),
      )
    ) {
      throw new SchemaIncompatibilityError(
        bf,
        'Row key (e.g. PRIMARY KEY or INDEX) has changed',
      );
    }
    const allCols = [
      ...Object.entries(tableMeta.rowKey),
      ...Object.entries(bf.columns).map(([col, {id}]) => [col, id] as const),
    ];
    for (const [col, val] of allCols) {
      const colSpec = spec.columns[col];
      if (!colSpec) {
        throw new SchemaIncompatibilityError(
          bf,
          `Column ${col} has been renamed or dropped`,
        );
      }
      const colMeta = v.parse(val, columnMetadataSchema);
      if (colMeta.attNum !== colSpec.pos) {
        throw new SchemaIncompatibilityError(
          bf,
          `Column ${col} no longer corresponds to the original column`,
        );
      }
    }
    const backfill: BackfillParams = {
      relation: {
        schema: bf.table.schema,
        name: bf.table.name,
        rowKey: {columns: Object.keys(tableMeta.rowKey)},
      },
      columns: Object.keys(bf.columns).filter(
        col => !(col in tableMeta.rowKey),
      ),
      watermark,
    };
    const progress = await getProgress(lc, sql, spec.oid, bf);
    return {tableSpec: spec, backfill, progress};
  });
}

/**
 * Determines the timeline of the table's progress marks (as of the snapshot),
 * and whether the backfill can be resumed from the requested progress.
 *
 * The table is locked (in ACCESS SHARE mode, as the COPY would, for the rest
 * of the transaction) so that its storage cannot be rewritten (by `VACUUM
 * FULL`, `CLUSTER`, `TRUNCATE`, or a rewriting `ALTER TABLE`, all of which
 * require ACCESS EXCLUSIVE) between reading the timeline and the COPY. Note
 * that `pg_relation_filenode()` reflects the table's current storage (only
 * refreshed upon acquiring a lock), whereas `pg_class.relfilenode` is read at
 * the transaction snapshot. If they differ, the storage was rewritten after the
 * snapshot, and the COPY would scan the new storage with the old snapshot,
 * which is not MVCC-safe for all rewrites (e.g. the table appears empty after
 * a rewriting `ALTER TABLE`). The backfill is then retried at a new snapshot.
 */
async function getProgress(
  lc: LogContext,
  sql: postgres.Sql,
  relationOID: number,
  bf: BackfillRequest,
): Promise<Progress> {
  const table = `${id(bf.table.schema)}.${id(bf.table.name)}`;
  await sql.unsafe(`LOCK TABLE ${table} IN ACCESS SHARE MODE`);
  const [{lockedOID, relkind, snapshotFilenode, filenode}] = await sql<
    {
      lockedOID: number | null;
      relkind: string;
      snapshotFilenode: string;
      filenode: string | null;
    }[]
  >`
    SELECT to_regclass(${table})::oid::int8 AS "lockedOID",
           relkind,
           relfilenode::text AS "snapshotFilenode",
           pg_relation_filenode(oid)::text AS filenode
      FROM pg_class WHERE oid = ${relationOID}`;
  if (Number(lockedOID) !== relationOID) {
    // The (current) table with the name is not the one at the snapshot.
    throw new SchemaIncompatibilityError(
      bf,
      `Table has been renamed or replaced`,
    );
  }
  if (relkind !== 'r' || filenode === null) {
    // Not a plain table (e.g. a partitioned table), for which ctids are not
    // unique. Such backfills are not resumable, which is achieved by using a
    // timeline that is unique to the run.
    return {
      timeline: `${relkind}:${randomCharacters(12)}`,
      resumable: false,
      start: undefined,
    };
  }
  if (filenode !== snapshotFilenode) {
    throw new Error(
      `${bf.table.schema}.${bf.table.name} was rewritten after the backfill ` +
        `snapshot (relfilenode ${snapshotFilenode} => ${filenode})`,
    );
  }
  const timeline = filenode;
  const requested = resumePoint(bf);
  if (requested && requested.timeline !== timeline) {
    lc.info?.(
      `restarting backfill from scratch: the requested progress is on ` +
        `timeline ${requested.timeline} but the table is on ${timeline}`,
    );
    return {timeline, resumable: true, start: undefined};
  }
  if (requested && !(await canResume(sql, relationOID, bf))) {
    lc.info?.(
      `restarting backfill from scratch: the table has TOASTed values ` +
        `and backfilling columns [${Object.keys(bf.columns).join(',')}] ` +
        `are TOAST-able`,
    );
    return {timeline, resumable: true, start: undefined};
  }
  return {timeline, resumable: true, start: requested};
}

/**
 * Whether resuming a backfill (in a new snapshot) is guaranteed to deliver
 * the values of every row that the preceding snapshot(s) had not reached.
 *
 * Within a single snapshot, every row is scanned exactly once, since the
 * version visible to the snapshot stays in place while it is held. Across
 * snapshots, however, a row that had not yet been reached (i.e. beyond the
 * resume mark `M`) can be updated between the snapshots, with its new
 * version placed at or before `M` (a non-HOT update into free space in an
 * earlier page, or a HOT update into a lower line pointer of `M`'s page).
 * Neither snapshot then scans the row, and it is only delivered by the
 * replication stream. The stream's UPDATE contains the full row, _except_
 * for unchanged values stored out-of-line in TOAST, which pgoutput omits
 * (unless the table has REPLICA IDENTITY FULL). A backfilling column with
 * such a value would be published without it.
 *
 * Resuming is therefore only done if no backfilling column can be TOASTed
 * (i.e. all have `attstorage = 'p'`, as for fixed-width types), or if the
 * table has no TOAST data. The latter is safe because an omitted value must
 * still be in the TOAST relation at the new snapshot (unless its row has since
 * been deleted, which the replication stream does deliver), and conservative
 * because the TOAST relation does not shrink to empty without a heap rewrite,
 * which changes the timeline.
 *
 * Potential optimization for TOAST-able backfills: have the resumed run also
 * deliver the rows at or before `M` that were modified since the snapshot of
 * the run that started the backfill from scratch. That run's snapshot `xmin`
 * (`T_origin`, as an xid8) would be encoded in the timeline, i.e.
 * `timeline = "<relfilenode>:<T_origin>"`, and carried over (rather than
 * replaced) by resumed runs, so that:
 * * Within a chain of runs, every row version with `xmin < T_origin` stayed
 *   in place across all of the runs' snapshots and was therefore scanned
 *   exactly once, and all other rows are re-delivered by the resumed run
 *   (`ctid > M OR xmin >= T_origin`).
 * * Continuations from a different chain (i.e. with a newer `T_origin`) are
 *   not accepted by subscribers, and requests from different chains are
 *   merged into a backfill from scratch, as the timelines differ. Note that
 *   using the _latest_ `T` would be incorrect: rows that moved before `M`
 *   between the earlier snapshots would never be re-delivered.
 * The resumed run would scan the whole table (but only deliver the modified
 * rows), comparing the 32-bit `xmin` with `T_origin` modulo 2^32, which
 * requires falling back to a backfill from scratch once the current xid is
 * 2^31 or more past `T_origin` (i.e. wraparound). This also relies on the
 * `xmin` system column returning the raw xmin of frozen tuples, which should
 * be verified.
 */
async function canResume(
  sql: postgres.Sql,
  relationOID: number,
  bf: BackfillRequest,
): Promise<boolean> {
  const [{toastable, toasted}] = await sql<
    {toastable: boolean; toasted: boolean}[]
  >`
    SELECT
      EXISTS (
        SELECT 1 FROM pg_attribute
          WHERE attrelid = ${relationOID}
            AND attname IN ${sql(Object.keys(bf.columns))}
            AND attstorage <> 'p'
      ) AS toastable,
      reltoastrelid <> 0 AND pg_relation_size(reltoastrelid) > 0 AS toasted
      FROM pg_class WHERE oid = ${relationOID}`;
  return !toastable || !toasted;
}
