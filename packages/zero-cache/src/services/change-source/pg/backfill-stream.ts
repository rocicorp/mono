import {
  PG_UNDEFINED_COLUMN,
  PG_UNDEFINED_TABLE,
} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import {nanoid} from 'nanoid';
import postgres from 'postgres';
import {assert} from '../../../../../shared/src/asserts.ts';
import {Queue} from '../../../../../shared/src/queue.ts';
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
import {
  connectPgClient,
  pgClient,
  type PostgresDB,
  type PostgresTransaction,
} from '../../../types/pg.ts';
import {
  SchemaIncompatibilityError,
  type BackfillMessage,
} from '../common/backfill-manager.ts';
import type {
  BackfillCompleted,
  BackfillRequest,
  BackfillStarted,
  DownloadStatus,
  JSONValue,
  Mark,
} from '../protocol/current.ts';
import {
  columnMetadataSchema,
  tableMetadataSchema,
} from './backfill-metadata.ts';
import {
  getKeyCollations,
  getKeyCorrelation,
  isCheaplyOrderable,
  isResumableKey,
  markOfLastRow,
  orderByRowKey,
  resumeWhere,
  type ResumeColumnSpec,
} from './backfill-resume.ts';
import {
  makeBinarySelectExprs,
  makeDownloadStatements,
  type DownloadOrder,
  type DownloadStatements,
} from './initial-sync.ts';
import {toStateVersionString} from './lsn.ts';
import {createReplicationSlot} from './replication-slots.ts';
import {getPublicationInfo} from './schema/published.ts';
import type {Replica} from './schema/shard.ts';

type BackfillParams = Omit<BackfillCompleted, 'tag'>;

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
   * Whether to order the backfill by the row key so that it can be resumed.
   * Defaults to true; the `backfillResume` kill switch turns it off, in which
   * case the COPY is unordered and no `lastKey` is attached, so no subscriber
   * ever holds a mark. Run announcements are emitted either way, because a
   * subscriber needs one in order to honor the run's completion.
   */
  resume?: boolean | undefined;

  /**
   * The minimum `pg_stats.correlation` of the row key's leading column for
   * the backfill to be ordered. Defaults to `MIN_KEY_CORRELATION`. See
   * `backfill-resume.ts` for what this costs and why.
   */
  minKeyCorrelation?: number | undefined;

  /** Mints the run's ID. Injectable for deterministic tests. */
  newRunID?: (() => string) | undefined;
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
    resume = true,
    minKeyCorrelation,
    newRunID = nanoid,
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
    const {tableSpec, backfill, run} = await validateSchema(
      lc,
      tx,
      publications,
      bf,
      watermark,
      {resume, minKeyCorrelation, runID: newRunID()},
    );

    // Note: validateSchema ensures that the rowKey and columns are disjoint
    const {relation, columns} = backfill;
    const cols = [...relation.rowKey.columns, ...columns];
    const rowKeyCols = relation.rowKey.columns;
    const order: DownloadOrder | undefined = run.ordered
      ? {
          by: orderByRowKey(rowKeyCols),
          after:
            run.resumeFrom === null
              ? undefined
              : resumeWhere(rowKeyCols, run.keySpecs, run.resumeFrom),
        }
      : undefined;
    const stmts = makeDownloadStatements(
      tableSpec,
      cols,
      undefined,
      undefined,
      undefined,
      order,
    );

    if (textCopy) {
      const types = await getTypeParsers(db, {returnJsonAsString: true});
      yield* stream(
        lc,
        tx,
        backfill,
        run,
        stmts,
        `COPY (${stmts.select}) TO STDOUT`,
        new TsvParser(),
        cols.map(col => {
          const parser = types.getTypeParser(tableSpec.columns[col].typeOID);
          return (text: string) => parser(text) as JSONValue;
        }),
        flushThresholdBytes,
      );
    } else {
      const binaryStmts = makeDownloadStatements(
        tableSpec,
        cols,
        undefined,
        undefined,
        makeBinarySelectExprs(tableSpec, cols),
        order,
      );

      yield* stream(
        lc,
        tx,
        backfill,
        run,
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

/**
 * The identity and shape of a single backfill run: one snapshot, one ordered
 * (or unordered) pass over the table's rows, one completion.
 */
type Run = {
  /** Random, and unique across replication-managers. */
  readonly runID: string;

  /**
   * Whether the COPY is ordered by the row key, which is what makes the run
   * resumable. False when the key's types or collation rule it out, when
   * ordering would be too expensive (see `isCheaplyOrderable`), or when
   * resume is turned off.
   */
  readonly ordered: boolean;

  /** The mark this run resumes after, or null for "from the beginning". */
  readonly resumeFrom: Mark | null;

  /** One spec per row key column, in `relation.rowKey.columns` order. */
  readonly keySpecs: readonly ResumeColumnSpec[];
};

async function* stream<T>(
  lc: LogContext,
  tx: TransactionPool,
  backfill: BackfillParams,
  run: Run,
  {
    getTotalRows,
    getTotalBytes,
  }: Pick<DownloadStatements, 'getTotalRows' | 'getTotalBytes'>,
  copyCommand: string,
  parser: {parse(chunk: Buffer): Iterable<T | null>},
  decoders: ((field: T) => JSONValue)[],
  flushThresholdBytes: number,
): AsyncGenerator<BackfillMessage> {
  // Backfill must read every row: TABLESAMPLE / LIMIT are reserved for shadow
  // sync and must never appear in a backfill COPY.
  assert(
    !SAMPLE_OR_LIMIT_RE.test(copyCommand),
    `backfill COPY must not sample or limit: ${copyCommand}`,
  );
  const {runID, ordered, resumeFrom, keySpecs} = run;

  /**
   * The mark of the last row of a batch, which a following subscriber
   * persists so that a later run can resume after it. Only an ordered run
   * produces one; a batch can also be empty when a chunk boundary falls
   * inside a row.
   */
  const lastKeyOf = (rows: JSONValue[][]): {lastKey?: Mark} =>
    ordered && rows.length > 0 ? {lastKey: markOfLastRow(keySpecs, rows)} : {};

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

  // Announce the run before it sends any rows. A subscriber that has processed
  // this announcement and every message since holds every row of the run after
  // `resumeFrom`, which is what lets it decide whether it may honor the run's
  // completion without ever comparing keys.
  //
  // Yielded here rather than at the top of the function: yielding suspends
  // this generator until the manager has reserved the change stream and
  // pushed the message, and doing that between opening the snapshot
  // transaction and querying it leaves the transaction idle long enough for
  // the pool to close it -- which surfaces as the table not existing.
  const started: BackfillStarted = {
    tag: 'backfill-started',
    relation: backfill.relation,
    columns: backfill.columns,
    watermark: backfill.watermark,
    runID,
    resumeFrom,
  };
  yield {message: started, byteSize: 0};

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

  // Tracks the row being parsed.
  let row: JSONValue[] = Array.from({length: decoders.length});
  let col = 0;

  for (;;) {
    const chunk = await chunks.dequeue();
    if (chunk === 'done') {
      break;
    }
    for (const field of parser.parse(chunk)) {
      row[col] = field === null ? null : decoders[col](field);

      if (++col === decoders.length) {
        rowValues.push(row);
        status.rows++;
        row = Array.from({length: decoders.length});
        col = 0;
      }
    }
    bufferedBytes += chunk.byteLength;
    totalBytes += chunk.byteLength;

    if (bufferedBytes >= flushThresholdBytes) {
      yield {
        message: {
          tag: 'backfill',
          ...backfill,
          rowValues,
          status,
          runID,
          ...lastKeyOf(rowValues),
        },
        byteSize: bufferedBytes,
      };
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
        runID,
        ...lastKeyOf(rowValues),
      },
      byteSize: bufferedBytes,
    };
    totalMsgs++;
    logFlushed();
  }

  yield {
    message: {tag: 'backfill-completed', ...backfill, status, runID},
    byteSize: 0,
  };
  elapsed = (performance.now() - start).toFixed(3);
  lc.info?.(
    `Finished streaming run ${runID}: ${status.rows} rows, ${totalMsgs} msgs, ` +
      `${totalBytes} bytes (${elapsed} ms)`,
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
  opts: {
    resume: boolean;
    minKeyCorrelation: number | undefined;
    runID: string;
  },
): Promise<{
  tableSpec: PublishedTableSpec;
  backfill: BackfillParams;
  run: Run;
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
      ...Object.entries(bf.columns),
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
    const rowKeyCols = Object.keys(tableMeta.rowKey);
    const backfill: BackfillParams = {
      relation: {
        schema: bf.table.schema,
        name: bf.table.name,
        rowKey: {columns: rowKeyCols},
      },
      columns: Object.keys(bf.columns).filter(
        col => !(col in tableMeta.rowKey),
      ),
      watermark,
    };
    const run = await planRun(lc, sql, spec, rowKeyCols, bf, opts);
    return {tableSpec: spec, backfill, run};
  });
}

/**
 * Decides whether this run is ordered — and therefore resumable — and, if so,
 * where it resumes from.
 *
 * A run is ordered only when every row key column has an exact, safely
 * inlinable text form (`isResumableKey`) *and* ordering by the key is cheap,
 * i.e. the heap is already close to key order (`isCheaplyOrderable`).
 * Otherwise the COPY is unordered, exactly as it is today, and the run
 * announces `resumeFrom: null`, which every subscriber follows.
 */
async function planRun(
  lc: LogContext,
  sql: PostgresTransaction,
  spec: PublishedTableSpec,
  rowKeyCols: string[],
  bf: BackfillRequest,
  {
    resume,
    minKeyCorrelation,
    runID,
  }: {resume: boolean; minKeyCorrelation: number | undefined; runID: string},
): Promise<Run> {
  const unordered: Run = {
    runID,
    ordered: false,
    resumeFrom: null,
    keySpecs: [],
  };
  if (!resume || rowKeyCols.length === 0) {
    return unordered;
  }
  const collations = await getKeyCollations(sql, spec.oid, rowKeyCols);
  const keySpecs = rowKeyCols.map((col): ResumeColumnSpec => ({
    ...spec.columns[col],
    collationIsDeterministic: collations.get(col) ?? null,
  }));
  if (!isResumableKey(keySpecs)) {
    lc.info?.(
      `run ${runID} is not resumable: the row key ` +
        `(${rowKeyCols.join(',')}) has a type or collation that cannot be ` +
        `resumed from`,
    );
    return unordered;
  }
  const correlation = await getKeyCorrelation(sql, spec.oid, rowKeyCols[0]);
  if (!isCheaplyOrderable(correlation, minKeyCorrelation)) {
    lc.info?.(
      `run ${runID} is not resumable: ordering by ${rowKeyCols[0]} would be ` +
        `a scattered heap scan (correlation ${correlation})`,
    );
    return unordered;
  }

  // A mark with the wrong arity is a mark from a different row key, i.e. one
  // recorded before a key change the change-streamer did not catch. Start
  // over rather than resume from it.
  const {resumeFrom = null} = bf;
  if (resumeFrom !== null && resumeFrom.length !== rowKeyCols.length) {
    lc.warn?.(
      `run ${runID} ignoring a mark with ${resumeFrom.length} values for a ` +
        `${rowKeyCols.length} column row key`,
    );
    return {runID, ordered: true, resumeFrom: null, keySpecs};
  }
  return {runID, ordered: true, resumeFrom, keySpecs};
}
