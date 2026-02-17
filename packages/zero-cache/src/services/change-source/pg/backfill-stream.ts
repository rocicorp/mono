import {
  PG_UNDEFINED_COLUMN,
  PG_UNDEFINED_TABLE,
} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import postgres from 'postgres';
import {equals} from '../../../../../shared/src/set-utils.ts';
import * as v from '../../../../../shared/src/valita.ts';
import {READONLY} from '../../../db/mode-enum.ts';
import {TsvParser} from '../../../db/pg-copy.ts';
import {getTypeParsers, type TypeParser} from '../../../db/pg-type-parser.ts';
import type {PublishedTableSpec} from '../../../db/specs.ts';
import {TransactionPool} from '../../../db/transaction-pool.ts';
import {pgClient} from '../../../types/pg.ts';
import {SchemaIncompatibilityError} from '../common/backfill-manager.ts';
import type {
  BackfillCompleted,
  BackfillRequest,
  JSONValue,
  MessageBackfill,
} from '../protocol/current.ts';
import {
  columnMetadataSchema,
  tableMetadataSchema,
} from './backfill-metadata.ts';
import {
  createReplicationSlot,
  makeSelectPublishedStmt,
} from './initial-sync.ts';
import {toStateVersionString} from './lsn.ts';
import {getPublicationInfo} from './schema/published.ts';
import type {Replica} from './schema/shard.ts';

type BackfillParams = Omit<BackfillCompleted, 'tag'>;

type StreamOptions = {
  /**
   * The number of bytes at which to flush a batch of rows in a
   * backfill message. Defaults to Node's getDefaultHighWatermark().
   */
  flushThresholdBytes?: number;
};

// The size of chunks that Postgres sends on COPY stream.
// This happens to match NodeJS's getDefaultHighWatermark()
// (for Node v20+).
const POSTGRES_COPY_CHUNK_SIZE = 64 * 1024;

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
): AsyncGenerator<MessageBackfill | BackfillCompleted> {
  lc = lc
    .withContext('component', 'backfill')
    .withContext('table', bf.table.name);

  const {flushThresholdBytes = POSTGRES_COPY_CHUNK_SIZE} = opts;
  const db = pgClient(lc, upstreamURI, {
    connection: {['application_name']: 'backfill-stream'},
  });
  const tx = new TransactionPool(lc, READONLY).run(db);
  try {
    const watermark = await setSnapshot(lc, upstreamURI, tx, slot);
    const {tableSpec, backfill} = await validateSchema(
      tx,
      publications,
      bf,
      watermark,
    );
    const types = await getTypeParsers(db, {returnJsonAsString: true});

    // Note: validateSchema ensures that the rowKey and columns are disjoint
    const {relation, columns} = backfill;
    const cols = [...relation.rowKey.columns, ...columns];

    yield* stream(
      lc,
      tx,
      backfill,
      makeSelectPublishedStmt(tableSpec, cols),
      cols.map(col => types.getTypeParser(tableSpec.columns[col].typeOID)),
      flushThresholdBytes,
    );
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
    tx.setDone();
    // errors are already thrown and handled from processReadTask()
    void tx.done().catch(() => {});
    // Workaround postgres.js hanging at the end of some COPY commands:
    // https://github.com/porsager/postgres/issues/499
    void db.end().catch(e => lc.warn?.(`error closing backfill connection`, e));
  }
}

async function* stream(
  lc: LogContext,
  tx: TransactionPool,
  backfill: BackfillParams,
  selectStmt: string,
  colParsers: TypeParser[],
  flushThresholdBytes: number,
): AsyncGenerator<MessageBackfill | BackfillCompleted> {
  const start = performance.now();
  lc.info?.(`Starting backfill copy stream:`, selectStmt);
  const copyStream = await tx.processReadTask(sql =>
    sql.unsafe(`COPY (${selectStmt}) TO STDOUT`).readable(),
  );

  const tsvParser = new TsvParser();
  let totalRows = 0;
  let totalBytes = 0;
  let totalMsgs = 0;
  let rowValues: JSONValue[][] = [];
  let bufferedBytes = 0;

  const logFlushed = () => {
    lc.debug?.(
      `Flushed ${rowValues.length} rows, ${bufferedBytes} bytes ` +
        `(total: rows=${totalRows}, msgs=${totalMsgs}, bytes=${totalBytes})`,
    );
  };

  // Tracks the row being parsed.
  let row: JSONValue[] = Array.from({length: colParsers.length});
  let col = 0;

  for await (const data of copyStream) {
    const chunk = data as Buffer;
    for (const text of tsvParser.parse(chunk)) {
      row[col] = text === null ? null : (colParsers[col](text) as JSONValue);

      if (++col === colParsers.length) {
        rowValues.push(row);
        totalRows++;
        row = Array.from({length: colParsers.length});
        col = 0;
      }
    }
    bufferedBytes += chunk.byteLength;
    totalBytes += chunk.byteLength;

    if (bufferedBytes >= flushThresholdBytes) {
      yield {tag: 'backfill', ...backfill, rowValues};
      totalMsgs++;
      logFlushed();
      rowValues = [];
      bufferedBytes = 0;
    }
  }

  // Flush the last batch of rows.
  if (rowValues.length > 0) {
    yield {tag: 'backfill', ...backfill, rowValues};
    totalMsgs++;
    logFlushed();
  }

  yield {tag: 'backfill-completed', ...backfill};
  const elapsed = performance.now() - start;
  lc.info?.(
    `Finished streaming ${totalRows} rows, ${totalMsgs} msgs, ${totalBytes} bytes ` +
      `(${elapsed.toFixed(3)} ms)`,
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
async function setSnapshot(
  lc: LogContext,
  upstreamURI: string,
  tx: TransactionPool,
  slotNamePrefix: string,
) {
  const replicationSession = pgClient(lc, upstreamURI, {
    ['fetch_types']: false, // Necessary for the streaming protocol
    connection: {replication: 'database'}, // https://www.postgresql.org/docs/current/protocol-replication.html
  });
  const tempSlot = `${slotNamePrefix}_bf_${Date.now()}`;
  try {
    const {snapshot_name: snapshot, consistent_point: lsn} =
      await createReplicationSlot(lc, replicationSession, tempSlot);

    await tx.processReadTask(sql =>
      sql.unsafe(`SET TRANSACTION SNAPSHOT '${snapshot}'`),
    );
    // Once the snapshot has been set, the replication session and slot can
    // be closed / dropped.
    await replicationSession.unsafe(`DROP_REPLICATION_SLOT "${tempSlot}"`);

    const watermark = toStateVersionString(lsn);
    lc.info?.(`Opened snapshot transaction at LSN ${lsn} (${watermark})`);
    return watermark;
  } catch (e) {
    // In the event of a failure, clean up the replication slot if created.
    await replicationSession.unsafe(
      /*sql*/
      `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
         WHERE slot_name = '${tempSlot}'`,
    );
    lc.error?.(`Failed to create backfill snapshot`, e);
    throw e;
  } finally {
    await replicationSession.end();
  }
}

function validateSchema(
  tx: TransactionPool,
  publications: string[],
  bf: BackfillRequest,
  watermark: string,
): Promise<{
  tableSpec: PublishedTableSpec;
  backfill: BackfillParams;
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
    return {tableSpec: spec, backfill};
  });
}
