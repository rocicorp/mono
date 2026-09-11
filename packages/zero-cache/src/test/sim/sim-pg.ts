import {must} from '../../../../shared/src/must.ts';
import * as v from '../../../../shared/src/valita.ts';
import type {ChangeStream} from '../../services/change-source/change-source.ts';
import {
  SchemaIncompatibilityError,
  type BackfillMessage,
} from '../../services/change-source/common/backfill-manager.ts';
import {
  columnMetadataSchema,
  tableMetadataSchema,
} from '../../services/change-source/pg/backfill-metadata.ts';
import type {
  MessageRelation,
  StreamedChange,
  TableMetadata,
} from '../../services/change-source/protocol/current/data.ts';
import type {ChangeStreamMessage} from '../../services/change-source/protocol/current/downstream.ts';
import type {
  BackfillRequest,
  ChangeSourceUpstream,
  Mark,
} from '../../services/change-source/protocol/current/upstream.ts';
import {versionToLexi} from '../../types/lexi-version.ts';
import {Subscription} from '../../types/subscription.ts';
import {currentIncarnation, type Incarnation} from './incarnation.ts';
import type {Trace} from './trace.ts';

export type SimColumnType = 'int8' | 'text' | 'bool';

export type SimColumn = {
  readonly name: string;
  readonly type: SimColumnType;
  readonly attnum: number;
};

export type SimValue = number | string | boolean | null;

export type SimRow = Readonly<Record<string, SimValue>>;

export type SimTable = {
  readonly oid: number;
  readonly name: string;
  /** In attnum order. */
  readonly columns: readonly SimColumn[];
  /** Postgres never reuses an attnum, even a dropped column's. */
  readonly nextAttnum: number;
  /** The row key: the primary key, and the table's one unique index. */
  readonly rowKey: readonly string[];
  readonly indexName: string;
  /** By {@link rowKeyString}. */
  readonly rows: ReadonlyMap<string, SimRow>;
};

/** Upstream as of one commit. A commit copies what it changes. */
export type SimState = {
  readonly tables: ReadonlyMap<string, SimTable>;
};

export type SimCommit = {
  readonly lsn: bigint;
  /** A lexi major, as the PG change source mints from an LSN. */
  readonly watermark: string;
  readonly changes: readonly StreamedChange[];
  /** Upstream after this commit. */
  readonly state: SimState;
};

const PUBLIC_SCHEMA_OID = 2200;
const FIRST_OID = 16384;
const INITIAL_LSN = 30n;

/**
 * How many messages postgres.js hands a consumer before it stops reading the
 * socket (`bufferMessages` in `types/streams.ts`). A read error reaches the
 * consumer once it drains below this, and the failure drops what was queued.
 */
const WIRE_WINDOW = 5;

export const TERMINATED_BY_TAKEOVER =
  'terminating connection due to administrator command';

/**
 * How the next backfill run fails: at its snapshot, before it announces
 * itself, or after sending `afterBatches` batches of rows.
 */
export type BackfillFault =
  | {readonly at: 'snapshot'}
  | {readonly at: 'copy'; readonly afterBatches: number};

/**
 * The upstream Postgres, as the replication code consumes it: committed
 * transactions at increasing LSNs, and one replication slot.
 *
 * It is not a SQL engine. Tables are maps of rows, and each commit records the
 * whole upstream state after it, so any past major can be read back; that is
 * what the oracles compare replicas against.
 *
 * Where Postgres's behavior can hide a bug, SimPG copies it rather than
 * improving on it:
 *
 * - A stream that asks to start below the slot's confirmed flush starts at the
 *   confirmed flush, and says nothing (`CreateDecodingContext` in `logical.c`).
 * - A new stream takes the slot. The previous holder learns at its next read:
 *   at once if it is waiting, or, if it is back-pressured, once its consumer
 *   drains the wire window, and the failure drops what was queued.
 *
 * Nothing is streamed until the simulator delivers it, in bursts, so the depth
 * of the upstream queue is the simulator's to choose.
 */
export class SimPG {
  readonly replicaVersion: string;
  readonly #trace: Trace;
  readonly #commits: SimCommit[];
  readonly #violations: string[] = [];
  #confirmedFlush: string;
  #holder: SimStream | undefined;
  #nextStreamID = 1;
  #nextOID = FIRST_OID;
  #nextName = 1;
  #backfillFault: BackfillFault | undefined;
  #nextRun = 1;

  constructor(trace: Trace, initialLSN = INITIAL_LSN) {
    this.#trace = trace;
    this.replicaVersion = versionToLexi(initialLSN);
    this.#commits = [
      {
        lsn: initialLSN,
        watermark: this.replicaVersion,
        changes: [],
        state: {tables: new Map()},
      },
    ];
    this.#confirmedFlush = this.replicaVersion;
  }

  /** The latest commit; the initial sync before any. */
  get head(): SimCommit {
    return must(this.#commits.at(-1));
  }

  /** Every commit, starting with the initial sync at the replica version. */
  get commits(): readonly SimCommit[] {
    return this.#commits;
  }

  get confirmedFlush(): string {
    return this.#confirmedFlush;
  }

  get holder(): SimStream | undefined {
    return this.#holder;
  }

  /** Protocol violations by the code under test, e.g. an ACK of a minor. */
  takeViolations(): string[] {
    return this.#violations.splice(0);
  }

  /** Starts a transaction on the latest state. */
  begin(): SimTransaction {
    return new SimTransaction(
      this.head.state,
      () => this.#nextOID++,
      () => `t${this.#nextName++}`,
    );
  }

  /**
   * Commits `tx` at an LSN `lsnGap` past the head. A transaction that changed
   * nothing commits nothing, as Postgres streams nothing for it.
   */
  commit(tx: SimTransaction, lsnGap: number): SimCommit | undefined {
    if (tx.changes.length === 0) {
      return undefined;
    }
    const lsn = this.head.lsn + BigInt(Math.max(1, lsnGap));
    const commit: SimCommit = {
      lsn,
      watermark: versionToLexi(lsn),
      changes: tx.changes,
      state: {tables: new Map(tx.tables)},
    };
    this.#commits.push(commit);
    this.#trace.emit('pg', 0, 'pg.commit', {
      watermark: commit.watermark,
      changes: commit.changes.map(c => c.tag),
    });
    return commit;
  }

  /** The last commit at or before `watermark`: what a replica there holds. */
  commitAt(watermark: string): SimCommit {
    return this.#commits[this.#indexAtOrBefore(watermark)];
  }

  /** Commits strictly after `watermark`, in order. */
  commitsAfter(watermark: string): readonly SimCommit[] {
    return this.#commits.slice(this.#indexAtOrBefore(watermark) + 1);
  }

  /** Whether `watermark` is a commit, or the initial sync. */
  isCommit(watermark: string): boolean {
    return (
      this.#commits[this.#indexAtOrBefore(watermark)].watermark === watermark
    );
  }

  /**
   * Starts the slot's stream after `after`, for the calling incarnation,
   * taking the slot from any previous holder.
   */
  startStream(after: string): ChangeStream {
    const stream = this.openStream(after);
    return {
      changes: stream.changes,
      acks: {push: msg => this.#ack(stream, msg)},
    };
  }

  /**
   * {@link startStream}, as the replication protocol has it: the stream
   * itself, whose positions are acknowledged by LSN with {@link ackLSN}.
   */
  openStream(after: string): SimStream {
    const incarnation = currentIncarnation();
    const previous = this.#holder;
    if (previous && !previous.done) {
      this.#trace.emit('pg', 0, 'pg.takeover', {
        stream: previous.id,
        holder: previous.incarnation?.name,
      });
      previous.terminate(new Error(TERMINATED_BY_TAKEOVER));
    }
    const start = after < this.#confirmedFlush ? this.#confirmedFlush : after;
    const stream: SimStream = new SimStream(
      this.#nextStreamID++,
      incarnation,
      this.#indexAtOrBefore(start),
      start,
      i => this.#commits[i].watermark,
      () => {
        if (this.#holder === stream) {
          this.#holder = undefined;
        }
      },
    );
    this.#holder = stream;
    this.#trace.emit('pg', 0, 'pg.start', {
      stream: stream.id,
      holder: incarnation?.name,
      after,
      ...(start === after ? {} : {movedForwardTo: start}),
    });
    return stream;
  }

  /**
   * Hands the holder's consumer the next `n` committed transactions, or a
   * keepalive when there are none.
   */
  deliver(n: number): number {
    const stream = this.#holder;
    if (!stream || stream.done) {
      return 0;
    }
    const txs = this.#commits.slice(stream.cursor + 1, stream.cursor + 1 + n);
    if (txs.length === 0) {
      this.keepalive();
      return 0;
    }
    stream.cursor += txs.length;
    this.#trace.emit('pg', 0, 'pg.deliver', {
      stream: stream.id,
      watermarks: txs.map(tx => tx.watermark),
    });
    stream.send(txs.flatMap(messagesOf));
    return txs.length;
  }

  /**
   * A keepalive that asks to be ACKed. Its position is the walsender's
   * `sentPtr`: how far this stream has been sent, which is never past a
   * commit it has not sent. (Postgres reads the WAL in order, and sends a
   * transaction when it reads the commit.)
   */
  keepalive(): void {
    const stream = this.#holder;
    if (stream && !stream.done) {
      stream.send([['status', {ack: true}, {watermark: stream.sentPosition}]]);
    }
  }

  /**
   * Drops the holder's connection. With `partialMessages`, it first sends
   * that many messages of the next transaction, which never commits on this
   * stream.
   */
  disconnect(partialMessages?: number): boolean {
    const stream = this.#holder;
    if (!stream || stream.done) {
      return false;
    }
    const next = this.#commits[stream.cursor + 1];
    if (partialMessages !== undefined && next) {
      const messages = messagesOf(next);
      const count = Math.min(Math.max(1, partialMessages), messages.length - 1);
      stream.send(messages.slice(0, count));
    }
    this.#trace.emit('pg', 0, 'pg.disconnect', {
      stream: stream.id,
      ...(partialMessages === undefined ? {} : {partialMessages}),
    });
    stream.terminate(new Error('connection to the upstream was lost'));
    return true;
  }

  /**
   * A backfill run ID unique across every session of the run, as a random one
   * is across replication-managers.
   */
  nextRunID(): string {
    return `run-${this.#nextRun++}`;
  }

  /**
   * Fails the next backfill run, as a lost connection does, which the manager
   * retries with backoff.
   */
  failNextBackfill(fault: BackfillFault): void {
    this.#backfillFault = fault;
  }

  /**
   * A backfill run of `req`, as `streamBackfill` makes one: a snapshot of the
   * head when the run starts, the request validated against the snapshot's
   * schema, an announcement, the rows after `resumeFrom` in row key order in
   * batches of `batchRows`, and a completion. Rows come from the snapshot, so a
   * commit after it is not in the run.
   */
  async *streamBackfill(
    req: BackfillRequest,
    {
      runID,
      batchRows,
      resume = true,
    }: {
      readonly runID: string;
      readonly batchRows: number;
      readonly resume?: boolean | undefined;
    },
  ): AsyncGenerator<BackfillMessage> {
    const fault = this.#backfillFault;
    this.#backfillFault = undefined;
    if (fault?.at === 'snapshot') {
      throw new Error('simulated backfill failure at its snapshot');
    }
    const {watermark, state} = this.head;
    const {table, rowKey, columns} = validateBackfill(state, req);
    const relation = {
      schema: 'public',
      name: table.name,
      rowKey: {columns: rowKey},
    };
    // A mark with the wrong arity is from a different row key: start over.
    const resumeFrom =
      resume && req.resumeFrom?.length === rowKey.length
        ? req.resumeFrom
        : null;
    const rows = [...table.rows.values()]
      .filter(
        row => resumeFrom === null || compareKeys(rowKey, row, resumeFrom) > 0,
      )
      .sort((a, b) =>
        compareKeys(
          rowKey,
          a,
          rowKey.map(c => String(b[c])),
        ),
      );

    yield {
      message: {
        tag: 'backfill-started',
        relation,
        columns,
        watermark,
        runID,
        resumeFrom,
      },
      byteSize: 0,
    };
    const status = {rows: 0, totalRows: rows.length};
    for (let batches = 0; batches * batchRows < rows.length; batches++) {
      if (fault?.at === 'copy' && batches === fault.afterBatches) {
        throw new Error('simulated backfill failure mid-copy');
      }
      const batch = rows.slice(batches * batchRows, (batches + 1) * batchRows);
      const rowValues = batch.map(row =>
        [...rowKey, ...columns].map(col => row[col] ?? null),
      );
      status.rows += batch.length;
      const last = must(batch.at(-1));
      yield {
        message: {
          tag: 'backfill',
          relation,
          columns,
          watermark,
          rowValues,
          status: {...status},
          runID,
          ...(resume ? {lastKey: rowKey.map(col => String(last[col]))} : {}),
        },
        byteSize: JSON.stringify(rowValues).length,
      };
    }
    yield {
      message: {
        tag: 'backfill-completed',
        relation,
        columns,
        watermark,
        status: {...status},
        runID,
      },
      byteSize: 0,
    };
  }

  /**
   * Whether the table of `req`, as it is now, has a row keyed in
   * `(from, to]`. A table that no longer goes by its name answers yes, which
   * restarts the run rather than resuming it.
   */
  rowsExist(
    req: BackfillRequest,
    from: Mark | null,
    to: Mark,
  ): Promise<boolean> {
    const table = this.head.state.tables.get(req.table.name);
    return Promise.resolve(
      table === undefined ||
        [...table.rows.values()].some(
          row =>
            (from === null || compareKeys(table.rowKey, row, from) > 0) &&
            compareKeys(table.rowKey, row, to) <= 0,
        ),
    );
  }

  /** Acknowledges `lsn` on `stream`. */
  ackLSN(stream: SimStream, lsn: bigint): void {
    this.#confirm(stream, versionToLexi(lsn));
  }

  #ack(stream: SimStream, msg: ChangeSourceUpstream): void {
    if (msg[0] === 'status') {
      this.#confirm(stream, msg[2].watermark);
    }
  }

  #confirm(stream: SimStream, watermark: string): void {
    if (stream !== this.#holder || stream.done || stream.incarnation?.fenced) {
      this.#trace.emit('pg', 0, 'pg.ack-dropped', {
        stream: stream.id,
        watermark,
      });
      return;
    }
    if (watermark.includes('.')) {
      this.#violation(`ACK of ${watermark}, a minor, which has no LSN`);
    } else if (watermark > this.head.watermark) {
      this.#violation(
        `ACK of ${watermark}, beyond head ${this.head.watermark}`,
      );
    } else if (watermark > this.#confirmedFlush) {
      this.#confirmedFlush = watermark;
      this.#trace.emit('pg', 0, 'pg.confirmed', {watermark});
    }
  }

  #violation(message: string): void {
    this.#violations.push(message);
    this.#trace.emit('pg', 0, 'pg.violation', {message});
  }

  #indexAtOrBefore(watermark: string): number {
    let lo = 0;
    let hi = this.#commits.length - 1;
    while (lo < hi) {
      const mid = (lo + hi + 1) >> 1;
      if (this.#commits[mid].watermark <= watermark) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }
    return lo;
  }
}

/** The slot's stream to one consumer. */
export class SimStream {
  readonly id: number;
  readonly incarnation: Incarnation | undefined;
  readonly changes: Subscription<ChangeStreamMessage>;
  /** Index of the last commit sent. */
  cursor: number;
  readonly #start: string;
  readonly #watermarkAt: (index: number) => string;
  readonly #wire: ChangeStreamMessage[] = [];
  readonly #onDone: () => void;
  #inflight = 0;
  #error: Error | undefined;
  #done = false;

  constructor(
    id: number,
    incarnation: Incarnation | undefined,
    cursor: number,
    start: string,
    watermarkAt: (index: number) => string,
    onDone: () => void,
  ) {
    this.id = id;
    this.incarnation = incarnation;
    this.cursor = cursor;
    this.#start = start;
    this.#watermarkAt = watermarkAt;
    this.#onDone = onDone;
    this.changes = Subscription.create<ChangeStreamMessage>({
      cleanup: () => this.#finish(),
    });
  }

  get done(): boolean {
    return this.#done;
  }

  /**
   * How far this stream has been sent: its start, or the last commit sent,
   * whichever is later.
   */
  get sentPosition(): string {
    const sent = this.#watermarkAt(this.cursor);
    return sent > this.#start ? sent : this.#start;
  }

  /** Messages sent that the consumer has not taken yet. */
  get queued(): number {
    return this.#wire.length + this.changes.queued;
  }

  send(messages: ChangeStreamMessage[]): void {
    if (!this.#done) {
      this.#wire.push(...messages);
      this.#feed();
    }
  }

  terminate(err: Error): void {
    this.#error ??= err;
    this.#feed();
  }

  #feed(): void {
    while (!this.#done && this.#inflight <= WIRE_WINDOW) {
      if (this.#error) {
        this.changes.fail(this.#error);
        return;
      }
      const message = this.#wire.shift();
      if (message === undefined) {
        return;
      }
      this.#inflight++;
      void this.changes.push(message).result.then(() => {
        this.#inflight--;
        this.#feed();
      });
    }
  }

  #finish(): void {
    this.#done = true;
    this.#wire.length = 0;
    this.#onDone();
  }
}

/**
 * Builds one transaction on a copy of upstream state. Tables are named, and
 * resolved by name at each statement, so a statement always sees the ones
 * before it. A statement that Postgres would reject changes nothing and
 * returns false.
 */
export class SimTransaction {
  readonly #tables: Map<string, SimTable>;
  readonly #changes: StreamedChange[] = [];
  readonly #nextOID: () => number;
  readonly #nextName: () => string;

  constructor(base: SimState, nextOID: () => number, nextName: () => string) {
    this.#tables = new Map(base.tables);
    this.#nextOID = nextOID;
    this.#nextName = nextName;
  }

  get tables(): ReadonlyMap<string, SimTable> {
    return this.#tables;
  }

  get changes(): readonly StreamedChange[] {
    return this.#changes;
  }

  /** The table at `index`, modulo the number of tables, in creation order. */
  tableAt(index: number): SimTable | undefined {
    const tables = [...this.#tables.values()];
    tables.sort((a, b) => a.oid - b.oid);
    return tables.length ? tables[index % tables.length] : undefined;
  }

  /**
   * Creates a table keyed by `id int8`, with a column of each of `types`,
   * named `name` or else a name that has never been used.
   */
  createTable(
    types: readonly SimColumnType[],
    name: string = this.#nextName(),
  ): SimTable {
    const columns: SimColumn[] = [
      {name: 'id', type: 'int8', attnum: 1},
      ...types.map((type, i) => ({name: `c${i + 2}`, type, attnum: i + 2})),
    ];
    const table: SimTable = {
      oid: this.#nextOID(),
      name,
      columns,
      nextAttnum: columns.length + 1,
      rowKey: ['id'],
      indexName: `${name}_pkey`,
      rows: new Map(),
    };
    this.#tables.set(name, table);
    this.#changes.push(
      {
        tag: 'create-table',
        spec: {
          schema: 'public',
          name,
          primaryKey: [...table.rowKey],
          columns: Object.fromEntries(
            columns.map(c => [
              c.name,
              {pos: c.attnum, dataType: c.type, notNull: c.name === 'id'},
            ]),
          ),
        },
        metadata: tableMetadata(table),
      },
      {
        tag: 'create-index',
        spec: {
          schema: 'public',
          tableName: name,
          name: table.indexName,
          unique: true,
          columns: Object.fromEntries(table.rowKey.map(col => [col, 'ASC'])),
        },
      },
    );
    return table;
  }

  insert(tableName: string, row: SimRow): boolean {
    const table = this.#tables.get(tableName);
    const key = table && rowKeyString(table, row);
    if (!table || key === undefined || table.rows.has(key)) {
      return false;
    }
    this.#setRows(table, rows => rows.set(key, row));
    this.#changes.push({tag: 'insert', relation: relationOf(table), new: row});
    return true;
  }

  /**
   * Replaces the row at `key` with `row`, which may move its key. `omit` names
   * columns left out of the message, as Postgres leaves out an unchanged
   * TOASTed value; the row keeps their old values.
   */
  update(
    tableName: string,
    key: SimRow,
    row: SimRow,
    omit: readonly string[] = [],
  ): boolean {
    const table = this.#tables.get(tableName);
    if (!table) {
      return false;
    }
    const oldKey = rowKeyString(table, key);
    const newKey = rowKeyString(table, row);
    const old = table.rows.get(oldKey);
    if (!old || (newKey !== oldKey && table.rows.has(newKey))) {
      return false;
    }
    const updated: Record<string, SimValue> = {...row};
    for (const col of omit) {
      updated[col] = old[col] ?? null;
    }
    this.#setRows(table, rows => {
      rows.delete(oldKey);
      rows.set(newKey, updated);
    });
    const sent: Record<string, SimValue> = {...updated};
    for (const col of omit) {
      delete sent[col];
    }
    this.#changes.push({
      tag: 'update',
      relation: relationOf(table),
      key: newKey === oldKey ? null : keyOf(table, key),
      new: sent,
    });
    return true;
  }

  delete(tableName: string, key: SimRow): boolean {
    const table = this.#tables.get(tableName);
    if (!table || !table.rows.has(rowKeyString(table, key))) {
      return false;
    }
    this.#setRows(table, rows => rows.delete(rowKeyString(table, key)));
    this.#changes.push({
      tag: 'delete',
      relation: relationOf(table),
      key: keyOf(table, key),
    });
    return true;
  }

  /**
   * Adds a column whose existing rows take `dflt`. A `backfilling` column's
   * default is volatile: replication never delivers the values it gives, so
   * the change announces a backfill, as the PG change source does for a
   * default it cannot replicate. With no `dflt`, it gives each row a value of
   * its own.
   */
  addColumn(
    tableName: string,
    type: SimColumnType,
    dflt: SimValue,
    backfilling = false,
  ): SimColumn | undefined {
    const table = this.#tables.get(tableName);
    if (!table) {
      return undefined;
    }
    const column: SimColumn = {
      name: `c${table.nextAttnum}`,
      type,
      attnum: table.nextAttnum,
    };
    const updated: SimTable = {
      ...table,
      columns: [...table.columns, column],
      nextAttnum: table.nextAttnum + 1,
      rows: new Map(
        Array.from(table.rows, ([k, row]): [string, SimRow] => [
          k,
          {
            ...row,
            [column.name]:
              backfilling && dflt === null
                ? volatileDefault(type, row, column)
                : dflt,
          },
        ]),
      ),
    };
    this.#tables.set(tableName, updated);
    this.#changes.push({
      tag: 'add-column',
      table: {schema: 'public', name: tableName},
      column: {
        name: column.name,
        spec: {
          pos: column.attnum,
          dataType: type,
          dflt: backfilling ? null : sqlDefault(dflt),
        },
      },
      tableMetadata: tableMetadata(updated),
      ...(backfilling ? {backfill: {attNum: column.attnum}} : {}),
    });
    return column;
  }

  dropColumn(tableName: string, column: string): boolean {
    const table = this.#tables.get(tableName);
    if (
      !table ||
      table.rowKey.includes(column) ||
      !table.columns.some(c => c.name === column)
    ) {
      return false;
    }
    this.#tables.set(tableName, {
      ...table,
      columns: table.columns.filter(c => c.name !== column),
      rows: new Map(
        Array.from(table.rows, ([k, row]): [string, SimRow] => {
          const {[column]: _dropped, ...rest} = row;
          return [k, rest];
        }),
      ),
    });
    this.#changes.push({
      tag: 'drop-column',
      table: {schema: 'public', name: tableName},
      column,
    });
    return true;
  }

  /** Renames a table to a name that has never been used. */
  renameTable(tableName: string): SimTable | undefined {
    const table = this.#tables.get(tableName);
    if (!table) {
      return undefined;
    }
    const renamed: SimTable = {...table, name: this.#nextName()};
    this.#tables.delete(tableName);
    this.#tables.set(renamed.name, renamed);
    this.#changes.push({
      tag: 'rename-table',
      old: {schema: 'public', name: tableName},
      new: {schema: 'public', name: renamed.name},
    });
    return renamed;
  }

  #setRows(table: SimTable, mutate: (rows: Map<string, SimRow>) => void): void {
    const rows = new Map(table.rows);
    mutate(rows);
    this.#tables.set(table.name, {...table, rows});
  }
}

export function rowKeyString(table: SimTable, row: SimRow): string {
  return JSON.stringify(table.rowKey.map(col => row[col] ?? null));
}

function keyOf(table: SimTable, row: SimRow): Record<string, SimValue> {
  return Object.fromEntries(table.rowKey.map(col => [col, row[col] ?? null]));
}

function relationOf(table: SimTable): MessageRelation {
  return {
    schema: 'public',
    name: table.name,
    rowKey: {columns: [...table.rowKey], type: 'default'},
  };
}

function tableMetadata(table: SimTable): TableMetadata {
  return {
    schemaOID: PUBLIC_SCHEMA_OID,
    relationOID: table.oid,
    rowKey: Object.fromEntries(
      table.rowKey.map(col => [
        col,
        {attNum: table.columns.find(c => c.name === col)?.attnum ?? 0},
      ]),
    ),
  };
}

/** A value of its own for each row and column, as a volatile default gives. */
function volatileDefault(
  type: SimColumnType,
  row: SimRow,
  column: SimColumn,
): SimValue {
  const id = Number(row['id']);
  switch (type) {
    case 'int8':
      return id * 1000 + column.attnum;
    case 'text':
      return `${column.name}-${id}`;
    case 'bool':
      return (id + column.attnum) % 2 === 0;
  }
}

/**
 * Compares `row`'s key with `mark`, the Postgres text form of a key, as the
 * backfill COPY's `ORDER BY` and resume condition do. SimPG's keys are int8.
 */
function compareKeys(
  rowKey: readonly string[],
  row: SimRow,
  mark: readonly string[],
): number {
  for (const [i, col] of rowKey.entries()) {
    const diff = Number(row[col]) - Number(mark[i]);
    if (diff !== 0) {
      return diff;
    }
  }
  return 0;
}

/**
 * `validateSchema` in `backfill-stream.ts`: the request's table and columns,
 * checked against the snapshot by OID and attnum, as renames and drops
 * separate them.
 */
function validateBackfill(state: SimState, req: BackfillRequest) {
  const incompatible = (msg: string) =>
    new SchemaIncompatibilityError(req, msg);
  const table = state.tables.get(req.table.name);
  if (!table) {
    throw incompatible(`Table has been renamed or dropped`);
  }
  const meta = v.parse(req.table.metadata, tableMetadataSchema);
  if (meta.schemaOID !== PUBLIC_SCHEMA_OID) {
    throw incompatible(`Schema no longer corresponds to the original schema`);
  }
  if (meta.relationOID !== table.oid) {
    throw incompatible(`Table no longer corresponds to the original table`);
  }
  const rowKey = Object.keys(meta.rowKey);
  if (
    rowKey.length !== table.rowKey.length ||
    rowKey.some(col => !table.rowKey.includes(col))
  ) {
    throw incompatible('Row key (e.g. PRIMARY KEY or INDEX) has changed');
  }
  for (const [col, val] of [
    ...Object.entries(meta.rowKey),
    ...Object.entries(req.columns),
  ]) {
    const column = table.columns.find(c => c.name === col);
    if (!column) {
      throw incompatible(`Column ${col} has been renamed or dropped`);
    }
    if (v.parse(val, columnMetadataSchema).attNum !== column.attnum) {
      throw incompatible(
        `Column ${col} no longer corresponds to the original column`,
      );
    }
  }
  return {
    table,
    rowKey,
    columns: Object.keys(req.columns).filter(col => !(col in meta.rowKey)),
  };
}

function sqlDefault(value: SimValue): string | null {
  switch (typeof value) {
    case 'number':
    case 'boolean':
      return String(value);
    case 'string':
      return `'${value.replaceAll("'", "''")}'::text`;
    default:
      return null;
  }
}

function messagesOf(commit: SimCommit): ChangeStreamMessage[] {
  const {watermark} = commit;
  return [
    ['begin', {tag: 'begin'}, {commitWatermark: watermark}],
    ...commit.changes.map(
      change => ['data', structuredClone(change)] as ChangeStreamMessage,
    ),
    ['commit', {tag: 'commit'}, {watermark}],
  ];
}
