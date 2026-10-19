import {trace, type Attributes} from '@opentelemetry/api';
import type {LogContext} from '@rocicorp/logger';
import SQLite3Database, {
  SqliteError,
  type ColumnDefinition,
  type RunResult,
  type Statement as SQLite3Statement,
  type StatementIterator as SQLite3StatementIterator,
} from '@rocicorp/zero-sqlite3';
import {manualSpan} from '../../otel/src/span.ts';
import {version} from '../../otel/src/version.ts';

const tracer = trace.getTracer('view-syncer', version);

// https://www.sqlite.org/pragma.html#pragma_auto_vacuum
const AUTO_VACUUM_INCREMENTAL = 2;

const MB = 1024 * 1024;

function mb(bytes: number): string {
  return (bytes / MB).toFixed(2);
}

export class Database implements Disposable {
  readonly #db: SQLite3Database.Database;
  readonly #threshold: number;
  readonly #lc: LogContext;
  readonly #pageSize: number;

  constructor(
    lc: LogContext,
    path: string,
    options?: SQLite3Database.Options,
    slowQueryThreshold = 100,
  ) {
    try {
      this.#lc = lc.withContext('class', 'Database').withContext('path', path);
      this.#db = new SQLite3Database(path, options);
      this.#threshold = slowQueryThreshold;

      // Keep the WAL when this is the last connection to close the database,
      // which would otherwise checkpoint and delete it. Deleting it discards
      // the WAL that a litestream backup continues from, forcing a new
      // snapshot. See persistWal() for switching out of WAL mode.
      this.#db.persistWal();

      // Match Postgres LIKE/ILIKE semantics. Postgres LIKE is case-sensitive,
      // but SQLite's LIKE operator is case-insensitive by default; enable
      // case-sensitive LIKE so the bare operator matches Postgres. Case-
      // insensitive ILIKE is handled in query-builder.ts by lower()-ing both
      // operands (using the Unicode-aware lower() that @rocicorp/zero-sqlite3
      // provides via ICU).
      this.pragma('case_sensitive_like = ON');

      const [{page_size: pageSize}] = this.pragma<{page_size: number}>(
        'page_size',
      );
      this.#pageSize = pageSize;
    } catch (cause) {
      throw new DatabaseInitError(String(cause), {cause});
    }
  }

  prepare(sql: string): Statement {
    return this.#run(
      'prepare',
      sql,
      () =>
        new Statement(
          this.#lc.withContext('sql', sql),
          {class: 'Statement', sql},
          this.#db.prepare(sql),
          this.#threshold,
        ),
    );
  }

  exec(sql: string): void {
    this.#run('exec', sql, () => this.#db.exec(sql));
  }

  pragma<T = unknown>(sql: string): T[] {
    return this.#run<T[]>('pragma', sql, () => this.#db.pragma(sql) as T[]);
  }

  #bytes(pages: number) {
    return pages * this.#pageSize;
  }

  compact(freeableBytesThreshold: number) {
    const [{freelist_count: freelistCount}] = this.pragma<{
      freelist_count: number;
    }>('freelist_count');

    const freeable = this.#bytes(freelistCount);
    if (freeable < freeableBytesThreshold) {
      this.#lc.debug?.(
        `Not compacting ${this.#db.name}: ${mb(freeable)} freeable MB`,
      );
      return;
    }
    const [{auto_vacuum: autoVacuumMode}] = this.pragma<{auto_vacuum: number}>(
      'auto_vacuum',
    );
    if (autoVacuumMode !== AUTO_VACUUM_INCREMENTAL) {
      this.#lc.warn?.(
        `Cannot compact ${mb(freeable)} MB of ` +
          `${this.#db.name} because AUTO_VACUUM mode is ${autoVacuumMode}.`,
      );
      return;
    }
    const start = Date.now();
    const [{page_count: pageCountBefore}] = this.pragma<{page_count: number}>(
      'page_count',
    );

    this.pragma('incremental_vacuum');

    const [{page_count: pageCountAfter}] = this.pragma<{page_count: number}>(
      'page_count',
    );

    this.#lc.info?.(
      `Compacted ${this.#db.name} from ` +
        `${mb(this.#bytes(pageCountBefore))} MB to ` +
        `${mb(this.#bytes(pageCountAfter))} MB ` +
        `(${Date.now() - start} ms)`,
    );
  }

  unsafeMode(unsafe: boolean) {
    this.#db.unsafeMode(unsafe);
  }

  /**
   * Toggles whether the WAL is kept when this is the last connection to close
   * the database (on by default). Turn it off before switching the database
   * out of WAL mode (e.g. `journal_mode = DELETE` or `OFF`): with it on, the
   * WAL file is left behind and reopened by the next transaction, so the
   * database effectively stays in WAL mode.
   */
  persistWal(persist: boolean) {
    this.#db.persistWal(persist);
  }

  #run<T>(method: string, sql: string, fn: () => T): T {
    const start = performance.now();
    try {
      return fn();
    } catch (e) {
      if (e instanceof SqliteError) {
        e.message += `: ${sql}`;
      }
      throw e;
    } finally {
      const elapsed = performance.now() - start;
      if (elapsed >= this.#threshold) {
        logSlow(this.#lc.withContext('method', method), elapsed, {method});
      }
    }
  }

  /**
   * Closes the database. For writable connections, `PRAGMA optimize` is run
   * first (as recommended by SQLite) unless `optimize` is `false`, which is
   * appropriate for short-lived handles on a database whose statistics are
   * maintained by a longer-lived writer.
   */
  close({optimize = true}: {optimize?: boolean | undefined} = {}): void {
    const start = Date.now();
    if (optimize && !this.#db.readonly) {
      try {
        this.#db.pragma('optimize');
        const elapsed = Date.now() - start;
        if (elapsed > 2) {
          this.#lc.debug?.(`PRAGMA optimized (${elapsed} ms)`);
        }
      } catch (e) {
        this.#lc.warn?.('error running PRAGMA optimize', e);
      }
    }
    this.#db.close();
  }

  transaction<T>(fn: () => T): T {
    return this.#db.transaction(fn)();
  }

  get name() {
    return this.#db.name;
  }

  get inTransaction() {
    return this.#db.inTransaction;
  }

  [Symbol.dispose](): void {
    this.close();
  }
}

export class Statement {
  readonly #stmt: SQLite3Statement;
  readonly #lc: LogContext;
  readonly #threshold: number;
  readonly #attrs: Attributes;
  // Mirrors the native statement's mode, so that setting the mode it is
  // already in, as callers sharing a cached statement do, stays in JS.
  #mode: 'object' | 'raw' | 'pluck' = 'object';
  readonly scanStatus: SQLite3Statement['scanStatusV2'];
  readonly scanStatusReset: SQLite3Statement['scanStatusReset'];
  #columnCount: number | undefined;
  #safeIntegers: boolean | undefined;
  #iterator: SQLite3StatementIterator | undefined;

  constructor(
    lc: LogContext,
    attrs: Attributes,
    stmt: SQLite3Statement,
    threshold: number,
  ) {
    this.#lc = lc.withContext('class', 'Statement');
    this.#attrs = attrs;
    this.#stmt = stmt;
    this.#threshold = threshold;
    this.scanStatus = this.#stmt.scanStatusV2.bind(this.#stmt);
    this.scanStatusReset = this.#stmt.scanStatusReset.bind(this.#stmt);
  }

  safeIntegers(useBigInt: boolean): this {
    // Tracked here, like the row mode, to skip a native call per fetch.
    if (this.#safeIntegers !== useBigInt) {
      this.#stmt.safeIntegers(useBigInt);
      this.#safeIntegers = useBigInt;
    }
    return this;
  }

  /**
   * Return rows as arrays of column values instead of objects. Turning it
   * off has no effect in pluck mode, as in better-sqlite3.
   */
  raw(enabled: boolean): this {
    if (enabled ? this.#mode !== 'raw' : this.#mode === 'raw') {
      this.#stmt.raw(enabled);
      this.#mode = enabled ? 'raw' : 'object';
    }
    return this;
  }

  /**
   * Return the first column of each row instead of the row. Turning it off
   * has no effect in raw mode, as in better-sqlite3.
   */
  pluck(enabled: boolean): this {
    if (enabled ? this.#mode !== 'pluck' : this.#mode === 'pluck') {
      this.#stmt.pluck(enabled);
      this.#mode = enabled ? 'pluck' : 'object';
    }
    return this;
  }

  /** The result columns, in the order of the values in a raw row. */
  columns(): ColumnDefinition[] {
    return this.#stmt.columns();
  }

  run(...params: unknown[]): RunResult {
    const start = performance.now();
    const ret = this.#stmt.run(...params);
    this.#logIfSlow('run', performance.now() - start);
    return ret;
  }

  get<T>(...params: unknown[]): T {
    const start = performance.now();
    const ret = this.#stmt.get(...params);
    this.#logIfSlow('get', performance.now() - start);
    return ret as T;
  }

  all<T>(...params: unknown[]): T[] {
    const start = performance.now();
    const ret = this.#stmt.all(...params);
    this.#logIfSlow('all', performance.now() - start);
    return ret as T[];
  }

  #logIfSlow(method: string, elapsed: number): void {
    if (elapsed >= this.#threshold) {
      logSlow(this.#lc.withContext('method', method), elapsed, {
        ...this.#attrs,
        method,
      });
    }
  }

  iterate<T>(...params: unknown[]): IterableIterator<T> {
    return new LoggingIterableIterator(
      this.#lc,
      this.#attrs,
      this.#stmt.iterate(...params),
      this.#threshold,
    ) as IterableIterator<T>;
  }

  /**
   * Reads the statement's rows in batches. Integers are read as with
   * `safeIntegers(true)`. The cursor must be {@link BatchCursor.close closed}.
   */
  batches(params: readonly unknown[]): BatchCursor {
    this.#columnCount ??= this.#stmt.columns().length;
    const log = new IterationLog(this.#lc, this.#attrs, this.#threshold);
    this.safeIntegers(true).raw(true);
    // Reuse the iterator of this statement's previous cursor, which is closed
    // by then: creating one per fetch is a noticeable part of short fetches.
    const it =
      this.#iterator === undefined
        ? (this.#iterator = this.#stmt.iterate(...params))
        : this.#iterator.restart(...params);
    return new BatchCursor(log, it, this.#columnCount);
  }
}

/**
 * Reads a statement's rows a batch at a time with one native call per batch
 * (`nextBatch`), instead of one per row as `next()` does.
 */
export class BatchCursor {
  readonly #log: IterationLog;
  readonly #it: SQLite3StatementIterator;
  readonly #columns: number;
  #done = false;

  constructor(
    log: IterationLog,
    it: SQLite3StatementIterator,
    columns: number,
  ) {
    this.#log = log;
    this.#it = it;
    this.#columns = columns;
  }

  /**
   * Reads up to `capacity` rows and returns their values in one row-major
   * array, empty once the rows are exhausted.
   */
  next(capacity: number): (bigint | number | string | null)[] {
    if (this.#done) {
      return [];
    }
    const start = performance.now();
    let values: (bigint | number | string | null)[];
    try {
      values = this.#it.nextBatch(capacity) as (
        | bigint
        | number
        | string
        | null
      )[];
    } catch (e) {
      this.#done = true;
      this.#log.log();
      throw e;
    } finally {
      this.#log.addSQLiteTime(performance.now() - start);
    }
    if (values.length < capacity * this.#columns) {
      // The iterator is finished, but the caller is still to consume this
      // batch: close() logs, so that the total time includes that work.
      this.#done = true;
    }
    return values;
  }

  /**
   * Ends the cursor, abandoning any rows that were not read, and logs if the
   * iteration was slow. Must be called once the rows are consumed.
   */
  close(): void {
    if (!this.#done) {
      this.#done = true;
      this.#it.return?.();
    }
    this.#log.log();
  }
}

// Bound consumer-time warnings across all databases and SQL statements in this
// process. Nested iterators can all outlive the threshold during the same stall.
const ITERATOR_TOTAL_LOG_WINDOW_MS = 60_000;
const ITERATOR_TOTAL_LOG_LIMIT = 10;
let iteratorTotalLogWindowStart = -Infinity;
let iteratorTotalLogsInWindow = 0;
let suppressedIteratorTotalLogs = 0;

function admitIteratorTotalLog(now: number): number | undefined {
  if (now - iteratorTotalLogWindowStart >= ITERATOR_TOTAL_LOG_WINDOW_MS) {
    iteratorTotalLogWindowStart = now;
    iteratorTotalLogsInWindow = 0;
  }
  if (iteratorTotalLogsInWindow >= ITERATOR_TOTAL_LOG_LIMIT) {
    suppressedIteratorTotalLogs++;
    return undefined;
  }
  iteratorTotalLogsInWindow++;
  const suppressedCount = suppressedIteratorTotalLogs;
  suppressedIteratorTotalLogs = 0;
  return suppressedCount;
}

/**
 * Logs slow iterations: once the time spent in SQLite, or the total time
 * including the consumer's work between rows, reaches the threshold.
 */
class IterationLog {
  readonly #lc: LogContext;
  readonly #threshold: number;
  readonly #attrs: Attributes;
  readonly #start: number;
  #sqliteTimeSum = 0;
  #logged = false;

  constructor(lc: LogContext, attrs: Attributes, slowQueryThreshold: number) {
    this.#lc = lc;
    this.#attrs = attrs;
    this.#start = performance.now();
    this.#threshold = slowQueryThreshold;
  }

  addSQLiteTime(elapsed: number): void {
    this.#sqliteTimeSum += elapsed;
  }

  log() {
    // TableSource closes iterators in finally, including after next() is done.
    if (this.#logged) {
      return;
    }
    this.#logged = true;

    // Total time includes downstream consumer work and waits between rows.
    // Check it independently of the time spent stepping SQLite.
    const now = performance.now();
    const totalMs = now - this.#start;
    if (totalMs >= this.#threshold) {
      const suppressedCount = admitIteratorTotalLog(now);
      if (suppressedCount !== undefined) {
        logSlow(
          this.#lc,
          totalMs,
          {
            ...this.#attrs,
            type: 'total',
            method: 'iterate',
            sqliteMs: this.#sqliteTimeSum,
            ...(suppressedCount > 0 ? {suppressedCount} : undefined),
          },
          'Slow SQLite iterator total time (including consumer work and waits between rows)',
        );
      }
    }
    if (this.#sqliteTimeSum >= this.#threshold) {
      logSlow(
        this.#lc.withContext('method', 'iterate').withContext('type', 'sqlite'),
        this.#sqliteTimeSum,
        {
          ...this.#attrs,
          type: 'sqlite',
          method: 'iterate',
          totalMs,
        },
      );
    }
  }
}

class LoggingIterableIterator<T> implements IterableIterator<T> {
  readonly #it: IterableIterator<T>;
  readonly #log: IterationLog;

  constructor(
    lc: LogContext,
    attrs: Attributes,
    it: IterableIterator<T>,
    slowQueryThreshold: number,
  ) {
    this.#it = it;
    this.#log = new IterationLog(lc, attrs, slowQueryThreshold);
  }

  next(): IteratorResult<T> {
    const start = performance.now();
    const ret = this.#it.next();
    this.#log.addSQLiteTime(performance.now() - start);
    if (ret.done) {
      this.#log.log();
    }
    return ret;
  }

  [Symbol.iterator](): IterableIterator<T> {
    return this;
  }

  return(): IteratorResult<T> {
    this.#log.log();
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    return this.#it.return?.() as any;
  }

  throw(e: unknown): IteratorResult<T> {
    this.#log.log();
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    return this.#it.throw?.(e) as any;
  }
}

function logSlow(
  lc: LogContext,
  elapsed: number,
  attrs: Attributes,
  message = 'Slow SQLite query',
): void {
  for (const [key, value] of Object.entries(attrs)) {
    lc = lc.withContext(key, value);
  }
  lc.warn?.(message, elapsed);
  manualSpan(tracer, 'db.slow-query', elapsed, attrs);
}

/**
 * An error indicating that the Database failed to open. This essentially
 * wraps the TypeError thrown by the better-sqlite3 package with something
 * more specific.
 */
export class DatabaseInitError extends Error {}
