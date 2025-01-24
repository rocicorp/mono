import {trace, type Attributes} from '@opentelemetry/api';
import type {LogContext} from '@rocicorp/logger';
import SQLite3Database, {
  SqliteError,
  type RunResult,
  type Statement as SQLite3Statement,
} from '@rocicorp/zero-sqlite3';
import {manualSpan} from '../../otel/src/span.js';
import {version} from '../../otel/src/version.js';

const tracer = trace.getTracer('view-syncer', version);

export class Database {
  readonly #db: SQLite3Database.Database;
  readonly #threshold: number;
  readonly #lc: LogContext;

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

  pragma(sql: string): unknown {
    return this.#run('pragma', sql, () => this.#db.pragma(sql));
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
      logIfSlow(
        this.#lc.withContext('method', method),
        performance.now() - start,
        {method},
        this.#threshold,
      );
    }
  }

  close(): void {
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
}

export class Statement {
  readonly #stmt: SQLite3Statement;
  readonly #lc: LogContext;
  readonly #threshold: number;
  readonly #attrs: Attributes;

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
  }

  safeIntegers(useBigInt: boolean): this {
    this.#stmt.safeIntegers(useBigInt);
    return this;
  }

  run(...params: unknown[]): RunResult {
    const start = performance.now();
    const ret = this.#stmt.run(...params);
    logIfSlow(
      this.#lc.withContext('method', 'run'),
      performance.now() - start,
      {...this.#attrs, method: 'run'},
      this.#threshold,
    );
    return ret;
  }

  get<T>(...params: unknown[]): T {
    const start = performance.now();
    const ret = this.#stmt.get(...params);
    logIfSlow(
      this.#lc.withContext('method', 'get'),
      performance.now() - start,
      {...this.#attrs, method: 'get'},
      this.#threshold,
    );
    return ret as T;
  }

  all<T>(...params: unknown[]): T[] {
    const start = performance.now();
    const ret = this.#stmt.all(...params);
    logIfSlow(
      this.#lc.withContext('method', 'all'),
      performance.now() - start,
      {...this.#attrs, method: 'all'},
      this.#threshold,
    );
    return ret as T[];
  }

  iterate<T>(...params: unknown[]): IterableIterator<T> {
    return new LoggingIterableIterator(
      this.#lc.withContext('method', 'iterate'),
      this.#attrs,
      this.#stmt.iterate(...params),
      this.#threshold,
    ) as IterableIterator<T>;
  }
}

class LoggingIterableIterator<T> implements IterableIterator<T> {
  readonly #lc: LogContext;
  readonly #it: IterableIterator<T>;
  readonly #threshold: number;
  readonly #attrs: Attributes;
  #start: number;
  #sqliteRowTimeSum: number;

  constructor(
    lc: LogContext,
    attrs: Attributes,
    it: IterableIterator<T>,
    slowQueryThreshold: number,
  ) {
    this.#lc = lc;
    this.#attrs = attrs;
    this.#it = it;
    this.#start = NaN;
    this.#threshold = slowQueryThreshold;
    this.#sqliteRowTimeSum = 0;
  }

  next(): IteratorResult<T> {
    const start = performance.now();
    const ret = this.#it.next();
    const elapsed = performance.now() - start;
    this.#sqliteRowTimeSum += elapsed;
    if (ret.done) {
      this.#log();
    }
    return ret;
  }

  #log() {
    logIfSlow(
      this.#lc.withContext('type', 'total'),
      performance.now() - this.#start,
      {...this.#attrs, type: 'total', method: 'iterate'},
      this.#threshold,
    );
    logIfSlow(
      this.#lc.withContext('type', 'sqlite'),
      this.#sqliteRowTimeSum,
      {...this.#attrs, type: 'sqlite', method: 'iterate'},
      this.#threshold,
    );
  }

  [Symbol.iterator](): IterableIterator<T> {
    this.#start = performance.now();
    return this;
  }

  return(): IteratorResult<T> {
    this.#log();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return this.#it.return?.() as any;
  }

  throw(e: unknown): IteratorResult<T> {
    this.#log();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return this.#it.throw?.(e) as any;
  }
}

function logIfSlow(
  lc: LogContext,
  elapsed: number,
  attrs: Attributes,
  threshold: number,
): void {
  if (elapsed >= threshold) {
    for (const [key, value] of Object.entries(attrs)) {
      lc = lc.withContext(key, value);
    }
    lc.warn?.('Slow query', elapsed);
    manualSpan(tracer, 'db.slow-query', elapsed, attrs);
  }
}

/**
 * An error indicating that the Database failed to open. This essentially
 * wraps the TypeError thrown by the better-sqlite3 package with something
 * more specific.
 */
export class DatabaseInitError extends Error {
  constructor(msg: string, options?: ErrorOptions) {
    super(msg, options);
  }
}
