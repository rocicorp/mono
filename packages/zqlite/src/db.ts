import type {LogContext} from '@rocicorp/logger';
import SQLite3Database, {Statement as SQLite3Statement} from 'better-sqlite3';

export class Database {
  readonly #db: SQLite3Database.Database;
  readonly #lc: LogContext;
  readonly #threshold: number;

  constructor(lc: LogContext, path: string, slowQueryThreshold = 1000) {
    this.#lc = lc
      .withContext('component', 'Database')
      .withContext('path', path);
    this.#db = new SQLite3Database(path);
    this.#threshold = slowQueryThreshold;
  }

  prepare(sql: string): Statement {
    return new Statement(
      this.#lc.withContext('component', 'Statement').withContext('sql', sql),
      this.#db.prepare(sql),
      this.#threshold,
    );
  }

  exec(sql: string): void {
    const start = performance.now();
    this.#db.exec(sql);
    logIfSlow(
      performance.now() - start,
      this.#lc.withContext('method', 'exec'),
      this.#threshold,
    );
  }
}

class Statement {
  readonly #stmt: SQLite3Statement;
  readonly #lc: LogContext;
  readonly #threshold: number;

  constructor(lc: LogContext, stmt: SQLite3Statement, threshold: number) {
    this.#lc = lc;
    this.#stmt = stmt;
    this.#threshold = threshold;
  }

  run(...params: unknown[]): void {
    const start = performance.now();
    this.#stmt.run(...params);
    logIfSlow(
      performance.now() - start,
      this.#lc.withContext('method', 'run'),
      this.#threshold,
    );
  }

  get<T>(...params: unknown[]): T {
    const start = performance.now();
    const ret = this.#stmt.get(...params);
    logIfSlow(
      performance.now() - start,
      this.#lc.withContext('method', 'get'),
      this.#threshold,
    );
    return ret;
  }

  all<T>(...params: unknown[]): T[] {
    const start = performance.now();
    const ret = this.#stmt.all(...params);
    logIfSlow(
      performance.now() - start,
      this.#lc.withContext('method', 'all'),
      this.#threshold,
    );
    return ret;
  }

  iterate<T>(...params: unknown[]): IterableIterator<T> {
    return new LoggingIterableIterator(
      this.#lc.withContext('method', 'iterate'),
      this.#stmt.iterate(...params),
      this.#threshold,
    );
  }
}

class LoggingIterableIterator<T> implements IterableIterator<T> {
  readonly #lc: LogContext;
  readonly #it: IterableIterator<T>;
  readonly #threshold: number;
  #start: number;
  #sqliteRowTimeSum: number;

  constructor(
    lc: LogContext,
    it: IterableIterator<T>,
    slowQueryThreshold: number,
  ) {
    this.#lc = lc;
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
      performance.now() - this.#start,
      this.#lc.withContext('type', 'total'),
      this.#threshold,
    );
    logIfSlow(
      this.#sqliteRowTimeSum,
      this.#lc.withContext('type', 'sqlite'),
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

function logIfSlow(elapsed: number, lc: LogContext, threshold: number): void {
  if (elapsed >= threshold) {
    lc.error?.('Slow query', elapsed);
  }
}
