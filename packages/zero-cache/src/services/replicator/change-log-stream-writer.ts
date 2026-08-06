import type {Statement} from '../../../../zqlite/src/db.ts';
import type {StatementRunner} from '../../db/statements.ts';
import {
  isSchemaChange,
  type DataOrSchemaChange,
} from '../change-source/protocol/current/data.ts';
import {extractChangeSubstring} from '../change-streamer/change-log-codec.ts';
import {ChangeLogTransactionHasher} from '../change-streamer/change-log-transaction-hash.ts';
import {ChangeLogCookieWriter, type CookieOpTag} from './change-log-cookies.ts';
import {CHANGE_LOG_STREAM_TABLE} from './change-log-db.ts';

export type ChangeLogStreamTransactionStats = {
  readonly rows: number;
  readonly estimatedBytes: number;
  readonly hash: string;
  /**
   * The cookie mutations this transaction's schema changes folded in, in stream
   * order. Empty for the overwhelming majority of transactions.
   */
  readonly cookieMutations: readonly CookieOpTag[];
};

const INTEGER_BYTES = 8;

/**
 * Estimates the retained payload size of a stream row. This deliberately does
 * not claim to measure SQLite b-tree/page overhead; it is stable across the
 * write path and the startup scan used by observability.
 */
export function estimateChangeLogStreamRowBytes(
  watermark: string,
  change: string,
  precommit?: string,
  hasWriteTime = false,
): number {
  return (
    Buffer.byteLength(watermark) +
    INTEGER_BYTES +
    Buffer.byteLength(change) +
    (precommit === undefined ? 0 : Buffer.byteLength(precommit)) +
    (hasWriteTime ? INTEGER_BYTES : 0)
  );
}

export class ChangeLogStreamInvariantError extends Error {
  override readonly name = 'ChangeLogStreamInvariantError';
}

function assertInvariant(
  condition: unknown,
  message: string,
): asserts condition {
  if (!condition) {
    throw new ChangeLogStreamInvariantError(message);
  }
}

/**
 * Appends the canonical downstream stream to the change-log database, which is
 * a separate file from the replica, and folds its schema changes into the log's
 * cookie jar (`change-log-cookies.ts`) as it goes.
 *
 * This class owns the change log's transaction, and its transaction alone. It is
 * driven from the change-streamer's stream loop, which commits it before
 * forwarding a transaction's `commit` message; see `SQLiteChangeLogWriter` in
 * `change-streamer/sqlite-change-log-writer.ts` for the ordering that makes a
 * hole unreachable, and for the fail-soft policy that wraps every call here.
 *
 * Because the cookies are written in that same transaction, they are atomic with
 * the head and {@link rollback} discards them with the rows they were folded
 * from. Neither needs any handling of its own.
 */
export class ChangeLogStreamWriter {
  readonly #db: StatementRunner;
  readonly #insertChange: Statement;
  readonly #insertCommit: Statement;
  readonly #cookies: ChangeLogCookieWriter;

  #watermark: string | undefined;
  #pos = 0;
  #estimatedBytes = 0;
  #hasher: ChangeLogTransactionHasher | undefined;
  #cookieMutations: CookieOpTag[] = [];

  constructor(db: StatementRunner) {
    this.#db = db;
    this.#insertChange = db.db.prepare(/*sql*/ `
      INSERT INTO "${CHANGE_LOG_STREAM_TABLE}"
        ("watermark", "pos", "change")
        VALUES (?, ?, ?)
    `);
    this.#insertCommit = db.db.prepare(/*sql*/ `
      INSERT INTO "${CHANGE_LOG_STREAM_TABLE}"
        ("watermark", "pos", "change", "precommit", "writeTimeMs")
        VALUES (?, ?, ?, ?, ?)
    `);
    this.#cookies = new ChangeLogCookieWriter(db.db);
  }

  begin(watermark: string, json: string): void {
    assertInvariant(
      this.#watermark === undefined,
      `change-log stream transaction already open at ${this.#watermark}`,
    );
    // This process is the sole writer of the change-log database, so the write
    // lock is taken up front rather than risking a failed upgrade from a
    // deferred transaction partway through the append.
    this.#db.beginImmediate();
    this.#watermark = watermark;
    this.#pos = 0;
    const change = extractChangeSubstring(json, 'begin');
    this.#insertChange.run(watermark, this.#pos, change);
    this.#hasher = new ChangeLogTransactionHasher();
    this.#hasher.add({
      watermark,
      pos: this.#pos,
      tag: 'begin',
      change,
      precommit: null,
    });
    this.#estimatedBytes = estimateChangeLogStreamRowBytes(watermark, change);
  }

  /**
   * Appends one data or schema change. The parsed message is taken rather than
   * just its tag because a schema change also mutates the cookie jar, and the
   * caller already holds it — so the fold costs no re-parse.
   *
   * The cookie statements run here, i.e. inside this transaction and in stream
   * order relative to the rows they were folded from. The Postgres change log
   * has to flush its buffered batch to get the same ordering
   * (`Storer.#processQueue()`); SQLite gets it from statement order.
   */
  append(json: string, change: DataOrSchemaChange): void {
    const watermark = this.#requireWatermark();
    const {tag} = change;
    const stored = extractChangeSubstring(json, tag);
    this.#insertChange.run(watermark, ++this.#pos, stored);
    this.#requireHasher().add({
      watermark,
      pos: this.#pos,
      tag,
      change: stored,
      precommit: null,
    });
    this.#estimatedBytes += estimateChangeLogStreamRowBytes(watermark, stored);

    if (isSchemaChange(change)) {
      for (const op of this.#cookies.apply(change)) {
        this.#cookieMutations.push(op.op);
      }
    }
  }

  commit(
    watermark: string,
    json: string,
    writeTimeMs: number,
  ): ChangeLogStreamTransactionStats {
    const precommit = this.#requireWatermark();
    assertInvariant(
      watermark === precommit,
      `change-log stream commit ${watermark} does not match begin ${precommit}`,
    );
    const change = extractChangeSubstring(json, 'commit');
    this.#insertCommit.run(
      watermark,
      ++this.#pos,
      change,
      precommit,
      writeTimeMs,
    );
    const hasher = this.#requireHasher();
    hasher.add({
      watermark,
      pos: this.#pos,
      tag: 'commit',
      change,
      precommit,
    });
    const stats = {
      rows: this.#pos + 1,
      estimatedBytes:
        this.#estimatedBytes +
        estimateChangeLogStreamRowBytes(watermark, change, precommit, true),
      hash: hasher.digest(),
      cookieMutations: this.#cookieMutations,
    };
    // The log is durable at `watermark` from here on, and nothing that can
    // advance the watermark the stream would resume from has run yet. A crash in
    // that window leaves a transaction the resumed stream re-delivers, which
    // reconciliation truncates.
    this.#db.commit();
    this.#reset();
    return stats;
  }

  /**
   * Discards the open transaction, if any. Safe to call when none is open — the
   * caller rolls back on an interrupted change stream, which can arrive between
   * transactions as well as inside one.
   */
  rollback(): void {
    const {inTransaction} = this.#db.db;
    this.#reset();
    if (inTransaction) {
      this.#db.rollback();
    }
  }

  #reset(): void {
    this.#watermark = undefined;
    this.#pos = 0;
    this.#estimatedBytes = 0;
    this.#hasher = undefined;
    // A fresh array rather than a truncation: the committed stats hold a
    // reference to the previous one.
    this.#cookieMutations = [];
  }

  #requireWatermark(): string {
    assertInvariant(
      this.#watermark !== undefined,
      'change-log stream message received outside of a transaction',
    );
    return this.#watermark;
  }

  #requireHasher(): ChangeLogTransactionHasher {
    assertInvariant(
      this.#hasher !== undefined,
      'change-log stream hash received outside of a transaction',
    );
    return this.#hasher;
  }
}
