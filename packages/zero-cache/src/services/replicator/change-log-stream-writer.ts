import type {Statement} from '../../../../zqlite/src/db.ts';
import type {StatementRunner} from '../../db/statements.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {extractChangeSubstring} from '../change-streamer/change-log-codec.ts';
import {ChangeLogTransactionHasher} from '../change-streamer/change-log-transaction-hash.ts';
import {CHANGE_LOG_STREAM_TABLE} from './change-log-db.ts';

type ChangeTag = ChangeStreamData[1]['tag'];

export type ChangeLogStreamTransactionStats = {
  readonly rows: number;
  readonly estimatedBytes: number;
  readonly hash: string;
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
 * a separate file from the replica.
 *
 * This class owns the change log's transaction, and its transaction alone. The
 * replica's remains with {@link ChangeProcessor}, which commits the two in a
 * fixed order — log first, then replica (design 4.4) — so that a crash can
 * leave a phantom transaction in the log but never a hole.
 */
export class ChangeLogStreamWriter {
  readonly #db: StatementRunner;
  readonly #insertChange: Statement;
  readonly #insertCommit: Statement;

  #watermark: string | undefined;
  #pos = 0;
  #estimatedBytes = 0;
  #hasher: ChangeLogTransactionHasher | undefined;

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

  append(json: string, tag: ChangeTag): void {
    const watermark = this.#requireWatermark();
    const change = extractChangeSubstring(json, tag);
    this.#insertChange.run(watermark, ++this.#pos, change);
    this.#requireHasher().add({
      watermark,
      pos: this.#pos,
      tag,
      change,
      precommit: null,
    });
    this.#estimatedBytes += estimateChangeLogStreamRowBytes(watermark, change);
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
    };
    // The log is durable at `watermark` from here on, while the replica is
    // still at the previous version. A crash in that window leaves a phantom,
    // which startup reconciliation truncates.
    this.#db.commit();
    this.#reset();
    return stats;
  }

  /**
   * Discards the open transaction, if any. Safe to call when none is open —
   * the caller's rollback path also runs after a commit that succeeded and a
   * subsequent replica write that did not.
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
