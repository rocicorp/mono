import type {Database, Statement} from '../../../../zqlite/src/db.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {extractChangeSubstring} from '../change-streamer/change-log-codec.ts';
import {CHANGE_LOG_STREAM_TABLE} from './schema/change-log-stream.ts';

type ChangeTag = ChangeStreamData[1]['tag'];

export type ChangeLogStreamTransactionStats = {
  readonly rows: number;
  readonly estimatedBytes: number;
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
 * Appends the canonical downstream stream to the replica-local change log.
 *
 * Transaction ownership deliberately remains with {@link ChangeProcessor}.
 * This class only tracks the current stream watermark and position, and every
 * statement it executes participates in the transaction already opened by the
 * processor.
 */
export class ChangeLogStreamWriter {
  readonly #insertChange: Statement;
  readonly #insertCommit: Statement;

  #watermark: string | undefined;
  #pos = 0;
  #estimatedBytes = 0;

  constructor(db: Database) {
    this.#insertChange = db.prepare(/*sql*/ `
      INSERT INTO "${CHANGE_LOG_STREAM_TABLE}"
        ("watermark", "pos", "change")
        VALUES (?, ?, ?)
    `);
    this.#insertCommit = db.prepare(/*sql*/ `
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
    this.#watermark = watermark;
    this.#pos = 0;
    const change = extractChangeSubstring(json, 'begin');
    this.#insertChange.run(watermark, this.#pos, change);
    this.#estimatedBytes = estimateChangeLogStreamRowBytes(watermark, change);
  }

  append(json: string, tag: ChangeTag): void {
    const watermark = this.#requireWatermark();
    const change = extractChangeSubstring(json, tag);
    this.#insertChange.run(watermark, ++this.#pos, change);
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
    const stats = {
      rows: this.#pos + 1,
      estimatedBytes:
        this.#estimatedBytes +
        estimateChangeLogStreamRowBytes(watermark, change, precommit, true),
    };
    this.#watermark = undefined;
    this.#pos = 0;
    this.#estimatedBytes = 0;
    return stats;
  }

  rollback(): void {
    this.#watermark = undefined;
    this.#pos = 0;
    this.#estimatedBytes = 0;
  }

  #requireWatermark(): string {
    assertInvariant(
      this.#watermark !== undefined,
      'change-log stream message received outside of a transaction',
    );
    return this.#watermark;
  }
}
