import {assert} from '../../../../shared/src/asserts.ts';
import type {Database, Statement} from '../../../../zqlite/src/db.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {extractChangeSubstring} from '../change-streamer/change-log-codec.ts';
import {CHANGE_LOG_STREAM_TABLE} from './schema/change-log-stream.ts';

type ChangeTag = ChangeStreamData[1]['tag'];

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
    assert(
      this.#watermark === undefined,
      `change-log stream transaction already open at ${this.#watermark}`,
    );
    this.#watermark = watermark;
    this.#pos = 0;
    this.#insertChange.run(
      watermark,
      this.#pos,
      extractChangeSubstring(json, 'begin'),
    );
  }

  append(json: string, tag: ChangeTag): void {
    const watermark = this.#requireWatermark();
    this.#insertChange.run(
      watermark,
      ++this.#pos,
      extractChangeSubstring(json, tag),
    );
  }

  commit(watermark: string, json: string, writeTimeMs: number): void {
    const precommit = this.#requireWatermark();
    assert(
      watermark === precommit,
      `change-log stream commit ${watermark} does not match begin ${precommit}`,
    );
    this.#insertCommit.run(
      watermark,
      ++this.#pos,
      extractChangeSubstring(json, 'commit'),
      precommit,
      writeTimeMs,
    );
    this.#watermark = undefined;
    this.#pos = 0;
  }

  rollback(): void {
    this.#watermark = undefined;
    this.#pos = 0;
  }

  #requireWatermark(): string {
    assert(
      this.#watermark !== undefined,
      'change-log stream message received outside of a transaction',
    );
    return this.#watermark;
  }
}
