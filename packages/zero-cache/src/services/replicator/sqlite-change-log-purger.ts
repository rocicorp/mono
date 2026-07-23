import {assert} from '../../../../shared/src/asserts.ts';
import type {Database, Statement} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {min} from '../../types/lexi-version.ts';
import {CHANGE_LOG_STREAM_TABLE} from './schema/change-log-stream.ts';

type WatermarkRow = {
  readonly watermark: string;
};

type HeadRow = {
  readonly headWatermark: string;
};

export type SQLiteChangeLogPurgeOptions = {
  /**
   * The minimum backup/subscriber/snapshot watermark supplied by the
   * change-streamer. Rows at this watermark are retained.
   */
  readonly externalFloor: string;
  /** The inclusive lower bound of the time-retention window. */
  readonly retentionCutoffMs: number;
  /**
   * Target number of rows to delete. One oversized oldest transaction can
   * exceed this target so that every productive call makes progress.
   */
  readonly maxRows: number;
};

export type SQLiteChangeLogPurgeResult = {
  readonly headWatermark: string;
  readonly timeFloor: string;
  readonly effectiveFloor: string;
  readonly deletedRows: number;
  /** All deleted rows had a watermark strictly below this value. */
  readonly deletedBeforeWatermark: string | undefined;
  readonly moreEligible: boolean;
};

export const SQLITE_CHANGE_LOG_HEAD_SQL = /*sql*/ `
  SELECT "stateVersion" AS "headWatermark"
  FROM "_zero.replicationState"
`;

/** Exported so a focused query-plan test covers the production query. */
export const SQLITE_CHANGE_LOG_TIME_FLOOR_SQL = /*sql*/ `
  SELECT "watermark"
  FROM "${CHANGE_LOG_STREAM_TABLE}"
  WHERE "writeTimeMs" IS NOT NULL
    AND "writeTimeMs" >= ?
  ORDER BY "writeTimeMs", "watermark"
  LIMIT 1
`;

export const SQLITE_CHANGE_LOG_OLDEST_ELIGIBLE_SQL = /*sql*/ `
  SELECT "watermark"
  FROM "${CHANGE_LOG_STREAM_TABLE}"
  WHERE "watermark" < ?
  ORDER BY "watermark", "pos"
  LIMIT 1
`;

export const SQLITE_CHANGE_LOG_OFFSET_CANDIDATE_SQL = /*sql*/ `
  SELECT "watermark"
  FROM "${CHANGE_LOG_STREAM_TABLE}"
  WHERE "watermark" < ?
  ORDER BY "watermark", "pos"
  LIMIT 1 OFFSET ?
`;

export const SQLITE_CHANGE_LOG_NEXT_WATERMARK_SQL = /*sql*/ `
  SELECT "watermark"
  FROM "${CHANGE_LOG_STREAM_TABLE}"
  WHERE "watermark" > ?
    AND "watermark" < ?
  ORDER BY "watermark", "pos"
  LIMIT 1
`;

export const SQLITE_CHANGE_LOG_DELETE_PREFIX_SQL = /*sql*/ `
  DELETE FROM "${CHANGE_LOG_STREAM_TABLE}"
  WHERE "watermark" < ?
`;

/**
 * Deletes a bounded, transaction-aligned prefix of the replica-local stream
 * log. The purger owns one short SQLite write transaction per call, but no
 * scheduling; callers must serialize it with replica application.
 */
export class SQLiteChangeLogPurger {
  readonly #db: Database;
  readonly #runner: StatementRunner;
  readonly #head: Statement;
  readonly #timeFloor: Statement;
  readonly #oldestEligible: Statement;
  readonly #offsetCandidate: Statement;
  readonly #nextWatermark: Statement;
  readonly #deletePrefix: Statement;

  constructor(db: Database) {
    this.#db = db;
    this.#runner = new StatementRunner(db);
    this.#head = db.prepare(SQLITE_CHANGE_LOG_HEAD_SQL);
    this.#timeFloor = db.prepare(SQLITE_CHANGE_LOG_TIME_FLOOR_SQL);
    this.#oldestEligible = db.prepare(SQLITE_CHANGE_LOG_OLDEST_ELIGIBLE_SQL);
    this.#offsetCandidate = db.prepare(SQLITE_CHANGE_LOG_OFFSET_CANDIDATE_SQL);
    this.#nextWatermark = db.prepare(SQLITE_CHANGE_LOG_NEXT_WATERMARK_SQL);
    this.#deletePrefix = db.prepare(SQLITE_CHANGE_LOG_DELETE_PREFIX_SQL);
  }

  purgeBatch({
    externalFloor,
    retentionCutoffMs,
    maxRows,
  }: SQLiteChangeLogPurgeOptions): SQLiteChangeLogPurgeResult {
    assert(
      Number.isSafeInteger(retentionCutoffMs),
      'SQLite change log retention cutoff must be a safe integer',
    );
    assert(
      Number.isSafeInteger(maxRows) && maxRows > 0,
      'SQLite change log purge batch size must be a positive safe integer',
    );
    assert(
      !this.#db.inTransaction,
      'SQLite change log purge must start outside a transaction',
    );

    let transactionStarted = false;
    try {
      this.#runner.beginImmediate();
      transactionStarted = true;

      const headRow = this.#head.get<HeadRow | undefined>();
      assert(headRow !== undefined, 'replication state must be initialized');
      const {headWatermark} = headRow;
      const timeFloor =
        this.#timeFloor.get<WatermarkRow | undefined>(retentionCutoffMs)
          ?.watermark ?? headWatermark;
      const effectiveFloor = min(externalFloor, timeFloor, headWatermark);

      const oldestEligible = this.#oldestEligible.get<WatermarkRow | undefined>(
        effectiveFloor,
      )?.watermark;
      if (oldestEligible === undefined) {
        this.#runner.commit();
        return {
          headWatermark,
          timeFloor,
          effectiveFloor,
          deletedRows: 0,
          deletedBeforeWatermark: undefined,
          moreEligible: false,
        };
      }

      const candidate = this.#offsetCandidate.get<WatermarkRow | undefined>(
        effectiveFloor,
        maxRows,
      )?.watermark;

      let deleteCeiling: string;
      if (candidate === undefined) {
        // The complete eligible prefix contains at most maxRows rows.
        deleteCeiling = effectiveFloor;
      } else if (candidate !== oldestEligible) {
        // The target offset reached a later transaction. Stop immediately
        // before it, keeping this batch at or below maxRows rows.
        deleteCeiling = candidate;
      } else {
        // The oldest transaction alone is larger than maxRows. Delete it as a
        // soft-limit batch so the purger cannot stall forever.
        deleteCeiling =
          this.#nextWatermark.get<WatermarkRow | undefined>(
            oldestEligible,
            effectiveFloor,
          )?.watermark ?? effectiveFloor;
      }

      const {changes: deletedRows} = this.#deletePrefix.run(deleteCeiling);
      assert(deletedRows > 0, 'SQLite change log purge must make progress');
      const moreEligible =
        this.#oldestEligible.get<WatermarkRow | undefined>(effectiveFloor) !==
        undefined;

      this.#runner.commit();
      return {
        headWatermark,
        timeFloor,
        effectiveFloor,
        deletedRows,
        deletedBeforeWatermark: deleteCeiling,
        moreEligible,
      };
    } catch (e) {
      if (transactionStarted && this.#db.inTransaction) {
        this.#runner.rollback();
      }
      throw e;
    }
  }
}
