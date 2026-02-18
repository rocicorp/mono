import type {LogContext} from '@rocicorp/logger';
import {type Resolver, resolver} from '@rocicorp/resolver';
import type {PendingQuery, Row} from 'postgres';
import {CustomKeyMap} from '../../../../shared/src/custom-key-map.ts';
import {must} from '../../../../shared/src/must.ts';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import * as Mode from '../../db/mode-enum.ts';
import {TransactionPool} from '../../db/transaction-pool.ts';
import {
  getOrCreateCounter,
  getOrCreateHistogram,
} from '../../observability/metrics.ts';
import {
  disableStatementTimeout,
  type PostgresDB,
  type PostgresTransaction,
} from '../../types/pg.ts';
import {rowIDString} from '../../types/row-key.ts';
import {cvrSchema, type ShardID} from '../../types/shards.ts';
import {checkVersion, type CVRFlushStats} from './cvr-store.ts';
import type {CVRSnapshot} from './cvr.ts';
import {
  rowRecordToRowsRow,
  type RowsRow,
  rowsRowToRowRecord,
} from './schema/cvr.ts';
import {
  cmpVersions,
  type CVRVersion,
  type NullableCVRVersion,
  type RowID,
  type RowRecord,
  versionString,
  versionToNullableCookie,
} from './schema/types.ts';

const FLUSH_TYPE_ATTRIBUTE = 'flush.type';

/**
 * The RowRecordCache is an in-memory cache of the `cvr.rows` tables that
 * operates as both a write-through and write-back cache.
 *
 * For "small" CVR updates (i.e. zero or small numbers of rows) the
 * RowRecordCache operates as write-through, executing commits in
 * {@link executeRowUpdates()} before they are {@link apply}-ed to the
 * in-memory state.
 *
 * For "large" CVR updates (i.e. with many rows), the cache switches to a
 * write-back mode of operation, in which {@link executeRowUpdates()} is a
 * no-op, and {@link apply()} initiates a background task to flush the pending
 * row changes to the store. This allows the client poke to be completed and
 * committed on the client without waiting for the heavyweight operation of
 * committing the row records to the CVR store.
 *
 * Note that when the cache is in write-back mode, all updates become
 * write-back (i.e. asynchronously flushed) until the pending update queue is
 * fully flushed. This is required because updates must be applied in version
 * order. As with all pending work systems in zero-cache, multiple pending
 * updates are coalesced to reduce buildup of work.
 *
 * ### High level consistency
 *
 * Note that the above caching scheme only applies to the row data in `cvr.rows`
 * and corresponding `cvr.rowsVersion` tables. CVR metadata and query
 * information, on the other hand, are always committed before completing the
 * client poke. In this manner, the difference between the `version` column in
 * `cvr.instances` and the analogous column in `cvr.rowsVersion` determines
 * whether the data in the store is consistent, or whether it is awaiting a
 * pending update.
 *
 * The logic in {@link CVRStore#load()} takes this into account by loading both
 * the `cvr.instances` version and the `cvr.rowsVersion` version and checking
 * if they are in sync, waiting for a configurable delay until they are.
 *
 * ### Eventual conversion
 *
 * In the event of a continual stream of mutations (e.g. an animation-style
 * app), it is conceivable that the row record data be continually behind
 * the CVR metadata. In order to effect eventual convergence, a new view-syncer
 * signals the current view-syncer to stop updating by writing new `owner`
 * information to the `cvr.instances` row. This effectively stops the mutation
 * processing (in {@link CVRStore.#checkVersionAndOwnership}) so that the row
 * data can eventually catch up, allowing the new view-syncer to take over.
 *
 * Of course, there is the pathological situation in which a view-syncer
 * process crashes before the pending row updates are flushed. In this case,
 * the wait timeout will elapse and the CVR considered invalid.
 */
export class RowRecordCache {
  // The state in the #cache is always in sync with the CVR metadata
  // (i.e. cvr.instances). It may contain information that has not yet
  // been flushed to cvr.rows.
  #cache: Promise<CustomKeyMap<RowID, RowRecord>> | undefined;
  readonly #lc: LogContext;
  readonly #db: PostgresDB;
  readonly #schema: string;
  readonly #cvrID: string;
  readonly #failService: (e: unknown) => void;
  readonly #deferredRowFlushThreshold: number;
  readonly #setTimeout: typeof setTimeout;

  // Write-back cache state.
  readonly #pending = new CustomKeyMap<RowID, RowRecord | null>(rowIDString);
  #pendingRowsVersion: CVRVersion | null = null;
  #flushedRowsVersion: CVRVersion | null = null;
  #flushing: Resolver<void> | null = null;

  readonly #cvrFlushTime = getOrCreateHistogram('sync', 'cvr.flush-time', {
    description:
      'Time to flush a CVR transaction. This includes both synchronous ' +
      'and asynchronous flushes, distinguished by the flush.type attribute',
    unit: 's',
  });
  readonly #cvrRowsFlushed = getOrCreateCounter(
    'sync',
    'cvr.rows-flushed',
    'Number of (changed) rows flushed to a CVR',
  );

  constructor(
    lc: LogContext,
    db: PostgresDB,
    shard: ShardID,
    cvrID: string,
    failService: (e: unknown) => void,
    deferredRowFlushThreshold = 100,
    setTimeoutFn = setTimeout,
  ) {
    this.#lc = lc;
    this.#db = db;
    this.#schema = cvrSchema(shard);
    this.#cvrID = cvrID;
    this.#failService = failService;
    this.#deferredRowFlushThreshold = deferredRowFlushThreshold;
    this.#setTimeout = setTimeoutFn;
  }

  recordSyncFlushStats(stats: CVRFlushStats, elapsedMs: number) {
    this.#cvrFlushTime.record(elapsedMs / 1000, {
      [FLUSH_TYPE_ATTRIBUTE]: 'sync',
    });
    if (stats.rowsDeferred === 0) {
      this.#cvrRowsFlushed.add(stats.rows);
    }
  }

  #recordAsyncFlushStats(rows: number, elapsedMs: number) {
    this.#cvrFlushTime.record(elapsedMs / 1000, {
      [FLUSH_TYPE_ATTRIBUTE]: 'async',
    });
    this.#cvrRowsFlushed.add(rows);
  }

  #cvr(table: string) {
    return this.#db(`${this.#schema}.${table}`);
  }

  async #ensureLoaded(): Promise<CustomKeyMap<RowID, RowRecord>> {
    if (this.#cache) {
      return this.#cache;
    }
    const start = Date.now();
    const r = resolver<CustomKeyMap<RowID, RowRecord>>();
    // Set this.#cache immediately (before await) so that only one db
    // query is made even if there are multiple callers.
    this.#cache = r.promise;
    try {
      const cache: CustomKeyMap<RowID, RowRecord> = new CustomKeyMap(
        rowIDString,
      );
      for await (const rows of this.#db<RowsRow[]>`
        SELECT * FROM ${this.#cvr(`rows`)}
          WHERE "clientGroupID" = ${this.#cvrID} AND "refCounts" IS NOT NULL`
        // TODO(arv): Arbitrary page size
        .cursor(5000)) {
        for (const row of rows) {
          const rowRecord = rowsRowToRowRecord(row);
          cache.set(rowRecord.id, rowRecord);
        }
      }
      this.#lc.debug?.(
        `Loaded ${cache.size} row records in ${Date.now() - start} ms`,
      );
      r.resolve(cache);
      return this.#cache;
    } catch (e) {
      r.reject(e); // Make sure the error is reflected in the cached promise
      throw e;
    }
  }

  getRowRecords(): Promise<ReadonlyMap<RowID, RowRecord>> {
    return this.#ensureLoaded();
  }

  /**
   * Applies the `rowRecords` corresponding to the `rowsVersion`
   * to the cache, indicating whether the corresponding updates
   * (generated by {@link executeRowUpdates}) were `flushed`.
   *
   * If `flushed` is false, the RowRecordCache will flush the records
   * asynchronously.
   *
   * Note that `apply()` indicates that the CVR metadata associated with
   * the `rowRecords` was successfully committed, which essentially means
   * that this process has the unconditional right (and responsibility) of
   * following up with a flush of the `rowRecords`. In particular, the
   * commit of row records are not conditioned on the version or ownership
   * columns of the `cvr.instances` row.
   */
  async apply(
    rowRecords: Map<RowID, RowRecord | null>,
    rowsVersion: CVRVersion,
    flushed: boolean,
  ): Promise<number> {
    const cache = await this.#ensureLoaded();
    for (const [id, row] of rowRecords.entries()) {
      if (row === null || row.refCounts === null) {
        cache.delete(id);
      } else {
        cache.set(id, row);
      }
      if (!flushed) {
        this.#pending.set(id, row);
      }
    }
    this.#pendingRowsVersion = rowsVersion;
    // Initiate a flush if not already flushing.
    if (!flushed && this.#flushing === null) {
      this.#flushing = resolver();
      this.#setTimeout(() => this.#flush(), 0);
    }
    return cache.size;
  }

  async #flush() {
    const flushing = must(this.#flushing);
    try {
      while (this.#pendingRowsVersion !== this.#flushedRowsVersion) {
        const start = performance.now();

        const {rows, rowsVersion} = await this.#db.begin(
          Mode.READ_COMMITTED,
          tx => {
            disableStatementTimeout(tx);

            // Note: This code block is synchronous, guaranteeing that the
            // #pendingRowsVersion is consistent with the #pending rows.
            const rows = this.#pending.size;
            const rowsVersion = must(this.#pendingRowsVersion);
            // Awaiting all of the individual statements incurs too much
            // overhead. Instead, just catch and log exception(s); the outer
            // transaction will properly fail.
            void Promise.all(
              this.executeRowUpdates(tx, rowsVersion, this.#pending, 'force'),
            ).catch(e => this.#lc.error?.(`error flushing cvr rows`, e));

            this.#pending.clear();
            return {rows, rowsVersion};
          },
        );
        const elapsed = performance.now() - start;
        this.#lc.debug?.(
          `flushed ${rows} rows@${versionString(rowsVersion)} (${elapsed} ms)`,
        );
        this.#recordAsyncFlushStats(rows, elapsed);
        this.#flushedRowsVersion = rowsVersion;
        // Note: apply() may have called while the transaction was committing,
        //       which will result in looping to commit the next #pendingRowsVersion.
      }
      this.#lc.debug?.(
        `up to date rows@${versionToNullableCookie(this.#flushedRowsVersion)}`,
      );
      flushing.resolve();
      this.#flushing = null;
    } catch (e) {
      flushing.reject(e);
      this.#failService(e);
    }
  }

  hasPendingUpdates() {
    return this.#flushing !== null;
  }

  /**
   * Returns a promise that resolves when all outstanding row-records
   * have been committed.
   */
  flushed(lc: LogContext): Promise<void> {
    if (this.#flushing) {
      lc.debug?.('awaiting pending row flush');
      return this.#flushing.promise;
    }
    return promiseVoid;
  }

  clear() {
    // Note: Only the #cache is cleared. #pending updates, on the other hand,
    // comprise canonical (i.e. already flushed) data and must be flushed
    // even if the snapshot of the present state (the #cache) is cleared.
    this.#cache = undefined;
  }

  async *catchupRowPatches(
    lc: LogContext,
    afterVersion: NullableCVRVersion,
    upToCVR: CVRSnapshot,
    current: CVRVersion,
    excludeQueryHashes: string[] = [],
  ): AsyncGenerator<RowsRow[], void, undefined> {
    if (cmpVersions(afterVersion, upToCVR.version) >= 0) {
      return;
    }

    const startMs = Date.now();
    const start = afterVersion ? versionString(afterVersion) : '';
    const end = versionString(upToCVR.version);
    lc.debug?.(`scanning row patches for clients from ${start}`);

    // Before accessing the CVR db, pending row records must be flushed.
    // Note that because catchupRowPatches() is called from within the
    // view syncer lock, this flush is guaranteed to complete since no
    // new CVR updates can happen while the lock is held.
    await this.flushed(lc);
    const flushMs = Date.now() - startMs;

    const reader = new TransactionPool(lc, Mode.READONLY).run(this.#db);
    try {
      // Verify that we are reading the right version of the CVR.
      await reader.processReadTask(tx =>
        checkVersion(tx, this.#schema, this.#cvrID, current),
      );

      const {query} = await reader.processReadTask(tx => {
        const query =
          excludeQueryHashes.length === 0
            ? tx<RowsRow[]>`SELECT * FROM ${this.#cvr('rows')}
        WHERE "clientGroupID" = ${this.#cvrID}
          AND "patchVersion" > ${start}
          AND "patchVersion" <= ${end}`
            : // Exclude rows that were already sent as part of query hydration.
              tx<RowsRow[]>`SELECT * FROM ${this.#cvr('rows')}
        WHERE "clientGroupID" = ${this.#cvrID}
          AND "patchVersion" > ${start}
          AND "patchVersion" <= ${end}
          AND ("refCounts" IS NULL OR NOT "refCounts" ?| ${excludeQueryHashes})`;
        return {query};
      });

      yield* query.cursor(10000);
    } finally {
      reader.setDone();
    }

    const totalMs = Date.now() - startMs;
    lc.debug?.(
      `finished row catchup (flush: ${flushMs} ms, total: ${totalMs} ms)`,
    );
  }

  executeRowUpdates(
    tx: PostgresTransaction,
    version: CVRVersion,
    rowUpdates: Map<RowID, RowRecord | null>,
    mode: 'allow-defer' | 'force',
    lc = this.#lc,
  ): PendingQuery<Row[]>[] {
    if (
      mode === 'allow-defer' &&
      // defer if pending rows are being flushed
      (this.#flushing !== null ||
        // or if the new batch is above the limit.
        rowUpdates.size > this.#deferredRowFlushThreshold)
    ) {
      return [];
    }
    const rowsVersion = {
      clientGroupID: this.#cvrID,
      version: versionString(version),
    };
    const pending: PendingQuery<Row[]>[] = [
      tx`INSERT INTO ${this.#cvr('rowsVersion')} ${tx(rowsVersion)}
           ON CONFLICT ("clientGroupID") 
           DO UPDATE SET ${tx(rowsVersion)}`,
    ];

    const rowRecordRows: RowsRow[] = [];
    for (const [id, row] of rowUpdates.entries()) {
      if (row === null) {
        pending.push(
          tx`
          DELETE FROM ${this.#cvr('rows')}
            WHERE "clientGroupID" = ${this.#cvrID}
              AND "schema" = ${id.schema}
              AND "table" = ${id.table}
              AND "rowKey" = ${id.rowKey}
       `,
        );
      } else {
        rowRecordRows.push(rowRecordToRowsRow(this.#cvrID, row));
      }
    }
    if (rowRecordRows.length) {
      pending.push(
        tx`
  INSERT INTO ${this.#cvr('rows')}(
      "clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts"
  ) SELECT
      "clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts"
    FROM json_to_recordset(${rowRecordRows}) AS x(
      "clientGroupID" TEXT,
      "schema" TEXT,
      "table" TEXT,
      "rowKey" JSONB,
      "rowVersion" TEXT,
      "patchVersion" TEXT,
      "refCounts" JSONB
  ) ON CONFLICT ("clientGroupID", "schema", "table", "rowKey")
    DO UPDATE SET "rowVersion" = excluded."rowVersion",
      "patchVersion" = excluded."patchVersion",
      "refCounts" = excluded."refCounts"
    `,
      );
      lc.debug?.(
        `flushing ${rowUpdates.size} rows (${rowRecordRows.length} inserts, ${
          rowUpdates.size - rowRecordRows.length
        } deletes)`,
      );
    }
    return pending;
  }
}
