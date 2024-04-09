import type {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {stringify} from 'json-custom-numbers';
import type postgres from 'postgres';
import {assert} from 'shared/src/asserts.js';
import {lookupRowsWithKeys} from '../../db/queries.js';
import type {TransactionPool} from '../../db/transaction-pool.js';
import {
  RowTag,
  invalidationHash,
  parseFilterSpec,
  type InvalidationFilterSpec,
  type NormalizedInvalidationFilterSpec,
} from '../../types/invalidation.js';
import type {LexiVersion} from '../../types/lexi-version.js';
import type {RowKeyType, RowValue} from '../../types/row-key.js';
import {rowKeyString} from '../../types/row-key.js';
import type {
  RegisterInvalidationFiltersRequest,
  RegisterInvalidationFiltersResponse,
} from './replicator.js';
import type {EffectiveRowChange, TableTracker} from './types/table-tracker.js';

/**
 * Metadata, used for selective invalidation and catchup.
 *
 * These tables are created atomically in {@link setupReplicationTables} after
 * the logical replication handoff when initial data synchronization has completed.
 */
export const CREATE_INVALIDATION_TABLES =
  // Invalidation registry.
  //
  // * `id` of the NormalizedInvalidationFilterSpec as computed by
  //   {@link normalizeInvalidationFilterSpec}. This is a base36 encoded 64-bit value,
  //   which has a maximum length of 13 characters.
  //
  // * `spec` contains the InvalidationFilterSpec
  //
  // * `fromStateVersion` indicates when the Replicator first started running
  //   the filter. CVRs at or newer than the version are considered covered.
  //
  // * `lastRequested` records (approximately) the last time the spec was
  //   requested. This is not exact. It may only be updated if the difference
  //   exceeds some interval, for example. This is used to clean up specs that
  //   are no longer used.
  `
CREATE TABLE _zero."InvalidationRegistry" (
  id                 VARCHAR(13) NOT NULL,
  spec               JSONB       NOT NULL,
  "fromStateVersion" VARCHAR(38) NOT NULL,
  "lastRequested"    TIMESTAMPTZ NOT NULL,
  CONSTRAINT "PK_InvalidationRegistry" PRIMARY KEY(id),
  CONSTRAINT "ID_InvalidationRegistry" CHECK (spec ->> 'id' = id)
);
` +
  // A btree over the InvalidationRegistry's "lastRequested" column allows
  // efficient deprecation of invalidation functions.
  `
CREATE INDEX "InvalidationRegistry_lastRequested_btree" 
  ON _zero."InvalidationRegistry" 
  USING BTREE ("lastRequested");
` +
  // A btree over the InvalidationRegistry's "fromStateVersion" column allows
  // efficient sorting of invalidation functions upon registration.
  `
CREATE INDEX "InvalidationRegistry_fromStateVersion_btree" 
  ON _zero."InvalidationRegistry" 
  USING BTREE ("fromStateVersion");
` +
  // A single-row table that tracks the "stateVersion" at which the last change
  // to the InvalidationRegistry's set of `spec`s happened. This is updated, for
  // example, when a new spec is added (with the value being equal to the new spec's
  // `fromStateVersion` column), or when specs are deleted for cleanup.
  //
  // The Invalidator caches this version along with the set of invalidation filter specs,
  // checking the version on every transaction to ensure that it's cache is consistent
  // with the state of the database. If the version has advanced, it reloads the specs
  // from the InvalidationRegistry table.
  //
  // Note: The `lock` column transparently ensures that at most one row exists.
  `
CREATE TABLE _zero."InvalidationRegistryVersion" (
  "stateVersionAtLastSpecChange" VARCHAR(38) NOT NULL,

  lock char(1) NOT NULL CONSTRAINT "DF_InvalidationRegistryVersion" DEFAULT 'v',
  CONSTRAINT "PK_InvalidationRegistryVersion" PRIMARY KEY (lock),
  CONSTRAINT "CK_InvalidationRegistryVersion" CHECK (lock='v')
);
` +
  // Invalidation index.
  //
  // * `hash` is the XXH64 hash of the invalidation tag produced by an invalidation function.
  // * `stateVersion` is the latest stateVersion in which the hash was produced.
  `
CREATE TABLE _zero."InvalidationIndex" (
  hash           BYTEA       NOT NULL,
  "stateVersion" VARCHAR(38) NOT NULL,
  CONSTRAINT "PK_InvalidationIndex" PRIMARY KEY(hash)
);
` +
  // A btree over the InvalidationIndex's "stateVersion" column allows
  // efficient `WHERE "stateVersion"` inequality conditions used for:
  // 1. Determining if newer-than-CVR hashes exist
  // 2. Cleaning up old hashes to keep storage usage in check
  `
CREATE INDEX "InvalidationIndex_stateVersion_btree" 
  ON _zero."InvalidationIndex" 
  USING BTREE ("stateVersion");
`;

export class Invalidator {
  readonly #replica: postgres.Sql;
  readonly #txSerializer: Lock;
  readonly #filters: InvalidationFilters;
  readonly #lastRequestedTimes = new Map<string, Date>();

  constructor(
    replica: postgres.Sql,
    txSerializer: Lock,
    invalidationFilters: InvalidationFilters,
  ) {
    this.#replica = replica;
    this.#txSerializer = txSerializer;
    this.#filters = invalidationFilters;
  }

  async registerInvalidationFilters(
    lc: LogContext,
    req: RegisterInvalidationFiltersRequest,
    now = new Date(),
  ): Promise<RegisterInvalidationFiltersResponse> {
    const specsByID = new Map<string, NormalizedInvalidationFilterSpec>();
    req.specs.forEach(spec => specsByID.set(spec.id, spec));
    if (!specsByID.size) {
      throw new Error(`No specs specified in ${stringify(req)}`);
    }

    // TODO: When spec cleanup is implemented, guarantee that these specs won't disappear.
    for (const id of specsByID.keys()) {
      this.#lastRequestedTimes.set(id, now);
    }

    const values = [...specsByID.keys()].map(id => this.#replica`${id}`);
    const getVersions = (db: postgres.Sql) => db<
      {id: string; fromStateVersion: LexiVersion | null}[]
    >`
    WITH ids (id) AS (VALUES (${values.flatMap((id, i) =>
      i ? [db`),(`, id] : id,
    )}))
      SELECT id, "fromStateVersion" FROM ids
      LEFT JOIN _zero."InvalidationRegistry" USING (id)
      ORDER BY "fromStateVersion";
  `;

    const versions = await getVersions(this.#replica);
    const latest = versions[versions.length - 1].fromStateVersion;
    if (latest) {
      // Common case: All specs are already registered. Return the latest version.
      return {invalidatingFromVersion: latest};
    }

    // Register the specs from within the txSerializer.
    return this.#txSerializer.withLock(() =>
      this.#replica.begin(async tx => {
        // Check again in case registration happened while waiting for the lock
        // (e.g. a concurrent request).
        const versions = await getVersions(tx);
        const latest = versions[versions.length - 1].fromStateVersion;
        if (latest) {
          return {invalidatingFromVersion: latest};
        }

        // Get the current stateVersion.
        const stateVersion = await tx<{max: LexiVersion | null}[]>`
        SELECT MAX("stateVersion") FROM _zero."TxLog";`;
        const fromStateVersion = stateVersion[0].max ?? '00';

        const unregistered = versions.filter(
          row => row.fromStateVersion === null,
        );
        for (const {id} of unregistered) {
          const spec = specsByID.get(id);
          const row = {id, spec, fromStateVersion, lastRequested: now};
          void tx`
          INSERT INTO _zero."InvalidationRegistry" ${tx(row)}
          `.execute();
        }

        // UPSERT the latest version into the InvalidationRegistryVersion.
        void tx`
        INSERT INTO _zero."InvalidationRegistryVersion" ${tx({
          stateVersionAtLastSpecChange: fromStateVersion,
        })}
          ON CONFLICT ON CONSTRAINT "PK_InvalidationRegistryVersion"
          DO UPDATE SET "stateVersionAtLastSpecChange" = EXCLUDED."stateVersionAtLastSpecChange";
          `.execute();

        await this.#filters.ensureCachedFilters(lc, tx, fromStateVersion);

        return {invalidatingFromVersion: fromStateVersion};
      }),
    );
  }
}

export type CachedFilters = {
  readonly specs: InvalidationFilterSpec[];
  readonly version: LexiVersion;
};

/**
 * InvalidationFilters is a shared reference to the most recently loaded and parsed
 * filters from the InvalidationRegistry.
 */
export class InvalidationFilters {
  // Versioned cache of the InvalidationRegistry.
  #cachedFilters: CachedFilters | undefined;

  /**
   * Refreshes the CachedFilters, called whenever the set of specs needs to be loaded or
   * verified against an `expectedVersion`. This must always be called from within the
   * `txSerializer`.
   *
   * @param expectedVersion The expected version as read from the database. This is checked
   *        against any existing CachedFilters to see if they need to be reloaded. If unset,
   *        cached filters are loaded if not yet loaded.
   */
  async ensureCachedFilters(
    lc: LogContext,
    db: postgres.Sql,
    expectedVersion?: LexiVersion,
  ): Promise<CachedFilters> {
    const cached = this.#cachedFilters;
    if (cached && cached.version === (expectedVersion ?? cached.version)) {
      return cached;
    }
    const results = await db`
    SELECT spec FROM _zero."InvalidationRegistry";
    SELECT "stateVersionAtLastSpecChange" FROM _zero."InvalidationRegistryVersion";
    `.simple();

    const loaded: CachedFilters = {
      specs: results[0].map((row: {spec: unknown}) =>
        parseFilterSpec(row.spec),
      ),
      version: results[1].length
        ? results[1][0].stateVersionAtLastSpecChange
        : '00',
    };
    lc.info?.(
      `Loaded ${loaded.specs.length} filters at version ${loaded.version}`,
    );
    this.#cachedFilters = loaded;
    return loaded;
  }
}

export class InvalidationProcessor {
  readonly #filters: InvalidationFilters;
  readonly #invalidationHashes = new Set<string>();

  #cachedFilters: Promise<CachedFilters> | undefined;

  constructor(filters: InvalidationFilters) {
    this.#filters = filters;
  }

  /**
   * Runs Tasks on the `readers` and `writer` pools to initialize the invalidation processing.
   *
   * Implementation: The `writer` task checks the version of the InvalidationRegistry and
   * loads the filters if the version differs from the cached filters. This must be done on
   * the `writer` pool to prevent a concurrent update to the InvalidationRegistry via a
   * `SELECT ... FOR UPDATE`.
   */
  processInitTasks(_readers: TransactionPool, writer: TransactionPool) {
    const {promise, resolve, reject} = resolver<CachedFilters>();
    this.#cachedFilters = promise;

    writer.process((tx, lc) => {
      // Perform a SELECT ... FOR UPDATE query to prevent concurrent invalidation filter registration.
      // Although concurrency should not happen in the steady state because registration is serialized
      // with transaction processing via the `txSerializer`, enforcing this at the database level provides
      // protection in the face of multiple Durable Objects running during an update.
      const stmt = tx`
      SELECT "stateVersionAtLastSpecChange" as version
        FROM _zero."InvalidationRegistryVersion" 
        FOR UPDATE
      `.simple();

      stmt.then(result => {
        this.#filters
          .ensureCachedFilters(lc, tx, result.length ? result[0].version : '00')
          .then(resolve, reject);
      }, reject);
      return [stmt];
    });
  }

  /**
   * Runs Tasks on the `readers` and `writer` pools to complete invalidation processing.
   * Starts reader tasks to lookup any necessary rows for the given row changes and
   * computes the corresponding invalidation hashes. Returns a task for the `writer`
   * pool to commit the hashes, which will block until the hashes are computed.
   */

  processFinalTasks(
    readers: TransactionPool,
    writer: TransactionPool,
    stateVersion: LexiVersion,
    tables: Iterable<TableTracker>,
  ) {
    // Fire off tasks on the `readers` pool to process the effective row changes of each
    // table in parallel. Workers will be spawned as necessary up to the configured
    // maxWorkers parameter of the TransactionPool.
    for (const table of tables) {
      readers.process((tx, lc) =>
        this.#computeInvalidationHashes(lc, tx, table),
      );
    }

    writer.process(async (tx, lc) => {
      // Wait for all #computeInvalidationHashes Tasks on the #readers pool to complete.
      await readers.done();

      lc.debug?.(
        `Committing ${this.#invalidationHashes.size} invalidation tags`,
        [...this.#invalidationHashes],
      );
      return [...this.#invalidationHashes].map(
        hash => tx`
      INSERT INTO _zero."InvalidationIndex" ${tx({
        hash: Buffer.from(hash, 'hex'),
        stateVersion,
      })}
        ON CONFLICT ON CONSTRAINT "PK_InvalidationIndex"
        DO UPDATE SET "stateVersion" = EXCLUDED."stateVersion";
        `,
      );
    });
  }

  /**
   * The Task run on the `readers` pool to compute invalidation hashes for the row
   * changes of a `table`.
   */
  async #computeInvalidationHashes(
    lc: LogContext,
    tx: postgres.TransactionSql,
    table: TableTracker,
  ) {
    const {truncated, changes} = table.getEffectiveRowChanges();
    if (truncated) {
      this.#invalidationHashes.add(
        invalidationHash({
          schema: table.schema,
          table: table.table,
          allRows: true,
        }),
      );
      // When a table is truncated, all queries for the table are effected.
      // There is no need to compute any finer-grained invalidation tags.
      return;
    }
    // Lookup preValues for UPDATEs and DELETEs.
    await lookupUnknownPreValues(
      lc,
      tx,
      table.schema,
      table.table,
      table.rowKeyType,
      changes,
    );

    lc.info?.(
      `Computing invalidation tags for ${changes.size} rows in ${table.schema}.${table.table}`,
    );

    // Await the cached filters
    assert(this.#cachedFilters, `#cachedFilters is setup in processInitTasks`);
    const cachedFilters = await this.#cachedFilters;

    const filters = cachedFilters.specs.filter(
      f => f.schema === table.schema && f.table === table.table,
    );
    const processRow = (row: RowValue) => {
      // Lazily stringified values of filtered columns.
      const stringified: Record<string, string> = {};

      for (const filter of filters) {
        const rowTag: RowTag = {
          schema: table.schema,
          table: table.table,
          filteredColumns: Object.fromEntries(
            Object.keys(filter.filteredColumns).map(col => [
              col,
              (stringified[col] ??= stringify(row[col])),
            ]),
          ),
          selectedColumns: filter.selectedColumns,
        };
        this.#invalidationHashes.add(invalidationHash(rowTag));
      }
    };

    for (const row of changes.values()) {
      assert(row.preValue !== 'unknown'); // all preValues should have looked up.
      if (row.preValue !== 'none') {
        processRow(row.preValue);
      }
      if (row.postValue !== 'none') {
        processRow(row.postValue);
      }
      // TODO: For UPDATEs there will be both a preValue and postValue.
      // Updates only need to produce invalidations if a filter's selectedColumns
      // or filteredColumns changed. Otherwise, the filter can be skipped.
    }
  }
}

async function lookupUnknownPreValues(
  lc: LogContext,
  tx: postgres.Sql,
  schema: string,
  table: string,
  rowKeyType: RowKeyType,
  changes: Map<string, EffectiveRowChange>,
) {
  const keys = [...changes.values()]
    .filter(change => change.preValue === 'unknown')
    .map(change => change.rowKey);
  if (keys.length === 0) {
    return;
  }

  lc.debug?.(`Looking up ${keys.length} pre-tx values from ${schema}.${table}`);

  let count = 0;
  const keyCols = Object.keys(rowKeyType);
  await lookupRowsWithKeys(tx, schema, table, rowKeyType, keys).forEach(row => {
    const key = rowKeyString(
      Object.fromEntries(keyCols.map(col => [col, row[col]])),
    );
    const c = changes.get(key);
    assert(c, `Row does not correspond to change ${key}`);
    assert(c.preValue === 'unknown', `Unexpected preValue: ${key}`);
    c.preValue = row;
    count++;
  });

  assert(count === keys.length, `Expected ${keys.length}, got ${count}`);
}
