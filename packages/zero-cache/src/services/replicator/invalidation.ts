import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {stringify} from 'json-custom-numbers';
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
import type {PostgresDB, PostgresTransaction} from '../../types/pg.js';
import type {RowKeyType, RowValue} from '../../types/row-key.js';
import {rowKeyString} from '../../types/row-key.js';
import type {
  RegisterInvalidationFiltersRequest,
  RegisterInvalidationFiltersResponse,
} from './replicator.js';
import type {TransactionTrain} from './transaction-train.js';
import type {EffectiveRowChange, TableTracker} from './types/table-tracker.js';

export class Invalidator {
  readonly #replica: PostgresDB;
  readonly #txTrain: TransactionTrain;
  readonly #filters: InvalidationFilters;
  readonly #lastRequestedTimes = new Map<string, Date>();

  constructor(
    replica: PostgresDB,
    txTrain: TransactionTrain,
    invalidationFilters: InvalidationFilters,
  ) {
    this.#replica = replica;
    this.#txTrain = txTrain;
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
    const getVersions = (db: PostgresDB) => db<
      {id: string; fromStateVersion: LexiVersion}[]
    >`
    WITH ids (id) AS (VALUES (${values.flatMap((id, i) =>
      i ? [db`),(`, id] : id,
    )}))
      SELECT id, "fromStateVersion" FROM ids
      LEFT JOIN _zero."InvalidationRegistry" USING (id)
      ORDER BY "fromStateVersion";
  `;

    const specs = (await getVersions(this.#replica)).map(row => ({
      id: row.id,
      fromStateVersion: row.fromStateVersion,
    }));
    const latest = specs[specs.length - 1].fromStateVersion;
    if (latest) {
      // Common case: All specs are already registered. Return the latest version.
      return {specs};
    }

    // Register the specs from within the transaction train.
    return this.#txTrain.runNext(async (writer, _readers, fromStateVersion) => {
      // Check again in case registration happened while waiting for the train
      // (e.g. a concurrent request).
      const specs = await writer.processReadTask(async tx =>
        (await getVersions(tx)).map(row => ({
          id: row.id,
          fromStateVersion: row.fromStateVersion,
        })),
      );
      const latest = specs[specs.length - 1].fromStateVersion;
      if (latest) {
        return {specs};
      }

      const unregistered = specs.filter(row => row.fromStateVersion === null);
      writer.process(tx => [
        ...unregistered.map(row => {
          row.fromStateVersion = fromStateVersion;
          const {id} = row;
          const spec = specsByID.get(id);
          const reg = {
            id,
            spec,
            fromStateVersion,
            lastRequested: now,
          };
          return tx`INSERT INTO _zero."InvalidationRegistry" ${tx(reg)}`;
        }),
        // UPDATE the InvalidationRegistryVersion.
        tx`UPDATE _zero."InvalidationRegistryVersion" SET ${tx({
          stateVersionAtLastSpecChange: fromStateVersion,
        })}`,
      ]);

      await writer.processReadTask(tx =>
        this.#filters.ensureCachedFilters(lc, tx, fromStateVersion),
      );

      return {specs};
    });
  }
}

export type CachedFilters = {
  readonly specs: InvalidationFilterSpec[];
  readonly version: LexiVersion | null;
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
    db: PostgresDB,
    expectedVersion?: LexiVersion | null,
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
      version: results[1][0].stateVersionAtLastSpecChange,
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

  #cachedFilters: Promise<CachedFilters> | undefined;
  #invalidations: Set<string> | undefined;

  constructor(filters: InvalidationFilters) {
    this.#filters = filters;
  }

  /**
   * Runs Tasks to initialize the invalidation processing.
   */
  processInitTasks(
    readers: TransactionPool,
    invalidationRegistryVersion: LexiVersion | null,
  ) {
    const {promise, resolve, reject} = resolver<CachedFilters>();
    this.#cachedFilters = promise;

    void readers.processReadTask((tx, lc) =>
      this.#filters
        .ensureCachedFilters(lc, tx, invalidationRegistryVersion)
        .then(resolve, reject),
    );
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
    const cachedFilters = this.#cachedFilters;
    assert(cachedFilters, `#cachedFilters is setup in processInitTasks`);

    // Fire off reads on the `readers` pool to process the effective row changes of each
    // table in parallel. Workers will be spawned as necessary up to the configured
    // maxWorkers parameter of the TransactionPool.
    const hashers: Promise<Set<string>>[] = [];
    for (const table of tables) {
      hashers.push(
        readers.processReadTask((tx, lc) =>
          computeInvalidationHashes(lc, tx, table, cachedFilters),
        ),
      );
    }

    writer.process(async (tx, lc) => {
      const hashSets = await Promise.all(hashers);
      const allHashes = new Set<string>();
      hashSets.forEach(set => set.forEach(hash => allHashes.add(hash)));
      this.#invalidations = allHashes;

      lc.debug?.(`Committing ${allHashes.size} invalidation tags`);
      return [...allHashes].map(
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

  /** Must only be called after reader and writer pools are done. */
  getInvalidations(): Set<string> {
    assert(this.#invalidations, `Invalidations not yet processed`);
    return this.#invalidations;
  }
}

/**
 * The ReadTask run on the `readers` pool to compute invalidation hashes for
 * the row changes of a `table`.
 */
async function computeInvalidationHashes(
  lc: LogContext,
  tx: PostgresTransaction,
  table: TableTracker,
  cachedFilters: Promise<CachedFilters>,
): Promise<Set<string>> {
  const hashes = new Set<string>();
  const {truncated, changes} = table.getEffectiveRowChanges();
  if (truncated) {
    hashes.add(
      invalidationHash({
        schema: table.schema,
        table: table.table,
        allRows: true,
      }),
    );
    // When a table is truncated, all queries for the table are effected.
    // There is no need to compute any finer-grained invalidation tags.
    return hashes;
  }
  // Lookup preValues for UPDATEs and DELETEs.
  const preValues = await lookupUnknownPreValues(
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

  const filters = (await cachedFilters).specs.filter(
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
      hashes.add(invalidationHash(rowTag));
    }
  };

  for (const row of changes.values()) {
    if (row.preValue !== 'none') {
      if (row.preValue !== 'unknown') {
        processRow(row.preValue);
      } else {
        const rowKey = rowKeyString(row.rowKey);
        const preValue = preValues.get(rowKeyString(row.rowKey));
        assert(preValue, `Missing preValue for ${rowKey}`);
        processRow(preValue);
      }
    }
    if (row.postValue !== 'none') {
      processRow(row.postValue);
    }
    // TODO: For UPDATEs there will be both a preValue and postValue.
    // Updates only need to produce invalidations if a filter's selectedColumns
    // or filteredColumns changed. Otherwise, the filter can be skipped.
  }
  return hashes;
}

async function lookupUnknownPreValues(
  lc: LogContext,
  tx: PostgresTransaction,
  schema: string,
  table: string,
  rowKeyType: RowKeyType,
  changes: Map<string, EffectiveRowChange>,
): Promise<Map<string, RowValue>> {
  const keys = [...changes.values()]
    .filter(change => change.preValue === 'unknown')
    .map(change => change.rowKey);
  if (keys.length === 0) {
    return new Map();
  }

  const keyCols = Object.keys(rowKeyType);
  const start = Date.now();
  const rows = await lookupRowsWithKeys(tx, schema, table, rowKeyType, keys);
  lc.debug?.(
    `Looked up ${keys.length} pre-tx values from ${schema}.${table} (${
      Date.now() - start
    } ms)`,
  );
  return new Map(
    rows.map(row => [
      rowKeyString(Object.fromEntries(keyCols.map(col => [col, row[col]]))),
      row,
    ]),
  );
}
