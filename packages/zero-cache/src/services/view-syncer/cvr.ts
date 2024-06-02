import type {LogContext} from '@rocicorp/logger';
import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import {compareUTF8} from 'compare-utf8';
import {assert, unreachable} from 'shared/src/asserts.js';
import {difference, intersection, union} from 'shared/src/set-utils.js';
import type {DurableStorage} from '../../storage/durable-storage.js';
import type {Storage} from '../../storage/storage.js';
import {WriteCache} from '../../storage/write-cache.js';
import {LexiVersion, versionToLexi} from '../../types/lexi-version.js';
import type {Patch, PatchToVersion} from './client-handler.js';
import type {ParsedRow} from './queries.js';
import {CVRPaths, lastActiveIndex} from './schema/paths.js';
import {
  ClientPatch,
  ClientQueryRecord,
  CvrID,
  InternalQueryRecord,
  MetadataPatch,
  NullableCVRVersion,
  QueryPatch,
  RowID,
  RowPatch,
  RowRecord,
  cmpVersions,
  metaRecordSchema,
  metadataPatchSchema,
  oneAfter,
  rowPatchSchema,
  rowRecordSchema,
  type CVRVersion,
  type ClientRecord,
  type LastActive,
  type QueryRecord,
} from './schema/types.js';

/** Internally used mutable CVR type. */
type CVR = {
  id: string;
  version: CVRVersion;
  lastActive: LastActive;
  clients: Record<string, ClientRecord>;
  queries: Record<string, QueryRecord>;
};

/** Exported immutable CVR type. */
// TODO: Use Immutable<CVR> when the AST is immutable.
export type CVRSnapshot = {
  readonly id: string;
  readonly version: CVRVersion;
  readonly lastActive: LastActive;
  readonly clients: Readonly<Record<string, ClientRecord>>;
  readonly queries: Readonly<Record<string, QueryRecord>>;
};

/** Loads the CVR metadata from storage. */
export async function loadCVR(
  lc: LogContext,
  storage: Storage,
  id: string,
): Promise<CVRSnapshot> {
  const start = Date.now();
  const cvr: CVR = {
    id,
    version: {stateVersion: versionToLexi(0)},
    lastActive: {epochMillis: 0},
    clients: {},
    queries: {},
  };

  const paths = new CVRPaths(id);
  const metaRecords = await storage.list(
    {prefix: paths.metaPrefix()},
    metaRecordSchema, // TODO: Consider an alternative API to union type + casting.
  );
  for (const [key, value] of metaRecords) {
    if (key.endsWith('/version')) {
      cvr.version = value as CVRVersion;
    } else if (key.endsWith('/lastActive')) {
      cvr.lastActive = value as LastActive;
    } else if (key.includes('/c/')) {
      const client = value as ClientRecord;
      cvr.clients[client.id] = client;
    } else if (key.includes('/q/')) {
      const query = value as QueryRecord;
      cvr.queries[query.id] = query;
    }
  }
  lc.debug?.(
    `loaded CVR (${Date.now() - start} ms), ${metaRecords.size} meta entries`,
  );
  return cvr;
}

const CLIENT_LMID_QUERY_ID = 'lmids';

function assertNotInternal(
  query: QueryRecord,
): asserts query is ClientQueryRecord {
  if (query.internal) {
    // This should never happen for behaving clients, as query ids should be hashes.
    throw new Error(`Query ID ${query.id} is reserved for internal use`);
  }
  query satisfies ClientQueryRecord;
}

/**
 * The base CVRUpdater contains logic common to the {@link CVRConfigDrivenUpdater} and
 * {@link CVRQueryDrivenUpdater}. The CVRUpdater class itself is exported for updating
 * the `lastActive` time of the CVR in the absence of any changes to the CVR contents.
 * Although activity is automatically tracked when the CVR contents change, there may be
 * edge cases in which a client actively connects to a CVR that doesn't itself change.
 * Calling `new CVRUpdater(...).flush()` will explicitly update the active index and
 * prevent the CVR from being garbage collected.
 */
export class CVRUpdater {
  protected readonly _directStorage: DurableStorage;
  protected readonly _paths: CVRPaths;
  protected readonly _writes: WriteCache;
  protected readonly _orig: CVRSnapshot;
  protected readonly _cvr: CVR;

  /**
   * @param cvr The current CVR
   * @param stateVersion The db `stateVersion` of the InvalidationUpdate for which this CVR
   *                     is being updated, or absent for config-only updates.
   */
  constructor(storage: DurableStorage, cvr: CVRSnapshot) {
    this._directStorage = storage;
    this._paths = new CVRPaths(cvr.id);
    this._writes = new WriteCache(storage);
    this._orig = cvr;
    this._cvr = structuredClone(cvr) as CVR; // mutable deep copy
  }

  protected _setVersion(version: CVRVersion) {
    assert(cmpVersions(this._cvr.version, version) < 0);
    this._cvr.version = version;
    void this._writes.put(this._paths.version(), this._cvr.version);
    return version;
  }

  /**
   * Ensures that the new CVR has a higher version than the original.
   * This method is idempotent in that it will always return the same
   * (possibly bumped) version.
   */
  protected _ensureNewVersion(): CVRVersion {
    if (cmpVersions(this._orig.version, this._cvr.version) === 0) {
      this._setVersion(oneAfter(this._cvr.version));
    }
    return this._cvr.version;
  }

  #setLastActive(now = new Date()) {
    const oldMillis = this._cvr.lastActive.epochMillis;
    const newMillis = now.getTime();

    // The global index has per-day granularity. Only update if the day changes.
    const oldDay = lastActiveIndex.dayPrefix(oldMillis);
    const newDay = lastActiveIndex.dayPrefix(newMillis);
    if (oldDay !== newDay) {
      void this._writes.del(lastActiveIndex.entry(this._cvr.id, oldMillis));
      void this._writes.put(lastActiveIndex.entry(this._cvr.id, newMillis), {
        id: this._cvr.id,
      } satisfies CvrID);
    }

    this._cvr.lastActive = {epochMillis: newMillis};
    void this._writes.put(this._paths.lastActive(), this._cvr.lastActive);
  }

  async flush(lc: LogContext, lastActive = new Date()): Promise<CVRSnapshot> {
    const start = Date.now();

    this.#setLastActive(lastActive);
    const numEntries = this._writes.pendingSize();
    await this._writes.flush(); // Calls put() and del() with a final `await`
    await this._directStorage.flush(); // DurableObjectStorage.sync();

    lc.debug?.(`flushed ${numEntries} CVR entries (${Date.now() - start} ms)`);
    return this._cvr;
  }
}

/**
 * A {@link CVRConfigDrivenUpdater} is used for updating a CVR with config-driven
 * changes. Note that this may result in row deletion (e.g. if queries get dropped),
 * but the `stateVersion` of the CVR does not change.
 */
export class CVRConfigDrivenUpdater extends CVRUpdater {
  constructor(storage: DurableStorage, cvr: CVRSnapshot) {
    super(storage, cvr);
  }

  #ensureClient(id: string): ClientRecord {
    let client = this._cvr.clients[id];
    if (client) {
      return client;
    }
    // Add the ClientRecord and PutPatch
    const newVersion = this._ensureNewVersion();
    client = {id, patchVersion: newVersion, desiredQueryIDs: []};
    this._cvr.clients[id] = client;

    void this._writes.put(this._paths.client(client), client);
    void this._writes.put(this._paths.clientPatch(newVersion, client), {
      type: 'client',
      op: 'put',
      id,
    } satisfies ClientPatch);

    const lmidsQuery: InternalQueryRecord = {
      id: CLIENT_LMID_QUERY_ID,
      ast: {
        schema: 'zero',
        table: 'clients',
        select: [
          [['clients', 'clientGroupID'], 'clientGroupID'],
          [['clients', 'clientID'], 'clientID'],
          [['clients', 'lastMutationID'], 'lastMutationID'],
        ],
        where: {
          type: 'conjunction',
          op: 'AND',
          conditions: [
            {
              type: 'simple',
              field: ['clients', 'clientGroupID'],
              op: '=',
              value: {
                type: 'value',
                value: this._cvr.id,
              },
            },
            {
              type: 'conjunction',
              op: 'OR',
              conditions: Object.keys(this._cvr.clients).map(clientID => ({
                type: 'simple',
                field: ['clients', 'clientID'],
                op: '=',
                value: {
                  type: 'value',
                  value: clientID,
                },
              })),
            },
          ],
        },
      },
      internal: true,
    };
    this._cvr.queries[CLIENT_LMID_QUERY_ID] = lmidsQuery;
    void this._writes.put(this._paths.query(lmidsQuery), lmidsQuery);

    return client;
  }

  putDesiredQueries(
    clientID: string,
    queries: {[id: string]: AST},
  ): {id: string; ast: AST}[] {
    const client = this.#ensureClient(clientID);
    const current = new Set(client.desiredQueryIDs);
    const additional = new Set(Object.keys(queries));
    const needed = difference(additional, current);
    if (needed.size === 0) {
      return [];
    }
    const newVersion = this._ensureNewVersion();
    client.desiredQueryIDs = [...union(current, needed)].sort(compareUTF8);
    void this._writes.put(this._paths.client(client), client);

    const added: {id: string; ast: AST}[] = [];
    for (const id of needed) {
      const ast = queries[id];
      const query = this._cvr.queries[id] ?? {id, ast, desiredBy: {}};
      assertNotInternal(query);

      query.desiredBy[clientID] = newVersion;
      this._cvr.queries[id] = query;
      added.push({id, ast});

      void this._writes.put(this._paths.query(query), query);
      void this._writes.put(
        this._paths.desiredQueryPatch(newVersion, query, client),
        {type: 'query', op: 'put', id, clientID} satisfies QueryPatch,
      );
    }
    return added;
  }

  deleteDesiredQueries(clientID: string, queries: string[]) {
    const client = this.#ensureClient(clientID);
    const current = new Set(client.desiredQueryIDs);
    const unwanted = new Set(queries);
    const remove = intersection(unwanted, current);
    if (remove.size === 0) {
      return;
    }
    const newVersion = this._ensureNewVersion();
    client.desiredQueryIDs = [...difference(current, remove)].sort(compareUTF8);
    void this._writes.put(this._paths.client(client), client);

    for (const id of remove) {
      const query = this._cvr.queries[id];
      if (!query) {
        continue; // Query itself has already been removed. Should not happen?
      }
      assertNotInternal(query);

      // Delete the old put-desired-patch
      const oldPutVersion = query.desiredBy[clientID];
      delete query.desiredBy[clientID];
      void this._writes.del(
        this._paths.desiredQueryPatch(oldPutVersion, query, client),
      );

      void this._writes.put(this._paths.query(query), query);
      void this._writes.put(
        this._paths.desiredQueryPatch(newVersion, query, client),
        {type: 'query', op: 'del', id, clientID} satisfies QueryPatch,
      );
    }
  }

  clearDesiredQueries(clientID: string) {
    const client = this.#ensureClient(clientID);
    this.deleteDesiredQueries(clientID, client.desiredQueryIDs);
  }

  flush(lc: LogContext, lastActive = new Date()): Promise<CVRSnapshot> {
    // TODO: Add cleanup of no-longer-desired got queries and constituent rows.
    return super.flush(lc, lastActive);
  }
}

type QueriedColumns = Record<string, string[]>;

/**
 * A {@link CVRQueryDrivenUpdater} is used for updating a CVR after making queries.
 * The caller should invoke:
 *
 * * {@link trackQueries} for queries that are being executed or removed.
 * * {@link received} for all rows received from the executed queries
 * * {@link deleteUnreferencedColumnsAndRows} to remove any columns or
 *                    rows that have fallen out of the query result view.
 * * {@link generateConfigPatches} to send any config changes
 * * {@link flush}
 */
export class CVRQueryDrivenUpdater extends CVRUpdater {
  readonly #removedOrExecutedQueryIDs = new Set<string>();
  readonly #receivedRows = new Map<string, QueriedColumns>();
  readonly #newConfigPatches: MetadataPatch[] = [];
  #existingRows: Promise<Map<string, RowRecord>> | undefined;
  #catchupRowPatches: Promise<Map<string, RowPatch>> | undefined;
  #catchupConfigPatches: Promise<Map<string, MetadataPatch>> | undefined;

  /**
   * @param stateVersion The `stateVersion` at which the queries were executed.
   */
  constructor(
    storage: DurableStorage,
    cvr: CVRSnapshot,
    stateVersion: LexiVersion,
  ) {
    super(storage, cvr);

    assert(stateVersion >= cvr.version.stateVersion);
    if (stateVersion > cvr.version.stateVersion) {
      this._setVersion({stateVersion});
    }
  }

  /**
   * Initiates the tracking of the specified `executed` and `removed` queries.
   * This kicks of a lookup of existing {@link RowRecord}s currently associated with
   * those queries, which will be used to reconcile the columns and rows to keep
   * after all rows have been {@link received()}.
   *
   * @param returns The new CVRVersion to will be used when all changes are committed.
   */
  trackQueries(
    lc: LogContext,
    executed: {id: string; transformationHash: string}[],
    removed: string[],
    catchupFrom: NullableCVRVersion,
  ): CVRVersion {
    assert(this.#existingRows === undefined, `trackQueries already called`);

    executed.forEach(q => this.#trackExecuted(q.id, q.transformationHash));
    removed.forEach(id => this.#trackRemoved(id));

    this.#existingRows = this.#lookupRowsForExecutedAndRemovedQueries(lc);

    if (cmpVersions(catchupFrom, this._orig.version) >= 0) {
      this.#catchupRowPatches = Promise.resolve(new Map());
      this.#catchupConfigPatches = Promise.resolve(new Map());
    } else {
      const startingVersion = oneAfter(catchupFrom);
      this.#catchupRowPatches = this._directStorage.list(
        {
          prefix: this._paths.rowPatchPrefix(),
          start: {key: this._paths.rowPatchVersionPrefix(startingVersion)},
        },
        rowPatchSchema,
      );
      this.#catchupConfigPatches = this._directStorage.list(
        {
          prefix: this._paths.metadataPatchPrefix(),
          start: {key: this._paths.metadataPatchVersionPrefix(startingVersion)},
        },
        metadataPatchSchema,
      );
    }
    return this._cvr.version;
  }

  async #lookupRowsForExecutedAndRemovedQueries(
    lc: LogContext,
  ): Promise<Map<string, RowRecord>> {
    const results = new Map<string, RowRecord>();

    if (this.#removedOrExecutedQueryIDs.size === 0) {
      // Query-less update. This can happen for config only changes.
      return results;
    }

    // Currently this performs a full scan of the CVR row records. In the future this
    // can be optimized by tracking an index from query to row, either manually or
    // when DO storage is backed by SQLite.
    //
    // Note that it is important here to use _directStorage rather than _writes because
    // (1) We are interested in the existing storage state, and not pending mutations
    // (2) WriteCache.batchScan() (i.e. list()) will perform computationally expensive
    //     (and unnecessary) sorting over the entry Map to adhere to the DO contract.
    const allRowRecords = this._directStorage.batchScan(
      {prefix: this._paths.rowPrefix()},
      rowRecordSchema,
      2000, // Arbitrary batch size. Limits how many row records are in memory at a time.
    );
    let total = 0;
    for await (const existingRows of allRowRecords) {
      total += existingRows.size;
      for (const [path, existing] of existingRows) {
        if (existing.queriedColumns === null) {
          continue; // Tombstone
        }
        for (const queries of Object.values(existing.queriedColumns)) {
          if (queries.some(id => this.#removedOrExecutedQueryIDs.has(id))) {
            results.set(path, existing);
            break;
          }
        }
      }
    }

    lc.debug?.(
      `found ${
        results.size
      } (of ${total}) rows for executed / removed queries ${[
        ...this.#removedOrExecutedQueryIDs,
      ]}`,
    );
    return results;
  }

  /**
   * Tracks an executed query, ensures that it is marked as "gotten",
   * updating the CVR and creating put patches if necessary.
   *
   * This must be called for all executed queries.
   */
  #trackExecuted(queryID: string, transformationHash: string) {
    assert(!this.#removedOrExecutedQueryIDs.has(queryID));
    this.#removedOrExecutedQueryIDs.add(queryID);

    const query = this._cvr.queries[queryID];
    if (query.transformationHash !== transformationHash) {
      const transformationVersion = this._ensureNewVersion();

      if (!query.internal && query.patchVersion === undefined) {
        // client query: desired -> gotten
        query.patchVersion = transformationVersion;
        const queryPatch: QueryPatch = {type: 'query', op: 'put', id: query.id};
        void this._writes.put(
          this._paths.queryPatch(transformationVersion, {id: query.id}),
          queryPatch,
        );
        this.#newConfigPatches.push(queryPatch);
      }

      query.transformationHash = transformationHash;
      query.transformationVersion = transformationVersion;
      void this._writes.put(this._paths.query(query), query);
    }
  }

  /**
   * Tracks a query removed from the "gotten" set. In addition to producing the
   * appropriate patches for deleting the query, the removed query is taken into
   * account when computing the final row records in
   * {@link deleteUnreferencedColumnsAndRows}.
   * Namely, any rows with columns that are no longer referenced by a query are
   * patched, or deleted if no columns are referenced.
   *
   * This must only be called on queries that are not "desired" by any client.
   */
  #trackRemoved(queryID: string) {
    const query = this._cvr.queries[queryID];
    assertNotInternal(query);

    assert(!this.#removedOrExecutedQueryIDs.has(queryID));
    this.#removedOrExecutedQueryIDs.add(queryID);
    delete this._cvr.queries[queryID];

    const newVersion = this._ensureNewVersion();
    void this._writes.del(this._paths.query({id: queryID}));
    const {patchVersion} = query;
    if (patchVersion) {
      void this._writes.del(
        this._paths.queryPatch(patchVersion, {id: queryID}),
      );
    }
    const queryPatch: QueryPatch = {type: 'query', op: 'del', id: queryID};
    void this._writes.put(
      this._paths.queryPatch(newVersion, {id: queryID}),
      queryPatch,
    );
    this.#newConfigPatches.push(queryPatch);
  }

  /**
   * Asserts that a new version has already been set.
   *
   * After {@link #executed} and {@link #removed} are called, we must have properly
   * decided on the final CVR version because the poke-start message declares the
   * final cookie (i.e. version), and that must be sent before any poke parts
   * generated from {@link received} are sent.
   */
  #assertNewVersion(): CVRVersion {
    assert(cmpVersions(this._orig.version, this._cvr.version) < 0);
    return this._cvr.version;
  }

  updatedVersion(): CVRVersion {
    return this._cvr.version;
  }

  /**
   * Tracks rows received from executing queries. This will update row records and
   * row patches if the received rows have a new version or contain columns that
   * are not currently in the view. The method also returns (merge) patches to be
   * returned to update their state, versioned by patchVersion so that only the
   * patches new to the clients are sent.
   */
  async received(
    _: LogContext,
    rows: Map<string, ParsedRow>,
  ): Promise<PatchToVersion[]> {
    const merges: PatchToVersion[] = [];

    const existingRows = await this._writes.getEntries(
      [...rows.keys()],
      rowRecordSchema,
    );

    for (const [path, row] of rows) {
      const {
        contents,
        record: {id, rowVersion, queriedColumns},
      } = row;

      assert(queriedColumns !== null); // We never "receive" tombstones.

      const existing = existingRows.get(path);

      // Accumulate all received columns to determine which columns to prune at the end.
      const previouslyReceived = this.#receivedRows.get(path);
      const merged = previouslyReceived
        ? mergeQueriedColumns(previouslyReceived, queriedColumns)
        : mergeQueriedColumns(
            existing?.queriedColumns,
            queriedColumns,
            this.#removedOrExecutedQueryIDs,
          );

      this.#receivedRows.set(path, merged);

      const patchVersion =
        existing?.rowVersion === rowVersion &&
        Object.keys(merged).every(col => existing.queriedColumns?.[col])
          ? existing.patchVersion
          : this.#assertNewVersion();

      if (existing) {
        void this._writes.del(this._paths.rowPatch(existing.patchVersion, id));
      }
      const updated = {id, rowVersion, patchVersion, queriedColumns: merged};
      void this._writes.put(path, updated satisfies RowRecord);
      void this._writes.put(this._paths.rowPatch(patchVersion, id), {
        type: 'row',
        op: 'put',
        id,
        rowVersion,
        columns: Object.keys(merged),
      } satisfies RowPatch);

      merges.push({
        patch: {
          type: 'row',
          op: existing?.queriedColumns ? 'merge' : 'put',
          id,
          contents,
        },
        toVersion: patchVersion,
      });
    }
    return merges;
  }

  /**
   * Computes and updates the row records based on:
   * * The {@link #executed} queries
   * * The {@link #removed} queries
   * * The {@link received} rows
   *
   * Returns the final delete and patch ops that must be sent to the client
   * to delete rows or columns that are no longer referenced by any query.
   *
   * This is Step [5] of the
   * [CVR Sync Algorithm](https://www.notion.so/replicache/Sync-and-Client-View-Records-CVR-a18e02ec3ec543449ea22070855ff33d?pvs=4#7874f9b80a514be2b8cd5cf538b88d37).
   *
   * @param generatePatchesAfter Generates delete and constrain patches from the
   *        version after `generatePatchesAfter`.
   */
  async deleteUnreferencedColumnsAndRows(
    lc: LogContext,
  ): Promise<PatchToVersion[]> {
    if (this.#removedOrExecutedQueryIDs.size === 0) {
      // Query-less update. This can happen for config-only changes.
      assert(this.#receivedRows.size === 0);
      return [];
    }

    // patches to send to the client.
    const patches: PatchToVersion[] = [];

    assert(this.#existingRows, `trackQueries() was not called`);
    for (const [path, existing] of await this.#existingRows) {
      const update = this.#deleteUnreferencedColumnsOrRow(path, existing);
      if (update === null) {
        continue;
      }

      const {id, columns} = update;
      if (columns) {
        patches.push({
          toVersion: this._cvr.version,
          patch: {type: 'row', op: 'constrain', id, columns},
        });
      } else {
        patches.push({
          toVersion: this._cvr.version,
          patch: {type: 'row', op: 'del', id},
        });
      }
    }

    // Now catch up clients with row patches that haven't been deleted.
    assert(this.#catchupRowPatches, `trackQueries must first be called`);
    const catchupRowPatches = await this.#catchupRowPatches;
    lc.debug?.(`processing ${catchupRowPatches.size} row patches`);
    for (const [path, rowPatch] of catchupRowPatches) {
      if (this._writes.isPendingDelete(path)) {
        continue; // row patch has been replaced.
      }
      const toVersion = this._paths.versionFromPatchPath(path);
      const {id} = rowPatch;
      if (rowPatch.op === 'put') {
        const {columns} = rowPatch;
        patches.push({
          patch: {type: 'row', op: 'constrain', id, columns},
          toVersion,
        });
      } else {
        patches.push({patch: {type: 'row', op: 'del', id}, toVersion});
      }
    }

    return patches;
  }

  async generateConfigPatches(lc: LogContext) {
    const patches: PatchToVersion[] = [];

    assert(this.#catchupConfigPatches, `trackQueries must first be called`);
    const catchupConfigPatches = await this.#catchupConfigPatches;
    lc.debug?.(`processing ${catchupConfigPatches.size} config patches`);

    const convert = (patchRecord: MetadataPatch): Patch => {
      switch (patchRecord.type) {
        case 'client':
          return patchRecord;
        case 'query': {
          const {id, op} = patchRecord;
          if (op === 'put') {
            return {...patchRecord, op, ast: this._cvr.queries[id].ast};
          }
          return {...patchRecord, op};
        }
        default:
          unreachable();
      }
    };

    for (const [path, patchRecord] of catchupConfigPatches) {
      if (this._writes.isPendingDelete(path)) {
        continue; // config patch has been replaced.
      }
      const toVersion = this._paths.versionFromPatchPath(path);
      patches.push({patch: convert(patchRecord), toVersion});
    }
    for (const patchRecord of this.#newConfigPatches) {
      patches.push({patch: convert(patchRecord), toVersion: this._cvr.version});
    }
    return patches;
  }

  #deleteUnreferencedColumnsOrRow(
    rowRecordPath: string,
    existing: RowRecord,
  ): {id: RowID; columns?: string[]} | null {
    const received = this.#receivedRows.get(rowRecordPath);

    const newQueriedColumns =
      received ?? // optimization: already merged in received()
      mergeQueriedColumns(
        existing.queriedColumns,
        undefined,
        this.#removedOrExecutedQueryIDs,
      );
    if (
      existing.queriedColumns &&
      Object.keys(existing.queriedColumns).every(col => newQueriedColumns[col])
    ) {
      return null; // No columns deleted.
    }
    const patchVersion = this.#assertNewVersion();
    const {id, rowVersion} = existing;
    const columns = Object.keys(newQueriedColumns);
    const op = columns.length ? 'put' : 'del';

    const rowRecord: RowRecord = {
      ...existing,
      patchVersion,
      queriedColumns: op === 'put' ? newQueriedColumns : null,
    };
    void this._writes.put(rowRecordPath, rowRecord);
    void this._writes.del(this._paths.rowPatch(existing.patchVersion, id));

    if (op === 'del') {
      void this._writes.put(this._paths.rowPatch(patchVersion, id), {
        type: 'row',
        op: 'del',
        id,
      } satisfies RowPatch);
      return {id}; // DeleteOp
    }

    void this._writes.put(this._paths.rowPatch(patchVersion, id), {
      type: 'row',
      op: 'put',
      id,
      rowVersion,
      columns,
    } satisfies RowPatch);

    return {id, columns}; // UpdateOp
  }
}

function mergeQueriedColumns(
  existing: QueriedColumns | null | undefined,
  received: QueriedColumns | null | undefined,
  removeIDs?: Set<string>,
): QueriedColumns {
  if (!existing) {
    return received ?? {};
  }
  const merged: QueriedColumns = {};

  [existing, received].forEach((row, i) => {
    if (!row) {
      return;
    }
    for (const [col, queries] of Object.entries(row)) {
      for (const id of queries) {
        if (i === 0 /* existing */ && removeIDs?.has(id)) {
          continue; // removeIDs from existing row.
        }
        if (!merged[col]?.includes(id)) {
          (merged[col] ??= []).push(id);
        }
      }
    }
  });

  return merged;
}
