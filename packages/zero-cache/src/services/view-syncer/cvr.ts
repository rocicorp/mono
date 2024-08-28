import type {LogContext} from '@rocicorp/logger';
import {compareUTF8} from 'compare-utf8';
import {assert, unreachable} from 'shared/src/asserts.js';
import {CustomKeyMap} from 'shared/src/custom-key-map.js';
import {deepEqual, type ReadonlyJSONValue} from 'shared/src/json.js';
import {difference, intersection, union} from 'shared/src/set-utils.js';
import {JSONObject} from 'zero-cache/src/types/bigint-json.js';
import type {AST} from 'zql/src/zql/ast2/ast.js';
import type {LexiVersion} from '../../types/lexi-version.js';
import {rowIDHash} from '../../types/row-key.js';
import type {Patch, PatchToVersion} from './client-handler.js';
import type {CVRStore} from './cvr-store.js';
import {
  ClientQueryRecord,
  InternalQueryRecord,
  MetadataPatch,
  NullableCVRVersion,
  QueryPatch,
  RowID,
  RowPatch,
  RowRecord,
  cmpVersions,
  getAllColumns,
  getAllColumnsSorted,
  oneAfter,
  type CVRVersion,
  type ClientRecord,
  type LastActive,
  type QueryRecord,
} from './schema/types.js';

// From obsolete queries.ts. Will need to be reworked for IVM.
export type ParsedRow = {
  record: Omit<RowRecord, 'patchVersion'>;
  contents: JSONObject;
};

/** Internally used mutable CVR type. */
export type CVR = {
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

const CLIENT_LMID_QUERY_ID = 'lmids';

function assertNotInternal(
  query: QueryRecord,
): asserts query is ClientQueryRecord {
  if (query.internal) {
    // This should never happen for behaving clients, as query ids should be hashes.
    throw new Error(`Query ID ${query.id} is reserved for internal use`);
  }
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
  protected readonly _orig: CVRSnapshot;
  protected readonly _cvr: CVR;

  protected readonly _cvrStore: CVRStore;

  /**
   * @param cvrStore The CVRStore to use for storage
   * @param cvr The current CVR
   */
  constructor(cvrStore: CVRStore, cvr: CVRSnapshot) {
    this._cvrStore = cvrStore;
    this._orig = cvr;
    this._cvr = structuredClone(cvr) as CVR; // mutable deep copy
  }

  protected _setVersion(version: CVRVersion) {
    assert(cmpVersions(this._cvr.version, version) < 0);
    this._cvr.version = version;
    this._cvrStore.putInstance(this._cvr.version, this._cvr.lastActive);
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
    const newMillis = now.getTime();
    this._cvr.lastActive = {epochMillis: newMillis};
    this._cvrStore.putInstance(this._cvr.version, this._cvr.lastActive);
  }

  // Exposed for testing.
  numPendingWrites() {
    return this._cvrStore.numPendingWrites();
  }

  async flush(lc: LogContext, lastActive = new Date()): Promise<CVRSnapshot> {
    const start = Date.now();

    this.#setLastActive(lastActive);
    const numEntries = this._cvrStore.numPendingWrites();
    await this._cvrStore.flush();

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
  #ensureClient(id: string): ClientRecord {
    let client = this._cvr.clients[id];
    if (client) {
      return client;
    }
    // Add the ClientRecord and PutPatch
    const newVersion = this._ensureNewVersion();
    client = {id, patchVersion: newVersion, desiredQueryIDs: []};
    this._cvr.clients[id] = client;

    this._cvrStore.insertClient(client);

    const lmidsQuery: InternalQueryRecord = {
      id: CLIENT_LMID_QUERY_ID,
      ast: {
        schema: '',
        table: 'zero.clients',
        where: [
          {
            type: 'simple',
            field: 'clientGroupID',
            op: '=',
            value: this._cvr.id,
          },
        ],
        orderBy: [['clientID', 'asc']],
      },
      internal: true,
    };
    this._cvr.queries[CLIENT_LMID_QUERY_ID] = lmidsQuery;
    this._cvrStore.putQuery(lmidsQuery);

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

    const added: {id: string; ast: AST}[] = [];
    for (const id of needed) {
      const ast = queries[id];
      const query = this._cvr.queries[id] ?? {id, ast, desiredBy: {}};
      assertNotInternal(query);

      query.desiredBy[clientID] = newVersion;
      this._cvr.queries[id] = query;
      added.push({id, ast});

      this._cvrStore.putQuery(query);
      this._cvrStore.insertDesiredQuery(newVersion, query, client, false);
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
    this._cvrStore.updateClientPatchVersion(client.id, client.patchVersion);

    for (const id of remove) {
      const query = this._cvr.queries[id];
      if (!query) {
        continue; // Query itself has already been removed. Should not happen?
      }
      assertNotInternal(query);

      // Delete the old put-desired-patch
      const oldPutVersion = query.desiredBy[clientID];
      delete query.desiredBy[clientID];
      this._cvrStore.delDesiredQuery(oldPutVersion, query, client);

      this._cvrStore.putQuery(query);
      this._cvrStore.insertDesiredQuery(newVersion, query, client, true);
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

type Hash = string;
export type Column = string;
export type QueriedColumns = Record<Hash, Column[]>;

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
  readonly #receivedRows = new CustomKeyMap<RowID, QueriedColumns>(rowIDHash);
  readonly #newConfigPatches: MetadataPatch[] = [];
  #existingRows: Promise<RowRecord[]> | undefined = undefined;
  #catchupRowPatches: Promise<[RowPatch, CVRVersion][]> | undefined = undefined;
  #catchupConfigPatches: Promise<[MetadataPatch, CVRVersion][]> | undefined =
    undefined;

  /**
   * @param stateVersion The `stateVersion` at which the queries were executed.
   */
  constructor(cvrStore: CVRStore, cvr: CVRSnapshot, stateVersion: LexiVersion) {
    super(cvrStore, cvr);

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
   * @returns The new CVRVersion to be used when all changes are committed.
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
      this.#catchupRowPatches = Promise.resolve([]);
      this.#catchupConfigPatches = Promise.resolve([]);
    } else {
      const startingVersion = oneAfter(catchupFrom);
      this.#catchupRowPatches =
        this._cvrStore.catchupRowPatches(startingVersion);
      this.#catchupConfigPatches =
        this._cvrStore.catchupConfigPatches(startingVersion);
    }
    return this._cvr.version;
  }

  async #lookupRowsForExecutedAndRemovedQueries(
    lc: LogContext,
  ): Promise<RowRecord[]> {
    const results = new CustomKeyMap<RowID, RowRecord>(rowIDHash);

    if (this.#removedOrExecutedQueryIDs.size === 0) {
      // Query-less update. This can happen for config only changes.
      return [];
    }

    // Currently this performs a full scan of the CVR row records. In the future this
    // can be optimized by tracking an index from query to row.
    //
    // We can use something like:
    //   SELECT * FROM cvr.rows WHERE "queriedColumns" ?| array[...queryHashes...];

    const allRowRecords = this._cvrStore.allRowRecords();

    let total = 0;
    for await (const existing of allRowRecords) {
      total++;
      assert(existing.queriedColumns !== null); // allRowRecords does not include null.
      for (const id of Object.keys(existing.queriedColumns)) {
        if (this.#removedOrExecutedQueryIDs.has(id)) {
          results.set(existing.id, existing);
          break;
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
    return [...results.values()];
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
        this.#newConfigPatches.push(queryPatch);
      }

      query.transformationHash = transformationHash;
      query.transformationVersion = transformationVersion;
      this._cvrStore.updateQuery(query);
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
    this._cvrStore.delQuery(queryID);
    const oldQueryPatchVersion = query.patchVersion;
    const queryPatch: QueryPatch = {type: 'query', op: 'del', id: queryID};
    this._cvrStore.markQueryAsDeleted(
      newVersion,
      queryPatch,
      oldQueryPatchVersion,
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
    rows: Map<RowID, ParsedRow>,
  ): Promise<PatchToVersion[]> {
    const merges: PatchToVersion[] = [];

    const existingRows = await this._cvrStore.getMultipleRowEntries(
      rows.keys(),
    );

    for (const parsedRow of rows.values()) {
      const {
        contents,
        record: {id, rowVersion, queriedColumns},
      } = parsedRow;

      assert(queriedColumns !== null); // We never "receive" tombstones.

      const existing = existingRows.get(id);

      // Accumulate all received columns to determine which columns to prune at the end.
      const previouslyReceived = this.#receivedRows.get(id);
      const merged = previouslyReceived
        ? mergeQueriedColumns(previouslyReceived, queriedColumns)
        : mergeQueriedColumns(
            existing?.queriedColumns,
            queriedColumns,
            this.#removedOrExecutedQueryIDs,
          );

      this.#receivedRows.set(id, merged);

      const patchVersion =
        // not the same
        existing?.rowVersion === rowVersion &&
        columnsAreASubset(merged, existing?.queriedColumns)
          ? existing.patchVersion
          : this.#assertNewVersion();

      const updated = {
        id,
        rowVersion,
        patchVersion,
        queriedColumns: merged,
      };
      this._cvrStore.putRowRecord(updated, existing?.patchVersion);

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
    for (const existing of await this.#existingRows) {
      const update = this.#deleteUnreferencedColumnsOrRow(existing);
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
    lc.debug?.(`processing ${catchupRowPatches.length} row patches`);
    for (const [rowPatch, toVersion] of catchupRowPatches) {
      if (this._cvrStore.isRowVersionPendingDelete(rowPatch.id, toVersion)) {
        continue;
      }

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
    lc.debug?.(`processing ${catchupConfigPatches.length} config patches`);

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

    for (const [patchRecord, toVersion] of catchupConfigPatches) {
      if (this._cvrStore.isQueryVersionPendingDelete(patchRecord, toVersion)) {
        continue; // config patch has been replaced.
      }

      patches.push({patch: convert(patchRecord), toVersion});
    }
    for (const patchRecord of this.#newConfigPatches) {
      patches.push({patch: convert(patchRecord), toVersion: this._cvr.version});
    }
    return patches;
  }

  #deleteUnreferencedColumnsOrRow(
    existing: RowRecord,
  ): {id: RowID; columns?: string[]} | null {
    const received = this.#receivedRows.get(existing.id);

    const newQueriedColumns =
      received ?? // optimization: already merged in received()
      mergeQueriedColumns(
        existing.queriedColumns,
        undefined,
        this.#removedOrExecutedQueryIDs,
      );
    if (
      existing.queriedColumns &&
      sameColumns(existing.queriedColumns, newQueriedColumns)
    ) {
      if (received) {
        const pending = this._cvrStore.getPendingRowRecord(existing.id);
        if (
          pending &&
          deepEqual(pending as ReadonlyJSONValue, existing as ReadonlyJSONValue)
        ) {
          // Remove no-op writes from the WriteCache.
          this._cvrStore.cancelPendingRowRecordWrite(existing.id);
        }
      }

      // No columns deleted.
      return null;
    }
    const newPatchVersion = this.#assertNewVersion();
    const {id} = existing;
    const columns = getAllColumnsSorted(newQueriedColumns);
    const isPut = columns.length > 0;

    const rowRecord: RowRecord = {
      ...existing,
      patchVersion: newPatchVersion,
      queriedColumns: isPut ? newQueriedColumns : null,
    };

    this._cvrStore.putRowRecord(rowRecord, existing.patchVersion);

    return isPut
      ? // TODO(arv): Should this be updated?
        {id, columns} // UpdateOp
      : {id}; // DeleteOp
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
    for (const [id, columns] of Object.entries(row)) {
      if (i === 0 /* existing */ && removeIDs?.has(id)) {
        continue; // removeIDs from existing row.
      }
      let col = merged[id];
      if (!col) {
        col = merged[id] = [];
      }
      const set = new Set([...col, ...columns]);
      merged[id] = [...set];
      if (set.size > 1) {
        merged[id].sort(); // Determinism is needed for diffing.
      }
    }

    return merged;
  });

  return merged;
}

function sameColumns(a: QueriedColumns, b: QueriedColumns | null): boolean {
  const aColumns = getAllColumns(a);
  if (b === null) {
    return aColumns.size === 0;
  }
  const bColumns = getAllColumns(b);
  if (aColumns.size !== bColumns.size) {
    return false;
  }
  for (const col of aColumns) {
    if (!bColumns.has(col)) {
      return false;
    }
  }

  return true;
}

/**
 * Determines if b has all the columns a has. b can have more columns than a.
 */
function columnsAreASubset(
  a: QueriedColumns,
  b: QueriedColumns | null,
): boolean {
  const aColumns = getAllColumns(a);
  if (b === null) {
    return aColumns.size === 0;
  }
  const bColumns = new Set(getAllColumns(b));
  if (aColumns.size > bColumns.size) {
    return false;
  }
  for (const col of aColumns) {
    if (!bColumns.has(col)) {
      return false;
    }
  }
  return true;
}
