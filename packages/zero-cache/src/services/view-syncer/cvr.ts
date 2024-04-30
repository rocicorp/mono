import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import {compareUTF8} from 'compare-utf8';
import {assert} from 'shared/src/asserts.js';
import type {Immutable} from 'shared/src/immutable.js';
import {difference, equals, intersection, union} from 'shared/src/set-utils.js';
import type {DurableStorage} from '../../storage/durable-storage.js';
import type {Storage} from '../../storage/storage.js';
import {WriteCache} from '../../storage/write-cache.js';
import {LexiVersion, versionToLexi} from '../../types/lexi-version.js';
import {union as arrayUnion, type ParsedRow} from './queries.js';
import {CVRPaths, lastActiveIndex} from './schema/paths.js';
import {
  ClientPatch,
  CvrID,
  QueryPatch,
  RowID,
  RowPatch,
  RowRecord,
  cmpVersions,
  metaRecordSchema,
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
export type CVRSnapshot = Immutable<CVR>;

/** Loads the CVR metadata from storage. */
export async function loadCVR(
  storage: Storage,
  id: string,
): Promise<CVRSnapshot> {
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
  return cvr;
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
  readonly #storage: DurableStorage;
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
    this.#storage = storage;
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
      const {stateVersion, minorVersion = 0} = this._cvr.version;
      this._setVersion({stateVersion, minorVersion: minorVersion + 1});
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

  async flush(lastActive = new Date()): Promise<CVRSnapshot> {
    this.#setLastActive(lastActive);
    await this._writes.flush(); // Calls put() and del() with a final `await`
    await this.#storage.flush(); // DurableObjectStorage.sync();
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
    client = {id, putPatch: newVersion, desiredQueryIDs: []};
    this._cvr.clients[id] = client;

    void this._writes.put(this._paths.client(client), client);
    void this._writes.put(this._paths.clientPatch(newVersion, client), {
      type: 'client',
      op: 'put',
      id,
    } satisfies ClientPatch);

    return client;
  }

  putDesiredQueries(clientID: string, queries: {[id: string]: AST}): AST[] {
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

    const added: AST[] = [];
    for (const id of needed) {
      const ast = queries[id];
      const query = this._cvr.queries[id] ?? {id, ast, desiredBy: {}};
      query.desiredBy[clientID] = newVersion;
      this._cvr.queries[id] = query;
      added.push(ast);

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

  flush(lastActive = new Date()): Promise<CVRSnapshot> {
    // TODO: Add cleanup of no-longer-desired got queries and constituent rows.
    return super.flush(lastActive);
  }
}

/**
 * A {@link CVRQueryDrivenUpdater} is used for updating a CVR after making
 * queries.
 */
export class CVRQueryDrivenUpdater extends CVRUpdater {
  readonly #executedQueryIDs = new Set<string>();
  readonly #removedQueryIDs = new Set<string>();
  readonly #receivedRows = new Map<string, Omit<RowRecord, 'putPatch'>>();

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
   * Tracks an executed query, ensures that it is marked as "gotten",
   * updating the CVR and creating put patches if necessary.
   *
   * This must be called for all executed queries.
   */
  executed(queryID: string, transformationHash: string) {
    assert(!this.#removedQueryIDs.has(queryID));
    this.#executedQueryIDs.add(queryID);

    const query = this._cvr.queries[queryID];
    if (query.transformationHash !== transformationHash) {
      const transformationVersion = this._ensureNewVersion();

      if (query.putPatch === undefined) {
        // desired -> gotten
        query.putPatch = transformationVersion;
        void this._writes.put(
          this._paths.queryPatch(transformationVersion, {id: query.id}),
          {type: 'query', op: 'put', id: query.id} satisfies QueryPatch,
        );
      }

      query.transformationHash = transformationHash;
      query.transformationVersion = transformationVersion;
      void this._writes.put(this._paths.query(query), query);
    }
  }

  /**
   * Tracks a query removed from the "gotten" set. In addition to producing the
   * appropriate patches for deleting the query, the removed query is taken into
   * account when computing the final row records in {@link updateRowRecords}.
   * Namely, any rows with columns that are no longer referenced by a query are
   * patched, or deleted if no columns are referenced.
   *
   * This must only be called on queries that are not "desired" by any client.
   */
  removed(queryID: string) {
    const {desiredBy, putPatch} = this._cvr.queries[queryID];
    assert(putPatch);
    assert(
      Object.keys(desiredBy).length === 0,
      `Cannot remove query ${queryID}`,
    );

    assert(!this.#executedQueryIDs.has(queryID));
    this.#removedQueryIDs.add(queryID);
    delete this._cvr.queries[queryID];

    const newVersion = this._ensureNewVersion();
    void this._writes.del(this._paths.query({id: queryID}));
    void this._writes.del(this._paths.queryPatch(putPatch, {id: queryID}));
    void this._writes.put(this._paths.queryPatch(newVersion, {id: queryID}), {
      type: 'query',
      op: 'del',
      id: queryID,
    } satisfies QueryPatch);
  }

  /** Tracks rows received from executing queries. */
  received(rows: Map<string, ParsedRow>) {
    for (const [path, row] of rows) {
      const received = this.#receivedRows.get(path);
      if (!received) {
        this.#receivedRows.set(path, row.record);
      } else {
        // Merge the previously received row with the newly received row.
        assert(received.rowVersion === row.record.rowVersion);
        for (const [col, queryIDs] of Object.entries(
          row.record.queriedColumns,
        )) {
          received.queriedColumns[col] = arrayUnion(
            received.queriedColumns[col],
            queryIDs,
          );
        }
      }
    }
  }

  /**
   * Computes and updates the row records based on:
   * * The {@link executed} queries
   * * The {@link removed} queries
   * * The {@link received} rows
   *
   * Returns the final delete and patch ops that must be sent to the client
   * to delete rows or columns that are no longer referenced by any query.
   *
   * This is Step [5] of the
   * [CVR Sync Algorithm](https://www.notion.so/replicache/Sync-and-Client-View-Records-CVR-a18e02ec3ec543449ea22070855ff33d?pvs=4#7874f9b80a514be2b8cd5cf538b88d37).
   */
  async updateRowRecords() {
    // patchOps to send to the client.
    const patchRows: [row: RowID, deleted: string[]][] = [];
    const deleteRows: RowID[] = [];

    const removedOrExecutedQueryIDs = union(
      this.#removedQueryIDs,
      this.#executedQueryIDs,
    );
    if (removedOrExecutedQueryIDs.size === 0) {
      // Query-less update. This can happen for config-only changes.
      assert(this.#receivedRows.size === 0);
      return {patchRows, deleteRows};
    }

    // If queries were removed or executed, all row records must be examined to update
    // which queryIDs reference their columns, if any. This is where PatchOps to remove
    // columns, and DeleteOps to remove rows, are computed.
    //
    // (Note that the only way to avoid an full CVR scan is to keep a separate index of
    //  queryIDs -> rows. Consider this if there is evidence that the row scan is worth avoiding.)
    const allRowRecords = this._writes.batchScan(
      {prefix: this._paths.rowPrefix()},
      rowRecordSchema,
      2000, // Arbitrary batch size. Determines how many row records are in memory at a time.
    );
    for await (const existingRows of allRowRecords) {
      for (const [path, existing] of existingRows) {
        const clientOp = this.#updateExistingRowRecord(
          path,
          existing,
          removedOrExecutedQueryIDs,
        );
        if (Array.isArray(clientOp)) {
          patchRows.push(clientOp);
        } else if (clientOp) {
          deleteRows.push(clientOp);
        } else {
          assert(clientOp === null); // Mostly code documentation here.
        }
      }
    }

    // Remaining #receivedRows are those not encountered in existing row scan.
    for (const [rowRecordPath, received] of this.#receivedRows) {
      const {id, rowVersion, queriedColumns} = received;
      const putPatch = this._ensureNewVersion();
      const rowRecord: RowRecord = {...received, putPatch};

      void this._writes.put(rowRecordPath, rowRecord);
      void this._writes.put(this._paths.rowPatch(putPatch, id), {
        type: 'row',
        op: 'put',
        id,
        rowVersion,
        columns: Object.keys(queriedColumns),
      } satisfies RowPatch);
    }

    return {patchRows, deleteRows};
  }

  #updateExistingRowRecord(
    rowRecordPath: string,
    existing: RowRecord,
    removedOrExecutedQueryIDs: Set<string>,
  ): null | RowID | [row: RowID, deleteColumns: string[]] {
    const received = this.#receivedRows.get(rowRecordPath);
    if (received) {
      this.#receivedRows.delete(rowRecordPath); // Considers the row "processed".
    }
    let updated = received && received.rowVersion !== existing.rowVersion;

    // Compute the final queriedColumns based on existing, received, and removed.
    const deleteColumns: string[] = [];
    const queriedColumns: Record<string, string[]> = {};

    for (const col of new Set([
      ...Object.keys(existing.queriedColumns),
      ...(received ? Object.keys(received.queriedColumns) : []),
    ])) {
      const existingQueryIDs = new Set(existing.queriedColumns[col]);
      const receivedQueryIDs = new Set(received?.queriedColumns[col]);
      const finalQueryIDs = union(
        difference(existingQueryIDs, removedOrExecutedQueryIDs),
        receivedQueryIDs,
      );
      if (equals(existingQueryIDs, finalQueryIDs)) {
        queriedColumns[col] = existing.queriedColumns[col];
      } else {
        updated = true; // RowRecord has changed.
        if (finalQueryIDs.size > 0) {
          queriedColumns[col] = [...finalQueryIDs];
        } else {
          deleteColumns.push(col);
        }
      }
    }

    if (!updated) {
      return null; // rowVersion and all column references remain the same.
    }
    const newVersion = this._ensureNewVersion();

    if (Object.keys(queriedColumns).length === 0) {
      // No columns are referenced by any query. Delete the row.
      void this._writes.del(rowRecordPath);
      void this._writes.del(
        this._paths.rowPatch(existing.putPatch, existing.id),
      );
      void this._writes.put(this._paths.rowPatch(newVersion, existing.id), {
        type: 'row',
        op: 'del',
        id: existing.id,
      } satisfies RowPatch);
      return existing.id; // DeleteOp
    }

    // rowVersion or column references have changed.
    const putPatch = newVersion;
    const {id, rowVersion} = received ?? existing;
    const rowRecord: RowRecord = {id, rowVersion, putPatch, queriedColumns};
    void this._writes.put(rowRecordPath, rowRecord);
    void this._writes.del(this._paths.rowPatch(existing.putPatch, id));
    void this._writes.put(this._paths.rowPatch(putPatch, id), {
      type: 'row',
      op: 'put',
      id,
      rowVersion,
      columns: Object.keys(queriedColumns),
    } satisfies RowPatch);

    return deleteColumns.length > 0 ? [rowRecord.id, deleteColumns] : null;
  }
}
