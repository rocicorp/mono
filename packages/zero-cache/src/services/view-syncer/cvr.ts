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
  CvrID,
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

  async generateConfigPatches(after: NullableCVRVersion) {
    const patches: PatchToVersion[] = [];
    const configPatches = this._writes.batchScan(
      {
        prefix: this._paths.metadataPatchPrefix(),
        start: {key: this._paths.metadataPatchVersionPrefix(oneAfter(after))},
      },
      metadataPatchSchema,
      2000, // Arbitrary batch size. Determines how many row records are in memory at a time.
    );
    for await (const batch of configPatches) {
      for (const [path, patchRecord] of batch) {
        const toVersion = this._paths.versionFromPatchPath(path);
        let patch: Patch;
        switch (patchRecord.type) {
          case 'client':
            patch = patchRecord;
            break;
          case 'query': {
            const {id, op} = patchRecord;
            if (op === 'put') {
              patch = {...patchRecord, op, ast: this._cvr.queries[id].ast};
            } else {
              patch = {...patchRecord, op};
            }
            break;
          }
          default:
            unreachable();
        }
        patches.push({patch, toVersion});
      }
    }
    return patches;
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
    client = {id, patchVersion: newVersion, desiredQueryIDs: []};
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

type QueriedColumns = Record<string, string[]>;

/**
 * A {@link CVRQueryDrivenUpdater} is used for updating a CVR after making queries.
 * The caller should invoke:
 *
 * * {@link removed} for any queries that are no longer being synced
 * * {@link executed} for all queries that were executed (i.e. because of invalidation,
 *                    or because they are new)
 * * {@link received} for all rows received from the executed queries
 * * {@link deleteUnreferencedColumnsAndRows} to remove any columns or
 *                    rows that have fallen out of the query result view.
 * * {@link generateConfigPatches} to send any config changes
 * * {@link flush}
 */
export class CVRQueryDrivenUpdater extends CVRUpdater {
  readonly #executedQueryIDs = new Set<string>();
  readonly #removedQueryIDs = new Set<string>();
  readonly #receivedRows = new Map<string, QueriedColumns>();

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

      if (query.patchVersion === undefined) {
        // desired -> gotten
        query.patchVersion = transformationVersion;
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
   * account when computing the final row records in
   * {@link deleteUnreferencedColumnsAndRows}.
   * Namely, any rows with columns that are no longer referenced by a query are
   * patched, or deleted if no columns are referenced.
   *
   * This must only be called on queries that are not "desired" by any client.
   */
  removed(queryID: string) {
    const {desiredBy, patchVersion} = this._cvr.queries[queryID];
    assert(patchVersion);
    assert(
      Object.keys(desiredBy).length === 0,
      `Cannot remove query ${queryID}`,
    );

    assert(!this.#executedQueryIDs.has(queryID));
    this.#removedQueryIDs.add(queryID);
    delete this._cvr.queries[queryID];

    const newVersion = this._ensureNewVersion();
    void this._writes.del(this._paths.query({id: queryID}));
    void this._writes.del(this._paths.queryPatch(patchVersion, {id: queryID}));
    void this._writes.put(this._paths.queryPatch(newVersion, {id: queryID}), {
      type: 'query',
      op: 'del',
      id: queryID,
    } satisfies QueryPatch);
  }

  /**
   * Asserts that a new version has already been set.
   *
   * After {@link executed} and {@link removed} are called, we must have properly
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
    lc: LogContext,
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
      lc.debug?.(`received ${JSON.stringify(id)}`);

      const existing = existingRows.get(path);
      const {merged, putColumns} = mergeQueriedColumns(
        existing?.queriedColumns,
        queriedColumns,
      );
      let patchVersion: CVRVersion;
      if (existing?.rowVersion === rowVersion && putColumns.length === 0) {
        // No CVR changes necessary. Just send the content patch to interested clients
        // (i.e. that are catching up), if any.
        patchVersion = existing.patchVersion;
      } else {
        patchVersion = this.#assertNewVersion();
        if (existing) {
          void this._writes.del(
            this._paths.rowPatch(existing.patchVersion, id),
          );
        }
        const updated = {id, rowVersion, patchVersion, queriedColumns: merged};
        void this._writes.put(path, updated);
        void this._writes.put(this._paths.rowPatch(patchVersion, id), {
          type: 'row',
          op: 'put',
          id,
          rowVersion,
          columns: Object.keys(merged),
        } satisfies RowPatch);
      }
      merges.push({
        patch: {type: 'row', op: 'merge', id, contents},
        toVersion: patchVersion,
      });

      // Keep track of queried columns to determine which columns to prune at the end.
      const {merged: allQueriedColumns} = mergeQueriedColumns(
        this.#receivedRows.get(path),
        queriedColumns,
      );
      this.#receivedRows.set(path, allQueriedColumns);
    }
    return merges;
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
   *
   * @param generatePatchesAfter Generates delete and constrain patches from the
   *        version after `generatePatchesAfter`.
   */
  async deleteUnreferencedColumnsAndRows(
    lc: LogContext,
    generatePatchesAfter: NullableCVRVersion,
  ): Promise<PatchToVersion[]> {
    const removedOrExecutedQueryIDs = union(
      this.#removedQueryIDs,
      this.#executedQueryIDs,
    );
    if (removedOrExecutedQueryIDs.size === 0) {
      // Query-less update. This can happen for config-only changes.
      assert(this.#receivedRows.size === 0);
      return [];
    }

    // patches to send to the client.
    const patches: PatchToVersion[] = [];

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
        const update = this.#deleteUnreferencedColumnsOrRow(
          lc,
          path,
          existing,
          removedOrExecutedQueryIDs,
        );
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
    }

    if (cmpVersions(generatePatchesAfter, this._orig.version) < 0) {
      // Scan the CVR patch log to generate patchRows and deleteRows to catch clients up to
      // the original CVR version.
      const catchupRowPatches = this._writes.batchScan(
        {
          // Include all row patches starting from the version after `generatePatchesAfter`.
          start: {
            key: this._paths.rowPatchVersionPrefix(
              oneAfter(generatePatchesAfter),
            ),
          },
          // `end` is exclusive but we want to include patches in the `_orig.version`.
          end: this._paths.rowPatchVersionPrefix(oneAfter(this._orig.version)),
        },
        rowPatchSchema,
        2000,
      );
      for await (const batch of catchupRowPatches) {
        for (const [path, rowPatch] of batch) {
          const toVersion = this._paths.versionFromPatchPath(path);
          const {id} = rowPatch;
          if (rowPatch.op === 'put') {
            const {columns} = rowPatch;
            patches.push({
              patch: {type: 'row', op: 'constrain', id, columns},
              toVersion,
            });
          } else {
            lc.debug?.(`catchup delete: ${JSON.stringify(id)}`);
            patches.push({patch: {type: 'row', op: 'del', id}, toVersion});
          }
        }
      }
    }

    return patches;
  }

  #deleteUnreferencedColumnsOrRow(
    lc: LogContext,
    rowRecordPath: string,
    existing: RowRecord,
    removedOrExecutedQueryIDs: Set<string>,
  ): {id: RowID; columns?: string[]} | null {
    const received = this.#receivedRows.get(rowRecordPath);

    const {merged: newQueriedColumns, delColumns} = mergeQueriedColumns(
      existing.queriedColumns,
      received,
      removedOrExecutedQueryIDs,
    );

    if (!delColumns.length) {
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
      lc.debug?.(
        `deleting ${JSON.stringify(id)}, before: ${JSON.stringify(
          existing.queriedColumns,
        )}, merged: ${JSON.stringify(newQueriedColumns)}`,
      );
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
): {
  merged: QueriedColumns;
  putColumns: string[];
  delColumns: string[];
} {
  const merged: QueriedColumns = {};
  const putColumns = [];
  const delColumns = [];

  for (const col of new Set([
    ...(existing ? Object.keys(existing) : []),
    ...(received ? Object.keys(received) : []),
  ])) {
    const existingQueryIDs = new Set(existing?.[col]);
    const receivedQueryIDs = new Set(received?.[col]);
    const finalQueryIDs = union(
      removeIDs ? difference(existingQueryIDs, removeIDs) : existingQueryIDs,
      receivedQueryIDs,
    );
    if (finalQueryIDs.size === 0) {
      delColumns.push(col);
    } else {
      merged[col] = [...finalQueryIDs].sort(compareUTF8);
      if (!existing?.[col]) {
        putColumns.push(col);
      }
    }
  }

  return {merged, putColumns, delColumns};
}
