import type {LogContext} from '@rocicorp/logger';
import {compareUTF8} from 'compare-utf8';
import {assert} from '../../../../shared/src/asserts.ts';
import {CustomKeyMap} from '../../../../shared/src/custom-key-map.ts';
import {must} from '../../../../shared/src/must.ts';
import {
  difference,
  intersection,
  union,
} from '../../../../shared/src/set-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {stringify, type JSONObject} from '../../types/bigint-json.ts';
import type {LexiVersion} from '../../types/lexi-version.ts';
import {rowIDString} from '../../types/row-key.ts';
import {unescapedSchema as schema} from '../change-source/pg/schema/shard.ts';
import type {Patch, PatchToVersion} from './client-handler.ts';
import type {CVRFlushStats, CVRStore} from './cvr-store.ts';
import {KeyColumns} from './key-columns.ts';
import {
  cmpVersions,
  oneAfter,
  versionString,
  type CVRVersion,
  type ClientQueryRecord,
  type ClientRecord,
  type InternalQueryRecord,
  type QueryRecord,
  type RowID,
  type RowRecord,
} from './schema/types.ts';

export type RowUpdate = {
  version?: string; // Undefined for an unref.
  contents?: JSONObject; // Undefined for an unref.
  refCounts: {[hash: string]: number}; // Counts are negative when a row is unrefed.
};

/** Internally used mutable CVR type. */
export type CVR = {
  id: string;
  version: CVRVersion;
  lastActive: number;
  replicaVersion: string | null;
  clients: Record<string, ClientRecord>;
  queries: Record<string, QueryRecord>;
};

/** Exported immutable CVR type. */
// TODO: Use Immutable<CVR> when the AST is immutable.
export type CVRSnapshot = {
  readonly id: string;
  readonly version: CVRVersion;
  readonly lastActive: number;
  readonly replicaVersion: string | null;
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
  constructor(
    cvrStore: CVRStore,
    cvr: CVRSnapshot,
    replicaVersion: string | null,
  ) {
    this._cvrStore = cvrStore;
    this._orig = cvr;
    this._cvr = structuredClone(cvr) as CVR; // mutable deep copy
    this._cvr.replicaVersion = replicaVersion;
  }

  protected _setVersion(version: CVRVersion) {
    assert(cmpVersions(this._cvr.version, version) < 0);
    this._cvr.version = version;
    this._cvrStore.putInstance(this._cvr);
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

  async flush(
    lc: LogContext,
    skipNoopFlushes: boolean,
    lastConnectTime: number,
    lastActive = Date.now(),
  ): Promise<{
    cvr: CVRSnapshot;
    flushed: CVRFlushStats | false;
  }> {
    const start = Date.now();

    const flushed = await this._cvrStore.flush(
      this._orig.version,
      this._cvr.version,
      skipNoopFlushes,
      lastConnectTime,
      lastActive,
    );

    if (!flushed) {
      return {cvr: this._orig, flushed: false};
    }
    lc.debug?.(
      `flushed cvr@${versionString(this._cvr.version)} ` +
        `${JSON.stringify(flushed)} in (${Date.now() - start} ms)`,
    );
    this._cvr.lastActive = lastActive;
    return {cvr: this._cvr, flushed};
  }
}

/**
 * A {@link CVRConfigDrivenUpdater} is used for updating a CVR with config-driven
 * changes. Note that this may result in row deletion (e.g. if queries get dropped),
 * but the `stateVersion` of the CVR does not change.
 */
export class CVRConfigDrivenUpdater extends CVRUpdater {
  readonly #shardID;

  constructor(cvrStore: CVRStore, cvr: CVRSnapshot, shardID: string) {
    super(cvrStore, cvr, cvr.replicaVersion);
    this.#shardID = shardID;
  }

  #ensureClient(id: string): ClientRecord {
    let client = this._cvr.clients[id];
    if (client) {
      return client;
    }
    // Add the ClientRecord and PutPatch
    client = {id, desiredQueryIDs: []};
    this._cvr.clients[id] = client;

    const newVersion = this._ensureNewVersion();
    this._cvrStore.insertClient(client, newVersion);

    if (!this._cvr.queries[CLIENT_LMID_QUERY_ID]) {
      const lmidsQuery: InternalQueryRecord = {
        id: CLIENT_LMID_QUERY_ID,
        ast: {
          schema: '',
          table: `${schema(this.#shardID)}.clients`,
          where: {
            type: 'simple',
            left: {
              type: 'column',
              name: 'clientGroupID',
            },
            op: '=',
            right: {
              type: 'literal',
              value: this._cvr.id,
            },
          },
          orderBy: [
            ['clientGroupID', 'asc'],
            ['clientID', 'asc'],
          ],
        },
        internal: true,
      };
      this._cvr.queries[CLIENT_LMID_QUERY_ID] = lmidsQuery;
      this._cvrStore.putQuery(lmidsQuery);
    }
    return client;
  }

  putDesiredQueries(
    clientID: string,
    queries: Readonly<{hash: string; ast: AST}>[],
  ): PatchToVersion[] {
    const patches: PatchToVersion[] = [];
    const client = this.#ensureClient(clientID);
    const current = new Set(client.desiredQueryIDs);
    const additional = new Set(queries.map(({hash}) => hash));
    const needed = difference(additional, current);
    if (needed.size === 0) {
      return patches;
    }
    const newVersion = this._ensureNewVersion();
    client.desiredQueryIDs = [...union(current, needed)].sort(compareUTF8);

    for (const id of needed) {
      const {ast} = must(queries.find(({hash}) => hash === id));
      const query = this._cvr.queries[id] ?? {id, ast, desiredBy: {}};
      assertNotInternal(query);

      query.desiredBy[clientID] = newVersion;
      this._cvr.queries[id] = query;
      patches.push({
        toVersion: newVersion,
        patch: {type: 'query', op: 'put', id, ast, clientID},
      });

      this._cvrStore.putQuery(query);
      this._cvrStore.insertDesiredQuery(newVersion, query, client, false);
    }
    return patches;
  }

  deleteDesiredQueries(clientID: string, queries: string[]): PatchToVersion[] {
    const patches: PatchToVersion[] = [];
    const client = this.#ensureClient(clientID);
    const current = new Set(client.desiredQueryIDs);
    const unwanted = new Set(queries);
    const remove = intersection(unwanted, current);
    if (remove.size === 0) {
      return patches;
    }
    const newVersion = this._ensureNewVersion();
    client.desiredQueryIDs = [...difference(current, remove)].sort(compareUTF8);

    for (const id of remove) {
      const query = this._cvr.queries[id];
      if (!query) {
        continue; // Query itself has already been removed. Should not happen?
      }
      assertNotInternal(query);

      delete query.desiredBy[clientID];
      this._cvrStore.putQuery(query);
      this._cvrStore.insertDesiredQuery(newVersion, query, client, true);
      patches.push({
        toVersion: newVersion,
        patch: {type: 'query', op: 'del', id, clientID},
      });
    }
    return patches;
  }

  clearDesiredQueries(clientID: string): PatchToVersion[] {
    const client = this.#ensureClient(clientID);
    return this.deleteDesiredQueries(clientID, client.desiredQueryIDs);
  }

  flush(
    lc: LogContext,
    skipNoopFlushes: boolean,
    lastConnectTime: number,
    lastActive = Date.now(),
  ) {
    // TODO: Add cleanup of no-longer-desired got queries and constituent rows.
    return super.flush(lc, skipNoopFlushes, lastConnectTime, lastActive);
  }
}

type Hash = string;
export type Column = string;
export type RefCounts = Record<Hash, number>;

/**
 * A {@link CVRQueryDrivenUpdater} is used for updating a CVR after making queries.
 * The caller should invoke:
 *
 * * {@link trackQueries} for queries that are being executed or removed.
 * * {@link received} for all rows received from the executed queries
 * * {@link deleteUnreferencedRows} to remove any rows that have
 *       fallen out of the query result view.
 * * {@link flush}
 *
 * After flushing, the caller should perform any necessary catchup of
 * config and row patches for clients that are behind. See
 * {@link CVRStore.catchupConfigPatches} and {@link CVRStore.catchupRowPatches}.
 */
export class CVRQueryDrivenUpdater extends CVRUpdater {
  readonly #removedOrExecutedQueryIDs = new Set<string>();
  readonly #receivedRows = new CustomKeyMap<RowID, RefCounts | null>(
    rowIDString,
  );
  readonly #replacedRows = new CustomKeyMap<RowID, boolean>(rowIDString);

  #existingRows: Promise<RowRecord[]> | undefined = undefined;

  /**
   * @param stateVersion The `stateVersion` at which the queries were executed.
   */
  constructor(
    cvrStore: CVRStore,
    cvr: CVRSnapshot,
    stateVersion: LexiVersion,
    replicaVersion: string,
  ) {
    super(cvrStore, cvr, replicaVersion);

    assert(
      // We should either be setting the cvr.replicaVersion for the first time, or it should
      // be something newer than the current cvr.replicaVersion. Otherwise, the CVR should
      // have been rejected by the ViewSyncer.
      (cvr.replicaVersion ?? replicaVersion) <= replicaVersion,
      `Cannot sync from an older replicaVersion: CVR=${cvr.replicaVersion}, DB=${replicaVersion}`,
    );
    assert(stateVersion >= cvr.version.stateVersion);
    if (stateVersion > cvr.version.stateVersion) {
      this._setVersion({stateVersion});
    }
  }

  /**
   * Initiates the tracking of the specified `executed` and `removed` queries.
   * This kicks of a lookup of existing {@link RowRecord}s currently associated
   * with those queries, which will be used to reconcile the rows to keep
   * after all rows have been {@link received()}.
   *
   * "transformed" queries are queries that are currently
   * gotten and running in the pipeline driver but
   * received a new transformation hash due to an auth token
   * update.
   *
   * @returns The new CVRVersion to be used when all changes are committed.
   */
  trackQueries(
    lc: LogContext,
    executed: {id: string; transformationHash: string}[],
    removed: {id: string; transformationHash: string}[],
  ): {newVersion: CVRVersion; queryPatches: PatchToVersion[]} {
    assert(this.#existingRows === undefined, `trackQueries already called`);

    const queryPatches: Patch[] = [
      executed.map(q => this.#trackExecuted(q.id, q.transformationHash)),
      removed.map(q => this.#trackRemoved(q.id)),
    ].flat(2);

    this.#existingRows = this.#lookupRowsForExecutedAndRemovedQueries(lc);

    return {
      newVersion: this._cvr.version,
      queryPatches: queryPatches.map(patch => ({
        patch,
        toVersion: this._cvr.version,
      })),
    };
  }

  async #lookupRowsForExecutedAndRemovedQueries(
    lc: LogContext,
  ): Promise<RowRecord[]> {
    const results = new CustomKeyMap<RowID, RowRecord>(rowIDString);

    if (this.#removedOrExecutedQueryIDs.size === 0) {
      // Query-less update. This can happen for config only changes.
      return [];
    }

    // Utilizes the in-memory RowCache.
    const allRowRecords = (await this._cvrStore.getRowRecords()).values();
    let total = 0;
    for (const existing of allRowRecords) {
      total++;
      assert(existing.refCounts !== null); // allRowRecords does not include null.
      for (const id of Object.keys(existing.refCounts)) {
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
  #trackExecuted(queryID: string, transformationHash: string): Patch[] {
    assert(!this.#removedOrExecutedQueryIDs.has(queryID));
    this.#removedOrExecutedQueryIDs.add(queryID);

    let gotQueryPatch: Patch | undefined;
    const query = this._cvr.queries[queryID];
    if (query.transformationHash !== transformationHash) {
      const transformationVersion = this._ensureNewVersion();

      if (!query.internal && query.patchVersion === undefined) {
        // client query: desired -> gotten
        query.patchVersion = transformationVersion;
        gotQueryPatch = {
          type: 'query',
          op: 'put',
          id: query.id,
          ast: query.ast,
        };
      }

      query.transformationHash = transformationHash;
      query.transformationVersion = transformationVersion;
      this._cvrStore.updateQuery(query);
    }
    return gotQueryPatch ? [gotQueryPatch] : [];
  }

  /**
   * Tracks a query removed from the "gotten" set. In addition to producing the
   * appropriate patches for deleting the query, the removed query is taken into
   * account when computing the final row records in
   * {@link deleteUnreferencedRows}.
   * Namely, any rows with columns that are no longer referenced by a
   * query are deleted.
   *
   * This must only be called on queries that are not "desired" by any client.
   */
  #trackRemoved(queryID: string): Patch[] {
    const query = this._cvr.queries[queryID];
    assertNotInternal(query);

    assert(!this.#removedOrExecutedQueryIDs.has(queryID));
    this.#removedOrExecutedQueryIDs.add(queryID);
    delete this._cvr.queries[queryID];

    const newVersion = this._ensureNewVersion();
    const queryPatch = {type: 'query', op: 'del', id: queryID} as const;
    this._cvrStore.markQueryAsDeleted(newVersion, queryPatch);
    return [queryPatch];
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

  #keyColumns: KeyColumns | undefined;

  /**
   * Tracks rows received from executing queries. This will update row records
   * and row patches if the received rows have a new version. The method also
   * returns (put) patches to be returned to update their state, versioned by
   * patchVersion so that only the patches new to the clients are sent.
   */
  async received(
    lc: LogContext,
    rows: Map<RowID, RowUpdate>,
  ): Promise<PatchToVersion[]> {
    const patches: PatchToVersion[] = [];

    const existingRows = await this._cvrStore.getRowRecords();
    this.#keyColumns ??= new KeyColumns(existingRows.values());

    for (const [id, update] of rows.entries()) {
      const {contents, version, refCounts} = update;

      let existing = existingRows.get(id);
      if (!existing && contents) {
        // See if the row being put is referenced in the CVR using a different ID.
        const oldID = this.#keyColumns.getOldRowID(id, contents);
        if (oldID) {
          existing = existingRows.get(oldID);
          if (existing && !this.#replacedRows.get(oldID)) {
            lc.debug?.(`replacing ${stringify(oldID)} with ${stringify(id)}`);
            this.#replacedRows.set(oldID, true);
            this._cvrStore.delRowRecord(oldID);
            // Force the updates for these rows to happen, even if they look like
            // no-ops on their own.
            this._cvrStore.forceUpdates(oldID, id);
          }
        }
      }

      // Accumulate all received refCounts to determine which rows to prune.
      const previouslyReceived = this.#receivedRows.get(id);

      const merged =
        previouslyReceived !== undefined
          ? mergeRefCounts(previouslyReceived, refCounts)
          : mergeRefCounts(
              existing?.refCounts,
              refCounts,
              this.#removedOrExecutedQueryIDs,
            );

      this.#receivedRows.set(id, merged);

      const patchVersion =
        existing && existing?.rowVersion === version
          ? existing.patchVersion
          : this.#assertNewVersion();
      const rowVersion = version ?? existing?.rowVersion;
      assert(rowVersion, `Cannot delete a row that is not in the CVR`);

      this._cvrStore.putRowRecord({
        id,
        rowVersion,
        patchVersion,
        refCounts: merged,
      });

      if (merged === null) {
        // All refCounts have gone to zero, if row was previously synced
        // delete it.
        if (existing || previouslyReceived) {
          patches.push({
            patch: {
              type: 'row',
              op: 'del',
              id,
            },
            toVersion: patchVersion,
          });
        }
      } else if (contents) {
        patches.push({
          patch: {
            type: 'row',
            op: 'put',
            id,
            contents,
          },
          toVersion: patchVersion,
        });
      }
    }
    return patches;
  }

  /**
   * Computes and updates the row records based on:
   * * The {@link #executed} queries
   * * The {@link #removed} queries
   * * The {@link received} rows
   *
   * Returns the final delete and patch ops that must be sent to the client
   * to delete rows that are no longer referenced by any query.
   *
   * This is Step [5] of the
   * [CVR Sync Algorithm](https://www.notion.so/replicache/Sync-and-Client-View-Records-CVR-a18e02ec3ec543449ea22070855ff33d?pvs=4#7874f9b80a514be2b8cd5cf538b88d37).
   */
  async deleteUnreferencedRows(lc?: LogContext): Promise<PatchToVersion[]> {
    if (this.#removedOrExecutedQueryIDs.size === 0) {
      // Query-less update. This can happen for config-only changes.
      assert(this.#receivedRows.size === 0);
      return [];
    }

    // patches to send to the client.
    const patches: PatchToVersion[] = [];

    const start = Date.now();
    assert(this.#existingRows, `trackQueries() was not called`);
    for (const existing of await this.#existingRows) {
      const deletedID = this.#deleteUnreferencedRow(existing);
      if (deletedID === null) {
        continue;
      }
      patches.push({
        toVersion: this._cvr.version,
        patch: {type: 'row', op: 'del', id: deletedID},
      });
    }
    lc?.debug?.(
      `computed ${patches.length} delete patches (${Date.now() - start} ms)`,
    );

    return patches;
  }

  #deleteUnreferencedRow(existing: RowRecord): RowID | null {
    if (
      this.#receivedRows.get(existing.id) ||
      this.#replacedRows.get(existing.id)
    ) {
      return null;
    }

    const newRefCounts = mergeRefCounts(
      existing.refCounts,
      undefined,
      this.#removedOrExecutedQueryIDs,
    );
    // If a row is still referenced, we update the refCounts but not the
    // patchVersion (as the existence and contents of the row have not
    // changed from the clients' perspective). If the row is deleted, it
    // gets a new patchVersion (and corresponding poke).
    const patchVersion = newRefCounts
      ? existing.patchVersion
      : this.#assertNewVersion();
    const rowRecord: RowRecord = {
      ...existing,
      patchVersion,
      refCounts: newRefCounts,
    };

    this._cvrStore.putRowRecord(rowRecord);

    // Return the id to delete if no longer referenced.
    return newRefCounts ? null : existing.id;
  }
}

function mergeRefCounts(
  existing: RefCounts | null | undefined,
  received: RefCounts | null | undefined,
  removeHashes?: Set<string>,
): RefCounts | null {
  let merged: RefCounts = {};
  if (!existing) {
    merged = received ?? {};
  } else {
    [existing, received].forEach((refCounts, i) => {
      if (!refCounts) {
        return;
      }
      for (const [hash, count] of Object.entries(refCounts)) {
        if (i === 0 /* existing */ && removeHashes?.has(hash)) {
          continue; // removeHashes from existing row.
        }
        merged[hash] = (merged[hash] ?? 0) + count;
        if (merged[hash] === 0) {
          delete merged[hash];
        }
      }

      return merged;
    });
  }

  return Object.values(merged).some(v => v > 0) ? merged : null;
}
