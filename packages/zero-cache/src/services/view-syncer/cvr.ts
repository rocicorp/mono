import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import {type JSONObject} from '../../../../shared/src/bigint-json.ts';
import {CustomKeyMap} from '../../../../shared/src/custom-key-map.ts';
import {
  deepEqual,
  type ReadonlyJSONValue,
} from '../../../../shared/src/json.ts';
import {must} from '../../../../shared/src/must.ts';
import {
  difference,
  intersection,
  union,
} from '../../../../shared/src/set-utils.ts';
import {stringCompare} from '../../../../shared/src/string-compare.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import {ErrorOrigin} from '../../../../zero-protocol/src/error-origin.ts';
import {ProtocolError} from '../../../../zero-protocol/src/error.ts';
import {
  clampTTL,
  compareTTL,
  DEFAULT_TTL_MS,
} from '../../../../zql/src/query/ttl.ts';
import {recordQuery} from '../../server/anonymous-otel-start.ts';
import type {LexiVersion} from '../../types/lexi-version.ts';
import {rowIDString} from '../../types/row-key.ts';
import {upstreamSchema, type ShardID} from '../../types/shards.ts';
import type {Patch, PatchToVersion} from './client-handler.ts';
import {type CVRFlushStats, type CVRStore} from './cvr-store.ts';
import {
  cmpVersions,
  maxVersion,
  oneAfter,
  type ClientQueryRecord,
  type ClientRecord,
  type CustomQueryRecord,
  type CVRVersion,
  type InternalQueryRecord,
  type QueryRecord,
  type RowID,
  type RowRecord,
} from './schema/types.ts';
import {ttlClockAsNumber, type TTLClock} from './ttl-clock.ts';

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
  ttlClock: TTLClock;
  replicaVersion: string | null;
  clients: Record<string, ClientRecord>;
  queries: Record<string, QueryRecord>;
  clientSchema: ClientSchema | null;
  profileID: string | null;
};

/** Exported immutable CVR type. */
// TODO: Use Immutable<CVR> when the AST is immutable.
export type CVRSnapshot = {
  readonly id: string;
  readonly version: CVRVersion;
  readonly lastActive: number;
  readonly ttlClock: TTLClock;
  readonly replicaVersion: string | null;
  readonly clients: Readonly<Record<string, ClientRecord>>;
  readonly queries: Readonly<Record<string, QueryRecord>>;
  readonly clientSchema: ClientSchema | null;
  readonly profileID: string | null;
};

const CLIENT_LMID_QUERY_ID = 'lmids';
const CLIENT_MUTATION_RESULTS_QUERY_ID = 'mutationResults';

function assertNotInternal(
  query: QueryRecord,
): asserts query is ClientQueryRecord {
  if (query.type === 'internal') {
    // This should never happen for behaving clients, as query ids should be hashes.
    throw new Error(`Query ID ${query.id} is reserved for internal use`);
  }
}

export function getMutationResultsQuery(
  upstreamSchema: string,
  clientGroupID: string,
): InternalQueryRecord {
  return {
    id: CLIENT_MUTATION_RESULTS_QUERY_ID,
    type: 'internal',
    ast: {
      schema: '',
      table: `${upstreamSchema}.mutations`,
      where: {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            left: {
              type: 'column',
              name: 'clientGroupID',
            },
            op: '=',
            right: {
              type: 'literal',
              value: clientGroupID,
            },
          },
        ],
      },
      orderBy: [
        ['clientGroupID', 'asc'],
        ['clientID', 'asc'],
        ['mutationID', 'asc'],
      ],
    },
  };
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
    lastConnectTime: number,
    lastActive: number,
    ttlClock: TTLClock,
  ): Promise<{
    cvr: CVRSnapshot;
    flushed: CVRFlushStats | false;
  }> {
    this._cvr.ttlClock = ttlClock;
    this._cvr.lastActive = lastActive;
    const flushed = await this._cvrStore.flush(
      lc,
      this._orig.version,
      this._cvr,
      lastConnectTime,
    );
    if (!flushed) {
      return {cvr: this._orig, flushed: false};
    }
    return {cvr: this._cvr, flushed};
  }
}

/**
 * A {@link CVRConfigDrivenUpdater} is used for updating a CVR with config-driven
 * changes. Note that this may result in row deletion (e.g. if queries get dropped),
 * but the `stateVersion` of the CVR does not change.
 */
export class CVRConfigDrivenUpdater extends CVRUpdater {
  readonly #shard: ShardID;

  constructor(cvrStore: CVRStore, cvr: CVRSnapshot, shard: ShardID) {
    super(cvrStore, cvr, cvr.replicaVersion);
    this.#shard = shard;
  }

  ensureClient(id: string): ClientRecord {
    let client = this._cvr.clients[id];
    if (client) {
      return client;
    }
    // Add the ClientRecord and PutPatch
    client = {id, desiredQueryIDs: []};
    this._cvr.clients[id] = client;

    this._ensureNewVersion();
    this._cvrStore.insertClient(client);

    if (!this._cvr.queries[CLIENT_LMID_QUERY_ID]) {
      const lmidsQuery: InternalQueryRecord = {
        id: CLIENT_LMID_QUERY_ID,
        ast: {
          schema: '',
          table: `${upstreamSchema(this.#shard)}.clients`,
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
        type: 'internal',
      };
      this._cvr.queries[CLIENT_LMID_QUERY_ID] = lmidsQuery;
      this._cvrStore.putQuery(lmidsQuery);
    }
    if (!this._cvr.queries[CLIENT_MUTATION_RESULTS_QUERY_ID]) {
      const mutationResultsQuery: InternalQueryRecord = getMutationResultsQuery(
        upstreamSchema(this.#shard),
        this._cvr.id,
      );
      this._cvr.queries[CLIENT_MUTATION_RESULTS_QUERY_ID] =
        mutationResultsQuery;
      this._cvrStore.putQuery(mutationResultsQuery);
    }

    return client;
  }

  setClientSchema(lc: LogContext, clientSchema: ClientSchema) {
    if (this._cvr.clientSchema === null) {
      this._cvr.clientSchema = clientSchema;
      this._cvrStore.putInstance(this._cvr);
    } else if (!deepEqual(this._cvr.clientSchema, clientSchema)) {
      // This should not be possible with a correct Zero client, as clients
      // of a CVR should all have the same schema (given that the schema hash
      // is part of the idb key). In fact, clients joining an existing group
      // (i.e. non-empty baseCookie) do not send the clientSchema message.
      lc.warn?.(
        `New schema ${JSON.stringify(
          clientSchema,
        )} does not match existing schema ${JSON.stringify(
          this._cvr.clientSchema,
        )}`,
      );
      throw new ProtocolError({
        kind: 'InvalidConnectionRequest',
        message: `Provided schema does not match previous schema`,
        origin: ErrorOrigin.ZeroCache,
      });
    }
  }

  setProfileID(lc: LogContext, profileID: string) {
    if (this._cvr.profileID !== profileID) {
      if (
        this._cvr.profileID !== null &&
        !this._cvr.profileID.startsWith('cg')
      ) {
        // We expect profile ID's to change from null or from the back-filled
        // "cg..." value. Log a warning otherwise to surface unexpected or
        // pathological conditions.
        lc.warn?.(
          `changing profile ID from ${this._cvr.profileID} to ${profileID}`,
        );
      }
      this._cvr.profileID = profileID;
      this._cvrStore.putInstance(this._cvr);
    }
  }

  putDesiredQueries(
    clientID: string,
    queries: Readonly<{
      hash: string;
      ast?: AST | undefined;
      name?: string | undefined;
      args?: readonly ReadonlyJSONValue[] | undefined;
      ttl?: number | undefined;
    }>[],
  ): PatchToVersion[] {
    const patches: PatchToVersion[] = [];
    const client = this.ensureClient(clientID);
    const current = new Set(client.desiredQueryIDs);

    // Find the new/changed desired queries.
    const needed: Set<string> = new Set();

    const recordQueryForTelemetry = (q: (typeof queries)[0]) => {
      const {ast, name, args} = q;
      if (ast) {
        recordQuery('crud');
      } else if (name && args) {
        recordQuery('custom');
      }
    };

    for (const q of queries) {
      const {hash, ttl = DEFAULT_TTL_MS} = q;
      const query = this._cvr.queries[hash];
      if (!query) {
        // New query - record for telemetry
        recordQueryForTelemetry(q);
        needed.add(hash);
        continue;
      }
      if (query.type === 'internal') {
        continue;
      }

      const oldClientState = query.clientState[clientID];
      // Old query was inactivated or never desired by this client.
      if (!oldClientState || oldClientState.inactivatedAt !== undefined) {
        // Reactivated query - record for telemetry
        recordQueryForTelemetry(q);
        needed.add(hash);
        continue;
      }

      if (compareTTL(ttl, oldClientState.ttl) > 0) {
        // TTL update only - don't record for telemetry
        needed.add(hash);
      }
    }

    if (needed.size === 0) {
      return patches;
    }
    const newVersion = this._ensureNewVersion();
    client.desiredQueryIDs = [...union(current, needed)].sort(stringCompare);

    for (const id of needed) {
      const q = must(queries.find(({hash}) => hash === id));
      const {ast, name, args} = q;

      const ttl = clampTTL(q.ttl ?? DEFAULT_TTL_MS);
      const query =
        this._cvr.queries[id] ?? newQueryRecord(id, ast, name, args);
      assertNotInternal(query);

      const inactivatedAt = undefined;

      query.clientState[clientID] = {
        inactivatedAt,
        ttl,
        version: newVersion,
      };
      this._cvr.queries[id] = query;
      patches.push({
        toVersion: newVersion,
        patch: {type: 'query', op: 'put', id, clientID},
      });

      this._cvrStore.putQuery(query);
      this._cvrStore.putDesiredQuery(
        newVersion,
        query,
        client,
        false,
        inactivatedAt,
        ttl,
      );
    }
    return patches;
  }

  markDesiredQueriesAsInactive(
    clientID: string,
    queryHashes: string[],
    ttlClock: TTLClock,
  ): PatchToVersion[] {
    return this.#deleteQueries(clientID, queryHashes, ttlClock);
  }

  deleteDesiredQueries(
    clientID: string,
    queryHashes: string[],
  ): PatchToVersion[] {
    return this.#deleteQueries(clientID, queryHashes, undefined);
  }

  #deleteQueries(
    clientID: string,
    queryHashes: string[],
    inactivatedAt: TTLClock | undefined,
  ): PatchToVersion[] {
    const patches: PatchToVersion[] = [];
    const client = this.ensureClient(clientID);
    const current = new Set(client.desiredQueryIDs);
    const unwanted = new Set(queryHashes);
    const remove = intersection(unwanted, current);
    if (remove.size === 0) {
      return patches;
    }

    const newVersion = this._ensureNewVersion();
    client.desiredQueryIDs = [...difference(current, remove)].sort(
      stringCompare,
    );

    for (const id of remove) {
      const query = this._cvr.queries[id];
      if (!query) {
        continue; // Query itself has already been removed. Should not happen?
      }
      assertNotInternal(query);

      let ttl = DEFAULT_TTL_MS;
      if (inactivatedAt === undefined) {
        delete query.clientState[clientID];
      } else {
        // client state can be missing if the query never transformed so we never
        // recorded it.
        const clientState = query.clientState[clientID];
        if (clientState !== undefined) {
          assert(
            clientState.inactivatedAt === undefined,
            `Query ${id} is already inactivated`,
          );
          // Clamp TTL to ensure we don't propagate historical unclamped values.
          ttl = clampTTL(clientState.ttl);
          query.clientState[clientID] = {
            inactivatedAt,
            ttl,
            version: newVersion,
          };
        }
      }

      this._cvrStore.putQuery(query);
      this._cvrStore.putDesiredQuery(
        newVersion,
        query,
        client,
        true,
        inactivatedAt,
        ttl,
      );
      patches.push({
        toVersion: newVersion,
        patch: {type: 'query', op: 'del', id, clientID},
      });
    }
    return patches;
  }

  clearDesiredQueries(clientID: string): PatchToVersion[] {
    const client = this.ensureClient(clientID);
    return this.#deleteQueries(clientID, client.desiredQueryIDs, undefined);
  }

  deleteClient(clientID: string, ttlClock: TTLClock): PatchToVersion[] {
    // clientID might not be part of this client group but if it is, this delete
    // may generate changes to the desired queries.

    const client = this._cvr.clients[clientID];
    if (!client) {
      // Clients in different client groups are no longer deleted, leaving
      // cleanup to inactive CVR purging logic.
      return [];
    }

    // When a client is deleted we mark all of its desired queries as inactive.
    // They will then be removed when the queries expire.
    const patches = this.markDesiredQueriesAsInactive(
      clientID,
      client.desiredQueryIDs,
      ttlClock,
    );
    delete this._cvr.clients[clientID];
    this._cvrStore.deleteClient(clientID);

    return patches;
  }
}

type Hash = string;
export type Column = string;
export type RefCounts = Record<Hash, number>;

type RowPatchInfo = {
  rowVersion: string | null; // null for a row-del
  toVersion: CVRVersion; // patchVersion
};

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
  readonly #lastPatches = new CustomKeyMap<RowID, RowPatchInfo>(rowIDString);

  #existingRows: Promise<Iterable<RowRecord>> | undefined = undefined;

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
    assert(
      stateVersion >= cvr.version.stateVersion,
      () =>
        `stateVersion (${stateVersion}) must be >= cvr.version.stateVersion (${cvr.version.stateVersion})`,
    );
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
    removed: {id: string}[],
  ): {newVersion: CVRVersion; queryPatches: PatchToVersion[]} {
    assert(this.#existingRows === undefined, `trackQueries already called`);

    const queryPatches: Patch[] = [
      executed.map(q => this.#trackExecuted(q.id, q.transformationHash)),
      removed.map(q => this.#trackRemoved(q.id)),
    ].flat(2);

    this.#existingRows = this.#lookupRowsForExecutedAndRemovedQueries(lc);
    // Immediately attach a rejection handler to avoid unhandled rejections.
    // The error will surface when this.#existingRows is awaited.
    void this.#existingRows.then(() => {});

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
  ): Promise<Iterable<RowRecord>> {
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
      assert(
        existing.refCounts !== null,
        'allRowRecords should not include null refCounts',
      );
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
    return results.values();
  }

  /**
   * Tracks an executed query, ensures that it is marked as "gotten",
   * updating the CVR and creating put patches if necessary.
   *
   * This must be called for all executed queries.
   */
  #trackExecuted(queryID: string, transformationHash: string): Patch[] {
    assert(
      !this.#removedOrExecutedQueryIDs.has(queryID),
      () => `Query ${queryID} already tracked as executed or removed`,
    );
    this.#removedOrExecutedQueryIDs.add(queryID);

    let gotQueryPatch: Patch | undefined;
    const query = this._cvr.queries[queryID];
    if (query.transformationHash !== transformationHash) {
      const transformationVersion = this._ensureNewVersion();

      if (query.type !== 'internal' && query.patchVersion === undefined) {
        // client query: desired -> gotten
        query.patchVersion = transformationVersion;
        gotQueryPatch = {
          type: 'query',
          op: 'put',
          id: query.id,
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

    assert(
      !this.#removedOrExecutedQueryIDs.has(queryID),
      () => `Query ${queryID} already tracked as executed or removed`,
    );
    this.#removedOrExecutedQueryIDs.add(queryID);
    delete this._cvr.queries[queryID];

    const newVersion = this._ensureNewVersion();
    const queryPatch = {type: 'query', op: 'del', id: queryID} as const;
    this._cvrStore.markQueryAsDeleted(newVersion, queryPatch);
    return [queryPatch];
  }

  /**
   * Marks the given query IDs as re-executed so that their old refCounts
   * will be stripped when re-hydration rows arrive via {@link received()}.
   * This is lighter than {@link trackQueries}: it does not access
   * `cvr.queries` (which would crash for companion query IDs like
   * `scalar:q1:0` that aren't in the CVR) and does not produce query
   * patches.
   */
  markReExecuted(lc: LogContext, queryIDs: string[]) {
    for (const id of queryIDs) {
      this.#removedOrExecutedQueryIDs.add(id);
    }

    // Retroactively strip old refCounts for the newly-marked query IDs
    // from rows already processed by received() during the push phase.
    // Without this, the previouslyReceived path in received() would
    // skip stripping, leaving stale refCounts that cause double-counting.
    for (const [rowID, refCounts] of this.#receivedRows) {
      if (refCounts === null) {
        continue;
      }
      let changed = false;
      for (const id of queryIDs) {
        if (id in refCounts) {
          delete refCounts[id];
          changed = true;
        }
      }
      if (changed) {
        const corrected = Object.values(refCounts).some(v => v > 0)
          ? refCounts
          : null;
        if (corrected === null) {
          this.#receivedRows.set(rowID, null);
        }
        // Correct the pending store entry to match the stripped refCounts.
        // received() already committed a putRowRecord with the pre-strip
        // refCounts; this overwrites just the refCounts field.
        this._cvrStore.updatePendingRowRefCounts(rowID, corrected);
      }
    }

    // Kick off async lookup of existing rows (awaited by deleteUnreferencedRows).
    this.#existingRows = this.#lookupRowsForExecutedAndRemovedQueries(lc);
    void this.#existingRows.then(() => {});
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
   * Tracks rows received from executing queries. This will update row records
   * and row patches if the received rows have a new version. The method also
   * returns (put) patches to be returned to update their state, versioned by
   * patchVersion so that only the patches new to the clients are sent.
   */
  async received(
    _lc: LogContext,
    rows: Map<RowID, RowUpdate>,
  ): Promise<PatchToVersion[]> {
    const patches: PatchToVersion[] = [];
    const existingRows = await this._cvrStore.getRowRecords();

    for (const [id, update] of rows.entries()) {
      const {contents, version, refCounts} = update;

      let existing = existingRows.get(id);
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

      const newRowVersion = merged === null ? undefined : version;
      const patchVersion =
        existing && existing.rowVersion === newRowVersion
          ? existing.patchVersion // existing row is unchanged
          : this.#assertNewVersion();

      // Note: for determining what to commit to the CVR store, use the
      // `version` of the update even if `merged` is null (i.e. don't
      // use `newRowVersion`). This will be deduped by the cvr-store flush
      // if it is redundant. In rare cases--namely, if the row key has
      // changed--we _do_ want to add row-put for the new row key with
      // `refCounts: null` in order to correctly record a delete patch
      // for that row, as the row with the old key will be removed.
      const rowVersion = version ?? existing?.rowVersion;
      if (rowVersion) {
        this._cvrStore.putRowRecord({
          id,
          rowVersion,
          patchVersion,
          refCounts: merged,
        });
      } else {
        // This means that a row that was not in the CVR was added during
        // this update, and then subsequently removed. Since there's no
        // corresponding row in the CVR itself, cancel the previous put.
        // Note that we still send a 'del' patch to the client in order to
        // cancel the previous 'put' patch.
        this._cvrStore.delRowRecord(id);
      }

      // Dedupe against the lastPatch sent for the row, and ensure that
      // toVersion never backtracks (lest it be undesirably filtered).
      const lastPatch = this.#lastPatches.get(id);
      const toVersion = maxVersion(patchVersion, lastPatch?.toVersion);

      if (merged === null) {
        // All refCounts have gone to zero, if row was previously synced
        // delete it.
        if (existing || previouslyReceived) {
          // dedupe
          if (lastPatch?.rowVersion !== null) {
            patches.push({
              patch: {
                type: 'row',
                op: 'del',
                id,
              },
              toVersion,
            });
            this.#lastPatches.set(id, {rowVersion: null, toVersion});
          }
        }
      } else if (contents) {
        assert(rowVersion, 'rowVersion is required when contents is present');
        // dedupe
        if (!lastPatch?.rowVersion || lastPatch.rowVersion < rowVersion) {
          patches.push({
            patch: {
              type: 'row',
              op: 'put',
              id,
              contents,
            },
            toVersion,
          });
          this.#lastPatches.set(id, {rowVersion, toVersion});
        }
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
      assert(
        this.#receivedRows.size === 0,
        () =>
          `Expected no received rows for query-less update, got ${this.#receivedRows.size}`,
      );
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
    if (this.#receivedRows.get(existing.id)) {
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

/**
 * The query must be inactive for all clients to be considered inactive.
 * This is because expiration is defined that way: a query is expired for a client group
 * only if it is expired for all clients in the group.
 *
 * If all clients have inactivated the query, we return
 * the one with the expiration furthest in the future.
 */
export function getInactiveQueries(cvr: CVR): {
  hash: string;
  inactivatedAt: TTLClock;
  ttl: number;
}[] {
  // We no longer support a TTL larger than 10 minutes.
  const inactive: Map<
    string,
    {
      hash: string;
      inactivatedAt: TTLClock;
      ttl: number;
    }
  > = new Map();
  for (const [queryID, query] of Object.entries(cvr.queries)) {
    if (query.type === 'internal') {
      continue;
    }
    for (const clientState of Object.values(query.clientState)) {
      // 1. Take the longest TTL
      // 2. If the query is not inactivated (for any client), do not return it
      const {inactivatedAt, ttl} = clientState;
      const existing = inactive.get(queryID);
      if (inactivatedAt === undefined) {
        if (existing) {
          inactive.delete(queryID);
        }
        break;
      }

      const clampedTTL = clampTTL(ttl);
      if (existing) {
        // The stored one might be too large because from a previous version of
        // zero
        const existingTTL = clampTTL(existing.ttl);
        // Use the last eviction time.
        if (
          existingTTL + ttlClockAsNumber(existing.inactivatedAt) <
          ttlClockAsNumber(inactivatedAt) + clampedTTL
        ) {
          existing.ttl = clampedTTL;
          existing.inactivatedAt = inactivatedAt;
        }
      } else {
        inactive.set(queryID, {
          hash: queryID,
          inactivatedAt,
          ttl: clampedTTL,
        });
      }
    }
  }

  // First sort all the queries that have TTL. Oldest first.
  return [...inactive.values()].sort((a, b) => {
    if (a.ttl === b.ttl) {
      return (
        ttlClockAsNumber(a.inactivatedAt) - ttlClockAsNumber(b.inactivatedAt)
      );
    }
    return (
      ttlClockAsNumber(a.inactivatedAt) +
      a.ttl -
      ttlClockAsNumber(b.inactivatedAt) -
      b.ttl
    );
  });
}

export function nextEvictionTime(cvr: CVR): TTLClock | undefined {
  let next: number | undefined;
  for (const {inactivatedAt, ttl} of getInactiveQueries(cvr)) {
    const expire = ttlClockAsNumber(inactivatedAt) + ttl;
    if (next === undefined || expire < next) {
      next = expire;
    }
  }
  return next as TTLClock | undefined;
}

function newQueryRecord(
  id: string,
  ast: AST | undefined,
  name: string | undefined,
  args: readonly ReadonlyJSONValue[] | undefined,
): ClientQueryRecord | CustomQueryRecord {
  if (ast !== undefined) {
    assert(
      name === undefined && args === undefined,
      'Cannot provide name or args with ast',
    );
    return {
      id,
      type: 'client',
      ast,
      clientState: {},
    } satisfies ClientQueryRecord;
  }

  assert(
    name !== undefined && args !== undefined,
    'Must provide name and args',
  );
  return {
    id,
    type: 'custom',
    name,
    args,
    clientState: {},
  } satisfies CustomQueryRecord;
}
