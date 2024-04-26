import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import {compareUTF8} from 'compare-utf8';
import {assert} from 'shared/src/asserts.js';
import type {DeepReadonly} from 'shared/src/json.js';
import {difference, intersection, union} from 'shared/src/set-utils.js';
import type {DurableStorage} from '../../storage/durable-storage.js';
import type {Storage} from '../../storage/storage.js';
import {WriteCache} from '../../storage/write-cache.js';
import {LexiVersion, versionToLexi} from '../../types/lexi-version.js';
import {CVRPaths, lastActiveIndex} from './schema/paths.js';
import {
  ClientPatch,
  CvrID,
  QueryPatch,
  cmpVersions,
  metaRecordSchema,
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
export type CVRSnapshot = DeepReadonly<CVR>;

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
    } else if (key.includes('/clients/')) {
      const client = value as ClientRecord;
      cvr.clients[client.id] = client;
    } else if (key.includes('/queries/')) {
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
  protected _ensureMinorVersionBump(): CVRVersion {
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
    const newVersion = this._ensureMinorVersionBump();
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
    const newVersion = this._ensureMinorVersionBump();
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
    const newVersion = this._ensureMinorVersionBump();
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

  // TODO: Add business logic here.
}
