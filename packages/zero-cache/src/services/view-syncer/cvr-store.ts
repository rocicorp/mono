import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import type {MaybeRow, PendingQuery} from 'postgres';
import {assert} from '../../../../shared/src/asserts.js';
import {CustomKeyMap} from '../../../../shared/src/custom-key-map.js';
import {
  deepEqual,
  type ReadonlyJSONValue,
} from '../../../../shared/src/json.js';
import {must} from '../../../../shared/src/must.js';
import {astSchema} from '../../../../zero-protocol/src/ast.js';
import type {JSONValue} from '../../types/bigint-json.js';
import {versionToLexi} from '../../types/lexi-version.js';
import type {PostgresDB, PostgresTransaction} from '../../types/pg.js';
import {rowIDHash} from '../../types/row-key.js';
import type {Patch, PatchToVersion} from './client-handler.js';
import type {CVR, CVRSnapshot} from './cvr.js';
import {
  type ClientsRow,
  type DesiresRow,
  type InstancesRow,
  type QueriesRow,
  rowRecordToRowsRow,
  type RowsRow,
  rowsRowToRowRecord,
} from './schema/cvr.js';
import {
  type ClientQueryRecord,
  type ClientRecord,
  cmpVersions,
  type CVRVersion,
  type InternalQueryRecord,
  type NullableCVRVersion,
  type QueryPatch,
  type QueryRecord,
  type RowID,
  type RowRecord,
  versionFromString,
  versionString,
} from './schema/types.js';

type NotNull<T> = T extends null ? never : T;

export type CVRFlushStats = {
  instances: number;
  queries: number;
  desires: number;
  clients: number;
  rows: number;
  statements: number;
};

class RowRecordCache {
  #cache: Promise<CustomKeyMap<RowID, RowRecord>> | undefined;
  readonly #db: PostgresDB;
  readonly #cvrID: string;

  constructor(db: PostgresDB, cvrID: string) {
    this.#db = db;
    this.#cvrID = cvrID;
  }

  async #ensureLoaded(): Promise<CustomKeyMap<RowID, RowRecord>> {
    if (this.#cache) {
      return this.#cache;
    }
    const r = resolver<CustomKeyMap<RowID, RowRecord>>();
    const cache: CustomKeyMap<RowID, RowRecord> = new CustomKeyMap(rowIDHash);
    for await (const rows of this.#db<
      RowsRow[]
    >`SELECT * FROM cvr.rows WHERE "clientGroupID" = ${
      this.#cvrID
    } AND "refCounts" IS NOT NULL`
      // TODO(arv): Arbitrary page size
      .cursor(5000)) {
      for (const row of rows) {
        const rowRecord = rowsRowToRowRecord(row);
        cache.set(rowRecord.id, rowRecord);
      }
    }
    r.resolve(cache);
    this.#cache = r.promise;
    return this.#cache;
  }

  getRowRecords(): Promise<ReadonlyMap<RowID, RowRecord>> {
    return this.#ensureLoaded();
  }

  async flush(rowRecords: Iterable<RowRecord>) {
    const cache = await this.#ensureLoaded();
    for (const row of rowRecords) {
      if (row.refCounts === null) {
        cache.delete(row.id);
      } else {
        cache.set(row.id, row);
      }
    }
  }

  clear() {
    this.#cache = undefined;
  }
}

type QueryRow = {
  queryHash: string;
  clientAST: NotNull<JSONValue>;
  patchVersion: string | null;
  transformationHash: string | null;
  transformationVersion: string | null;
  internal: boolean | null;
  deleted: boolean | null;
};

function asQuery(row: QueryRow): QueryRecord {
  const ast = astSchema.parse(row.clientAST);
  const maybeVersion = (s: string | null) =>
    s === null ? undefined : versionFromString(s);
  return row.internal
    ? ({
        id: row.queryHash,
        ast,
        transformationHash: row.transformationHash ?? undefined,
        transformationVersion: maybeVersion(row.transformationVersion),
        internal: true,
      } satisfies InternalQueryRecord)
    : ({
        id: row.queryHash,
        ast,
        patchVersion: maybeVersion(row.patchVersion),
        desiredBy: {},
        transformationHash: row.transformationHash ?? undefined,
        transformationVersion: maybeVersion(row.transformationVersion),
      } satisfies ClientQueryRecord);
}

export class CVRStore {
  readonly #lc: LogContext;
  readonly #id: string;
  readonly #db: PostgresDB;
  readonly #writes: Set<{
    stats: Partial<CVRFlushStats>;
    write: (tx: PostgresTransaction) => PendingQuery<MaybeRow[]>;
  }> = new Set();
  readonly #pendingRowRecordPuts = new CustomKeyMap<RowID, RowRecord>(
    rowIDHash,
  );
  readonly #rowCache: RowRecordCache;

  constructor(lc: LogContext, db: PostgresDB, cvrID: string) {
    this.#lc = lc;
    this.#db = db;
    this.#id = cvrID;
    this.#rowCache = new RowRecordCache(db, cvrID);
  }

  async load(): Promise<CVR> {
    const start = Date.now();

    const id = this.#id;
    const cvr: CVR = {
      id,
      version: {stateVersion: versionToLexi(0)},
      lastActive: {epochMillis: 0},
      clients: {},
      queries: {},
    };

    const [versionAndLastActive, clientsRows, queryRows, desiresRows] =
      await this.#db.begin(tx => [
        tx<
          Pick<InstancesRow, 'version' | 'lastActive'>[]
        >`SELECT version, "lastActive" FROM cvr.instances WHERE "clientGroupID" = ${id}`,
        tx<
          Pick<ClientsRow, 'clientID' | 'patchVersion'>[]
        >`SELECT "clientID", "patchVersion" FROM cvr.clients WHERE "clientGroupID" = ${id}`,
        tx<
          QueryRow[]
        >`SELECT * FROM cvr.queries WHERE "clientGroupID" = ${id} AND (deleted IS NULL OR deleted = FALSE)`,
        tx<
          DesiresRow[]
        >`SELECT * FROM cvr.desires WHERE "clientGroupID" = ${id} AND (deleted IS NULL OR deleted = FALSE)`,
      ]);

    if (versionAndLastActive.length !== 0) {
      assert(versionAndLastActive.length === 1);
      const {version, lastActive} = versionAndLastActive[0];
      cvr.version = versionFromString(version);
      cvr.lastActive = {epochMillis: lastActive.getTime()};
    } else {
      // This is the first time we see this CVR.
      const change: InstancesRow = {
        clientGroupID: id,
        version: versionString(cvr.version),
        lastActive: new Date(0),
      };
      this.#writes.add({
        stats: {instances: 1},
        write: tx => tx`INSERT INTO cvr.instances ${tx(change)}`,
      });
    }

    for (const row of clientsRows) {
      const version = versionFromString(row.patchVersion);
      cvr.clients[row.clientID] = {
        id: row.clientID,
        patchVersion: version,
        desiredQueryIDs: [],
      };
    }

    for (const row of queryRows) {
      const query = asQuery(row);
      cvr.queries[row.queryHash] = query;
    }

    for (const row of desiresRows) {
      const client = cvr.clients[row.clientID];
      assert(client, 'Client not found');
      client.desiredQueryIDs.push(row.queryHash);

      const query = cvr.queries[row.queryHash];
      if (query && !query.internal) {
        query.desiredBy[row.clientID] = versionFromString(row.patchVersion);
      }
    }
    this.#lc.debug?.(
      `loaded CVR @${versionString(cvr.version)} (${Date.now() - start} ms)`,
    );

    return cvr;
  }

  getRowRecords(): Promise<ReadonlyMap<RowID, RowRecord>> {
    return this.#rowCache.getRowRecords();
  }

  getPendingRowRecord(id: RowID): RowRecord | undefined {
    return this.#pendingRowRecordPuts.get(id);
  }

  putRowRecord(row: RowRecord): void {
    this.#pendingRowRecordPuts.set(row.id, row);
  }

  putInstance(version: CVRVersion, lastActive: {epochMillis: number}): void {
    const change: InstancesRow = {
      clientGroupID: this.#id,
      version: versionString(version),
      lastActive: new Date(lastActive.epochMillis),
    };
    this.#writes.add({
      stats: {instances: 1},
      write: tx =>
        tx`INSERT INTO cvr.instances ${tx(
          change,
        )} ON CONFLICT ("clientGroupID") DO UPDATE SET ${tx(change)}`,
    });
  }

  markQueryAsDeleted(version: CVRVersion, queryPatch: QueryPatch): void {
    this.#writes.add({
      stats: {queries: 1},
      write: tx => tx`UPDATE cvr.queries SET ${tx({
        patchVersion: versionString(version),
        deleted: true,
        transformationHash: null,
        transformationVersion: null,
      })}
      WHERE "clientGroupID" = ${this.#id} AND "queryHash" = ${queryPatch.id}`,
    });
  }

  putQuery(query: QueryRecord): void {
    const maybeVersionString = (v: CVRVersion | undefined) =>
      v ? versionString(v) : null;

    const change: QueriesRow = query.internal
      ? {
          clientGroupID: this.#id,
          queryHash: query.id,
          clientAST: query.ast,
          patchVersion: null,
          transformationHash: query.transformationHash ?? null,
          transformationVersion: maybeVersionString(
            query.transformationVersion,
          ),
          internal: true,
          deleted: false, // put vs del "got" query
        }
      : {
          clientGroupID: this.#id,
          queryHash: query.id,
          clientAST: query.ast,
          patchVersion: maybeVersionString(query.patchVersion),
          transformationHash: query.transformationHash ?? null,
          transformationVersion: maybeVersionString(
            query.transformationVersion,
          ),
          internal: null,
          deleted: false, // put vs del "got" query
        };
    this.#writes.add({
      stats: {queries: 1},
      write: tx => tx`INSERT INTO cvr.queries ${tx(change)}
      ON CONFLICT ("clientGroupID", "queryHash")
      DO UPDATE SET ${tx(change)}`,
    });
  }

  updateQuery(query: QueryRecord) {
    const maybeVersionString = (v: CVRVersion | undefined) =>
      v ? versionString(v) : null;

    const change: Pick<
      QueriesRow,
      | 'patchVersion'
      | 'transformationHash'
      | 'transformationVersion'
      | 'deleted'
    > = {
      patchVersion: query.internal
        ? null
        : maybeVersionString(query.patchVersion),
      transformationHash: query.transformationHash ?? null,
      transformationVersion: maybeVersionString(query.transformationVersion),
      deleted: false,
    };

    this.#writes.add({
      stats: {queries: 1},
      write: tx => tx`UPDATE cvr.queries SET ${tx(change)}
      WHERE "clientGroupID" = ${this.#id} AND "queryHash" = ${query.id}`,
    });
  }

  updateClientPatchVersion(clientID: string, patchVersion: CVRVersion): void {
    this.#writes.add({
      stats: {clients: 1},
      write: tx => tx`UPDATE cvr.clients
      SET "patchVersion" = ${versionString(patchVersion)}
      WHERE "clientGroupID" = ${this.#id} AND "clientID" = ${clientID}`,
    });
  }

  insertClient(client: ClientRecord): void {
    const change: ClientsRow = {
      clientGroupID: this.#id,
      clientID: client.id,
      patchVersion: versionString(client.patchVersion),
      // TODO(arv): deleted is never set to true
      deleted: false,
    };

    this.#writes.add({
      stats: {clients: 1},
      write: tx => tx`INSERT INTO cvr.clients ${tx(change)}`,
    });
  }

  insertDesiredQuery(
    newVersion: CVRVersion,
    query: {id: string},
    client: {id: string},
    deleted: boolean,
  ): void {
    const change: DesiresRow = {
      clientGroupID: this.#id,
      clientID: client.id,
      queryHash: query.id,
      patchVersion: versionString(newVersion),
      deleted,
    };
    this.#writes.add({
      stats: {desires: 1},
      write: tx => tx`
      INSERT INTO cvr.desires ${tx(change)}
        ON CONFLICT ("clientGroupID", "clientID", "queryHash")
        DO UPDATE SET ${tx(change)}
      `,
    });
  }

  delDesiredQuery(
    oldPutVersion: CVRVersion,
    query: {id: string},
    client: {id: string},
  ): void {
    this.#writes.add({
      stats: {desires: 1},
      write: tx =>
        tx`DELETE FROM cvr.desires WHERE "clientGroupID" = ${
          this.#id
        } AND "clientID" = ${client.id} AND "queryHash" = ${
          query.id
        } AND "patchVersion" = ${versionString(oldPutVersion)}`,
    });
  }

  async *catchupRowPatches(
    lc: LogContext,
    afterVersion: NullableCVRVersion,
    upToCVR: CVRSnapshot,
    excludeQueryHashes: string[] = [],
  ): AsyncGenerator<RowsRow[], void, undefined> {
    if (cmpVersions(afterVersion, upToCVR.version) >= 0) {
      lc.debug?.('all clients up to date. no config catchup.');
      return;
    }

    const startMs = Date.now();
    const sql = this.#db;
    const start = afterVersion ? versionString(afterVersion) : '';
    const end = versionString(upToCVR.version);
    lc.debug?.(`catching up clients from ${start}`);

    const query =
      excludeQueryHashes.length === 0
        ? sql<RowsRow[]>`SELECT * FROM cvr.rows
        WHERE "clientGroupID" = ${this.#id}
          AND "patchVersion" > ${start}
          AND "patchVersion" <= ${end}`
        : // Exclude rows that were already sent as part of query hydration.
          sql<RowsRow[]>`SELECT * FROM cvr.rows
        WHERE "clientGroupID" = ${this.#id}
          AND "patchVersion" > ${start}
          AND "patchVersion" <= ${end}
          AND ("refCounts" IS NULL OR NOT "refCounts" ?| ${excludeQueryHashes})`;

    yield* query.cursor(10000);

    lc.debug?.(`finished row catchup (${Date.now() - startMs} ms)`);
  }

  async catchupConfigPatches(
    lc: LogContext,
    afterVersion: NullableCVRVersion,
    upToCVR: CVRSnapshot,
  ): Promise<PatchToVersion[]> {
    if (cmpVersions(afterVersion, upToCVR.version) >= 0) {
      lc.debug?.('all clients up to date. no config catchup.');
      return [];
    }

    const startMs = Date.now();
    const sql = this.#db;
    const start = afterVersion ? versionString(afterVersion) : '';
    const end = versionString(upToCVR.version);

    const [allDesires, clientRows, queryRows] = await Promise.all([
      sql<DesiresRow[]>`SELECT * FROM cvr.desires
       WHERE "clientGroupID" = ${this.#id}
        AND "patchVersion" > ${start}
        AND "patchVersion" <= ${end}`,
      sql<ClientsRow[]>`SELECT * FROM cvr.clients
       WHERE "clientGroupID" = ${this.#id}
        AND "patchVersion" > ${start}
        AND "patchVersion" <= ${end}`,
      sql<
        Pick<QueriesRow, 'deleted' | 'queryHash' | 'patchVersion'>[]
      >`SELECT deleted, "queryHash", "patchVersion" FROM cvr.queries
      WHERE "clientGroupID" = ${this.#id}
        AND "patchVersion" > ${start}
        AND "patchVersion" <= ${end}`,
    ]);

    const ast = (id: string) => must(upToCVR.queries[id]).ast;

    const patches: PatchToVersion[] = [];
    for (const row of queryRows) {
      const {queryHash: id} = row;
      const patch: Patch = row.deleted
        ? {type: 'query', op: 'del', id}
        : {type: 'query', op: 'put', id, ast: ast(id)};
      const v = row.patchVersion;
      assert(v);
      patches.push({patch, toVersion: versionFromString(v)});
    }
    for (const row of clientRows) {
      const patch: Patch = {
        type: 'client',
        op: row.deleted ? 'del' : 'put',
        id: row.clientID,
      };
      patches.push({patch, toVersion: versionFromString(row.patchVersion)});
    }
    for (const row of allDesires) {
      const {clientID, queryHash: id} = row;
      const patch: Patch = row.deleted
        ? {type: 'query', op: 'del', id, clientID}
        : {type: 'query', op: 'put', id, clientID, ast: ast(id)};
      patches.push({patch, toVersion: versionFromString(row.patchVersion)});
    }

    lc.debug?.(`${patches.length} config patches (${Date.now() - startMs} ms)`);
    return patches;
  }

  async #abortIfNotVersion(
    tx: PostgresTransaction,
    expectedCurrentVersion: CVRVersion,
  ): Promise<void> {
    const expected = versionString(expectedCurrentVersion);
    const result = await tx<
      {version: string}[]
    >`SELECT version FROM cvr.instances WHERE "clientGroupID" = ${
      this.#id
    }`.execute(); // Note: execute() immediately to send the query before others.
    const currVersion =
      result.length === 0 ? versionToLexi(0) : result[0].version;
    if (currVersion !== expected) {
      throw new ConcurrentModificationException(expected, currVersion);
    }
  }

  async #flush(expectedCurrentVersion: CVRVersion): Promise<CVRFlushStats> {
    const stats: CVRFlushStats = {
      instances: 0,
      queries: 0,
      desires: 0,
      clients: 0,
      rows: 0,
      statements: 0,
    };
    const existingRowRecords = await this.getRowRecords();
    const rowRecordsToFlush = [...this.#pendingRowRecordPuts.values()].filter(
      row => {
        const existing = existingRowRecords.get(row.id);
        return (
          (existing !== undefined || row.refCounts !== null) &&
          !deepEqual(
            row as ReadonlyJSONValue,
            existing as ReadonlyJSONValue | undefined,
          )
        );
      },
    );
    stats.rows = rowRecordsToFlush.length;
    await this.#db.begin(tx => {
      const pipelined: Promise<unknown>[] = [
        // Test-and-set the version to guard against concurrent writes.
        // TODO: Add homing logic.
        this.#abortIfNotVersion(tx, expectedCurrentVersion),
      ];

      if (this.#pendingRowRecordPuts.size > 0) {
        const rowRecordRows = rowRecordsToFlush.map(r =>
          rowRecordToRowsRow(this.#id, r),
        );
        let i = 0;
        while (i < rowRecordRows.length) {
          pipelined.push(
            tx`INSERT INTO cvr.rows ${tx(
              rowRecordRows.slice(i, i + ROW_RECORD_UPSERT_BATCH_SIZE),
            )} 
            ON CONFLICT ("clientGroupID", "schema", "table", "rowKey")
            DO UPDATE SET "rowVersion" = excluded."rowVersion",
              "patchVersion" = excluded."patchVersion",
              "refCounts" = excluded."refCounts"`.execute(),
          );
          i += ROW_RECORD_UPSERT_BATCH_SIZE;
          stats.statements++;
        }
      }
      for (const write of this.#writes) {
        stats.instances += write.stats.instances ?? 0;
        stats.queries += write.stats.queries ?? 0;
        stats.desires += write.stats.desires ?? 0;
        stats.clients += write.stats.clients ?? 0;

        pipelined.push(write.write(tx).execute());
        stats.statements++;
      }

      // Make sure Errors thrown by pipelined statements
      // are propagated up the stack.
      return Promise.all(pipelined);
    });
    await this.#rowCache.flush(rowRecordsToFlush);
    return stats;
  }

  async flush(expectedCurrentVersion: CVRVersion): Promise<CVRFlushStats> {
    try {
      return await this.#flush(expectedCurrentVersion);
    } catch (e) {
      // Clear cached state if an error (e.g. ConcurrentModificationException) is encountered.
      this.#rowCache.clear();
      throw e;
    } finally {
      this.#writes.clear();
      this.#pendingRowRecordPuts.clear();
    }
  }
}

// Max number of parameters for our sqlite build is 65534.
// Each row record has 7 parameters (1 per column).
// 65534 / 7 = 9362
const ROW_RECORD_UPSERT_BATCH_SIZE = 9_360;

export class ConcurrentModificationException extends Error {
  readonly name = 'ConcurrentModificationException';

  constructor(expectedVersion: string, actualVersion: string) {
    super(
      `CVR has been concurrently modified. Expected ${expectedVersion}, got ${actualVersion}`,
    );
  }
}
