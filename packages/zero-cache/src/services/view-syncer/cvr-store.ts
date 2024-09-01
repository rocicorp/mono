import type {LogContext} from '@rocicorp/logger';
import {MaybeRow, PendingQuery} from 'postgres';
import {assert} from 'shared/src/asserts.js';
import {CustomKeyMap} from 'shared/src/custom-key-map.js';
import {CustomKeySet} from 'shared/src/custom-key-set.js';
import {astSchema} from 'zero-protocol';
import type {JSONValue} from '../../types/bigint-json.js';
import {versionToLexi} from '../../types/lexi-version.js';
import type {PostgresDB, PostgresTransaction} from '../../types/pg.js';
import {rowIDHash} from '../../types/row-key.js';
import type {CVR} from './cvr.js';
import {
  rowRecordToRowsRow,
  RowsRow,
  rowsRowToRowRecord,
  type ClientsRow,
  type DesiresRow,
  type InstancesRow,
  type QueriesRow,
} from './schema/cvr.js';
import {
  ClientPatch,
  ClientQueryRecord,
  ClientRecord,
  DelRowPatch,
  InternalQueryRecord,
  MetadataPatch,
  PutRowPatch,
  QueryPatch,
  versionFromString,
  versionString,
  type CVRVersion,
  type QueryRecord,
  type RowID,
  type RowPatch,
  type RowRecord,
} from './schema/types.js';

type NotNull<T> = T extends null ? never : T;

class RowRecordCache {
  #cache: CustomKeyMap<RowID, RowRecord> | undefined;
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
    this.#cache = cache;
    return this.#cache;
  }

  getRowRecords(): Promise<ReadonlyMap<RowID, RowRecord>> {
    return this.#ensureLoaded();
  }

  async flush(rowRecords: IterableIterator<RowRecord>) {
    const cache = await this.#ensureLoaded();
    for (const row of rowRecords) {
      if (row.refCounts === null) {
        cache.delete(row.id);
      } else {
        cache.set(row.id, row);
      }
    }
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
  readonly #writes: Set<(tx: PostgresTransaction) => PendingQuery<MaybeRow[]>> =
    new Set();
  readonly #pendingQueryVersionDeletes = new CustomKeySet<
    [{id: string}, CVRVersion]
  >(([patchRecord, version]) => patchRecord.id + '-' + versionString(version));
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
      this.#writes.add(tx => tx`INSERT INTO cvr.instances ${tx(change)}`);
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

    this.#lc.debug?.(`loaded CVR (${Date.now() - start} ms)`);

    return cvr;
  }

  cancelPendingRowRecordWrite(id: RowID): void {
    this.#pendingRowRecordPuts.delete(id);
  }

  getPendingRowRecord(id: RowID): RowRecord | undefined {
    return this.#pendingRowRecordPuts.get(id);
  }

  isQueryVersionPendingDelete(
    patchRecord: {id: string},
    version: CVRVersion,
  ): boolean {
    return this.#pendingQueryVersionDeletes.has([patchRecord, version]);
  }

  getRowRecords(): Promise<ReadonlyMap<RowID, RowRecord>> {
    return this.#rowCache.getRowRecords();
  }

  putRowRecord(row: RowRecord): void {
    // If we are writing the same again then delete the old write.
    this.cancelPendingRowRecordWrite(row.id);

    this.#pendingRowRecordPuts.set(row.id, row);
  }

  putInstance(version: CVRVersion, lastActive: {epochMillis: number}): void {
    const change: InstancesRow = {
      clientGroupID: this.#id,
      version: versionString(version),
      lastActive: new Date(lastActive.epochMillis),
    };
    this.#writes.add(
      tx =>
        tx`INSERT INTO cvr.instances ${tx(
          change,
        )} ON CONFLICT ("clientGroupID") DO UPDATE SET ${tx(change)}`,
    );
  }

  numPendingWrites(): number {
    return this.#writes.size + this.#pendingRowRecordPuts.size;
  }

  markQueryAsDeleted(
    version: CVRVersion,
    queryPatch: QueryPatch,
    oldQueryPatchVersionToDelete: CVRVersion | undefined,
  ): void {
    this.#pendingQueryVersionDeletes.delete([queryPatch, version]);

    if (oldQueryPatchVersionToDelete) {
      this.#pendingQueryVersionDeletes.add([
        queryPatch,
        oldQueryPatchVersionToDelete,
      ]);
    }

    this.#writes.add(
      tx => tx`UPDATE cvr.queries SET ${tx({
        patchVersion: versionString(version),
        deleted: true,
      })}
      WHERE "clientGroupID" = ${this.#id} AND "queryHash" = ${queryPatch.id}`,
    );
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
    this.#writes.add(
      tx => tx`INSERT INTO cvr.queries ${tx(change)}
      ON CONFLICT ("clientGroupID", "queryHash")
      DO UPDATE SET ${tx(change)}`,
    );
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
    this.#writes.add(
      tx => tx`UPDATE cvr.queries SET ${tx(change)}
      WHERE "clientGroupID" = ${this.#id} AND "queryHash" = ${query.id}`,
    );
  }

  delQuery(queryHash: string): void {
    this.#writes.add(
      tx =>
        tx`DELETE FROM cvr.queries WHERE "clientGroupID" = ${
          this.#id
        } AND "queryHash" = ${queryHash}`,
    );
  }

  updateClientPatchVersion(clientID: string, patchVersion: CVRVersion): void {
    this.#writes.add(
      tx => tx`UPDATE cvr.clients
      SET "patchVersion" = ${versionString(patchVersion)}
      WHERE "clientGroupID" = ${this.#id} AND "clientID" = ${clientID}`,
    );
  }

  insertClient(client: ClientRecord): void {
    const change: ClientsRow = {
      clientGroupID: this.#id,
      clientID: client.id,
      patchVersion: versionString(client.patchVersion),
      // TODO(arv): deleted is never set to true
      deleted: false,
    };
    this.#writes.add(tx => tx`INSERT INTO cvr.clients ${tx(change)}`);
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
    this.#writes.add(tx => tx`INSERT INTO cvr.desires ${tx(change)}`);
  }

  delDesiredQuery(
    oldPutVersion: CVRVersion,
    query: {id: string},
    client: {id: string},
  ): void {
    this.#writes.add(
      tx =>
        tx`DELETE FROM cvr.desires WHERE "clientGroupID" = ${
          this.#id
        } AND "clientID" = ${client.id} AND "queryHash" = ${
          query.id
        } AND "patchVersion" = ${versionString(oldPutVersion)}`,
    );
  }

  async catchupRowPatches(
    startingVersion: CVRVersion,
  ): Promise<[RowPatch, CVRVersion][]> {
    const sql = this.#db;
    const version = versionString(startingVersion);
    const rows = await sql<
      RowsRow[]
    >`SELECT * FROM cvr.rows WHERE "clientGroupID" = ${
      this.#id
    } AND "patchVersion" >= ${version}`;
    return rows.map(row => {
      const id = {
        schema: row.schema,
        table: row.table,
        rowKey: row.rowKey as Record<string, JSONValue>,
      } as const;
      const rowPatch: RowPatch = row.refCounts
        ? ({
            type: 'row',
            op: 'put',
            id,
            rowVersion: row.rowVersion,
          } satisfies PutRowPatch)
        : ({
            type: 'row',
            op: 'del',
            id,
          } satisfies DelRowPatch);
      const version: CVRVersion = versionFromString(row.patchVersion);
      return [rowPatch, version];
    });
  }

  async catchupConfigPatches(
    startingVersion: CVRVersion,
  ): Promise<[MetadataPatch, CVRVersion][]> {
    const sql = this.#db;
    const version = versionString(startingVersion);

    const [allDesires, clientRows, queryRows] = await Promise.all([
      sql<DesiresRow[]>`SELECT * FROM cvr.desires WHERE "clientGroupID" = ${
        this.#id
      } AND "patchVersion" >= ${version} AND "deleted" IS NOT NULL`,
      sql<ClientsRow[]>`SELECT * FROM cvr.clients WHERE "clientGroupID" = ${
        this.#id
      } AND "patchVersion" >= ${version} AND "deleted" IS NOT NULL`,
      sql<
        Pick<QueriesRow, 'deleted' | 'queryHash' | 'patchVersion'>[]
      >`SELECT deleted, "queryHash", "patchVersion" FROM cvr.queries
      WHERE "clientGroupID" = ${
        this.#id
      } AND "patchVersion" >= ${version} AND "deleted" IS NOT NULL`,
    ]);

    const rv: [MetadataPatch, CVRVersion][] = [];
    for (const row of queryRows) {
      const queryPatch: QueryPatch = {
        type: 'query',
        op: row.deleted ? 'del' : 'put',
        id: row.queryHash,
      };
      const v = row.patchVersion;
      assert(v);
      rv.push([queryPatch, versionFromString(v)]);
    }
    for (const row of clientRows) {
      const clientPatch: ClientPatch = {
        type: 'client',
        op: row.deleted ? 'del' : 'put',
        id: row.clientID,
      };
      rv.push([clientPatch, versionFromString(row.patchVersion)]);
    }
    for (const row of allDesires) {
      const queryPatch: QueryPatch = {
        type: 'query',
        op: row.deleted ? 'del' : 'put',
        id: row.queryHash,
        clientID: row.clientID,
      };
      rv.push([queryPatch, versionFromString(row.patchVersion)]);
    }

    return rv;
  }

  async flush(): Promise<number> {
    const statements = await this.#db.begin(tx => {
      let statements = 0;
      if (this.#pendingRowRecordPuts.size > 0) {
        const rowRecordRows = [...this.#pendingRowRecordPuts.values()].map(r =>
          rowRecordToRowsRow(this.#id, r),
        );
        let i = 0;
        while (i < rowRecordRows.length) {
          void tx`INSERT INTO cvr.rows ${tx(
            rowRecordRows.slice(i, i + ROW_RECORD_UPSERT_BATCH_SIZE),
          )} 
            ON CONFLICT ("clientGroupID", "schema", "table", "rowKey")
            DO UPDATE SET "rowVersion" = excluded."rowVersion",
              "patchVersion" = excluded."patchVersion",
              "refCounts" = excluded."refCounts"`.execute();
          i += ROW_RECORD_UPSERT_BATCH_SIZE;
          statements++;
        }
      }
      for (const write of this.#writes) {
        void write(tx).execute();
        statements++;
      }
      return statements;
    });
    await this.#rowCache.flush(this.#pendingRowRecordPuts.values());

    this.#writes.clear();
    this.#pendingQueryVersionDeletes.clear();
    this.#pendingRowRecordPuts.clear();
    return statements;
  }
}

// Max number of parameters for our sqlite build is 65534.
// Each row record has 7 parameters (1 per column).
// 65534 / 7 = 9362
const ROW_RECORD_UPSERT_BATCH_SIZE = 9_360;
