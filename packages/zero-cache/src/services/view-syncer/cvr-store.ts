import type {LogContext} from '@rocicorp/logger';
import pg from 'pg';
import {assert} from 'shared/src/asserts.js';
import {CustomKeyMap} from 'shared/src/custom-key-map.js';
import {CustomKeySet} from 'shared/src/custom-key-set.js';
import {lookupRowsWithKeys} from 'zero-cache/src/db/queries.js';
import type {JSONValue} from 'zero-cache/src/types/bigint-json.js';
import type {PostgresDB, PostgresTransaction} from 'zero-cache/src/types/pg.js';
import {rowIDHash} from 'zero-cache/src/types/row-key.js';
import {astSchema} from 'zero-protocol';
import {versionToLexi} from 'zqlite-zero-cache-shared/src/lexi-version.js';
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
  getAllColumnsSorted,
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

const {builtins} = pg.types;

type NotNull<T> = T extends null ? never : T;

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
  readonly #writes: Set<(tx: PostgresTransaction) => Promise<unknown>> =
    new Set();
  readonly #pendingQueryVersionDeletes = new CustomKeySet<
    [{id: string}, CVRVersion]
  >(([patchRecord, version]) => patchRecord.id + '-' + versionString(version));
  readonly #pendingRowRecordPuts = new CustomKeyMap<
    RowID,
    [RowRecord, (tx: PostgresTransaction) => Promise<unknown>]
  >(rowIDHash);
  readonly #pendingRowVersionDeletes = new CustomKeySet<[RowID, CVRVersion]>(
    ([id, version]) => rowIDHash(id) + '-' + versionString(version),
  );

  constructor(lc: LogContext, db: PostgresDB, cvrID: string) {
    this.#lc = lc;
    this.#db = db;
    this.#id = cvrID;
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
    const pair = this.#pendingRowRecordPuts.get(id);
    if (!pair) {
      return;
    }
    this.#pendingRowRecordPuts.delete(id);
    const w = pair[1];
    this.#writes.delete(w);
  }

  getPendingRowRecord(id: RowID): RowRecord | undefined {
    const pair = this.#pendingRowRecordPuts.get(id);
    if (!pair) {
      return undefined;
    }
    return pair[0];
  }

  isQueryVersionPendingDelete(
    patchRecord: {id: string},
    version: CVRVersion,
  ): boolean {
    return this.#pendingQueryVersionDeletes.has([patchRecord, version]);
  }

  isRowVersionPendingDelete(rowID: RowID, version: CVRVersion): boolean {
    return this.#pendingRowVersionDeletes.has([rowID, version]);
  }

  async getMultipleRowEntries(
    rowIDs: Iterable<RowID>,
  ): Promise<Map<RowID, RowRecord>> {
    const rows = await lookupRowsWithKeys(
      this.#db,
      'cvr',
      'rows',
      {
        schema: {typeOid: builtins.TEXT},
        table: {typeOid: builtins.TEXT},
        rowKey: {typeOid: builtins.JSONB},
      },
      rowIDs,
    );
    const rv = new CustomKeyMap<RowID, RowRecord>(rowIDHash);
    for (const row of rows) {
      rv.set(row as RowID, rowsRowToRowRecord(row as RowsRow));
    }
    return rv;
  }

  putRowRecord(
    row: RowRecord,
    oldRowPatchVersionToDelete: CVRVersion | undefined,
  ): void {
    if (oldRowPatchVersionToDelete) {
      // add pending delete for the old patch version.
      this.#pendingRowVersionDeletes.add([row.id, oldRowPatchVersionToDelete]);

      // No need to delete the old row because it will be replaced by the new one.
    }

    // Clear any pending deletes for this row and patchVersion.
    this.#pendingRowVersionDeletes.delete([row.id, row.patchVersion]);

    // If we are writing the same again then delete the old write.
    this.cancelPendingRowRecordWrite(row.id);

    const change = rowRecordToRowsRow(this.#id, row);
    const w = (tx: PostgresTransaction) => tx`INSERT INTO cvr.rows ${tx(change)}
    ON CONFLICT ("clientGroupID", "schema", "table", "rowKey")
    DO UPDATE SET ${tx(change)}`;
    this.#writes.add(w);

    this.#pendingRowRecordPuts.set(row.id, [row, w]);
  }

  putInstance(version: CVRVersion, lastActive: {epochMillis: number}): void {
    const change: InstancesRow = {
      clientGroupID: this.#id,
      version: versionString(version),
      lastActive: new Date(lastActive.epochMillis),
    };
    this.#writes.add(async tx => {
      await tx`INSERT INTO cvr.instances ${tx(
        change,
      )} ON CONFLICT ("clientGroupID") DO UPDATE SET ${tx(change)}`;
    });
  }

  numPendingWrites(): number {
    return this.#writes.size;
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
      const rowPatch: RowPatch = row.queriedColumns
        ? ({
            type: 'row',
            op: 'put',
            id,
            rowVersion: row.rowVersion,
            columns: getAllColumnsSorted(row.queriedColumns),
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

  async *allRowRecords(): AsyncIterable<
    RowRecord & {queriedColumns: JSONValue}
  > {
    for await (const rows of this.#db<
      RowsRow[]
    >`SELECT * FROM cvr.rows WHERE "clientGroupID" = ${
      this.#id
    } AND "queriedColumns" IS NOT NULL`
      // TODO(arv): Arbitrary page size
      .cursor(1000)) {
      for (const row of rows) {
        yield rowsRowToRowRecord(row);
      }
    }
  }

  async flush(): Promise<void> {
    await this.#db.begin(async tx => {
      for (const write of this.#writes) {
        await write(tx);
      }
    });

    this.#writes.clear();
    this.#pendingRowVersionDeletes.clear();
    this.#pendingQueryVersionDeletes.clear();
    this.#pendingRowRecordPuts.clear();
  }
}
