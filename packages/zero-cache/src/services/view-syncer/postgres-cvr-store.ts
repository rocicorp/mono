import type {LogContext} from '@rocicorp/logger';
import pg from 'pg';
import {assert} from 'shared/src/asserts.js';
import {CustomKeyMap} from 'shared/src/custom-key-map.js';
import {CustomKeySet} from 'shared/src/custom-key-set.js';
import {lookupRowsWithKeys} from 'zero-cache/src/db/queries.js';
import type {JSONValue} from 'zero-cache/src/types/bigint-json.js';
import {versionToLexi} from 'zero-cache/src/types/lexi-version.js';
import type {PostgresDB, PostgresTransaction} from 'zero-cache/src/types/pg.js';
import {rowIDHash} from 'zero-cache/src/types/row-key.js';
import {astSchema} from 'zero-protocol';
import type {CVRStore} from './cvr-store.js';
import type {CVR} from './cvr.js';
import {
  RowsRow,
  rowRecordToRowsRow,
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
  isInternalQueryRecord,
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
  const queryRecord: QueryRecord = row.internal
    ? ({
        id: row.queryHash,
        ast,
        transformationHash: row.transformationHash ?? undefined,
        transformationVersion: row.transformationVersion
          ? versionFromString(row.transformationVersion)
          : undefined,
        internal: true,
      } satisfies InternalQueryRecord)
    : ({
        id: row.queryHash,
        ast,
        patchVersion: row.patchVersion
          ? versionFromString(row.patchVersion)
          : undefined,
        desiredBy: {},
        transformationHash: row.transformationHash ?? undefined,
        transformationVersion: row.transformationVersion
          ? versionFromString(row.transformationVersion)
          : undefined,
      } satisfies ClientQueryRecord);

  return queryRecord;
}

export class PostgresCVRStore implements CVRStore {
  readonly #lc: LogContext;
  readonly #id: string;
  readonly #db: PostgresDB;

  readonly #writes: Set<(tx: PostgresTransaction) => Promise<unknown>> =
    new Set();
  readonly #pendingRowPatchDeletes: Map<CVRVersion, Set<RowID>> =
    new CustomKeyMap(versionString);
  readonly #pendingQueryPatchDeletes: Map<CVRVersion, Set<string>> =
    new CustomKeyMap(versionString);
  readonly #pendingRowRecordPuts = new CustomKeyMap<
    RowID,
    [RowRecord, (tx: PostgresTransaction) => Promise<unknown>]
  >(rowIDHash);

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

    const [versionAndLastActive, clientsRows, queryRows, desiresRow] =
      await this.#db.begin(async tx => {
        const versionAndLastActive = await tx<
          Pick<InstancesRow, 'version' | 'lastActive'>[]
        >`SELECT version, "lastActive" FROM cvr.instances WHERE "clientGroupID" = ${id} AND NOT deleted`;

        const clientRows = await tx<
          Pick<ClientsRow, 'clientID' | 'patchVersion' | 'deleted'>[]
        >`SELECT "clientID", "patchVersion", deleted FROM cvr.clients WHERE "clientGroupID" = ${id}`;

        const queryRows = await tx<
          QueryRow[]
        >`SELECT * FROM cvr.queries WHERE "clientGroupID" = ${id} AND NOT (deleted = true)`;

        const desiresRows = await tx<
          DesiresRow[]
        >`SELECT * FROM cvr.desires WHERE "clientGroupID" = ${id}`;

        return [
          versionAndLastActive,
          clientRows,
          queryRows,
          desiresRows,
        ] as const;
      });

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
        deleted: false,
      };
      this.#writes.add(tx => tx`INSERT INTO cvr.instances ${tx(change)}`);
    }

    for (const value of clientsRows) {
      const version = versionFromString(value.patchVersion);
      cvr.clients[value.clientID] = {
        id: value.clientID,
        patchVersion: version,
        desiredQueryIDs: [],
      };
    }

    for (const row of queryRows) {
      const query = asQuery(row);
      cvr.queries[row.queryHash] = query;
    }

    for (const row of desiresRow) {
      const client = cvr.clients[row.clientID];
      if (!client) {
        throw new Error('Client not found');
      }
      if (!row.deleted) {
        client.desiredQueryIDs.push(row.queryHash);
      }

      const query = cvr.queries[row.queryHash];
      if (!query) {
        throw new Error('Query not found: ' + row.queryHash);
      }
      if (!isInternalQueryRecord(query) && !row.deleted) {
        query.desiredBy[row.clientID] = versionFromString(row.patchVersion);
      }
    }

    this.#lc.debug?.(`loaded CVR (${Date.now() - start} ms)`);

    return cvr;
  }

  cancelPendingRowPatch(_version: CVRVersion, _id: RowID): void {
    // No op. cancelPendingRowRecord takes care of this
  }

  cancelPendingRowRecord(id: RowID): void {
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

  isQueryPatchPendingDelete(
    patchRecord: {id: string},
    version: CVRVersion,
  ): boolean {
    const set = this.#pendingQueryPatchDeletes.get(version);
    return set !== undefined && set.has(patchRecord.id);
  }

  isRowPatchPendingDelete(rowPatch: RowPatch, version: CVRVersion): boolean {
    const rowIDSet = this.#pendingRowPatchDeletes.get(version);
    return rowIDSet !== undefined && rowIDSet.has(rowPatch.id);
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
      let oldSet = this.#pendingRowPatchDeletes.get(oldRowPatchVersionToDelete);
      if (!oldSet) {
        oldSet = new CustomKeySet<RowID>(rowIDHash);
        this.#pendingRowPatchDeletes.set(oldRowPatchVersionToDelete, oldSet);
      }
      oldSet.add(row.id);

      // No need to delete the old row because it will be replaced by the new one.
    }

    // Clear any pending deletes for this row and patchVersion.
    const set = this.#pendingRowPatchDeletes.get(row.patchVersion);
    if (set) {
      set.delete(row.id);
    }

    // If we are writing the same again then delete the old write.
    this.cancelPendingRowRecord(row.id);

    const change = rowRecordToRowsRow(this.#id, row);
    const w = (tx: PostgresTransaction) => tx`INSERT INTO cvr.rows ${tx(change)}
    ON CONFLICT ("clientGroupID", "schema", "table", "rowKey")
    DO UPDATE SET ${tx(change)}`;
    this.#writes.add(w);

    this.#pendingRowRecordPuts.set(row.id, [row, w]);
  }

  putLastActive(lastActive: {epochMillis: number}): void {
    const d = new Date(lastActive.epochMillis);
    this.#writes.add(
      tx =>
        tx`UPDATE cvr.instances SET ${tx({
          lastActive: d,
        })} WHERE "clientGroupID" = ${this.#id}`,
    );
  }

  putLastActiveIndex(_cvrID: string, _newMillis: number): void {
    // TODO(arv): Not used AFAICT.
    // But even if we wanted this in Postgres we use an index on the cvr.instances tables instead.
  }

  delLastActiveIndex(_cvrID: string, _oldMillis: number): void {
    // TODO(arv): Not used AFAICT.
    // But even if we wanted this in Postgres we use an index on the cvr.instances tables instead.
  }

  numPendingWrites(): number {
    return this.#writes.size;
  }

  putQueryPatch(
    version: CVRVersion,
    queryPatch: QueryPatch,
    oldQueryPatchVersionToDelete: CVRVersion | undefined,
  ): void {
    {
      const set = this.#pendingQueryPatchDeletes.get(version);
      if (set) {
        set.delete(queryPatch.id);
      }
    }
    if (oldQueryPatchVersionToDelete) {
      let set = this.#pendingQueryPatchDeletes.get(
        oldQueryPatchVersionToDelete,
      );
      if (!set) {
        set = new Set();
        this.#pendingQueryPatchDeletes.set(oldQueryPatchVersionToDelete, set);
      }
      set.add(queryPatch.id);
    }

    this.#writes.add(
      tx => tx`UPDATE cvr.queries SET ${tx({
        patchVersion: versionString(version),
        deleted: queryPatch.op === 'del',
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
          transformationVersion: null,
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

  delQuery(_query: {id: string}): void {
    // No op here. queries and query patches are not two distinct entities in the Postgres schema.
  }

  putClient(client: ClientRecord): void {
    const change: ClientsRow = {
      clientGroupID: this.#id,
      clientID: client.id,
      patchVersion: versionString(client.patchVersion),
      deleted: false,
    };
    this.#writes.add(
      tx => tx`
      INSERT INTO cvr.clients ${tx(change)}
      ON CONFLICT ("clientGroupID", "clientID")
      DO UPDATE SET ${tx(change)}`,
    );
  }

  putClientPatch(
    newVersion: CVRVersion,
    client: ClientRecord,
    clientPatch: ClientPatch,
  ): void {
    const change: ClientsRow = {
      clientGroupID: this.#id,
      clientID: client.id,
      patchVersion: versionString(newVersion),
      deleted: clientPatch.op === 'del',
    };
    this.#writes.add(
      tx => tx`
      INSERT INTO cvr.clients ${tx(change)}
      ON CONFLICT ("clientGroupID", "clientID")
      DO UPDATE SET ${tx(change)}`,
    );
  }

  putDesiredQueryPatch(
    newVersion: CVRVersion,
    query: {id: string},
    client: {id: string},
    queryPatch: QueryPatch,
  ): void {
    assert(queryPatch.clientID === client.id);
    assert(query.id === queryPatch.id);
    const change: DesiresRow = {
      clientGroupID: this.#id,
      clientID: client.id,
      queryHash: query.id,
      patchVersion: versionString(newVersion),
      deleted: queryPatch.op === 'del',
    };
    this.#writes.add(tx => tx`INSERT INTO cvr.desires ${tx(change)}`);
  }

  delDesiredQueryPatch(
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

  putVersion(version: CVRVersion): void {
    const change: Pick<InstancesRow, 'version'> = {
      version: versionString(version),
    };
    this.#writes.add(
      tx =>
        tx`UPDATE cvr.instances SET ${tx(change)} WHERE "clientGroupID" = ${
          this.#id
        }`,
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
            // TODO(arv): Update schema to match design doc
            columns: Object.keys(row.queriedColumns),
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
    const queryRows = await sql<
      Pick<QueriesRow, 'deleted' | 'queryHash' | 'patchVersion'>[]
    >`SELECT deleted, "queryHash", "patchVersion" FROM cvr.queries
      WHERE
        cvr.queries."clientGroupID" = ${this.#id}
        AND cvr.queries."patchVersion" >= ${version}
        AND cvr.queries."transformationVersion" IS NULL`;
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

    return rv;
  }

  async *allRowRecords(): AsyncIterable<RowRecord> {
    const sql = this.#db;
    const rows = await sql<
      RowsRow[]
    >`SELECT * FROM cvr.rows WHERE "clientGroupID" = ${this.#id}`;

    for (const row of rows) {
      yield rowsRowToRowRecord(row);
    }
  }

  async flush(): Promise<void> {
    await this.#db.begin(tx => Array.from(this.#writes, write => write(tx)));

    this.#pendingRowPatchDeletes.clear();
    this.#pendingQueryPatchDeletes.clear();
  }
}
