import type {LogContext} from '@rocicorp/logger';
import {CustomKeyMap} from 'shared/src/custom-key-map.js';
import {versionToLexi} from 'zero-cache/src/types/lexi-version.js';
import {rowIDHash} from 'zero-cache/src/types/row-key.js';
import type {DurableStorage} from '../../storage/durable-storage.js';
import {DelOp, PutOp, WriteCache} from '../../storage/write-cache.js';
import type {CVRStore} from './cvr-store.js';
import type {CVR} from './cvr.js';
import {CVRPaths, lastActiveIndex} from './schema/paths.js';
import {
  ClientPatch,
  LastActive,
  MetadataPatch,
  QueryPatch,
  RowID,
  RowPatch,
  RowRecord,
  metaRecordSchema,
  metadataPatchSchema,
  rowPatchSchema,
  rowRecordSchema,
  type CVRVersion,
  type ClientRecord,
  type QueryRecord,
} from './schema/types.js';

export class DurableObjectCVRStore implements CVRStore {
  readonly #lc: LogContext;
  readonly #id: string;
  readonly #storage: DurableStorage;
  readonly #writes: WriteCache;
  readonly #paths: CVRPaths;

  constructor(lc: LogContext, storage: DurableStorage, cvrID: string) {
    this.#lc = lc;
    this.#id = cvrID;
    this.#storage = storage;
    this.#writes = new WriteCache(storage);
    this.#paths = new CVRPaths(cvrID);
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

    const paths = new CVRPaths(id);
    const metaRecords = await this.#storage.list(
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

    this.#lc.debug?.(
      `loaded CVR (${Date.now() - start} ms), ${metaRecords.size} meta entries`,
    );

    return cvr;
  }

  cancelPendingRowPatch(patchVersion: CVRVersion, id: RowID): void {
    this.#writes.cancelPending(this.#paths.rowPatch(patchVersion, id));
  }

  cancelPendingRowRecord(id: RowID): void {
    this.#writes.cancelPending(this.#paths.row(id));
  }

  getPendingRowRecord(id: RowID): PutOp | DelOp | undefined {
    return this.#writes.getPending(this.#paths.row(id));
  }

  isPatchRecordPendingDelete(
    patchRecord: MetadataPatch,
    version: CVRVersion,
  ): boolean {
    const path = this.#paths.queryPatch(version, patchRecord);
    return this.#writes.isPendingDelete(path);
  }

  isRowPatchPendingDelete(rowPatch: RowPatch, version: CVRVersion): boolean {
    const path = this.#paths.rowPatch(version, rowPatch.id);
    return this.#writes.isPendingDelete(path);
  }

  async getMultipleRowEntries(
    rowIDs: Iterable<RowID>,
  ): Promise<Map<RowID, RowRecord>> {
    const pathsMapping = new Map<string, RowID>();
    const keys: string[] = [];
    for (const rowID of rowIDs) {
      const path = this.#paths.row(rowID);
      pathsMapping.set(path, rowID);
      keys.push(path);
    }

    const entryMap = await this.#writes.getEntries(keys, rowRecordSchema);
    const rv = new CustomKeyMap<RowID, RowRecord>(rowIDHash);
    for (const [path, record] of entryMap) {
      rv.set(pathsMapping.get(path)!, record);
    }
    return rv;
  }

  putRowRecord(rowRecord: RowRecord): void {
    void this.#writes.put(this.#paths.row(rowRecord.id), rowRecord);
  }

  delRowPatch(rowPatch: {patchVersion: CVRVersion; id: RowID}): void {
    void this.#writes.del(
      this.#paths.rowPatch(rowPatch.patchVersion, rowPatch.id),
    );
  }

  putRowPatch(patchVersion: CVRVersion, rowPatch: RowPatch): void {
    void this.#writes.put(
      this.#paths.rowPatch(patchVersion, rowPatch.id),
      rowPatch,
    );
  }

  putLastActive(lastActive: {epochMillis: number}): void {
    void this.#writes.put(this.#paths.lastActive(), lastActive);
  }

  putLastActiveIndex(cvrID: string, newMillis: number): void {
    void this.#writes.put(lastActiveIndex.entry(cvrID, newMillis), {
      id: cvrID,
    });
  }

  delLastActiveIndex(cvrID: string, oldMillis: number): void {
    void this.#writes.del(lastActiveIndex.entry(cvrID, oldMillis));
  }

  numPendingWrites(): number {
    return this.#writes.pendingSize();
  }

  delQueryPatch(patchVersion: CVRVersion, query: {id: string}): void {
    void this.#writes.del(this.#paths.queryPatch(patchVersion, query));
  }

  delQuery(query: {id: string}): void {
    void this.#writes.del(this.#paths.query(query));
  }

  putClientPatch(
    newVersion: CVRVersion,
    client: ClientRecord,
    clientPatch: ClientPatch,
  ): void {
    void this.#writes.put(
      this.#paths.clientPatch(newVersion, client),
      clientPatch,
    );
  }

  async catchupRowPatches(
    startingVersion: CVRVersion,
  ): Promise<[RowPatch, CVRVersion][]> {
    const doEntries = await this.#storage.list(
      {
        prefix: this.#paths.rowPatchPrefix(),
        start: {key: this.#paths.rowPatchVersionPrefix(startingVersion)},
      },
      rowPatchSchema,
    );

    return Array.from(doEntries, entry => [
      entry[1],
      this.#paths.versionFromPatchPath(entry[0]),
    ]);
  }

  async catchupConfigPatches(
    startingVersion: CVRVersion,
  ): Promise<[MetadataPatch, CVRVersion][]> {
    const doEntries = await this.#storage.list(
      {
        prefix: this.#paths.metadataPatchPrefix(),
        start: {key: this.#paths.metadataPatchVersionPrefix(startingVersion)},
      },
      metadataPatchSchema,
    );

    return Array.from(doEntries, entry => [
      entry[1],
      this.#paths.versionFromPatchPath(entry[0]),
    ]);
  }

  async *allRowRecords(): AsyncIterable<RowRecord> {
    for await (const entry of this.#storage.batchScan(
      {prefix: this.#paths.rowPrefix()},
      rowRecordSchema,
      2000,
    )) {
      yield* entry.values();
    }
  }

  putVersion(version: CVRVersion): void {
    void this.#writes.put(this.#paths.version(), version);
  }

  delDesiredQueryPatch(
    oldPutVersion: CVRVersion,
    query: QueryRecord,
    client: ClientRecord,
  ): void {
    void this.#writes.del(
      this.#paths.desiredQueryPatch(oldPutVersion, query, client),
    );
  }

  putDesiredQueryPatch(
    newVersion: CVRVersion,
    query: QueryRecord,
    client: ClientRecord,
    queryPath: QueryPatch,
  ): void {
    void this.#writes.put(
      this.#paths.desiredQueryPatch(newVersion, query, client),
      queryPath,
    );
  }

  putQuery(query: QueryRecord): void {
    void this.#writes.put(this.#paths.query(query), query);
  }

  async flush(): Promise<void> {
    await this.#writes.flush(); // Calls put() and del() with a final `await`
    await this.#storage.flush(); // DurableObjectStorage.sync();
  }

  putClient(client: ClientRecord): void {
    void this.#writes.put(this.#paths.client(client), client);
  }

  putQueryPatch(
    transformationVersion: CVRVersion,
    queryPatch: QueryPatch,
  ): void {
    void this.#writes.put(
      this.#paths.queryPatch(transformationVersion, queryPatch),
      queryPatch,
    );
  }
}
