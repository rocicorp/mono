import type {DelOp, PutOp} from '../../storage/write-cache.js';
import type {CVR} from './cvr.js';
import type {
  CVRVersion,
  ClientPatch,
  ClientRecord,
  MetadataPatch,
  QueryPatch,
  QueryRecord,
  RowID,
  RowPatch,
  RowRecord,
} from './schema/types.js';

export interface CVRStore {
  load(): Promise<CVR>;
  cancelPendingRowPatch(patchVersion: CVRVersion, id: RowID): void;
  cancelPendingRowRecord(id: RowID): void;
  getPendingRowRecord(id: RowID): PutOp | DelOp | undefined;
  isPatchRecordPendingDelete(
    patchRecord: MetadataPatch,
    version: CVRVersion,
  ): boolean;
  isRowPatchPendingDelete(rowPatch: RowPatch, version: CVRVersion): boolean;
  getMultipleRowEntries(
    rowIDs: Iterable<RowID>,
  ): Promise<Map<RowID, RowRecord>>;
  putRowRecord(updated: RowRecord): void;
  delRowPatch(rowPatch: {patchVersion: CVRVersion; id: RowID}): void;
  putRowPatch(patchVersion: CVRVersion, rowPatch: RowPatch): void;
  putLastActive(lastActive: {epochMillis: number}): void;
  putLastActiveIndex(cvrID: string, newMillis: number): void;
  delLastActiveIndex(cvrID: string, oldMillis: number): void;
  numPendingWrites(): number;
  delQueryPatch(patchVersion: CVRVersion, queryPatchID: {id: string}): void;
  putQueryPatch(
    transformationVersion: CVRVersion,
    queryPatch: QueryPatch,
  ): void;
  putQuery(query: QueryRecord): void;
  delQuery(query: {id: string}): void;
  putClient(client: ClientRecord): void;
  putClientPatch(
    newVersion: CVRVersion,
    client: ClientRecord,
    clientPatch: ClientPatch,
  ): void;
  putDesiredQueryPatch(
    newVersion: CVRVersion,
    query: QueryRecord,
    client: ClientRecord,
    queryPath: QueryPatch,
  ): void;
  delDesiredQueryPatch(
    oldPutVersion: CVRVersion,
    query: QueryRecord,
    client: ClientRecord,
  ): void;
  putVersion(version: CVRVersion): void;
  catchupRowPatches(
    startingVersion: CVRVersion,
  ): Promise<[RowPatch, CVRVersion][]>;
  catchupConfigPatches(
    startingVersion: CVRVersion,
  ): Promise<[MetadataPatch, CVRVersion][]>;
  allRowRecords(): AsyncIterable<RowRecord>;
  flush(): Promise<void>;
}
