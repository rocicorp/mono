import type {Row} from '../../zero-protocol/src/data.ts';

export const runtimeDebugFlags = {
  trackRowCountsVended: false,
  trackRowsVended: false,
};

type ClientGroupID = string;
type SourceName = string;
type SQL = string;

type RowCountsByCg = Map<ClientGroupID, RowCountsBySource>;
type RowCountsBySource = Map<SourceName, RowCountsByQuery>;
type RowCountsByQuery = Map<SQL, number>;

type RowsByCg = Map<ClientGroupID, RowsBySource>;
type RowsBySource = Map<SourceName, RowsByQuery>;
type RowsByQuery = Map<SQL, Row[]>;

const rowCountsByCg: RowCountsByCg = new Map();
const rowsByCg: RowsByCg = new Map();

export const runtimeDebugStats = {
  /**
   * Pass `undefined` at startup to initialize the stats.
   */
  rowVended(
    clientGroupID: ClientGroupID,
    source: SourceName,
    query: SQL,
    row: Row | undefined,
  ) {
    if (runtimeDebugFlags.trackRowCountsVended) {
      let sourceMap = rowCountsByCg.get(clientGroupID);
      if (!sourceMap) {
        sourceMap = new Map();
        rowCountsByCg.set(clientGroupID, sourceMap);
      }
      let queryMap = sourceMap.get(source);
      if (!queryMap) {
        queryMap = new Map();
        sourceMap.set(source, queryMap);
      }
      queryMap.set(query, row ? (queryMap.get(query) ?? 0) + 1 : 0);
    }
    if (runtimeDebugFlags.trackRowsVended) {
      let sourceMap = rowsByCg.get(clientGroupID);
      if (!sourceMap) {
        sourceMap = new Map();
        rowsByCg.set(clientGroupID, sourceMap);
      }
      let queryMap = sourceMap.get(source);
      if (!queryMap) {
        queryMap = new Map();
        sourceMap.set(source, queryMap);
      }
      let rowArray = queryMap.get(query);
      if (!rowArray) {
        rowArray = [];
        queryMap.set(query, rowArray);
      }
      if (row) {
        rowArray.push(row);
      }
    }
  },

  resetRowsVended(clientGroupID: ClientGroupID) {
    if (runtimeDebugFlags.trackRowCountsVended) {
      rowCountsByCg.delete(clientGroupID);
    }

    if (runtimeDebugFlags.trackRowsVended) {
      rowsByCg.delete(clientGroupID);
    }
  },

  getVendedRowCounts() {
    return rowCountsByCg;
  },

  getVendedRows() {
    return rowsByCg;
  },
};
