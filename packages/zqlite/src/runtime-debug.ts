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
  initQuery(clientGroupID: ClientGroupID, source: SourceName, query: SQL) {
    const {counts} = getRowStats(clientGroupID, source);
    if (counts) {
      if (!counts.has(query)) {
        counts.set(query, 0);
      }
    }
  },

  rowVended(
    clientGroupID: ClientGroupID,
    source: SourceName,
    query: SQL,
    row: Row,
  ) {
    const {counts, rows} = getRowStats(clientGroupID, source);
    if (counts) {
      counts.set(query, (counts.get(query) ?? 0) + 1);
    }
    if (rows) {
      rows.set(query, [...(rows.get(query) ?? []), row]);
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

function getRowStats(clientGroupID: ClientGroupID, source: SourceName) {
  let counts: RowCountsByQuery | undefined;
  let rows: RowsByQuery | undefined;
  if (runtimeDebugFlags.trackRowCountsVended) {
    let sourceMap = rowCountsByCg.get(clientGroupID);
    if (!sourceMap) {
      sourceMap = new Map();
      rowCountsByCg.set(clientGroupID, sourceMap);
    }
    counts = sourceMap.get(source);
    if (!counts) {
      counts = new Map();
      sourceMap.set(source, counts);
    }
  }
  if (runtimeDebugFlags.trackRowsVended) {
    let sourceMap = rowsByCg.get(clientGroupID);
    if (!sourceMap) {
      sourceMap = new Map();
      rowsByCg.set(clientGroupID, sourceMap);
    }
    rows = sourceMap.get(source);
    if (!rows) {
      rows = new Map();
      sourceMap.set(source, rows);
    }
  }
  return {counts, rows};
}
