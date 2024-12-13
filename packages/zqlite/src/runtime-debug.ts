export const runtimeDebugFlags = {
  trackRowsVended: false,
};

type ClientGroupID = string;
type SourceName = string;
type SQL = string;

type RowsByCg = Map<ClientGroupID, RowsBySource>;
type RowsBySource = Map<SourceName, RowsByQuery>;
type RowsByQuery = Map<SQL, number>;

const rowsVendedByCg: RowsByCg = new Map<
  ClientGroupID,
  Map<SourceName, Map<SQL, number>>
>();

export const runtimeDebugStats = {
  rowVended(clientGroupID: ClientGroupID, source: SourceName, query: SQL) {
    let sourceMap = rowsVendedByCg.get(clientGroupID);
    if (!sourceMap) {
      sourceMap = new Map<SourceName, RowsByQuery>();
      rowsVendedByCg.set(clientGroupID, sourceMap);
    }
    let queryMap = sourceMap.get(source);
    if (!queryMap) {
      queryMap = new Map<SQL, number>();
      sourceMap.set(source, queryMap);
    }

    queryMap.set(query, (queryMap.get(query) ?? 0) + 1);
  },

  resetRowsVended(clientGroupID: ClientGroupID) {
    rowsVendedByCg.delete(clientGroupID);
  },

  getRowsVended(clientGroupID: ClientGroupID): RowsBySource | undefined {
    return rowsVendedByCg.get(clientGroupID);
  },

  all() {
    return rowsVendedByCg;
  },
};
