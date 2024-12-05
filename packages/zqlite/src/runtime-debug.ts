import {assert} from '../../shared/src/asserts.js';

export const runtimeDebugFlags = {
  trackRowsVended: false,
};

const rowsVendedBySourceAndQuery = new Map<string, Map<string, number>>();

export const runtimeDebugStats = {
  rowVended(source: string, query: string) {
    assert(runtimeDebugFlags.trackRowsVended);
    let sourceMap = rowsVendedBySourceAndQuery.get(source);
    if (!sourceMap) {
      sourceMap = new Map<string, number>();
      rowsVendedBySourceAndQuery.set(source, sourceMap);
    }

    sourceMap.set(query, (sourceMap.get(query) ?? 0) + 1);
  },

  get rowsVended() {
    return rowsVendedBySourceAndQuery;
  },
};
