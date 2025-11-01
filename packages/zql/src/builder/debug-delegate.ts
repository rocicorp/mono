import type {
  RowCountsByQuery,
  RowCountsBySource,
  RowsByQuery,
  RowsBySource,
} from '../../../zero-protocol/src/analyze-query-result.ts';
import {type Row} from '../../../zero-protocol/src/data.ts';

export const runtimeDebugFlags = {
  trackRowCountsVended: false,
  trackRowsVended: false,
};

type SourceName = string;
type SQL = string;

export interface DebugDelegate {
  initQuery(table: SourceName, query: SQL): void;
  rowVended(table: SourceName, query: SQL, row: Row): void;
  getVendedRowCounts(): RowCountsBySource;
  getVendedRows(): RowsBySource;
  rowsVisited(table: SourceName, query: SQL, count: number): void;
  getVisitedRowCounts(): RowCountsBySource;
  // clears all internal state
  reset(): void;
}

export class Debug implements DebugDelegate {
  #rowCountsBySource: RowCountsBySource;
  #rowsBySource: RowsBySource;
  #visitedRowCountsBySource: RowCountsBySource;

  constructor() {
    this.#rowCountsBySource = {};
    this.#rowsBySource = {};
    this.#visitedRowCountsBySource = {};
  }

  getVendedRowCounts(): RowCountsBySource {
    return this.#rowCountsBySource;
  }

  getVendedRows(): RowsBySource {
    return this.#rowsBySource;
  }

  getVisitedRowCounts(): RowCountsBySource {
    return this.#visitedRowCountsBySource;
  }

  initQuery(table: SourceName, query: SQL): void {
    const {counts} = this.#getRowStats(table);
    if (counts) {
      if (!counts[query]) {
        counts[query] = 0;
      }
    }
  }

  reset(): void {
    this.#rowCountsBySource = {};
    this.#rowsBySource = {};
    this.#visitedRowCountsBySource = {};
  }

  rowVended(table: SourceName, query: SQL, row: Row): void {
    const {counts, rows} = this.#getRowStats(table);
    if (counts) {
      counts[query] = (counts[query] ?? 0) + 1;
    }
    if (rows) {
      rows[query] = [...(rows[query] ?? []), row];
    }
  }

  rowsVisited(table: SourceName, query: SQL, count: number): void {
    const counts = this.#getVisitedRowStats(table);
    if (counts) {
      counts[query] = (counts[query] ?? 0) + count;
    }
  }

  #getRowStats(table: SourceName): {
    counts: RowCountsByQuery | undefined;
    rows: RowsByQuery | undefined;
  } {
    if (!runtimeDebugFlags.trackRowCountsVended) {
      return {counts: undefined, rows: undefined};
    }
    let counts = this.#rowCountsBySource[table];
    if (!counts) {
      counts = {};
      this.#rowCountsBySource[table] = counts;
    }
    let rows = undefined;
    if (runtimeDebugFlags.trackRowsVended) {
      rows = this.#rowsBySource[table];
      if (!rows) {
        rows = {};
        this.#rowsBySource[table] = rows;
      }
    }
    return {counts, rows};
  }

  #getVisitedRowStats(source: SourceName): RowCountsByQuery {
    let counts = this.#visitedRowCountsBySource[source];
    if (!counts) {
      counts = {};
      this.#visitedRowCountsBySource[source] = counts;
    }
    return counts;
  }
}

export class NoOpDebug implements DebugDelegate {
  initQuery(_table: SourceName, _query: SQL): void {}
  rowVended(_table: SourceName, _query: SQL, _row: Row): void {}
  rowsVisited(_table: SourceName, _query: SQL, _count: number): void {}
  getVendedRowCounts(): RowCountsBySource {
    return {};
  }
  getVendedRows(): RowsBySource {
    return {};
  }
  getVisitedRowCounts(): RowCountsBySource {
    return {};
  }
  reset(): void {}
}
