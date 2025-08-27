import type {Row} from '../../../zero-protocol/src/data.ts';

export const runtimeDebugFlags = {
  trackRowCountsVended: false,
  trackRowsVended: false,
};

type SourceName = string;
type SQL = string;
export type RowCountsBySource = Record<SourceName, RowCountsByQuery>;
export type RowsBySource = Record<SourceName, RowsByQuery>;
type RowCountsByQuery = Record<SQL, number>;
type RowsByQuery = Record<SQL, Row[]>;

export interface DebugDelegate {
  initQuery(table: SourceName, query: SQL): void;
  rowVended(table: SourceName, query: SQL, row: Row): void;
  getVendedRowCounts(): RowCountsBySource;
  getVendedRows(): RowsBySource;
  // clears all internal state
  reset(): void;
}

export class Debug implements DebugDelegate {
  #rowCountsBySource: RowCountsBySource;
  #rowsBySource: RowsBySource;

  constructor() {
    this.#rowCountsBySource = {};
    this.#rowsBySource = {};
  }

  getVendedRowCounts(): RowCountsBySource {
    return this.#rowCountsBySource;
  }

  getVendedRows(): RowsBySource {
    return this.#rowsBySource;
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

  #getRowStats(source: SourceName) {
    let counts: RowCountsByQuery | undefined;
    let rows: RowsByQuery | undefined;
    counts = this.#rowCountsBySource[source];
    if (!counts) {
      counts = {};
      this.#rowCountsBySource[source] = counts;
    }
    rows = this.#rowsBySource[source];
    if (!rows) {
      rows = {};
      this.#rowsBySource[source] = rows;
    }
    return {counts, rows};
  }
}
