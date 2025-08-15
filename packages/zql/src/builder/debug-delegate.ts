import type {Row} from '../../../zero-protocol/src/data.ts';

export const runtimeDebugFlags = {
  trackRowCountsVended: false,
  trackRowsVended: false,
};

type SourceName = string;
type SQL = string;
type RowCountsBySource = Map<SourceName, RowCountsByQuery>;
type RowsBySource = Map<SourceName, RowsByQuery>;
type RowCountsByQuery = Map<SQL, number>;
type RowsByQuery = Map<SQL, Row[]>;

export interface DebugDelegate {
  initQuery(table: SourceName, query: SQL): void;
  rowVended(table: SourceName, query: SQL, row: Row): void;
  getVendedRowCounts(): RowCountsBySource;
  getVendedRows(): RowsBySource;
  // clears all internal state
  reset(): void;
}

export class Debug implements DebugDelegate {
  readonly #rowCountsBySource: RowCountsBySource;
  readonly #rowsBySource: RowsBySource;

  constructor() {
    this.#rowCountsBySource = new Map();
    this.#rowsBySource = new Map();
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
      if (!counts.has(query)) {
        counts.set(query, 0);
      }
    }
  }

  reset(): void {
    this.#rowCountsBySource.clear();
    this.#rowsBySource.clear();
  }

  rowVended(table: SourceName, query: SQL, row: Row): void {
    const {counts, rows} = this.#getRowStats(table);
    if (counts) {
      counts.set(query, (counts.get(query) ?? 0) + 1);
    }
    if (rows) {
      rows.set(query, [...(rows.get(query) ?? []), row]);
    }
  }

  #getRowStats(source: SourceName) {
    let counts: RowCountsByQuery | undefined;
    let rows: RowsByQuery | undefined;
    if (runtimeDebugFlags.trackRowCountsVended) {
      counts = this.#rowCountsBySource.get(source);
      if (!counts) {
        counts = new Map();
        this.#rowCountsBySource.set(source, counts);
      }
    }
    if (runtimeDebugFlags.trackRowsVended) {
      rows = this.#rowsBySource.get(source);
      if (!rows) {
        rows = new Map();
        this.#rowsBySource.set(source, rows);
      }
    }
    return {counts, rows};
  }
}
