import {assert} from '../../../../shared/src/asserts.ts';

/**
 * Bounds the memory that deferred IVM writes (see `deferIvmWrites`) use
 * across the client groups of one syncer process.
 *
 * Each client group holds its own in-memory copy of the changes of the
 * advancement it is processing, so the copies add up across the groups that
 * advance at the same time. Before an advancement starts, its pipeline driver
 * reserves the advancement's number of change log entries. That number bounds
 * the rows its sources can hold: the change log has one entry per row key
 * (a truncation, which does not, resets the pipelines instead), and a row
 * displaced through a unique key must itself have changed in the same
 * interval, so it has its own entry. An advancement that does not fit is
 * written through to the replica snapshot instead, which SQLite bounds with
 * its page cache and spills to disk.
 */
export class DeferredWritesBudget {
  readonly #maxRows: number;
  #reservedRows = 0;

  /**
   * Estimated bytes one client group may hold for one advancement. The row
   * count is known before the advancement starts, but the width of the rows
   * is not, so this is checked as the rows are written.
   */
  readonly maxBytesPerClientGroup: number;

  constructor(maxRows: number, maxBytesPerClientGroup: number) {
    this.#maxRows = maxRows;
    this.maxBytesPerClientGroup = maxBytesPerClientGroup;
  }

  get reservedRows(): number {
    return this.#reservedRows;
  }

  /**
   * Reserves `rows` if they fit in what is left of the budget, and returns
   * whether they did. A successful reservation must be {@link release}d.
   */
  tryReserve(rows: number): boolean {
    if (this.#reservedRows + rows > this.#maxRows) {
      return false;
    }
    this.#reservedRows += rows;
    return true;
  }

  release(rows: number): void {
    assert(
      rows <= this.#reservedRows,
      () =>
        `Releasing ${rows} rows but only ${this.#reservedRows} are reserved`,
    );
    this.#reservedRows -= rows;
  }
}
