import {getHeapStatistics} from 'node:v8';
import {assert} from '../../../../shared/src/asserts.ts';

/**
 * The bytes assumed for each row held in memory, which converts the memory
 * given to deferred writes into rows. The width of the rows an advancement
 * will hold is not known before it starts.
 */
export const BYTES_PER_ROW = 1024;

/**
 * Bounds the memory that deferred IVM writes (see `deferIvmWrites`) use
 * across the client groups of one syncer process.
 *
 * Each client group holds its own in-memory copy of the changes of the
 * advancement it is processing, so the copies add up across the groups that
 * advance at the same time. Before an advancement starts, its pipeline driver
 * reserves the number of change log entries of the tables it reads. That
 * number bounds the rows its sources can hold: the change log has one entry
 * per row key (a truncation, which does not, resets the pipelines instead),
 * and a row displaced through a unique key must itself have changed in the
 * same interval, so it has its own entry. An advancement that does not fit is
 * written through to the replica snapshot instead, which SQLite bounds with
 * its page cache and spills to disk.
 *
 * The rows are reserved at an assumed {@link BYTES_PER_ROW}, but their width
 * is not known until they are held. So the advancements also add up the
 * bytes they are estimated to hold as they go, and the one that takes the
 * total past the budget writes what it holds through to the replica snapshot,
 * and the rest of its changes after them.
 *
 * The budget is a share of the heap, so it does not depend on how many
 * client groups are connected: when advancements do not overlap, each one
 * can use all of it, and when many do, each gets less and the rest write
 * through.
 */
export class DeferredWritesBudget {
  readonly #maxRows: number;
  readonly #maxBytes: number;
  #reservedRows = 0;
  #heldBytes = 0;

  /**
   * A budget of `proportion` of the heap limit, and the rows that fit in it
   * at {@link BYTES_PER_ROW}.
   */
  static forHeapProportion(proportion: number): DeferredWritesBudget {
    return DeferredWritesBudget.forBytes(
      Math.floor(getHeapStatistics().heap_size_limit * proportion),
    );
  }

  /** A budget of `bytes`, and the rows that fit in it at {@link BYTES_PER_ROW}. */
  static forBytes(bytes: number): DeferredWritesBudget {
    return new DeferredWritesBudget(Math.floor(bytes / BYTES_PER_ROW), bytes);
  }

  constructor(maxRows: number, maxBytes: number) {
    this.#maxRows = maxRows;
    this.#maxBytes = maxBytes;
  }

  get maxRows(): number {
    return this.#maxRows;
  }

  get maxBytes(): number {
    return this.#maxBytes;
  }

  get reservedRows(): number {
    return this.#reservedRows;
  }

  /** The estimated bytes held by the advancements that hold rows. */
  get heldBytes(): number {
    return this.#heldBytes;
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

  /**
   * Adds `bytes` (fewer if negative) to the estimated bytes held by an
   * advancement that has reserved rows, and returns whether the total is
   * still within the budget. If it is not, the advancement is expected to
   * stop holding rows and {@link release} them.
   */
  holdBytes(bytes: number): boolean {
    this.#heldBytes += bytes;
    assert(this.#heldBytes >= 0, () => `Holding ${this.#heldBytes} bytes`);
    return this.#heldBytes <= this.#maxBytes;
  }

  /** Returns an advancement's reserved `rows` and the `bytes` it held. */
  release(rows: number, bytes: number): void {
    assert(
      rows <= this.#reservedRows,
      () =>
        `Releasing ${rows} rows but only ${this.#reservedRows} are reserved`,
    );
    assert(
      bytes <= this.#heldBytes,
      () => `Releasing ${bytes} bytes but only ${this.#heldBytes} are held`,
    );
    this.#reservedRows -= rows;
    this.#heldBytes -= bytes;
  }
}
