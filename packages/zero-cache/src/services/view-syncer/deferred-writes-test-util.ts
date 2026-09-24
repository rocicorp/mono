import {BYTES_PER_ROW, DeferredWritesBudget} from './deferred-writes-budget.ts';

/**
 * A budget whose reservations cycle between three outcomes, so that a driver
 * switches between the ways its advancements can apply their changes: held
 * in memory; written through to the snapshot; and held in memory until the
 * first change, then written through. Drivers that share one cycle with each
 * other.
 */
class CyclingBudget extends DeferredWritesBudget {
  #reservations = 0;
  #overflowNext = false;

  override tryReserve(rows: number): boolean {
    switch (this.#reservations++ % 3) {
      case 0:
        return super.tryReserve(rows);
      case 1:
        return false;
      default:
        this.#overflowNext = super.tryReserve(rows);
        return this.#overflowNext;
    }
  }

  override holdBytes(bytes: number): boolean {
    const fits = super.holdBytes(bytes);
    if (this.#overflowNext) {
      this.#overflowNext = false;
      return false;
    }
    return fits;
  }
}

/**
 * The deferred writes budget for a pipeline driver under test, as selected by
 * `ZERO_TEST_DEFER_IVM_WRITES`:
 *
 * - unset (as `deferIvmWrites` is on by default) or `1`: every advancement
 *   is held in memory;
 * - `0`: none, so every advancement is written through;
 * - `mixed`: advancements cycle between being held in memory, written
 *   through, and written through from their second change.
 *
 * The budget is the default share of the heap (`deferIvmWritesHeapProportion`)
 * of a 4 GiB heap, rather than of the heap of the test process, so that tests
 * do not depend on the machine they run on.
 */
export function testDeferredWritesBudget(): DeferredWritesBudget | undefined {
  const maxBytes = 1024 * 1024 * 1024;
  const maxRows = maxBytes / BYTES_PER_ROW;
  const mode = process.env['ZERO_TEST_DEFER_IVM_WRITES'];
  switch (mode) {
    case undefined:
    case '':
    case '1':
      return new DeferredWritesBudget(maxRows, maxBytes);
    case '0':
      return undefined;
    case 'mixed':
      return new CyclingBudget(maxRows, maxBytes);
    default:
      throw new Error(`Unknown ZERO_TEST_DEFER_IVM_WRITES: ${mode}`);
  }
}
