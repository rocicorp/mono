import {BYTES_PER_ROW, DeferredWritesBudget} from './deferred-writes-budget.ts';

/**
 * A budget in which every other reservation fails, starting with one that
 * succeeds, so that a driver switches between holding its advancements'
 * changes in memory and writing them through to the snapshot. Drivers that
 * share one alternate with each other.
 */
class AlternatingBudget extends DeferredWritesBudget {
  #fits = false;

  override tryReserve(rows: number): boolean {
    this.#fits = !this.#fits;
    return this.#fits && super.tryReserve(rows);
  }
}

/**
 * The deferred writes budget for a pipeline driver under test, as selected by
 * `ZERO_TEST_DEFER_IVM_WRITES`:
 *
 * - unset: none, so every advancement is written through;
 * - `1`: every advancement is held in memory;
 * - `mixed`: every other advancement is written through.
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
      return undefined;
    case '1':
      return new DeferredWritesBudget(maxRows, maxBytes);
    case 'mixed':
      return new AlternatingBudget(maxRows, maxBytes);
    default:
      throw new Error(`Unknown ZERO_TEST_DEFER_IVM_WRITES: ${mode}`);
  }
}
