import {DeferredWritesBudget} from './deferred-writes-budget.ts';

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
 * The limits are the defaults of `deferIvmWritesMaxRows` and
 * `deferIvmWritesMaxBytes`.
 */
export function testDeferredWritesBudget(): DeferredWritesBudget | undefined {
  const maxRows = 200_000;
  const maxBytes = 32 * 1024 * 1024;
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
