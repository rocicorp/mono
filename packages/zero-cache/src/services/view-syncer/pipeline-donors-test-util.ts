import type {Row} from '../../../../zero-protocol/src/data.ts';
import {ChangeType} from '../../../../zql/src/ivm/change-type.ts';
import type {OutputRow} from './pipeline-donors.ts';
import type {RowChange} from './pipeline-driver.ts';

/**
 * Tracks the rows a pipeline has output, standing in for the CVR row records
 * that a view-syncer provides to a driver it does not run in these tests.
 */
export class TrackedRows {
  readonly #rows = new Map<string, OutputRow>();

  apply(changes: Iterable<RowChange | 'yield'>): (RowChange | 'yield')[] {
    const seen = [...changes];
    for (const change of seen) {
      if (change === 'yield' || change.type === ChangeType.EDIT) {
        continue;
      }
      const rowKey: Row = change.rowKey;
      const key = `${change.table}/${JSON.stringify(rowKey)}`;
      const delta = change.type === ChangeType.ADD ? 1 : -1;
      const count = (this.#rows.get(key)?.count ?? 0) + delta;
      if (count > 0) {
        this.#rows.set(key, {table: change.table, rowKey, count});
      } else {
        this.#rows.delete(key);
      }
    }
    return seen;
  }

  rows(): OutputRow[] {
    return [...this.#rows.values()];
  }
}
