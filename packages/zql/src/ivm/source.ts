import type {Condition} from '../../../zero-protocol/src/ast.ts';
import type {Ordering} from './constraint.ts';
import type {Row} from './data.ts';
import type {Input} from './operator.ts';

export type SourceChangeAdd = {
  type: 'add';
  row: Row;
};

export type SourceChangeRemove = {
  type: 'remove';
  row: Row;
};

export type SourceChangeEdit = {
  type: 'edit';
  row: Row;
  oldRow: Row;
};

export type SourceChangeSet = {
  type: 'set';
  row: Row;
};

export type SourceChange =
  | SourceChangeAdd
  | SourceChangeRemove
  | SourceChangeEdit;

/**
 * A source is an input that serves as the root data source of the pipeline.
 * Sources have multiple outputs. To add an output, call `connect()`, then
 * hook yourself up to the returned Connector, like:
 *
 * ```ts
 * class MyOperator implements Output {
 *   constructor(input: Input) {
 *     input.setOutput(this);
 *   }
 *
 *   push(change: Change): void {
 *     // Handle change
 *   }
 * }
 *
 * const connection = source.connect(ordering);
 * const myOperator = new MyOperator(connection);
 * ```
 */
export interface Source {
  /**
   * Creates an input that an operator can connect to. To free resources used
   * by connection, downstream operators call `destroy()` on the returned
   * input.
   *
   * @param sort The ordering of the rows. Source must return rows in this
   * order.
   * @param filters Filters to apply to the source.
   * @param splitEditKeys If an edit change modifies the values of any of the
   *   keys in splitEditKeys, the source should split the edit change into
   *   a remove of the old row followed by an add of the new row.
   */
  connect(
    sort: Ordering,
    filters?: Condition | undefined,
    splitEditKeys?: Set<string> | undefined,
  ): SourceInput;

  /**
   * Pushes a change into the source and into all connected outputs.
   */
  push(change: SourceChange | SourceChangeSet): void;

  /**
   * Pushes a change into the source.
   * Iterating the returned iterator will push the
   * change into one connected input at a time.
   *
   * Once the iterator is exhausted, the change will
   * have been pushed into all connected inputs and
   * committed to the source.
   */
  genPush(change: SourceChange): Iterable<void>;
}

export interface SourceInput extends Input {
  readonly fullyAppliedFilters: boolean;
}
