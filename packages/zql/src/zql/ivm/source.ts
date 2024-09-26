import type {Condition, Ordering} from '../ast/ast.js';
import type {Row} from './data.js';
import type {Input} from './operator.js';

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
   * @param optionalFilters Optional filters to apply to the source.
   */
  connect(
    sort: Ordering,
    optionalFilters?: readonly Condition[] | undefined,
  ): SourceInput;

  /**
   * Pushes a change into the source and into all connected outputs.
   */
  push(change: SourceChange): void;
}

export interface SourceInput extends Input {
  readonly appliedFilters: boolean;
}
