import {Ordering} from '../ast2/ast.js';
import {Row} from './data.js';
import {Input} from './operator.js';

export type SourceChange = {
  type: 'add' | 'remove';
  row: Row;
};

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
   */
  connect(sort: Ordering): Input;

  /**
   * Pushes a change into the source and into all connected outputs.
   */
  push(change: SourceChange): void;
}
