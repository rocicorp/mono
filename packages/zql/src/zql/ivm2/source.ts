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
  connect(sort: Ordering): Input;
  push(change: SourceChange): void;
}
