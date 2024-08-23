import {Ordering} from '../ast2/ast.js';
import {Connector} from './connector.js';
import {Row} from './data.js';

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
 * const connector = source.connect(ordering);
 * const myOperator = new MyOperator(connector);
 * connector.setOutput(myOperator);
 * ```
 */
export interface Source {
  connect(sort: Ordering): Connector;
  push(change: SourceChange): void;
}
