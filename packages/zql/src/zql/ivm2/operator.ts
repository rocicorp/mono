import type {JSONValue} from 'shared/src/json.js';
import type {Change} from './change.js';
import type {Node, Row, Value} from './data.js';
import type {Stream} from './stream.js';
import type {Schema} from './schema.js';

/**
 * Input to an operator.
 */
export interface Input {
  // The schema of the data this input returns.
  getSchema(): Schema;

  // Fetch data.
  // Does not modify current state.
  // Returns nodes sorted in order of schema().comparator.
  fetch(req: FetchRequest): Stream<Node>;

  // Cleanup maintained state. This is called when `output` will no longer need
  // the data returned by fetch(). The receiving operator should clean up any
  // resources it has allocated to service such requests.
  //
  // This is different from `destroy()` which means this input will no longer
  // be called at all, for any input.
  //
  // Returns the same thing as fetch(). This allows callers to properly
  // propagate the cleanup message through the graph.
  cleanup(req: FetchRequest): Stream<Node>;

  // Tell the input where to send its output.
  setOutput(output: Output): void;

  // Completely destroy the input. Destroying an input
  // causes it to call destroy on its upstreams, fully
  // cleaning up a pipeline.
  destroy(): void;
}

export type Constraint = {
  key: string;
  value: Value;
};

export type FetchRequest = {
  constraint?: Constraint | undefined;
  // If supplied, `start.row` must have previously been output by fetch or push.
  start?: Start | undefined;
};

export type Start = {
  row: Row;
  basis: 'before' | 'at' | 'after';
};

/**
 * An output for an operator. Typically another Operator but can also be
 * the code running the pipeline.
 */
export interface Output {
  // Push incremental changes to data previously received with fetch().
  // Consumers must apply all pushed changes or incremental result will
  // be incorrect.
  // Callers must maintain some invariants for correct operation:
  // - Only add rows which do not already exist (by deep equality).
  // - Only remove rows which do exist (by deep equality).
  push(change: Change): void;
}

/**
 * Operators are arranged into pipelines.
 * They are stateful.
 * Each operator is an input to the next operator in the chain and an output
 * to the previous.
 */
export interface Operator extends Input, Output {}

/**
 * Operators get access to storage that they can store their internal
 * state in.
 */
export interface Storage {
  set(key: string, value: JSONValue): void;
  get(key: string, def?: JSONValue): JSONValue | undefined;
  /**
   * If options is not specified, defaults to scanning all entries.
   * @param options
   */
  scan(options?: {prefix: string}): Stream<[string, JSONValue]>;
  del(key: string): void;
}
