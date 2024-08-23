// An input to an operator.
// Inputs "vend" (chosen to avoid confusion with "output") data in some order.

import type {JSONValue} from 'replicache';
import type {Change} from './change.js';
import type {Node, NormalizedValue, Row, Value} from './data.js';
import type {Stream} from './stream.js';
import type {Schema} from './schema.js';

/**
 * Input to an operator. Typically another Operator but can also be a Source.
 */
export interface Input {
  // The schema of the data this input returns.
  getSchema(output: Output): Schema;

  // Fetch data.
  // Does not modify current state.
  // Returns nodes sorted in order of schema().comparator.
  fetch(req: FetchRequest, output: Output): Stream<Node>;

  // Cleanup the operator. This is called when `output` will no longer
  // need the data returned by fetch(). The receiving operator should
  // clean up any resources it has allocated.
  // Returns the same thing as fetch(). This allows callers to properly
  // propagate the cleanup message through the graph.
  cleanup(req: FetchRequest, output: Output): Stream<Node>;

  setOutput(output: Output): void;
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
  push(change: Change, input: Input): void;
}

/**
 * Operators are arranged into pipelines.
 * They are stateful.
 * Each operator is an input to the next operator in the chain and an output
 * to the previous.
 */
export interface Operator extends Input, Output {}

export type StorageKey = readonly NormalizedValue[];
/**
 * Operators get access to storage that they can store their internal
 * state in.
 */
export interface Storage {
  set(key: StorageKey, value: JSONValue): void;
  get(key: StorageKey, def?: JSONValue): JSONValue | undefined;
  /**
   * If options is not specified, defaults to scanning all entries.
   * @param options
   */
  scan(options?: {prefix: readonly Value[]}): Stream<[StorageKey, JSONValue]>;
  del(key: StorageKey): void;
}
