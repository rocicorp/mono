import type {JSONValue} from '../../../shared/src/json.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Change} from './change.ts';
import type {Constraint} from './constraint.ts';
import type {Node} from './data.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';

/**
 * Input to an operator.
 */
export interface InputBase {
  /** The schema of the data this input returns. */
  getSchema(): SourceSchema;

  /**
   * Completely destroy the input. Destroying an input
   * causes it to call destroy on its upstreams, fully
   * cleaning up a pipeline.
   */
  destroy(): void;
}

export interface Input extends InputBase {
  /** Tell the input where to send its output. */
  setOutput(output: Output): void;

  /**
   * Fetch data. May modify the data in place.
   * Returns nodes sorted in order of `SourceSchema.compareRows`.
   *
   * The stream may contain 'yield' to yield control to the caller for purposes
   * of responsiveness.
   *
   * Contract:
   * - During fetch: If an input yields 'yield', 'yield' must be yielded to the
   * caller of fetch immediately.
   * - During push: If a fetch to an input consumed by the push logic yields
   * 'yield', it must be yielded to the caller of push immediately.
   */
  fetch(req: FetchRequest): Stream<Node | 'yield'>;
}

export type FetchRequest = {
  readonly constraint?: Constraint | undefined;
  /** If supplied, `start.row` must have previously been output by fetch or push. */
  readonly start?: Start | undefined;

  /** Whether to fetch in reverse order of the SourceSchema's sort. */
  readonly reverse?: boolean | undefined;
};

export type Start = {
  readonly row: Row;
  readonly basis: 'at' | 'after';
};

/**
 * An output for an operator. Typically another Operator but can also be
 * the code running the pipeline.
 */
export interface Output {
  /**
   * Push incremental changes to data previously received with fetch().
   * Consumers must apply all pushed changes or incremental result will
   * be incorrect.
   * Callers must maintain some invariants for correct operation:
   * - Only add rows which do not already exist (by deep equality).
   * - Only remove rows which do exist (by deep equality).
   * Implmentation can yield 'yield' to yield control to the caller for purposes
   * of responsiveness.
   * Yield contract:
   * - During a push: If a push call to an output yields 'yield', it must be
   * yielded to the caller of push immediately.
   */
  push(change: Change, pusher: InputBase): Stream<'yield'>;
}

/**
 * An implementation of Output that throws if pushed to. It is used as the
 * initial value for for an operator's output before it is set.
 */
export const throwOutput: Output = {
  push(_change: Change): Stream<'yield'> {
    throw new Error('Output not set');
  },
};

export function* skipYields(stream: Stream<Node | 'yield'>): Stream<Node> {
  for (const node of stream) {
    if (node !== 'yield') {
      yield node;
    }
  }
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
   */
  scan(options?: {prefix: string}): Stream<[string, JSONValue]>;
  del(key: string): void;
}
