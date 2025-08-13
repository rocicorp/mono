import type {FetchRequest, Input, InputBase, Output} from './operator.ts';
import {drainStreams, type Node} from './data.ts';
import type {Change} from './change.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';
import type {BuilderDelegate} from '../builder/builder.ts';

/**
 * The `where` clause of a ZQL query is implemented using a sub-graph of
 * `FilterOperators`.  This sub-graph starts with a `FilterStart` operator,
 * that adapts from the normal `Operator` `Output`, to the
 * `FilterOperator` `FilterInput`, and ends with a `FilterEnd` operator that
 * adapts from a `FilterOperator` `FilterOutput` to a normal `Operator` `Input`.
 * `FilterOperator'`s do not have `fetch` or `cleanup` instead they have a
 * `filter(node: Node, cleanup: boolean): boolean` method.
 * They also have `push` which is just like normal `Operator` push.
 * Not having a `fetch` means these `FilterOperator`'s cannot modify
 * `Node` `row`s or `relationship`s, but they shouldn't, they should just
 * filter.
 *
 * This `FilterOperator` abstraction enables much more efficient processing of
 * `fetch` for `where` clauses containing OR conditions.
 *
 * See https://github.com/rocicorp/mono/pull/4339
 */

export interface FilterInput extends InputBase {
  /** Tell the input where to send its output. */
  setFilterOutput(output: FilterOutput): void;
}

export interface FilterOutput extends Output {
  filter(node: Node, cleanup: boolean): boolean;
}

export interface FilterOperator extends FilterInput, FilterOutput {}

/**
 * An implementation of FilterOutput that throws if push or filter is called.
 * It is used as the initial value for for an operator's output before it is
 * set.
 */
export const throwFilterOutput: FilterOutput = {
  push(_change: Change): void {
    throw new Error('Output not set');
  },

  filter(_node: Node, _cleanup): boolean {
    throw new Error('Output not set');
  },
};

export class FilterStart implements FilterInput, Output {
  readonly #input: Input;
  #output: FilterOutput = throwFilterOutput;

  constructor(input: Input) {
    this.#input = input;
    input.setOutput(this);
  }

  setFilterOutput(output: FilterOutput) {
    this.#output = output;
  }

  destroy(): void {
    this.#input.destroy();
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  push(change: Change) {
    this.#output.push(change);
  }

  *fetch(req: FetchRequest): Stream<Node> {
    for (const node of this.#input.fetch(req)) {
      if (this.#output.filter(node, false)) {
        yield node;
      }
    }
  }

  *cleanup(req: FetchRequest): Stream<Node> {
    for (const node of this.#input.cleanup(req)) {
      if (this.#output.filter(node, true)) {
        yield node;
      } else {
        drainStreams(node);
      }
    }
  }
}

export class FilterEnd implements Input, FilterOutput {
  readonly #start: FilterStart;
  readonly #input: FilterInput;

  #output: Output = throwFilterOutput;

  constructor(start: FilterStart, input: FilterInput) {
    this.#start = start;
    this.#input = input;
    input.setFilterOutput(this);
  }

  *fetch(req: FetchRequest): Stream<Node> {
    for (const node of this.#start.fetch(req)) {
      yield node;
    }
  }

  *cleanup(req: FetchRequest): Stream<Node> {
    for (const node of this.#start.cleanup(req)) {
      yield node;
    }
  }

  filter(_node: Node, _cleanup: boolean) {
    return true;
  }

  setOutput(output: Output) {
    this.#output = output;
  }

  destroy(): void {
    this.#input.destroy();
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  push(change: Change) {
    this.#output.push(change);
  }
}

export function buildFilterPipeline(
  input: Input,
  delegate: BuilderDelegate,
  pipeline: (filterInput: FilterInput) => FilterInput,
): Input {
  const filterStart = new FilterStart(input);
  delegate.addEdge(input, filterStart);
  const middle = pipeline(filterStart);
  delegate.addEdge(filterStart, middle);
  const filterEnd = new FilterEnd(filterStart, middle);
  delegate.addEdge(middle, filterEnd);
  return filterEnd;
}
