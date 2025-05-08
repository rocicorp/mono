import type {FetchRequest, Input, InputBase, Output} from './operator.ts';
import {type Node} from './data.ts';
import type {Change} from './change.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';
import {assert} from '../../../shared/src/asserts.ts';

export interface FilterInput extends InputBase {
  /** Tell the input where to send its output. */
  setFilterOutput(output: FilterOutput): void;
}

export interface FilterOutput extends Output {
  filter(node: Node, cleanup: boolean): boolean;
}

export interface FilterOperator extends FilterInput, FilterOutput {}

/**
 * An implementation of FilterOutput that throws if pushed to. It is used as the
 * initial value for for an operator's output before it is set.
 */
export const throwFilterOutput: FilterOutput = {
  push(_change: Change): void {
    throw new Error('Output not set');
  },
  filter(_node: Node, _cleanup: boolean): boolean {
    throw new Error('Output not set');
  },
};

export class FilterStart implements FilterInput, Output {
  readonly #input: Input;

  #output: FilterOutput = throwFilterOutput;
  #inFilterFetchOrCleanup = false;

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

  *filterFetchOrCleanup(req: FetchRequest, cleanup: boolean): Stream<Node> {
    assert(!this.#inFilterFetchOrCleanup);
    this.#inFilterFetchOrCleanup = true;
    try {
      for (const node of this.#input.fetch(req)) {
        this.#output.filter(node, cleanup);
        yield node;
      }
    } finally {
      this.#inFilterFetchOrCleanup = false;
    }
  }
}

export class FilterEnd implements Input, FilterOutput {
  readonly #start: FilterStart;
  readonly #input: Input;

  #output: Output = throwFilterOutput;
  #receivedNodeViaFilter: Node = undefined;

  constructor(start: FilterStart, input: Input) {
    this.#start = start;
    this.#input = input;
    input.setOutput(this);
  }

  fetch(req: FetchRequest): Stream<Node> {
    assert(this.#receivedNodeViaFilter === undefined);
    for (const node of this.#start.filterFetch(req)) {
      if (this.#receivedNodeViaFilter) {
        assert(this.#receivedNodeViaFilter === node);
        yield node;
      }
    }
  }

  cleanup(req: FetchRequest): Stream<Node> {
    throw new Error('Method not implemented.');
  }

  filter(node: Node, cleanup: boolean) {}

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
