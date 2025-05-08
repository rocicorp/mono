import type {FetchRequest, Input, InputBase, Output} from './operator.ts';
import {drainStreams, type Node} from './data.ts';
import type {Change} from './change.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';

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
    for (const node of this.#input.fetch(req)) {
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
    for (const node of this.#start.fetch(req)) {
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
