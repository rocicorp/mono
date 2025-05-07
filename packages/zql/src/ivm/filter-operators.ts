import type {FetchRequest, Input, InputBase, Output} from './operator.ts';
import {type Node} from './data.ts';
import type {Change} from './change.ts';
import type {SourceSchema} from './schema.ts';

export interface FilterInput extends InputBase {
  /** Tell the input where to send its output. */
  setFilterOutput(output: FilterOutput): void;
}

export interface FilterOutput extends Output {
  filter(node: Node): void;
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
  filter(_node: Node): void {
    throw new Error('Output not set');
  },
};

export class FilterInputAdaptor implements FilterInput, Output {
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

  fetch(req: FetchRequest) {
    return this.#input.fetch(req);
  }

  cleanup(req: FetchRequest) {
    return this.#input.cleanup(req);
  }

  push(change: Change) {
    this.#output.push(change);
  }
}

export class FilterOutputAdaptor implements Input, FilterOutput {
  readonly #input: Input;

  #output: Output = throwFilterOutput;

  constructor(input: Input) {
    this.#input = input;
    input.setOutput(this);
  }

  filter(_node: Node) {
    throw Error('Unexpected filter call.');
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

  fetch(req: FetchRequest) {
    return this.#input.fetch(req);
  }

  cleanup(req: FetchRequest) {
    return this.#input.cleanup(req);
  }

  push(change: Change) {
    this.#output.push(change);
  }
}
