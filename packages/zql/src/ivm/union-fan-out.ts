import type {Change} from './change.ts';
import type {Node} from './data.ts';
import type {FetchRequest, Input, Operator, Output} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';

export class UnionFanOut implements Operator {
  #destroyCount: number = 0;
  readonly #input: Input;
  readonly #outputs: Output[] = [];

  constructor(input: Input) {
    this.#input = input;
    input.setOutput(this);
  }

  push(change: Change): void {
    for (const output of this.#outputs) {
      output.push(change);
    }
    // TODO: tell UFI we're done pushing to all branches
  }

  setOutput(output: Output): void {
    this.#outputs.push(output);
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  fetch(req: FetchRequest): Stream<Node> {
    return this.#input.fetch(req);
  }

  cleanup(_req: FetchRequest): Stream<Node> {
    // Cleanup is going away. Not implemented.
    return [];
  }

  destroy(): void {
    if (this.#destroyCount < this.#outputs.length) {
      ++this.#destroyCount;
      if (this.#destroyCount === this.#outputs.length) {
        this.#input.destroy();
      }
    } else {
      throw new Error('FanOut already destroyed once for each output');
    }
  }
}
