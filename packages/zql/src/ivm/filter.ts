import type {Row} from '../../../zero-protocol/src/data.js';
import type {Change} from './change.js';
import type {Node} from './data.js';
import {filterPush} from './filter-push.js';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Operator,
  type Output,
} from './operator.js';
import type {SourceSchema} from './schema.js';
import type {Stream} from './stream.js';

/**
 * The Filter operator filters data through a predicate. It is stateless.
 *
 * The predicate must be pure.
 */
export class Filter implements Operator {
  readonly #input: Input;
  readonly #predicate: (row: Row) => boolean;

  #output: Output = throwOutput;

  constructor(input: Input, predicate: (row: Row) => boolean) {
    this.#input = input;
    this.#predicate = predicate;
    input.setOutput(this);
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
    return this.#filter(this.#input.fetch(req));
  }

  cleanup(req: FetchRequest) {
    return this.#filter(this.#input.cleanup(req));
  }

  *#filter(stream: Stream<Node>) {
    for (const node of stream) {
      if (this.#predicate(node.row)) {
        yield node;
      }
    }
  }

  push(change: Change) {
    filterPush(change, this.#output, this.#predicate);
  }
}
