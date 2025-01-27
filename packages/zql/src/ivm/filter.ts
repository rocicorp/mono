import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Change} from './change.ts';
import type {Node} from './data.ts';
import {filterPush} from './filter-push.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Operator,
  type Output,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';

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
