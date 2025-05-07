import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Change} from './change.ts';
import {drainStreams} from './data.ts';
import {
  throwFilterOutput,
  type FilterInput,
  type FilterOperator,
  type FilterOutput,
} from './filter-operators.ts';
import {filterPush} from './filter-push.ts';
import {type FetchRequest} from './operator.ts';
import {type Node} from './data.ts';
import type {SourceSchema} from './schema.ts';

/**
 * The Filter operator filters data through a predicate. It is stateless.
 *
 * The predicate must be pure.
 */
export class Filter implements FilterOperator {
  readonly #input: FilterInput;
  readonly #predicate: (row: Row) => boolean;

  #output: FilterOutput = throwFilterOutput;

  constructor(input: FilterInput, predicate: (row: Row) => boolean) {
    this.#input = input;
    this.#predicate = predicate;
    input.setFilterOutput(this);
  }

  filter(node: Node) {
    if (this.#predicate(node.row)) {
      this.#output.filter(node);
    }
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

  *fetch(req: FetchRequest) {
    for (const node of this.#input.fetch(req)) {
      if (this.#predicate(node.row)) {
        yield node;
      }
    }
  }

  *cleanup(req: FetchRequest) {
    for (const node of this.#input.cleanup(req)) {
      if (this.#predicate(node.row)) {
        yield node;
      } else {
        drainStreams(node);
      }
    }
  }

  push(change: Change) {
    filterPush(change, this.#output, this.#predicate);
  }
}
