import {makeEmptyIteratorWithReturn} from '../../../shared/src/iterables.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Change} from './change.ts';
import {type Node} from './data.ts';
import {
  throwFilterOutput,
  type FilterInput,
  type FilterOperator,
  type FilterOutput,
} from './filter-operators.ts';
import {filterPush} from './filter-push.ts';
import type {InputBase} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';

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

  beginFilter(): void {
    this.#output.beginFilter();
  }

  endFilter(): void {
    this.#output.endFilter();
  }

  filter(node: Node): IterableIterator<'yield', boolean> {
    if (this.#predicate(node.row)) {
      return this.#output.filter(node);
    }
    return emptyIteratorReturnFalse;
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
    return filterPush(change, this.#output, this, this.#predicate);
  }

  *reconcile(_pusher: InputBase): Stream<'yield'> {
    if (this.#output.reconcile) {
      yield* this.#output.reconcile(this);
    }
  }
}

const emptyIteratorReturnFalse = makeEmptyIteratorWithReturn(false);
