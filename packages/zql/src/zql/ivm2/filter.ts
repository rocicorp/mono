import {assert} from 'shared/src/asserts.js';
import {Change} from './change.js';
import {Node, Row} from './data.js';
import {FetchRequest, Input, Operator, Output} from './operator.js';
import {Stream} from './stream.js';

/**
 * The Filter operator filters data through a predicate. It is stateless.
 *
 * The predicate must be pure.
 */
export class Filter implements Operator {
  readonly #input: Input;
  readonly #predicate: (row: Row) => boolean;

  #output: Output | undefined;

  constructor(input: Input, predicate: (row: Row) => boolean) {
    this.#input = input;
    this.#predicate = predicate;
    this.#input.setOutput(this);
  }

  setOutput(output: Output) {
    this.#output = output;
  }

  destroy(): void {
    this.#input.destroy();
  }

  getSchema() {
    return this.#input.getSchema();
  }

  *fetch(req: FetchRequest) {
    // In the future this should hoist the filters up to SQLite via "optionalFilters".
    // Waiting on hydrate/fetch merge.
    for (const node of this.#input.fetch(req)) {
      if (this.#predicate(node.row)) {
        yield node;
      }
    }
  }

  cleanup(req: FetchRequest): Stream<Node> {
    return this.fetch(req);
  }

  push(change: Change) {
    assert(this.#output, 'Output not set');

    const row =
      change.type === 'add' || change.type === 'remove'
        ? change.node.row
        : change.row;
    if (this.#predicate(row)) {
      this.#output.push(change);
    }
  }
}
