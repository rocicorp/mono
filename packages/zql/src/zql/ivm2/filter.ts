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

  getSchema(_output: Output) {
    return this.#input.getSchema(this);
  }

  *fetch(req: FetchRequest, _output: Output) {
    // In the future this should hoist the filters up to SQLite via "optionalFilters".
    // Waiting on hydrate/fetch merge.
    for (const node of this.#input.fetch(req, this)) {
      if (this.#predicate(node.row)) {
        yield node;
      }
    }
  }

  cleanup(req: FetchRequest, output: Output): Stream<Node> {
    return this.fetch(req, output);
  }

  push(change: Change, _input: Input) {
    assert(this.#output, 'Output not set');

    const row =
      change.type === 'add' || change.type === 'remove'
        ? change.node.row
        : change.row;
    if (this.#predicate(row)) {
      this.#output.push(change, this);
    }
  }
}
