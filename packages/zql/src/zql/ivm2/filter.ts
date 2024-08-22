import {assert} from 'shared/src/asserts.js';
import {Change} from './change.js';
import {Node, Row} from './data.js';
import {
  FetchRequest,
  HydrateRequest,
  Input,
  Operator,
  Output,
} from './operator.js';
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
  }

  setOutput(output: Output) {
    this.#output = output;
  }

  getSchema(_output: Output) {
    return this.#input.getSchema(this);
  }

  hydrate(req: HydrateRequest, output: Output): Stream<Node> {
    return this.fetch(req, output);
  }

  *fetch(req: FetchRequest, _output: Output) {
    // In the future this should hoist the filters up to SQLite via "optionalFilters".
    // Waiting on hydrate/fetch merge.
    for (const node of this.#input.hydrate(req, this)) {
      if (this.#predicate(node.row)) {
        yield node;
      }
    }
  }

  dehydrate(req: HydrateRequest, output: Output): Stream<Node> {
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
