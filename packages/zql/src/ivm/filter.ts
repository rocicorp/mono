import {assert, unreachable} from '../../../shared/src/asserts.js';
import type {Row} from '../../../zero-protocol/src/data.js';
import type {Change} from './change.js';
import type {Node} from './data.js';
import {maybeSplitAndPushEditChange} from './maybe-split-and-push-edit-change.js';
import type {FetchRequest, Input, Operator, Output} from './operator.js';
import type {TableSchema} from './schema.js';
import type {Stream} from './stream.js';

/**
 * Apply filter to all data flowing through, or just pushes. Pipeline builder
 * can hoist some filters up to source, so it will tell Filter to apply only to
 * pushes in that case.
 */
export type Mode = 'all' | 'push-only';

/**
 * The Filter operator filters data through a predicate. It is stateless.
 *
 * The predicate must be pure.
 */
export class Filter implements Operator {
  readonly #input: Input;
  readonly #mode: Mode;
  readonly #predicate: (row: Row) => boolean;

  #output: Output | undefined;

  constructor(input: Input, mode: Mode, predicate: (row: Row) => boolean) {
    this.#input = input;
    this.#mode = mode;
    this.#predicate = predicate;
    this.#input.setOutput(this);
  }

  setOutput(output: Output) {
    this.#output = output;
  }

  destroy(): void {
    this.#input.destroy();
  }

  getSchema(): TableSchema {
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
      if (this.#mode === 'push-only' || this.#predicate(node.row)) {
        yield node;
      }
    }
  }

  push(change: Change) {
    assert(this.#output, 'Output not set');

    switch (change.type) {
      case 'add':
      case 'remove':
        if (this.#predicate(change.node.row)) {
          this.#output.push(change);
        }
        break;
      case 'child':
        if (this.#predicate(change.row)) {
          this.#output.push(change);
        }
        break;
      case 'edit':
        maybeSplitAndPushEditChange(change, this.#predicate, this.#output);
        break;
      default:
        unreachable(change);
    }
  }
}
