import {assert} from '../../../shared/src/asserts.js';
import {mergeIterables} from '../../../shared/src/iterables.js';
import {must} from '../../../shared/src/must.js';
import type {Change} from './change.js';
import type {Node} from './data.js';
import type {FanOut} from './fan-out.js';
import type {FetchRequest, Input, Operator, Output} from './operator.js';
import type {TableSchema} from './schema.js';
import type {Stream} from './stream.js';

/**
 * The FanIn operator merges multiple streams into one.
 * It eliminates duplicates and must be paired with a fan-out operator
 * somewhere upstream of the fan-in.
 *
 *  issue
 *    |
 * fan-out
 * /      \
 * a      b
 *  \    /
 * fan-in
 *   |
 */
export class FanIn implements Operator {
  readonly #inputs: readonly Input[];
  readonly #fanOut: FanOut;
  #output: Output | undefined;
  #schema: TableSchema | undefined;

  constructor(fanOut: FanOut, inputs: [Input, ...Input[]]) {
    this.#inputs = inputs;
    this.#schema = inputs[0].getSchema();
    this.#fanOut = fanOut;
    for (const input of inputs) {
      input.setOutput(this);
      assert(this.#schema === input.getSchema(), `Schema mismatch in fan-in`);
    }
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  destroy(): void {
    for (const input of this.#inputs) {
      input.destroy();
    }
  }

  getSchema() {
    return this.#inputs[0].getSchema();
  }

  fetch(req: FetchRequest): Stream<Node> {
    return this.#fetchOrCleanup(input => input.fetch(req));
  }

  cleanup(req: FetchRequest): Stream<Node> {
    return this.#fetchOrCleanup(input => input.cleanup(req));
  }

  *#fetchOrCleanup(streamProvider: (input: Input) => Stream<Node>) {
    const iterables = this.#inputs.map(input => streamProvider(input));
    yield* mergeIterables(
      iterables,
      (l, r) => must(this.#schema).compareRows(l.row, r.row),
      true,
    );
  }

  push(change: Change) {
    this.#fanOut.onFanInReceivedPush();
    this.#output?.push(change);
  }
}
