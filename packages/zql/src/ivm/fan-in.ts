import {assert} from '../../../shared/src/asserts.ts';
import {mergeIterables} from '../../../shared/src/iterables.ts';
import {must} from '../../../shared/src/must.ts';
import type {Change} from './change.ts';
import type {Node} from './data.ts';
import type {FanOut} from './fan-out.ts';
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
  readonly #schema: SourceSchema;
  #output: Output = throwOutput;

  constructor(fanOut: FanOut, inputs: Input[]) {
    this.#inputs = inputs;
    this.#schema = fanOut.getSchema();
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
    return this.#schema;
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
    this.#output.push(change);
  }
}
