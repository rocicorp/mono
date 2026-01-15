import {assert} from '../../../shared/src/asserts.ts';
import {emptyArray, identity} from '../../../shared/src/sentinels.ts';
import type {Change} from './change.ts';
import {type Node} from './data.ts';
import type {FanOut} from './fan-out.ts';
import {
  throwFilterOutput,
  type FilterInput,
  type FilterOperator,
  type FilterOutput,
} from './filter-operators.ts';
import {pushAccumulatedChanges} from './push-accumulated.ts';
import type {SourceSchema} from './schema.ts';

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
export class FanIn implements FilterOperator {
  readonly #inputs: readonly FilterInput[];
  readonly #schema: SourceSchema;
  #output: FilterOutput = throwFilterOutput;
  #accumulatedPushes: Change[] = [];

  constructor(fanOut: FanOut, inputs: FilterInput[]) {
    this.#inputs = inputs;
    this.#schema = fanOut.getSchema();
    for (const input of inputs) {
      input.setFilterOutput(this);
      assert(this.#schema === input.getSchema(), `Schema mismatch in fan-in`);
    }
  }

  setFilterOutput(output: FilterOutput): void {
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

  beginFilter(): void {
    this.#output.beginFilter();
  }

  endFilter(): void {
    this.#output.endFilter();
  }

  *filter(node: Node): Generator<'yield', boolean> {
    return yield* this.#output.filter(node);
  }

  push(change: Change) {
    this.#accumulatedPushes.push(change);
    return emptyArray;
  }

  *fanOutDonePushingToAllBranches(fanOutChangeType: Change['type']) {
    if (this.#inputs.length === 0) {
      assert(
        this.#accumulatedPushes.length === 0,
        'If there are no inputs then fan-in should not receive any pushes.',
      );
      return;
    }

    yield* pushAccumulatedChanges(
      this.#accumulatedPushes,
      this.#output,
      this,
      fanOutChangeType,
      identity,
      identity,
    );
  }
}
