import {must} from '../../../shared/src/must.ts';
import type {Change} from './change.ts';
import type {FanIn} from './fan-in.ts';
import type {Node} from './data.ts';
import type {
  FilterInput,
  FilterOperator,
  FilterOutput,
} from './filter-operators.ts';

/**
 * Forks a stream into multiple streams.
 * Is meant to be paired with a `FanIn` operator which will
 * later merge the forks back together.
 */
export class FanOut implements FilterOperator {
  readonly #input: FilterInput;
  readonly #outputs: FilterOutput[] = [];
  #fanIn: FanIn | undefined;
  #destroyCount: number = 0;

  constructor(input: FilterInput) {
    this.#input = input;
    input.setFilterOutput(this);
  }

  setFanIn(fanIn: FanIn) {
    this.#fanIn = fanIn;
  }

  setFilterOutput(output: FilterOutput): void {
    this.#outputs.push(output);
  }

  destroy(): void {
    if (this.#destroyCount < this.#outputs.length) {
      ++this.#destroyCount;
      if (this.#destroyCount === this.#outputs.length) {
        this.#input.destroy();
      }
    } else {
      throw new Error('FanOut already destroyed once for each output');
    }
  }

  getSchema() {
    return this.#input.getSchema();
  }

  beginFilter(): void {
    for (const output of this.#outputs) {
      output.beginFilter();
    }
  }

  endFilter(): void {
    for (const output of this.#outputs) {
      output.endFilter();
    }
  }

  *filter(node: Node): Generator<'yield', boolean> {
    let result = false;
    for (const output of this.#outputs) {
      result = (yield* output.filter(node)) || result;
      if (result) {
        return true;
      }
    }
    return result;
  }

  *push(change: Change) {
    for (const out of this.#outputs) {
      yield* out.push(change, this);
    }
    yield* must(
      this.#fanIn,
      'fan-out must have a corresponding fan-in set!',
    ).fanOutDonePushingToAllBranches(change.type);
  }
}
