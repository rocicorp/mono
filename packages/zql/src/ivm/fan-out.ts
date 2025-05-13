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
      if (this.#destroyCount === 0) {
        this.#input.destroy();
      }
      ++this.#destroyCount;
    } else {
      throw new Error('FanOut already destroyed once for each output');
    }
  }

  getSchema() {
    return this.#input.getSchema();
  }

  filter(node: Node, cleanup: boolean): boolean {
    let result = false;
    for (const output of this.#outputs) {
      result = output.filter(node, cleanup) || result;
      // Cleanup needs to be forwarded to all outputs, don't short circuit
      // cleanup.  For non-cleanup we can short-circuit on first true.
      if (!cleanup && result) {
        return true;
      }
    }
    return result;
  }

  push(change: Change) {
    for (const out of this.#outputs) {
      out.push(change);
    }
    must(
      this.#fanIn,
      'fan-out must have a corresponding fan-in set!',
    ).fanOutDonePushingToAllBranches(change.type);
  }
}
