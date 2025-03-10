import {must} from '../../../shared/src/must.ts';
import type {Change} from './change.ts';
import type {FanIn} from './fan-in.ts';
import type {FetchRequest, Input, Operator, Output} from './operator.ts';

/**
 * Forks a stream into multiple streams.
 * Is meant to be paired with a `FanIn` operator which will
 * later merge the forks back together.
 */
export class FanOut implements Operator {
  readonly #input: Input;
  readonly #outputs: Output[] = [];
  #fanIn: FanIn | undefined;
  #destroyCount: number = 0;

  constructor(input: Input) {
    this.#input = input;
    input.setOutput(this);
  }

  setFanIn(fanIn: FanIn) {
    this.#fanIn = fanIn;
  }

  setOutput(output: Output): void {
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

  fetch(req: FetchRequest) {
    return this.#input.fetch(req);
  }

  cleanup(req: FetchRequest) {
    return this.#input.cleanup(req);
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
