import type {Change} from './change.ts';
import {
  InputBase,
  type FetchRequest,
  type Input,
  type Operator,
  type Output,
} from './operator.ts';

/**
 * Forks a stream into multiple streams.
 * Is meant to be paired with a `FanIn` operator which will
 * later merge the forks back together.
 */
export class FanOut extends InputBase implements Operator {
  readonly #input: Input;
  readonly #outputs: Output[] = [];
  // FanOut is paired with a FanIn.
  // Once FanIn has received a push from FanOut along
  // any branch, FanOut no longer needs to push that value
  // across the rest of its outputs..
  #fanInReceivedPush: boolean = false;
  #destroyCount: number = 0;

  constructor(input: Input) {
    super([input]);
    this.#input = input;
    input.setOutput(this);
  }

  setOutput(output: Output): void {
    this.#outputs.push(output);
  }

  getOutputs(): Output[] {
    return this.#outputs;
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

  // call this with the change type?
  // if type matches, we can short-circuit.
  // if type does not match... fanIn needs to wait
  // for next branch to push?
  // or collect all branches before pushing?
  onFanInReceivedPush() {
    this.#fanInReceivedPush = true;
  }

  push(change: Change) {
    this.#fanInReceivedPush = false;
    for (const out of this.#outputs) {
      out.push(change);
      if (this.#fanInReceivedPush) {
        return;
      }
    }
  }
}
