import type {Change} from './change.js';
import type {FetchRequest, Input, Operator, Output} from './operator.js';

/**
 * Forks a stream into multiple streams.
 * Is meant to be paired with a `FanIn` operator which will
 * later merge the forks back together.
 */
export class FanOut implements Operator {
  readonly #input: Input;
  readonly #outputs: Output[];
  #fanInReceivedPush: boolean;
  #destroyCount: number;

  constructor(input: Input) {
    this.#input = input;
    this.#input.setOutput(this);
    this.#outputs = [];
    this.#fanInReceivedPush = false;
    this.#destroyCount = 0;
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
