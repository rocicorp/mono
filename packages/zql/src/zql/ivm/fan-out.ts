import type {Change} from './change.js';
import type {FetchRequest, Input, Operator, Output} from './operator.js';

export class FanOut implements Operator {
  readonly #input: Input;
  readonly #outputs: Output[];
  #fanInReceivedPush: boolean;

  constructor(input: Input) {
    this.#input = input;
    this.#input.setOutput(this);
    this.#outputs = [];
    this.#fanInReceivedPush = false;
  }

  setOutput(output: Output): void {
    this.#outputs.push(output);
  }

  destroy(): void {
    this.#input.destroy();
  }

  getSchema() {
    return this.#input.getSchema();
  }

  *fetch(req: FetchRequest) {
    for (const node of this.#input.fetch(req)) {
      yield node;
    }
  }

  cleanup(req: FetchRequest) {
    return this.fetch(req);
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
