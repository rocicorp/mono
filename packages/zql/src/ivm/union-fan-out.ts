import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import type {Change} from './change.ts';
import type {Node} from './data.ts';
import type {FetchRequest, Input, Operator, Output} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';
import type {UnionFanIn} from './union-fan-in.ts';

export class UnionFanOut implements Operator {
  #destroyCount: number = 0;
  #unionFanIn?: UnionFanIn;
  readonly #input: Input;
  readonly #outputs: Output[] = [];

  constructor(input: Input) {
    this.#input = input;
    input.setOutput(this);
  }

  setFanIn(fanIn: UnionFanIn) {
    assert(!this.#unionFanIn, 'FanIn already set for this FanOut');
    this.#unionFanIn = fanIn;
  }

  *push(change: Change): Stream<'yield'> {
    must(this.#unionFanIn).fanOutStartedPushing();
    for (const output of this.#outputs) {
      yield* output.push(change, this);
    }
    yield* must(this.#unionFanIn).fanOutDonePushing(change.type);
  }

  setOutput(output: Output): void {
    this.#outputs.push(output);
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  fetch(req: FetchRequest): Stream<Node | 'yield'> {
    return this.#input.fetch(req);
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
}
