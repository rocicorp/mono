import {makeEmptyIteratorWithReturn} from '../../../shared/src/iterables.ts';
import {must} from '../../../shared/src/must.ts';
import {emptyArray} from '../../../shared/src/sentinels.ts';
import {ChangeIndex} from './change-index.ts';
import type {Change} from './change.ts';
import type {Node} from './data.ts';
import type {FanIn} from './fan-in.ts';
import type {
  FilterInput,
  FilterOperator,
  FilterOutput,
} from './filter-operators.ts';
import type {InputBase} from './operator.ts';
import {andThen, eachStream, type Stream} from './stream.ts';

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

  filter(node: Node): IterableIterator<'yield', boolean> {
    const outputs = this.#outputs;
    for (let i = 0; i < outputs.length; i++) {
      const it = outputs[i].filter(node);
      const r = it.next();
      if (!r.done) {
        // A branch yielded: the rest is read lazily.
        return this.#filterRest(node, i, it);
      }
      if (r.value) {
        return emptyIteratorReturnTrue;
      }
    }
    return emptyIteratorReturnFalse;
  }

  /** filter() from the branch `i`, whose answer `it` has just yielded. */
  *#filterRest(
    node: Node,
    i: number,
    it: Iterator<'yield', boolean>,
  ): IterableIterator<'yield', boolean> {
    let answered = false;
    try {
      yield 'yield';
      for (;;) {
        const r = it.next();
        if (r.done) {
          answered = true;
          if (r.value) {
            return true;
          }
          break;
        }
        yield r.value;
      }
    } finally {
      // The consumer stopped reading while the branch was yielding.
      if (!answered) {
        it.return?.();
      }
    }
    const outputs = this.#outputs;
    for (i++; i < outputs.length; i++) {
      if (yield* outputs[i].filter(node)) {
        return true;
      }
    }
    return false;
  }

  push(change: Change): Stream<'yield'> {
    const outputs = this.#outputs;
    let i = 0;
    return andThen(
      eachStream(() =>
        i === outputs.length ? undefined : outputs[i++].push(change, this),
      ),
      () =>
        must(
          this.#fanIn,
          'fan-out must have a corresponding fan-in set!',
        ).fanOutDonePushingToAllBranches(change[ChangeIndex.TYPE]),
    );
  }

  reconcile(_pusher: InputBase): Stream<'yield'> {
    const outputs = this.#outputs;
    let i = 0;
    return andThen(
      eachStream(() => {
        while (i < outputs.length) {
          const output = outputs[i++];
          if (output.reconcile) {
            return output.reconcile(this);
          }
        }
        return undefined;
      }),
      () => (this.#fanIn ? this.#fanIn.fanOutDoneReconciling() : emptyArray),
    );
  }
}

const emptyIteratorReturnTrue = makeEmptyIteratorWithReturn(true);
const emptyIteratorReturnFalse = makeEmptyIteratorWithReturn(false);
