import {Multiset} from '../../multiset.js';
import {Version} from '../../types.js';
import {DifferenceStream} from '../difference-stream.js';
import {UnaryOperator} from './unary-operator.js';

/**
 * Runs an effect _after_ a transaction has been committed.
 *
 * This is intended to let users introduce side-effects
 * to be run on changes to a query without having to materialize the query
 * results.
 */
export class DifferenceEffectOperator<T extends object> extends UnaryOperator<
  T,
  T
> {
  readonly #f: (input: T, mult: number) => void;
  #collected: Multiset<T>[] = [];

  constructor(
    input: DifferenceStream<T>,
    output: DifferenceStream<T>,
    f: (input: T, mult: number) => void,
  ) {
    const inner = (_version: Version, data: Multiset<T>) => {
      this.#collected.push(data);
      return data;
    };
    super(input, output, inner);
    this.#f = f;
  }

  commit(v: number): void {
    const collected = this.#collected;
    this.#collected = [];
    for (const collection of collected) {
      for (const [val, mult] of collection) {
        this.#f(val, mult);
      }
    }
    super.commit(v);
  }
}
