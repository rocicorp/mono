import {Multiset} from '../../multiset.js';
import {Version} from '../../types.js';
import {DifferenceStream, Listener} from '../difference-stream.js';
import {Request} from '../message.js';
import {Operator} from './operator.js';

/**
 * Operator that only takes a single argument
 */
export class UnaryOperator<I extends object, O extends object>
  implements Operator
{
  readonly #listener: Listener<I>;
  readonly #input: DifferenceStream<I>;
  readonly #output: DifferenceStream<O>;

  constructor(
    input: DifferenceStream<I>,
    output: DifferenceStream<O>,
    fn: (version: Version, data: Multiset<I>) => Multiset<O>,
  ) {
    this.#listener = {
      newDifference: (version, data) => {
        output.newData(version, fn(version, data));
      },
      commit: version => {
        this.commit(version);
      },
    };
    input.addDownstream(this.#listener);
    this.#input = input;
    this.#output = output;
  }

  commit(version: Version): void {
    this.#output.commit(version);
  }

  messageUpstream(message: Request): void {
    this.#input.messageUpstream(message, this.#listener);
  }

  destroy() {
    this.#input.removeDownstream(this.#listener);
  }
}
