import {DifferenceStream, Listener} from '../difference-stream.js';
import {PullMsg} from '../message.js';
import {Operator} from './operator.js';

/**
 * A dataflow operator (node) that has many incoming edges and
 * one outgoing edge (write handle). It just sends all the input messages from
 * all the incoming operator to the output operators.
 */
export class ConcatOperator<T extends object> implements Operator {
  readonly #listener: Listener<T>;
  readonly #inputs: DifferenceStream<T>[];
  readonly #output: DifferenceStream<T>;

  constructor(inputs: DifferenceStream<T>[], output: DifferenceStream<T>) {
    this.#inputs = inputs;
    this.#output = output;
    this.#listener = {
      newDifference: (version, data) => {
        output.newData(version, data);
      },
      commit: version => {
        this.commit(version);
      },
    };
    for (const input of inputs) {
      input.addDownstream(this.#listener);
    }
  }

  commit(version: number): void {
    this.#output.commit(version);
  }

  messageUpstream(message: PullMsg): void {
    for (const input of this.#inputs) {
      input.messageUpstream(message, this.#listener);
    }
  }

  destroy() {
    for (const input of this.#inputs) {
      input.removeDownstream(this.#listener);
    }
  }
}
