import type {Version} from '../../types.js';
import type {DifferenceStream} from '../difference-stream.js';
import type {Request} from '../message.js';

export interface Operator {
  /**
   * Notify along the graph that the transaction
   * has been committed
   */
  commit(version: Version): void;
  messageUpstream(message: Request): void;
  destroy(): void;
}

export class NoOp implements Operator {
  constructor() {}
  commit(_v: Version): void {}
  messageUpstream(): void {}
  destroy(): void {}
}

/**
 * A dataflow operator (node) that has many incoming edges (stream) and one outgoing edge (stream).
 */
export abstract class OperatorBase<O extends object> implements Operator {
  // downstream output
  readonly #output: DifferenceStream<O>;
  #lastCommit = -1;

  constructor(output: DifferenceStream<O>) {
    this.#output = output;
  }

  commit(v: Version) {
    if (v <= this.#lastCommit) {
      return;
    }
    this.#lastCommit = v;
    this.#output.commit(v);
  }

  abstract messageUpstream(message: Request): void;
  abstract destroy(): void;
}
