import type {Multiset} from '../../multiset.js';
import type {Version} from '../../types.js';
import type {DifferenceStream, Listener} from '../difference-stream.js';

import type {Reply, Request} from '../message.js';
import {OperatorBase} from './operator.js';

function convertReply(reply: Reply | undefined): Reply | undefined {
  if (reply !== undefined) {
    return {
      ...reply,
      // Join does not yet respect order coming from a source.
      order: undefined,
    };
  }
  return reply;
}

export class JoinOperatorBase<
  I1 extends object,
  I2 extends object,
  O extends object,
> extends OperatorBase<O> {
  readonly #listener1: Listener<I1>;
  readonly #input1: DifferenceStream<I1>;
  readonly #listener2: Listener<I2>;
  readonly #input2: DifferenceStream<I2>;

  constructor(
    input1: DifferenceStream<I1>,
    input2: DifferenceStream<I2>,
    output: DifferenceStream<O>,
    fn: (
      v: Version,
      inputA: Multiset<I1> | undefined,
      inputB: Multiset<I2> | undefined,
    ) => Multiset<O>,
  ) {
    super(output);
    this.#listener1 = {
      newDifference: (version, data, reply) => {
        output.newDifference(
          version,
          fn(version, data, undefined),
          convertReply(reply),
        );
      },
      commit: version => {
        this.commit(version);
      },
    };
    this.#listener2 = {
      newDifference: (version, data, reply) => {
        output.newDifference(
          version,
          fn(version, undefined, data),
          convertReply(reply),
        );
      },
      commit: version => {
        this.commit(version);
      },
    };
    input1.addDownstream(this.#listener1);
    input2.addDownstream(this.#listener2);
    this.#input1 = input1;
    this.#input2 = input2;
  }

  messageUpstream(message: Request): void {
    // join won't make use of the order yet
    // so don't pass it on.
    const newMessage: Request = {
      ...message,
      order: undefined,
    };
    this.#input1.messageUpstream(newMessage, this.#listener1);
    this.#input2.messageUpstream(newMessage, this.#listener2);
  }

  destroy() {
    this.#input1.removeDownstream(this.#listener1);
    this.#input2.removeDownstream(this.#listener2);
  }
}
