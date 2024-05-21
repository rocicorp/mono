import {assert} from 'shared/src/asserts.js';
import type {Selector} from '../../../ast/ast.js';
import type {Multiset} from '../../multiset.js';
import type {PipelineEntity, Version} from '../../types.js';
import type {DifferenceStream, Listener} from '../difference-stream.js';

import type {Reply, Request} from '../message.js';
import {OperatorBase} from './operator.js';

export class JoinOperatorBase<
  AValue extends PipelineEntity,
  BValue extends PipelineEntity,
  O extends PipelineEntity,
> extends OperatorBase<O> {
  readonly #listenerA: Listener<AValue>;
  readonly #inputA: DifferenceStream<AValue>;
  readonly #listenerB: Listener<BValue>;
  readonly #inputB: DifferenceStream<BValue>;
  readonly #fn: (
    v: Version,
    inputA: Multiset<AValue> | undefined,
    inputB: Multiset<BValue> | undefined,
  ) => Multiset<O>;
  readonly #output: DifferenceStream<O>;
  readonly #buffer: {
    aMsg: Reply | undefined;
    bMsg: Reply | undefined;
    inputA: Multiset<AValue> | undefined;
    inputB: Multiset<BValue> | undefined;
  } = {
    aMsg: undefined,
    bMsg: undefined,
    inputA: undefined,
    inputB: undefined,
  };
  readonly #aJoinColumn;

  constructor(
    inputA: DifferenceStream<AValue>,
    inputB: DifferenceStream<BValue>,
    output: DifferenceStream<O>,
    fn: (
      v: Version,
      inputA: Multiset<AValue> | undefined,
      inputB: Multiset<BValue> | undefined,
    ) => Multiset<O>,
    aJoinColumn: Selector,
  ) {
    super(output);
    this.#fn = fn;
    this.#output = output;
    this.#aJoinColumn = aJoinColumn;
    this.#listenerA = {
      newDifference: this.#onInputADifference,
      commit: version => {
        this.commit(version);
      },
    };
    this.#listenerB = {
      newDifference: this.#onInputBDifference,
      commit: version => {
        this.commit(version);
      },
    };
    inputA.addDownstream(this.#listenerA);
    inputB.addDownstream(this.#listenerB);
    this.#inputA = inputA;
    this.#inputB = inputB;
  }

  #onInputADifference = (
    version: Version,
    data: Multiset<AValue>,
    reply: Reply | undefined,
  ) => {
    if (reply !== undefined) {
      if (this.#buffer.inputB !== undefined) {
        this.#output.newDifference(
          version,
          this.#fn(version, data, this.#buffer.inputB),
          this.#getReply(reply),
        );
        this.#buffer.inputB = undefined;
        this.#buffer.bMsg = undefined;
      } else {
        this.#bufferA(data, reply);
      }
    } else {
      this.#output.newDifference(
        version,
        this.#fn(version, data, undefined),
        undefined,
      );
    }
  };

  #onInputBDifference = (
    version: Version,
    data: Multiset<BValue>,
    reply: Reply | undefined,
  ) => {
    if (reply !== undefined) {
      if (this.#buffer.inputA !== undefined) {
        this.#output.newDifference(
          version,
          this.#fn(version, this.#buffer.inputA, data),
          this.#getReply(),
        );
        this.#buffer.inputA = undefined;
        this.#buffer.aMsg = undefined;
      } else {
        this.#bufferB(data, reply);
      }
    } else {
      this.#output.newDifference(
        version,
        this.#fn(version, undefined, data),
        undefined,
      );
    }
  };

  // This is a current short-cut.
  // In reality, we should allow the implementation of `join` to re-order
  // the loop and pick the correct `reply` message based on the re-ordering.
  // Currently we always place input `a` as the outer loop.
  #getReply(aMsg?: Reply | undefined): Reply {
    let msg = aMsg ?? this.#buffer.aMsg;

    assert(msg !== undefined, 'aMsg must be defined');

    // This is a current short-cut.
    // In reality, we would assign `aJoinColumn` as a contiguous group
    // whenever it is represents a unique key.
    //
    // Joins against joins against joins... can only add rows to an already
    // contiguous group so we don't have to worry about that.
    if (this.#aJoinColumn[1] === 'id') {
      msg = {
        ...msg,
        contiguousGroup: [this.#aJoinColumn],
      };
    }

    return msg;
  }

  #bufferA(inputA: Multiset<AValue> | undefined, aMsg: Reply) {
    assert(inputA !== undefined, 'inputA must be defined');
    assert(this.#buffer.inputA === undefined, 'a must not already be buffered');
    this.#buffer.aMsg = aMsg;
    this.#buffer.inputA = inputA;
  }

  #bufferB(inputB: Multiset<BValue> | undefined, bMsg: Reply) {
    assert(inputB !== undefined, 'inputB must be defined');
    assert(this.#buffer.inputB === undefined, 'b must not already be buffered');
    this.#buffer.bMsg = bMsg;
    this.#buffer.inputB = inputB;
  }

  messageUpstream(message: Request): void {
    // join will only make use of `inputA` ordering at the moment
    // so strip order from `b` message.
    const bMessage: Request = {
      ...message,
      order: undefined,
    };
    this.#inputA.messageUpstream(message, this.#listenerA);
    this.#inputB.messageUpstream(bMessage, this.#listenerB);
  }

  destroy() {
    this.#inputA.removeDownstream(this.#listenerA);
    this.#inputB.removeDownstream(this.#listenerB);
  }
}
