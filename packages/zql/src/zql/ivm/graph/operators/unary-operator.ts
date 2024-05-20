import type {Multiset} from '../../multiset.js';
import type {PipelineEntity, Version} from '../../types.js';
import type {DifferenceStream, Listener} from '../difference-stream.js';
import type {Request} from '../message.js';
import type {Operator} from './operator.js';
import {OperatorBase} from './operator.js';

/**
 * Operator that only takes a single argument
 */
export class UnaryOperator<I extends PipelineEntity, O extends PipelineEntity>
  extends OperatorBase<O>
  implements Operator
{
  readonly #listener: Listener<I>;
  readonly #input: DifferenceStream<I>;

  constructor(
    input: DifferenceStream<I>,
    output: DifferenceStream<O>,
    fn: (version: Version, data: Multiset<I>) => Multiset<O>,
  ) {
    super(output);
    this.#listener = {
      newDifference: (version, data, reply) => {
        output.newDifference(version, fn(version, data), reply);
      },
      commit: version => {
        this.commit(version);
      },
    };
    input.addDownstream(this.#listener);
    this.#input = input;
  }

  messageUpstream(message: Request): void {
    this.#input.messageUpstream(message, this.#listener);
  }

  destroy() {
    this.#input.removeDownstream(this.#listener);
  }
}
