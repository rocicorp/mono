import {genMap} from '../../../util/iterables.js';
import type {Multiset} from '../../multiset.js';
import type {PipelineEntity, Version} from '../../types.js';
import type {DifferenceStream} from '../difference-stream.js';
import {UnaryOperator} from './unary-operator.js';

export class MapOperator<
  I extends PipelineEntity,
  O extends PipelineEntity,
> extends UnaryOperator<I, O> {
  constructor(
    input: DifferenceStream<I>,
    output: DifferenceStream<O>,
    f: (input: I) => O,
  ) {
    const inner = (_version: Version, collection: Multiset<I>) =>
      genMap(
        collection,
        ([value, multiplicity]) => [f(value), multiplicity] as const,
      );
    super(input, output, inner);
  }
}
