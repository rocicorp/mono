import {genMap} from '../../../util/iterables.js';
import {Multiset} from '../../multiset.js';
import {DifferenceStream} from '../difference-stream.js';
import {LinearUnaryOperator} from './linear-unary-operator.js';

export class MapOperator<
  I extends object,
  O extends object,
> extends LinearUnaryOperator<I, O> {
  constructor(
    input: DifferenceStream<I>,
    output: DifferenceStream<O>,
    f: (input: I) => O,
  ) {
    const inner = (collection: Multiset<I>) =>
      genMap(
        collection,
        ([value, multiplicity]) => [f(value), multiplicity] as const,
      );
    super(input, output, inner);
  }
}
