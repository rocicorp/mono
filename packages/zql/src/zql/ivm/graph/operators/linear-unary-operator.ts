import {Multiset} from '../../multiset.js';
import {Version} from '../../types.js';
import {DifferenceStream} from '../difference-stream.js';
import {UnaryOperator} from './unary-operator.js';

/**
 * Linear operator in that:
 * L(a + b) = L(a) + L(b)
 *
 * In other words, we can compute L(b - a) without computing L(a) first. `a`
 * represents some prior state of the database. `b - a` is a diff to the database.
 *
 * - map, filter, concat are linear
 *
 * Reduce and join are non-linear since they must use the prior state `a`, not just the diff `b - a`, in their
 * computations.
 */
export class LinearUnaryOperator<
  I extends object,
  O extends object,
> extends UnaryOperator<I, O> {
  constructor(
    input: DifferenceStream<I>,
    output: DifferenceStream<O>,
    f: (input: Multiset<I>) => Multiset<O>,
  ) {
    super(input, output, (_v: Version, data: Multiset<I>) => f(data));
  }
}
