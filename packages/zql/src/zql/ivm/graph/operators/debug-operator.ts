import {Multiset} from '../../multiset.js';
import {Version} from '../../types.js';
import {DifferenceStream} from '../difference-stream.js';
import {UnaryOperator} from './unary-operator.js';

/**
 * Allows someone to observe all data flowing through a spot
 * in a pipeline. Forwards the data with no changes made to it.
 */
export class DebugOperator<T extends object> extends UnaryOperator<T, T> {
  constructor(
    input: DifferenceStream<T>,
    output: DifferenceStream<T>,
    onMessage: (v: Version, data: Multiset<T>) => void,
  ) {
    const inner = (version: Version, data: Multiset<T>) => {
      onMessage(version, data);
      return data;
    };
    super(input, output, inner);
  }
}
