import {batch} from 'solid-js';
import {
  Zero,
  type CustomMutatorDefs,
  type Schema,
  type ZeroOptions,
} from '../../zero/src/zero.ts';

export function createZero<S extends Schema, MD extends CustomMutatorDefs<S>>(
  options: ZeroOptions<S, MD>,
): Zero<S, MD> {
  const opts = {
    ...options,
    batchViewUpdates: batch,
  };
  return new Zero(opts);
}
