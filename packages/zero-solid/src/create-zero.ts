import {batch} from 'solid-js';
import {Zero, type Schema, type ZeroOptions} from '../../zero/src/zero.ts';

export function createZero<S extends Schema>(options: ZeroOptions<S>): Zero<S> {
  const opts = {
    ...options,
    batchViewUpdates: batch,
  };
  return new Zero(opts);
}
