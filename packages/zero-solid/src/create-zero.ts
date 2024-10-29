import {batch} from 'solid-js';
import {
  Zero,
  type Schema,
  type ZeroOptions,
} from '../../zero-client/src/mod.js';

export function createZero<S extends Schema>(options: ZeroOptions<S>): Zero<S> {
  return new Zero({
    ...options,
    batchViewChanges: batch,
  });
}
