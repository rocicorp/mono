import {batch} from 'solid-js';
import {
  Zero,
  type Schema,
  type ZeroOptions,
} from '../../zero-client/src/mod.js';
import type {ZeroAdvancedOptions} from '../../zero-advanced/src/mod.js';

export function createZero<S extends Schema>(options: ZeroOptions<S>): Zero<S> {
  const opts: ZeroAdvancedOptions<S> = {
    ...options,
    batchViewUpdates: batch,
  };
  return new Zero(opts);
}
