import {batch} from 'solid-js';
import type {ZeroAdvancedOptions} from '../../zero/src/advanced.ts';
import {Zero, type Schema, type ZeroOptions} from '../../zero/src/zero.ts';

export function createZero<S extends Schema>(options: ZeroOptions<S>): Zero<S> {
  const opts: ZeroAdvancedOptions<S> = {
    ...options,
    batchViewUpdates: batch,
  };
  return new Zero(opts);
}
