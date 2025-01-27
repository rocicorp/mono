import {batch} from 'solid-js';
import type {ZeroAdvancedOptions} from '../../zero-advanced/src/mod.ts';
import {Zero, type ZeroOptions} from '../../zero-client/src/mod.ts';
import type {Schema} from '../../zero-schema/src/mod.ts';

export function createZero<S extends Schema>(options: ZeroOptions<S>): Zero<S> {
  const opts: ZeroAdvancedOptions<S> = {
    ...options,
    batchViewUpdates: batch,
  };
  return new Zero(opts);
}
