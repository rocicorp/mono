import {asyncIterableToArray} from '../async-iterable-to-array.ts';
import type {InternalDiff} from './node.ts';
import type {BTreeRead} from './read.ts';

export function diff(
  oldMap: BTreeRead,
  newMap: BTreeRead,
): Promise<InternalDiff> {
  // Return an array to ensure we do not compute the diff more than once.
  return asyncIterableToArray(newMap.diff(oldMap));
}
