import {asyncIterableToArray} from '../async-iterable-to-array.js';
import type {InternalDiff} from './node.js';
import type {BTreeRead} from './read.js';

export function diff(
  oldMap: BTreeRead,
  newMap: BTreeRead,
): Promise<InternalDiff> {
  // Return an array to ensure we do not compute the diff more than once.
  return asyncIterableToArray(newMap.diff(oldMap));
}
