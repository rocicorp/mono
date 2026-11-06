import type {InternalDiff} from './node.ts';
import type {BTreeRead} from './read.ts';

export function diff(
  oldMap: BTreeRead,
  newMap: BTreeRead,
): Promise<InternalDiff> {
  return newMap.diff(oldMap);
}
