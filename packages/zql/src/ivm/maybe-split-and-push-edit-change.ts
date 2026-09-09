import type {Row} from '../../../zero-protocol/src/data.ts';
import {ChangeIndex} from './change-index.ts';
import {makeAddChange, makeRemoveChange, type EditChange} from './change.ts';
import type {InputBase, Output} from './operator.ts';
import {EMPTY_YIELDS, type Stream} from './stream.ts';

/**
 * This takes an {@linkcode EditChange} and a predicate that determines if a row
 * should be present based on the row's data. It then splits the change and
 * pushes the appropriate changes to the output based on the predicate.
 */
export function maybeSplitAndPushEditChange(
  change: EditChange,
  predicate: (row: Row) => boolean,
  output: Output,
  pusher: InputBase,
): Stream<'yield'> {
  const oldWasPresent = predicate(change[ChangeIndex.OLD_NODE].row);
  const newIsPresent = predicate(change[ChangeIndex.NODE].row);

  if (oldWasPresent && newIsPresent) {
    return output.push(change, pusher);
  }
  if (oldWasPresent) {
    return output.push(makeRemoveChange(change[ChangeIndex.OLD_NODE]), pusher);
  }
  if (newIsPresent) {
    return output.push(makeAddChange(change[ChangeIndex.NODE]), pusher);
  }
  return EMPTY_YIELDS;
}
