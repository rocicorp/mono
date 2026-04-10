import type {Row} from '../../../zero-protocol/src/data.ts';
import {ChangeIndex} from './change-index.ts';
import {makeAddChange, makeRemoveChange, type EditChange} from './change.ts';
import type {InputBase, Output} from './operator.ts';

/**
 * This takes an {@linkcode EditChange} and a predicate that determines if a row
 * should be present based on the row's data. It then splits the change and
 * pushes the appropriate changes to the output based on the predicate.
 */
export function* maybeSplitAndPushEditChange(
  change: EditChange,
  predicate: (row: Row) => boolean,
  output: Output,
  pusher: InputBase,
) {
  const oldWasPresent = predicate(change[ChangeIndex.OLD_NODE].row);
  const newIsPresent = predicate(change[ChangeIndex.NODE].row);

  if (oldWasPresent && newIsPresent) {
    yield* output.push(change, pusher);
  } else if (oldWasPresent && !newIsPresent) {
    yield* output.push(makeRemoveChange(change[ChangeIndex.OLD_NODE]), pusher);
  } else if (!oldWasPresent && newIsPresent) {
    yield* output.push(makeAddChange(change[ChangeIndex.NODE]), pusher);
  }
}
