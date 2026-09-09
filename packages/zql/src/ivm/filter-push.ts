import {unreachable} from '../../../shared/src/asserts.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import type {Change} from './change.ts';
import {maybeSplitAndPushEditChange} from './maybe-split-and-push-edit-change.ts';
import type {InputBase, Output} from './operator.ts';
import {EMPTY_YIELDS, type Stream} from './stream.ts';

export function filterPush(
  change: Change,
  output: Output,
  pusher: InputBase,
  predicate?: (row: Row) => boolean,
): Stream<'yield'> {
  if (!predicate) {
    return output.push(change, pusher);
  }
  switch (change[ChangeIndex.TYPE]) {
    case ChangeType.ADD:
    case ChangeType.REMOVE:
    case ChangeType.CHILD:
      return predicate(change[ChangeIndex.NODE].row)
        ? output.push(change, pusher)
        : EMPTY_YIELDS;
    case ChangeType.EDIT:
      return maybeSplitAndPushEditChange(change, predicate, output, pusher);
    default:
      unreachable(change);
  }
}
