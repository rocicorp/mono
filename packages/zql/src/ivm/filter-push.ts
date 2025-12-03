import {unreachable} from '../../../shared/src/asserts.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Change} from './change.ts';
import {maybeSplitAndPushEditChange} from './maybe-split-and-push-edit-change.ts';
import type {InputBase, Output} from './operator.ts';
import type {Stream} from './stream.ts';

export function* filterPush(
  change: Change,
  output: Output,
  pusher: InputBase,
  predicate?: (row: Row) => boolean,
): Stream<'yield'> {
  if (!predicate) {
    yield* output.push(change, pusher);
    return;
  }
  switch (change.type) {
    case 'add':
    case 'remove':
      if (predicate(change.node.row)) {
        yield* output.push(change, pusher);
      }
      break;
    case 'child':
      if (predicate(change.node.row)) {
        yield* output.push(change, pusher);
      }
      break;
    case 'edit':
      yield* maybeSplitAndPushEditChange(change, predicate, output, pusher);
      break;
    default:
      unreachable(change);
  }
}
