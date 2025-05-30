import {unreachable} from '../../../shared/src/asserts.ts';
import type {Row} from './data.ts';
import type {Change} from './change.ts';
import {maybeSplitAndPushEditChange} from './maybe-split-and-push-edit-change.ts';
import type {Output} from './operator.ts';

export function filterPush(
  change: Change,
  output: Output,
  predicate?: ((row: Row) => boolean) | undefined,
) {
  if (!predicate) {
    output.push(change);
    return;
  }
  switch (change.type) {
    case 'add':
    case 'remove':
      if (predicate(change.node.row)) {
        output.push(change);
      }
      break;
    case 'child':
      if (predicate(change.node.row)) {
        output.push(change);
      }
      break;
    case 'edit':
      maybeSplitAndPushEditChange(change, predicate, output);
      break;
    default:
      unreachable(change);
  }
}
