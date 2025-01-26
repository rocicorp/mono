import {unreachable} from '../../../shared/src/asserts.js';
import type {Change} from './change.js';
import {maybeSplitAndPushEditChange} from './maybe-split-and-push-edit-change.js';
import type {Output} from './operator.js';
import type {Row} from '../../../zero-protocol/src/data.js';

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
    case 'edit':
      maybeSplitAndPushEditChange(change, predicate, output);
      break;
    case 'child': {
      const filteredRows = change.rows.filter(predicate);
      if (filteredRows.length > 0) {
        output.push({
          ...change,
          rows: filteredRows,
        });
      }
      break;
    }
    default:
      unreachable(change);
  }
}
