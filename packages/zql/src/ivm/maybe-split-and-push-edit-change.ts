import type {Row} from '../../../zero-protocol/src/data.js';
import type {EditChange} from './change.js';
import type {Output} from './operator.js';

/**
 * This takes an {@linkcode EditChange} and a predicate that determines if a row
 * should be present based on the row's data. It then splits the change and
 * pushes the appropriate changes to the output based on the predicate.
 */
export function maybeSplitAndPushEditChange(
  change: EditChange,
  predicate: (row: Row) => boolean,
  output: Output,
) {
  const oldWasPresent = predicate(change.oldNode.row);
  const newIsPresent = predicate(change.node.row);

  if (oldWasPresent && newIsPresent) {
    output.push(change);
  } else if (oldWasPresent && !newIsPresent) {
    // The relationships are empty at this point and that is fine since
    // splitAndPushEditChange is only used by operators that are before the Join
    // operator.
    output.push({
      type: 'remove',
      node: change.oldNode,
    });
  } else if (!oldWasPresent && newIsPresent) {
    output.push({
      type: 'add',
      node: change.node,
    });
  }
}
