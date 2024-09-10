import {EditChange} from './change.js';
import {Row} from './data.js';
import {Output} from './operator.js';

/**
 * This takes an {@linkcode EditChange} and a predicate that determines if a row
 * should be present based on the row's data. It then splits the change and
 * pushes the appropriate changes to the output based on the predicate.
 */
export function splitAndPushEditChange(
  change: EditChange,
  predicate: (row: Row) => boolean,
  output: Output,
) {
  const oldWasPresent = predicate(change.oldRow);
  const newIsPresent = predicate(change.row);

  if (oldWasPresent && newIsPresent) {
    output.push(change);
  } else if (oldWasPresent && !newIsPresent) {
    output!.push({
      type: 'remove',
      node: {
        row: change.oldRow,
        relationships: {},
      },
    });
  } else if (!oldWasPresent && newIsPresent) {
    output.push({
      type: 'add',
      node: {
        row: change.row,
        relationships: {},
      },
    });
  }
}
