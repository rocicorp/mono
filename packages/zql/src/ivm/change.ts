import type {Row} from '../../../zero-protocol/src/data.js';
import type {Node} from './data.js';

export type Change = AddChange | RemoveChange | ChildChange | EditChange;
export type ChangeType = Change['type'];

// TODO: We should change these to classes to achieve monomorphic dispatch.
// or add some runtime asserts that the order of the keys is always the same.

/**
 * Represents a node (and all its children) getting added to the result.
 */
export type AddChange = {
  type: 'add';
  node: Node;
};

/**
 * Represents a node (and all its children) getting removed from the result.
 */
export type RemoveChange = {
  type: 'remove';
  node: Node;
};

/**
 * The node itself is unchanged, but one of its descendants has changed.
 */
export type ChildChange = {
  type: 'child';
  row: Row;
  child: {
    relationshipName: string;
    change: Change;
  };
};

/**
 * The row changed (in a way that the {@linkcode Source} determines). Most
 * likely the PK stayed the same but there is really no restriction in how it
 * can change.
 *
 * The edit changes flows down in a {@linkcode Output.push}. There are cases
 * where an edit change gets split into a remove and an add change when the
 * presence of the row in the result changes (for example the row is no longer
 * present due to a filter)
 */
export type EditChange = {
  type: 'edit';
  row: Row;
  oldRow: Row;
};

export function rowForChange(change: Change): Row {
  const {type} = change;
  return type === 'add' || type === 'remove' ? change.node.row : change.row;
}
