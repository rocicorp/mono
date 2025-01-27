import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Node} from './data.ts';

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
 * The edit changes flows down in a {@linkcode Output.push}.
 * There are cases where an edit change gets split into a remove and/or an add
 * change.
 * 1. when the presence of the row in the result changes (for example the row
 *    is no longer present due to a filter)
 * 2. the edit results in the rows relationships changing
 *
 * If an edit is not split, the relationships of node and oldNode must
 * be the same, just the Row has changed.
 *
 * NOTE: It would be cleaner to just have the relationships once,
 * since they must be the same, however relationship Streams are single use
 * and if an Edit needs to be split into a remove and add a single map
 * of relationship Streams could not be used for the both the remove and
 * the add.  This cleanup could be done if we move to multi-use Streams
 * for relationships.
 */
export type EditChange = {
  type: 'edit';
  node: Node;
  oldNode: Node;
};

export function rowForChange(change: Change): Row {
  const {type} = change;
  return type === 'child' ? change.row : change.node.row;
}
