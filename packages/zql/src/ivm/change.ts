import {ChangeType} from './change-type.ts';
import type {Node} from './data.ts';

/**
 * The `child` payload carried by a {@linkcode ChildChange}.
 */
export type ChildData = {
  relationshipName: string;
  change: Change;
};

export type Change = AddChange | RemoveChange | ChildChange | EditChange;

/**
 * Represents a node (and all its children) getting added to the result.
 */
export type AddChange = [type: ChangeType.ADD, node: Node, extra: null];

/**
 * Represents a node (and all its children) getting removed from the result.
 */
export type RemoveChange = [type: ChangeType.REMOVE, node: Node, extra: null];

/**
 * The node's row is unchanged, but one of its descendants has changed.
 * The node's relationships will reflect the change, `child` specifies the
 * specific descendant change.
 */
export type ChildChange = [
  type: ChangeType.CHILD,
  node: Node,
  child: ChildData,
];

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
export type EditChange = [type: ChangeType.EDIT, node: Node, oldNode: Node];

// Factory functions — prefer these over constructing tuple literals directly.

export function makeAddChange(node: Node): AddChange {
  return [ChangeType.ADD, node, null];
}

export function makeRemoveChange(node: Node): RemoveChange {
  return [ChangeType.REMOVE, node, null];
}

export function makeChildChange(node: Node, child: ChildData): ChildChange {
  return [ChangeType.CHILD, node, child];
}

export function makeEditChange(node: Node, oldNode: Node): EditChange {
  return [ChangeType.EDIT, node, oldNode];
}
