import type {Node, Row} from './data.js';

export type Change = AddChange | RemoveChange | ChildChange;
export const enum ChangeType {
  Add = 1,
  Remove = 2,
  Child = 3,
}

/**
 * Represents a node (and all its children) getting added to the result.
 */
export type AddChange = {
  type: ChangeType.Add;
  node: Node;
};

/**
 * Represents a node (and all its children) getting removed from the result.
 */
export type RemoveChange = {
  type: ChangeType.Remove;
  node: Node;
};

/**
 * The node itself is unchanged, but one of its descendants has changed.
 */
export type ChildChange = {
  type: ChangeType.Child;
  row: Row;
  child: {
    relationshipName: string;
    change: Change;
  };
};
