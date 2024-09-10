import type {Node, Row} from './data.js';

export type Change = AddChange | RemoveChange | ChildChange | EditChange;
export type ChangeType = Change['type'];

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
 * We only represent changes to a single row as an "edit". If an edit causes
 * child rows to change, we convert it to the corresponding remove/add changes.
 */
export type EditChange = {
  type: 'edit';
  row: Row;
  oldRow: Row;
};
