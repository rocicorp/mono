import type {Row} from './data.js';

export type Change = AddChange | RemoveChange | NopChange;

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
 * Represents a change in a child relationship.
 */
export type NopChange = {
  type: 'child';
  row: Row;
  child: {
    relationshipName: string;
    change: Change;
  };
};
