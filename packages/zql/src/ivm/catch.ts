import {unreachable} from '../../../shared/src/asserts.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {AddChange, Change, RemoveChange} from './change.ts';
import type {Node} from './data.ts';
import type {FetchRequest, Input, Output} from './operator.ts';

export type CaughtChildChange = {
  type: 'child';
  row: Row;
  child: {
    relationshipName: string;
    change: CaughtChange;
  };
};

export type CaughtEditChange = {
  type: 'edit';
  oldRow: Row;
  row: Row;
};

export type CaughtChange =
  | AddChange
  | RemoveChange
  | CaughtChildChange
  | CaughtEditChange;

/**
 * Catch is an Output that collects all incoming stream data into arrays. Mainly
 * useful for testing.
 */
export class Catch implements Output {
  #input: Input;
  readonly pushes: CaughtChange[] = [];

  constructor(input: Input) {
    this.#input = input;
    input.setOutput(this);
  }

  fetch(req: FetchRequest = {}) {
    return [...this.#input.fetch(req)].map(expandNode);
  }

  cleanup(req: FetchRequest = {}) {
    return [...this.#input.cleanup(req)].map(expandNode);
  }

  push(change: Change) {
    this.pushes.push(expandChange(change));
  }

  reset() {
    this.pushes.length = 0;
  }

  destroy() {
    this.#input.destroy();
  }
}

export function expandChange(change: Change): CaughtChange {
  switch (change.type) {
    case 'add':
    case 'remove':
      return {
        ...change,
        node: expandNode(change.node),
      };
    case 'edit':
      return {
        type: 'edit',
        oldRow: change.oldNode.row,
        row: change.node.row,
      };
    case 'child':
      return {
        ...change,
        child: {
          ...change.child,
          change: expandChange(change.child.change),
        },
      };
    default:
      unreachable(change);
  }
}

export function expandNode(node: Node): Node {
  return {
    ...node,
    relationships: Object.fromEntries(
      Object.entries(node.relationships).map(([k, v]) => [
        k,
        [...v].map(expandNode),
      ]),
    ),
  };
}
