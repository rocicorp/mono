import {unreachable} from '../../../shared/src/asserts.ts';
import {emptyArray} from '../../../shared/src/sentinels.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Change} from './change.ts';
import type {Node} from './data.ts';
import {type FetchRequest, type Input, type Output} from './operator.ts';

export type CaughtNode =
  | {
      row: Row;
      relationships: Record<string, CaughtNode[]>;
    }
  | 'yield';

export type CaughtAddChange = {
  type: 'add';
  node: CaughtNode;
};

export type CaughtRemoveChange = {
  type: 'remove';
  node: CaughtNode;
};

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
  | CaughtAddChange
  | CaughtRemoveChange
  | CaughtChildChange
  | CaughtEditChange;

/**
 * Catch is an Output that collects all incoming stream data into arrays. Mainly
 * useful for testing.
 */
export class Catch implements Output {
  #input: Input;
  readonly #fetchOnPush: boolean;
  readonly pushes: CaughtChange[] = [];
  readonly pushesWithFetch: {change: CaughtChange; fetch: CaughtNode[]}[] = [];

  constructor(input: Input, fetchOnPush = false) {
    this.#input = input;
    this.#fetchOnPush = fetchOnPush;
    input.setOutput(this);
  }

  fetch(req: FetchRequest = {}) {
    return [...this.#input.fetch(req)].map(expandNode);
  }

  push(change: Change) {
    const fetch = this.#fetchOnPush
      ? [...this.#input.fetch({})].map(expandNode)
      : [];
    const expandedChange = expandChange(change);
    if (this.#fetchOnPush) {
      this.pushesWithFetch.push({
        change: expandedChange,
        fetch,
      });
    }
    this.pushes.push(expandedChange);
    return emptyArray;
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
        type: 'child',
        row: change.node.row,
        child: {
          ...change.child,
          change: expandChange(change.child.change),
        },
      };
    default:
      unreachable(change);
  }
}

export function expandNode(node: Node | 'yield'): CaughtNode {
  return node === 'yield'
    ? node
    : {
        row: node.row,
        relationships: Object.fromEntries(
          Object.entries(node.relationships).map(([k, v]) => [
            k,
            [...v()].map(expandNode),
          ]),
        ),
      };
}
