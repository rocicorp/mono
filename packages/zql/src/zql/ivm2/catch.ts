import type {Change} from './change.js';
import type {FetchRequest, HydrateRequest, Input, Output} from './operator.js';
import type {Node} from './data.js';

/**
 * Catch is an Output that collects all incoming stream data into arrays. Mainly
 * useful for testing.
 */
export class Catch implements Output {
  #input: Input;
  readonly pushes: Change[] = [];

  constructor(input: Input) {
    this.#input = input;
  }

  hydrate(req: HydrateRequest = {}) {
    return [...this.#input.hydrate(req, this)].map(expandNode);
  }

  fetch(req: FetchRequest = {}) {
    return [...this.#input.fetch(req, this)].map(expandNode);
  }

  dehydrate(req: HydrateRequest = {}) {
    return [...this.#input.dehydrate(req, this)].map(expandNode);
  }

  push(change: Change) {
    this.pushes.push(expandChange(change));
  }

  reset() {
    this.pushes.length = 0;
  }
}

export function expandChange(change: Change): Change {
  if (change.type === 'child') {
    return {
      ...change,
      child: {
        ...change.child,
        change: expandChange(change.child.change),
      },
    };
  }
  return {
    ...change,
    node: expandNode(change.node),
  };
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
