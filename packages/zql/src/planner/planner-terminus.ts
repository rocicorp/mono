import type {PlannerNode} from './planner-node.ts';

export class PlannerTerminus {
  readonly kind = 'terminus' as const;
  readonly #input: PlannerNode;

  constructor(input: PlannerNode) {
    this.#input = input;
  }

  propagateConstraints(): void {
    // After planning pins nodes, we send 'pinned' to trigger constraint propagation
    // If no nodes are pinned yet (initial cost estimation), we send 'unpinned'
    // We determine this by checking if the input is pinned
    const from = this.#input.kind === 'connection' && this.#input.pinned
      ? 'pinned'
      : this.#input.kind === 'join' && this.#input.pinned
      ? 'pinned'
      : 'unpinned';

    this.#input.propagateConstraints([], undefined, from);
  }
}
