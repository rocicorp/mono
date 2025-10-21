import type {
  CostEstimate,
  JoinOrConnection,
  PlannerNode,
} from './planner-node.ts';

export class PlannerTerminus {
  readonly kind = 'terminus' as const;
  readonly #input: PlannerNode;

  constructor(input: PlannerNode) {
    this.#input = input;
  }

  get pinned(): boolean {
    return true;
  }

  closestJoinOrSource(): JoinOrConnection {
    return this.#input.closestJoinOrSource();
  }

  propagateConstraints(): void {
    this.#input.propagateConstraints([], undefined, this);
  }

  estimateCost(): CostEstimate {
    // Terminus starts the cost estimation flow with empty branch pattern
    return this.#input.estimateCost([]);
  }
}
