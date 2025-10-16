import type {PlannerNode} from './planner-node.ts';

export class PlannerTerminus {
  readonly kind = 'terminus' as const;
  readonly #input: PlannerNode;

  constructor(input: PlannerNode) {
    this.#input = input;
  }

  propagateConstraints(): void {
    this.#input.propagateConstraints([], undefined, 'terminus');
  }

  estimateCost(): number {
    return this.#input.estimateCost();
  }
}
