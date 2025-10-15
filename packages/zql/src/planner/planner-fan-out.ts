import type {PlannerConstraint} from './planner-constraint.ts';
import type {ConstraintPropagationType, PlannerNode} from './planner-node.ts';

export class PlannerFanOut {
  readonly kind = 'fan-out' as const;
  #type: 'FO' | 'UFO';
  readonly #outputs: PlannerNode[] = [];
  readonly #input: PlannerNode;

  constructor(input: PlannerNode) {
    this.#type = 'FO';
    this.#input = input;
  }

  get type() {
    return this.#type;
  }

  addOutput(node: PlannerNode): void {
    this.#outputs.push(node);
  }

  get outputs(): PlannerNode[] {
    return this.#outputs;
  }

  propagateConstraints(
    branchPattern: number[],
    constraint: PlannerConstraint | undefined,
    from: ConstraintPropagationType,
  ): void {
    // Check if the input is pinned and adjust the 'from' value accordingly
    const inputFrom =
      (this.#input.kind === 'connection' && this.#input.pinned) ||
      (this.#input.kind === 'join' && this.#input.pinned)
        ? 'pinned'
        : from;
    this.#input.propagateConstraints(branchPattern, constraint, inputFrom);
  }

  convertToUFO(): void {
    this.#type = 'UFO';
  }

  reset(): void {
    this.#type = 'FO';
  }
}
