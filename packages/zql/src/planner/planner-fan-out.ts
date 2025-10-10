import type {PlannerConstraint} from './planner-constraint.ts';
import type {FromType, PlannerNode} from './planner-node.ts';

export class PlannerFanOut implements PlannerNode {
  #type: 'FO' | 'UFO';
  readonly #input: PlannerNode;

  constructor(input: PlannerNode) {
    this.#type = 'FO';
    this.#input = input;
  }

  get type() {
    return this.#type;
  }

  propagateConstraints(
    branchPattern: number[],
    constraint: PlannerConstraint | undefined,
    from: FromType,
  ): void {
    this.#input.propagateConstraints(branchPattern, constraint, from);
  }

  convertToUFO(): void {
    this.#type = 'UFO';
  }

  reset(): void {
    this.#type = 'FO';
  }
}
