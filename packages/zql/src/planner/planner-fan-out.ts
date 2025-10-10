import type {PlannerConstraint} from './planner-constraint.ts';
import type {FromType, PlannerNode} from './planner-node.ts';

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
