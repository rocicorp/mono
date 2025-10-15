import type {PlannerConstraint} from './planner-constraint.ts';
import type {PlannerNode} from './planner-node.ts';

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

  get pinned(): boolean {
    // if all of our outputs are pinned, we're pinned
    return this.#outputs.every(output => output.pinned);
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
    _from: PlannerNode,
  ): void {
    this.#input.propagateConstraints(branchPattern, constraint, this);
  }

  estimateCost(branchPattern?: number[]): number {
    return this.#input.estimateCost(branchPattern);
  }

  convertToUFO(): void {
    this.#type = 'UFO';
  }

  reset(): void {
    this.#type = 'FO';
  }
}
