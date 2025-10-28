import type {PlannerConstraint} from './planner-constraint.ts';
import type {
  CostEstimate,
  JoinOrConnection,
  PlannerNode,
} from './planner-node.ts';

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

  closestJoinOrSource(): JoinOrConnection {
    return this.#input.closestJoinOrSource();
  }

  propagateConstraints(
    branchPattern: number[],
    constraint: PlannerConstraint | undefined,
    _from: PlannerNode,
  ): void {
    this.#input.propagateConstraints(branchPattern, constraint, this);
  }

  estimateCost(branchPattern?: number[]): CostEstimate {
    return this.#input.estimateCost(branchPattern);
  }

  convertToUFO(): void {
    this.#type = 'UFO';
  }

  reset(): void {
    this.#type = 'FO';
  }

  /**
   * Propagate unlimiting when a parent join is flipped.
   * Fan-out propagates to its input.
   */
  propagateUnlimitFromFlippedJoin(): void {
    if (
      'propagateUnlimitFromFlippedJoin' in this.#input &&
      typeof this.#input.propagateUnlimitFromFlippedJoin === 'function'
    ) {
      (
        this.#input as {propagateUnlimitFromFlippedJoin(): void}
      ).propagateUnlimitFromFlippedJoin();
    }
  }
}
