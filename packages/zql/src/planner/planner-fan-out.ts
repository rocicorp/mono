import type {PlannerConstraint} from './planner-constraint.ts';
import type {PlanDebugger} from './planner-debug.ts';
import {omitFanout} from './planner-node.ts';
import type {
  CostEstimate,
  JoinOrConnection,
  PlannerNode,
} from './planner-node.ts';
import type {PlannerTerminus} from './planner-terminus.ts';

export class PlannerFanOut {
  readonly kind = 'fan-out' as const;
  #type: 'FO' | 'UFO';
  readonly #outputs: PlannerNode[] = [];
  readonly #input: Exclude<PlannerNode, PlannerTerminus>;

  constructor(input: Exclude<PlannerNode, PlannerTerminus>) {
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
    from?: PlannerNode,
    planDebugger?: PlanDebugger,
  ): void {
    planDebugger?.log({
      type: 'node-constraint',
      nodeType: 'fan-out',
      node: 'FO',
      branchPattern,
      constraint,
      from: from?.kind ?? 'unknown',
    });

    this.#input.propagateConstraints(
      branchPattern,
      constraint,
      this,
      planDebugger,
    );
  }

  estimateCost(
    downstreamChildSelectivity: number,
    branchPattern: number[],
    planDebugger?: PlanDebugger,
  ): CostEstimate {
    const ret = this.#input.estimateCost(
      downstreamChildSelectivity,
      branchPattern,
      planDebugger,
    );

    if (planDebugger) {
      planDebugger.log({
        type: 'node-cost',
        nodeType: 'fan-out',
        node: 'FO',
        branchPattern,
        downstreamChildSelectivity,
        costEstimate: omitFanout(ret),
      });
    }

    return ret;
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
