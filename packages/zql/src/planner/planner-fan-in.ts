import {assert} from '../../../shared/src/asserts.ts';
import type {PlannerConstraint} from './planner-constraint.ts';
import type {
  CostEstimate,
  JoinOrConnection,
  PlannerNode,
} from './planner-node.ts';

/**
 * A PlannerFanIn node can either be a normal FanIn or UnionFanIn.
 *
 * These have different performance characteristics so we need to distinguish them.
 *
 * A normal FanIn only does a single fetch to FanOut, regardless of how many internal
 * branches / inputs it has.
 *
 * A UnionFanIn does a fetch per internal branch / input. This causes an exponential
 * increase in cost if many UnionFanIns are chained after on another. E.g., `(A or B) AND (C or D)`.
 *
 * To capture this cost blow-up, union fan in assigns different branch patterns to their inputs.
 *
 * Since UFI will generate a unique branch pattern per input, planner-connection will yield a higher cost
 * each time a UFI is present. planner-connection will return the sum of the costs of each unique branch pattern.
 */
export class PlannerFanIn {
  readonly kind = 'fan-in' as const;
  #type: 'FI' | 'UFI';
  #output?: PlannerNode | undefined;
  readonly #inputs: PlannerNode[];

  constructor(inputs: PlannerNode[]) {
    this.#type = 'FI';
    this.#inputs = inputs;
  }

  get type() {
    return this.#type;
  }

  get pinned(): boolean {
    return false;
  }

  closestJoinOrSource(): JoinOrConnection {
    return 'join';
  }

  setOutput(node: PlannerNode): void {
    this.#output = node;
  }

  get output(): PlannerNode {
    assert(this.#output !== undefined, 'Output not set');
    return this.#output;
  }

  reset() {
    this.#type = 'FI';
  }

  convertToUFI(): void {
    this.#type = 'UFI';
  }

  estimateCost(branchPattern?: number[]): CostEstimate {
    // FanIn always sums costs of its inputs
    // But it needs to pass the correct branch pattern to each input
    let totalCost = {
      baseCardinality: 0,
      runningCost: 0,
    };

    if (this.#type === 'FI') {
      // Normal FanIn: all inputs get the same branch pattern with 0 prepended
      const updatedPattern =
        branchPattern === undefined ? undefined : [0, ...branchPattern];
      let maxBaseCardinality = 0;
      let maxRunningCost = 0;
      for (const input of this.#inputs) {
        const cost = input.estimateCost(updatedPattern);
        if (cost.baseCardinality > maxBaseCardinality) {
          maxBaseCardinality = cost.baseCardinality;
        }
        if (cost.runningCost > maxRunningCost) {
          maxRunningCost = cost.runningCost;
        }
      }

      totalCost.baseCardinality = maxBaseCardinality;
      totalCost.runningCost = maxRunningCost;
    } else {
      // Union FanIn (UFI): each input gets unique branch pattern
      let i = 0;
      for (const input of this.#inputs) {
        const updatedPattern =
          branchPattern === undefined ? undefined : [i, ...branchPattern];
        const cost = input.estimateCost(updatedPattern);
        totalCost.baseCardinality += cost.baseCardinality;
        totalCost.runningCost += cost.runningCost;
        i++;
      }
    }

    return totalCost;
  }

  propagateConstraints(
    branchPattern: number[],
    constraint: PlannerConstraint | undefined,
    from: PlannerNode,
  ): void {
    if (this.#type === 'FI') {
      const updatedPattern = [0, ...branchPattern];
      /**
       * All inputs get the same branch pattern.
       * 1. They cannot contribute differing constraints to their parent inputs because they are not flipped.
       *    If they were flipped this would be of type UFI.
       * 2. All inputs need to be called because they could be pinned. If they are pinned they could have constraints
       *    to send to their children.
       */
      for (const input of this.#inputs) {
        input.propagateConstraints(updatedPattern, constraint, from);
      }
      return;
    }

    let i = 0;
    for (const input of this.#inputs) {
      input.propagateConstraints([i, ...branchPattern], constraint, from);
      i++;
    }
  }
}
