import {assert} from '../../../shared/src/asserts.ts';
import type {PlannerConstraint} from './planner-constraint.ts';
import type {PlanDebugger} from './planner-debug.ts';
import {omitFanout} from './planner-node.ts';
import type {
  CostEstimate,
  JoinOrConnection,
  PlannerNode,
} from './planner-node.ts';
import type {PlannerTerminus} from './planner-terminus.ts';

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
  readonly #inputs: Exclude<PlannerNode, PlannerTerminus>[];

  constructor(inputs: Exclude<PlannerNode, PlannerTerminus>[]) {
    this.#type = 'FI';
    this.#inputs = inputs;
  }

  get type() {
    return this.#type;
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

  /**
   * Propagate unlimiting when a parent join is flipped.
   * Fan-in propagates to all of its inputs.
   */
  propagateUnlimitFromFlippedJoin(): void {
    for (const input of this.#inputs) {
      if (
        'propagateUnlimitFromFlippedJoin' in input &&
        typeof input.propagateUnlimitFromFlippedJoin === 'function'
      ) {
        (
          input as {propagateUnlimitFromFlippedJoin(): void}
        ).propagateUnlimitFromFlippedJoin();
      }
    }
  }

  estimateCost(
    downstreamChildSelectivity: number,
    branchPattern: number[],
    planDebugger?: PlanDebugger,
  ): CostEstimate {
    // FanIn always sums costs of its inputs
    // But it needs to pass the correct branch pattern to each input
    let totalCost: CostEstimate = {
      returnedRows: 0,
      cost: 0,
      scanEst: 0,
      startupCost: 0,
      selectivity: 0,
      limit: undefined,
      fanout: () => {
        throw new Error('Failed to set fanout model');
      },
    };

    if (this.#type === 'FI') {
      // Normal FanIn: all inputs get the same branch pattern with 0 prepended
      const updatedPattern = [0, ...branchPattern];
      let maxrows = 0;
      let maxRunningCost = 0;
      let maxStartupCost = 0;
      let maxScanEst = 0;

      let noMatchProb = 1.0;
      for (const input of this.#inputs) {
        const cost = input.estimateCost(
          downstreamChildSelectivity,
          updatedPattern,
          planDebugger,
        );
        totalCost.fanout = cost.fanout;
        if (cost.returnedRows > maxrows) {
          maxrows = cost.returnedRows;
        }
        if (cost.cost > maxRunningCost) {
          maxRunningCost = cost.cost;
        }
        if (cost.startupCost > maxStartupCost) {
          maxStartupCost = cost.startupCost;
        }
        if (cost.scanEst > maxScanEst) {
          maxScanEst = cost.scanEst;
        }

        // OR branches: combine selectivities assuming independent events
        // P(A OR B) = 1 - (1-A)(1-B)
        // Track probability of NO match in any branch
        noMatchProb *= 1 - cost.selectivity;

        // all inputs should have the same limit.
        assert(
          totalCost.limit === undefined || cost.limit === totalCost.limit,
          'All FanIn inputs should have the same limit',
        );
        totalCost.limit = cost.limit;
      }

      totalCost.returnedRows = maxrows;
      totalCost.cost = maxRunningCost;
      totalCost.selectivity = 1 - noMatchProb;
      totalCost.startupCost = maxStartupCost;
      totalCost.scanEst = maxScanEst;
    } else {
      // Union FanIn (UFI): each input gets unique branch pattern
      let i = 0;

      let noMatchProb = 1.0;
      for (const input of this.#inputs) {
        const updatedPattern = [i, ...branchPattern];
        const cost = input.estimateCost(
          downstreamChildSelectivity,
          updatedPattern,
          planDebugger,
        );
        totalCost.fanout = cost.fanout;
        totalCost.returnedRows += cost.returnedRows;
        totalCost.cost += cost.cost;
        totalCost.scanEst += cost.scanEst;
        totalCost.startupCost = totalCost.startupCost + cost.startupCost;

        // OR branches: combine selectivities assuming independent events
        // P(A OR B) = 1 - (1-A)(1-B)
        // Track probability of NO match in any branch
        noMatchProb *= 1 - cost.selectivity;

        // all inputs should have the same limit.
        assert(
          totalCost.limit === undefined || cost.limit === totalCost.limit,
          'All FanIn inputs should have the same limit',
        );
        totalCost.limit = cost.limit;
        i++;
      }
      totalCost.selectivity = 1 - noMatchProb;
    }

    if (planDebugger) {
      planDebugger.log({
        type: 'node-cost',
        nodeType: 'fan-in',
        node: this.#type,
        branchPattern,
        downstreamChildSelectivity,
        costEstimate: omitFanout(totalCost),
      });
    }

    return totalCost;
  }

  propagateConstraints(
    branchPattern: number[],
    constraint: PlannerConstraint | undefined,
    from?: PlannerNode,
    planDebugger?: PlanDebugger,
  ): void {
    planDebugger?.log({
      type: 'node-constraint',
      nodeType: 'fan-in',
      node: this.#type,
      branchPattern,
      constraint,
      from: from?.kind ?? 'unknown',
    });

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
        input.propagateConstraints(
          updatedPattern,
          constraint,
          this,
          planDebugger,
        );
      }
      return;
    }

    let i = 0;
    for (const input of this.#inputs) {
      input.propagateConstraints(
        [i, ...branchPattern],
        constraint,
        this,
        planDebugger,
      );
      i++;
    }
  }
}
