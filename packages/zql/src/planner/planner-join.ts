import {assert} from '../../../shared/src/asserts.ts';
import {
  mergeConstraints,
  type PlannerConstraint,
} from './planner-constraint.ts';
import type {
  CostEstimate,
  JoinOrConnection,
  PlannerNode,
} from './planner-node.ts';

/**
 * Represents a join between two data streams (parent and child).
 *
 * # Dual-State Pattern
 * Like all planner nodes, PlannerJoin separates:
 * 1. IMMUTABLE STRUCTURE: Parent/child nodes, constraints, flippability
 * 2. MUTABLE STATE: Join type (semi/flipped), pinned status
 *
 * # Join Flipping
 * A join can be in two states:
 * - 'semi': Parent is outer loop, child is inner (semi-join for EXISTS)
 * - 'flipped': Child is outer loop, parent is inner
 *
 * Flipping is the key optimization: choosing which table scans first.
 * NOT EXISTS joins cannot be flipped (#flippable = false).
 *
 * # Constraint Propagation
 * - Semi-join: Sends childConstraint to child, forwards received constraints to parent
 * - Flipped join: Sends undefined to child, merges parentConstraint with received to parent
 * - Unpinned join: Only forwards constraints to parent (doesn't constrain child yet)
 *
 * # Lifecycle
 * 1. Construct with immutable structure (parent, child, constraints, flippability)
 * 2. Wire to output node during graph construction
 * 3. Planning calls flipIfNeeded() based on connection selection order
 * 4. pin() locks the join type once chosen
 * 5. reset() clears mutable state (type → 'semi', pinned → false)
 */
export class PlannerJoin {
  readonly kind = 'join' as const;

  readonly #parent: PlannerNode;
  readonly #child: PlannerNode;
  readonly #parentConstraint: PlannerConstraint;
  readonly #childConstraint: PlannerConstraint;
  readonly #flippable: boolean;
  readonly planId: number;
  #output?: PlannerNode | undefined; // Set once during graph construction

  // Reset between planning attempts
  #type: 'semi' | 'flipped';

  constructor(
    parent: PlannerNode,
    child: PlannerNode,
    parentConstraint: PlannerConstraint,
    childConstraint: PlannerConstraint,
    flippable: boolean,
    planId: number,
  ) {
    this.#type = 'semi';
    this.#parent = parent;
    this.#child = child;
    this.#childConstraint = childConstraint;
    this.#parentConstraint = parentConstraint;
    this.#flippable = flippable;
    this.planId = planId;
  }

  setOutput(node: PlannerNode): void {
    this.#output = node;
  }

  get output(): PlannerNode {
    assert(this.#output !== undefined, 'Output not set');
    return this.#output;
  }

  closestJoinOrSource(): JoinOrConnection {
    return 'join';
  }

  flipIfNeeded(input: PlannerNode): void {
    if (input === this.#child) {
      this.flip();
    } else {
      assert(
        input === this.#parent,
        'Can only flip a join from one of its inputs',
      );
    }
  }

  flip(): void {
    assert(this.#type === 'semi', 'Can only flip a semi-join');
    if (!this.#flippable) {
      throw new UnflippableJoinError(
        'Cannot flip a non-flippable join (e.g., NOT EXISTS)',
      );
    }
    this.#type = 'flipped';
  }

  get type(): 'semi' | 'flipped' {
    return this.#type;
  }
  isFlippable(): boolean {
    return this.#flippable;
  }

  /**
   * Propagate unlimiting through the child subgraph when this join is flipped.
   * When a join is flipped, the child becomes the outer loop and should produce
   * all rows rather than stopping at an EXISTS limit.
   *
   * Propagation rules:
   * - Connection: call unlimit()
   * - Semi-join: continue to parent (outer loop)
   * - Flipped join: stop (already unlimited when it was flipped)
   * - Fan-out/Fan-in: propagate to all inputs
   */
  propagateUnlimit(): void {
    assert(this.#type === 'flipped', 'Can only unlimit a flipped join');
    propagateUnlimitToNode(this.#child);
  }

  /**
   * Called when a parent join is flipped and this join is part of its child subgraph.
   * - Semi-join: continue propagation to parent (the outer loop)
   * - Flipped join: stop propagation (already unlimited when it was flipped)
   */
  propagateUnlimitFromFlippedJoin(): void {
    if (this.#type === 'semi') {
      propagateUnlimitToNode(this.#parent);
    }
    // For flipped joins, stop propagation
  }

  propagateConstraints(
    branchPattern: number[],
    constraint: PlannerConstraint | undefined,
  ): void {
    if (this.#type === 'semi') {
      // A semi-join always has constraints for its child.
      // They are defined by the correlation between parent and child.
      this.#child.propagateConstraints(
        branchPattern,
        this.#childConstraint,
        this,
      );
      // A semi-join forwards constraints to its parent.
      this.#parent.propagateConstraints(branchPattern, constraint, this);
    } else if (this.#type === 'flipped') {
      // A flipped join has no constraints to pass to its child.
      // It is a standalone fetch that is relying on the filters of the child
      // connection to do the heavy work.
      this.#child.propagateConstraints(branchPattern, undefined, this);
      // A flipped join will have constraints to send to its parent.
      // - The constraints its output sent
      // - The constraints its child creates
      this.#parent.propagateConstraints(
        branchPattern,
        mergeConstraints(constraint, this.#parentConstraint),
        this,
      );
    }
  }

  reset(): void {
    this.#type = 'semi';
  }

  estimateCost(branchPattern?: number[]): CostEstimate {
    const parentCost = this.#parent.estimateCost(branchPattern);
    const childCost = this.#child.estimateCost(branchPattern);

    let scanEst = parentCost.baseCardinality;
    if (this.#type === 'semi' && parentCost.limit !== undefined) {
      if (childCost.selectivity !== 0) {
        scanEst = Math.min(scanEst, parentCost.limit / childCost.selectivity);
      }
    }

    if (this.#parent.closestJoinOrSource() === 'join') {
      // if the parent is a join, we're in a pipeline rather than nesting of joins.
      return {
        baseCardinality: parentCost.baseCardinality,
        runningCost:
          this.#type === 'flipped'
            ? childCost.startupCost +
              childCost.runningCost * (parentCost.startupCost + scanEst)
            : parentCost.runningCost +
              scanEst * (childCost.startupCost + childCost.runningCost),
        startupCost: parentCost.startupCost,
        selectivity: parentCost.selectivity,
        limit: parentCost.limit,
      };
    }

    // if the parent is a source, we're in a nested loop join
    return {
      baseCardinality: parentCost.baseCardinality,
      runningCost:
        this.#type === 'flipped'
          ? childCost.runningCost * (parentCost.startupCost + scanEst)
          : scanEst * (childCost.startupCost + childCost.runningCost),
      startupCost: parentCost.startupCost,
      selectivity: parentCost.selectivity,
      limit: parentCost.limit,
    };
  }

  /**
   * Get a human-readable name for this join for debugging.
   * Format: "parentName ⋈ childName"
   */
  getName(): string {
    const parentName = getNodeName(this.#parent);
    const childName = getNodeName(this.#child);
    return `${parentName} ⋈ ${childName}`;
  }

  /**
   * Get debug information about this join's state.
   */
  getDebugInfo(): {
    name: string;
    type: 'semi' | 'flipped';
    planId: number;
  } {
    return {
      name: this.getName(),
      type: this.#type,
      planId: this.planId,
    };
  }
}

export class UnflippableJoinError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'UnflippableJoinError';
  }
}

/**
 * Get a human-readable name for any planner node.
 * Used for debugging and tracing.
 */
function getNodeName(node: PlannerNode): string {
  switch (node.kind) {
    case 'connection':
      return node.name;
    case 'join':
      return node.getName();
    case 'fan-out':
      return 'FO';
    case 'fan-in':
      return 'FI';
    case 'terminus':
      return 'terminus';
  }
}

/**
 * Propagate unlimiting through a node in the planner graph.
 * Called recursively to unlimit all nodes in a subgraph when a join is flipped.
 *
 * This calls the propagateUnlimitFromFlippedJoin() method on each node,
 * which implements the type-specific logic.
 */
function propagateUnlimitToNode(node: PlannerNode): void {
  if (
    'propagateUnlimitFromFlippedJoin' in node &&
    typeof node.propagateUnlimitFromFlippedJoin === 'function'
  ) {
    (
      node as {propagateUnlimitFromFlippedJoin(): void}
    ).propagateUnlimitFromFlippedJoin();
  }
}
