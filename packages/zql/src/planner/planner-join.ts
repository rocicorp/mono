import {assert} from '../../../shared/src/asserts.ts';
import {
  mergeConstraints,
  type PlannerConstraint,
} from './planner-constraint.ts';
import type {PlanDebugger} from './planner-debug.ts';
import {omitFanout} from './planner-node.ts';
import type {
  CostEstimate,
  JoinOrConnection,
  PlannerNode,
} from './planner-node.ts';
import type {PlannerTerminus} from './planner-terminus.ts';

/**
 * Translate constraints for a flipped join from parent space to child space.
 * Matches the runtime behavior of FlippedJoin.fetch() which translates
 * parent constraints to child constraints using index-based key mapping.
 *
 * Example:
 *   parentConstraint = {issueID: undefined, projectID: undefined}
 *   childConstraint = {id: undefined, projectID: undefined}
 *   incomingConstraint = {issueID: 5}
 *   result = {id: 5}  // issueID at index 0 maps to id at index 0
 */
function translateConstraintsForFlippedJoin(
  incomingConstraint: PlannerConstraint | undefined,
  parentConstraint: PlannerConstraint,
  childConstraint: PlannerConstraint,
): PlannerConstraint | undefined {
  if (!incomingConstraint) return undefined;

  const parentKeys = Object.keys(parentConstraint);
  const childKeys = Object.keys(childConstraint);
  const translated: PlannerConstraint = {};

  for (const [key, value] of Object.entries(incomingConstraint)) {
    const index = parentKeys.indexOf(key);
    if (index !== -1) {
      // Found this key in parent at position `index`
      // Map to child key at same position
      translated[childKeys[index]] = value;
    }
  }

  return Object.keys(translated).length > 0 ? translated : undefined;
}

/**
 * Semi-join overhead multiplier.
 *
 * Semi-joins represent correlated subqueries (EXISTS checks) which have
 * execution overhead compared to flipped joins, even when logical row counts
 * are identical. This overhead comes from:
 * - Need to execute a separate correlation check for each parent row
 * - Cannot leverage combined constraint checking as effectively as flipped joins
 *
 * A multiplier of 1.5 means semi-joins are estimated to be ~50% more expensive
 * than equivalent flipped joins, which empirically matches observed performance
 * differences in production workloads (e.g., 1.7x in zbugs benchmarks).
 *
 * Flipped joins have a different overhead in that they become unlimited. This
 * is accounted for when propagating unlimits rather than here.
 */
// const SEMI_JOIN_OVERHEAD_MULTIPLIER = 1.5;

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

  readonly #parent: Exclude<PlannerNode, PlannerTerminus>;
  readonly #child: Exclude<PlannerNode, PlannerTerminus>;
  readonly #parentConstraint: PlannerConstraint;
  readonly #childConstraint: PlannerConstraint;
  readonly #flippable: boolean;
  readonly planId: number;
  #output?: PlannerNode | undefined; // Set once during graph construction

  // Reset between planning attempts
  #type: 'semi' | 'flipped';
  readonly #initialType: 'semi' | 'flipped';

  constructor(
    parent: Exclude<PlannerNode, PlannerTerminus>,
    child: Exclude<PlannerNode, PlannerTerminus>,
    parentConstraint: PlannerConstraint,
    childConstraint: PlannerConstraint,
    flippable: boolean,
    planId: number,
    initialType: 'semi' | 'flipped' = 'semi',
  ) {
    this.#type = initialType;
    this.#initialType = initialType;
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
   * Propagate unlimiting when this join is flipped.
   * When a join is flipped:
   * 1. Child becomes outer loop → produces all rows (unlimited)
   * 2. Parent is fetched once per child row → effectively unlimited
   *
   * Example: If child produces 896 rows, parent is fetched 896 times.
   * Even if each fetch returns 1 row, parent produces 896 total rows.
   *
   * Propagation rules:
   * - Connection: call unlimit()
   * - Semi-join: continue to parent (outer loop)
   * - Flipped join: stop (already unlimited when it was flipped)
   * - Fan-out/Fan-in: propagate to all inputs
   */
  propagateUnlimit(): void {
    assert(this.#type === 'flipped', 'Can only unlimit a flipped join');
    // Parent stays limited; child becomes unlimited
    this.#child.propagateUnlimitFromFlippedJoin(); // Up the child chain
  }

  /**
   * Called when a parent join is flipped and this join is part of its child subgraph.
   * Continue propagation to parent (the outer loop).
   * If we are hitting a semi-join, the parent drives.
   * If we are hitting a flip-join, well now we have to unlimit its parent too!
   */
  propagateUnlimitFromFlippedJoin(): void {
    this.#parent.propagateUnlimitFromFlippedJoin();
  }

  propagateConstraints(
    branchPattern: number[],
    constraint: PlannerConstraint | undefined,
    from?: PlannerNode,
    planDebugger?: PlanDebugger,
  ): void {
    planDebugger?.log({
      type: 'node-constraint',
      nodeType: 'join',
      node: this.getName(),
      branchPattern,
      constraint,
      from: from ? getNodeName(from) : 'unknown',
    });

    if (this.#type === 'semi') {
      // A semi-join always has constraints for its child.
      // They are defined by the correlation between parent and child.
      this.#child.propagateConstraints(
        branchPattern,
        this.#childConstraint,
        this,
        planDebugger,
      );
      // A semi-join forwards constraints to its parent.
      this.#parent.propagateConstraints(
        branchPattern,
        constraint,
        this,
        planDebugger,
      );
    } else if (this.#type === 'flipped') {
      // A flipped join translates constraints from parent space to child space.
      // This matches FlippedJoin.fetch() runtime behavior where parent constraints
      // on join keys are translated to child constraints.
      // Example: If parent has {issueID: 5} and join maps issueID→id,
      // child gets {id: 5} allowing index usage.
      const translatedConstraint = translateConstraintsForFlippedJoin(
        constraint,
        this.#parentConstraint,
        this.#childConstraint,
      );
      this.#child.propagateConstraints(
        branchPattern,
        translatedConstraint,
        this,
        planDebugger,
      );
      // A flipped join will have constraints to send to its parent.
      // - The constraints its output sent
      // - The constraints its child creates
      this.#parent.propagateConstraints(
        branchPattern,
        mergeConstraints(constraint, this.#parentConstraint),
        this,
        planDebugger,
      );
    }
  }

  reset(): void {
    this.#type = this.#initialType;
  }

  estimateCost(
    /**
     * This argument is to deal with consecutive `andExists` statements.
     * Each one will constrain how often a parent row passes all constraints.
     * This means that we have to scan more and more parent rows the more
     * constraints we add.
     *
     * DownstreamChildSelectivity factors in fanout factor
     * from parent -> child
     */
    downstreamChildSelectivity: number,
    /**
     * branchPattern uniquely identifies OR branches in the graph.
     * Each path through an OR will have unique constraints to apply to the source
     * connection.
     * branchPattern allows us to correlate a path through the graph
     * to the constraints that should be applied for that path.
     *
     * Example graph:
     *  UFO
     * /  \
     * J1   J2
     * \  /
     *  UFI
     *
     * J1 and J2 are joins inside an OR (FO).
     * branchPattern [0] = path through J1
     * branchPattern [1] = path through J2
     *
     * If many ORs are nested, branchPattern will have multiple elements
     * representing each level of OR.
     *
     * If no joins are flipped within the `OR`, then only a single
     * branchPattern element will be needed, as FO represents all sub-joins
     * as a single path.
     */
    branchPattern: number[],
    planDebugger?: PlanDebugger,
  ): CostEstimate {
    /**
     * downstreamChildSelectivity accumulates up a parent chain, not
     * up child chains. Child chains represent independent sub-graphs.
     * So we pass 1 for `downstreamChildSelectivity` when estimating child cost.
     * Put another way, downstreamChildSelectivity impacts how many parent
     * rows are returned.
     */
    const child = this.#child.estimateCost(1, branchPattern, planDebugger);

    const fanoutFactor = child.fanout(Object.keys(this.#childConstraint));
    // Factor in how many child rows match a parent row.
    // E.g., if an issue has 10 comments on average then we're more
    // likely to hit a comment compared to if an issue has 1 comment on average.
    // If an index is all nulls (no parents match any children)
    // this will collapse to 0.
    const scaledChildSelectivity =
      1 - Math.pow(1 - child.selectivity, fanoutFactor.fanout);

    // Why do we not need fanout in the other direction?
    // E.g., for an `inventory -> film` flipped-join, if each film has 100 inventories (100 copies)
    // then we're more likely to hit an inventory row compared to if each film has 1 inventory.
    // Flipped-join already accounts for this because the child selectivity is implicitly accounted
    // for. The returned row estimate of the child is already representative of how many
    // rows the child will have post-filtering.

    /**
     * How selective is the graph from this point forward?
     * If we are _very_ selective then we must scan more parent rows
     * before finding a match.
     * E.g., if childSelectivity = 0.1 and downstreamChildSelectivity = 0.5
     * then we only pass 5% of parent rows (0.1 * 0.5 = 0.05).
     *
     * This is used to estimate how many rows will be pulled from the parent
     * when trying to satisfy downstream constraints and a limit.
     *
     * NOTE: We do not know if the probabilities are correlated so we assume independence.
     * This is a fundamental limitation of the planner.
     */
    const parent = this.#parent.estimateCost(
      // Selectivity flows up the graph from child to parent
      // so we can determine the total selectivity of all ANDed exists checks.
      this.#type === 'flipped'
        ? 1 * downstreamChildSelectivity
        : scaledChildSelectivity * downstreamChildSelectivity,
      branchPattern,
      planDebugger,
    );

    let costEstimate: CostEstimate;

    if (this.type === 'semi') {
      costEstimate = {
        startupCost: parent.startupCost,
        scanEst:
          parent.limit === undefined
            ? parent.returnedRows
            : Math.min(
                parent.returnedRows,
                downstreamChildSelectivity === 0
                  ? 0
                  : parent.limit / downstreamChildSelectivity,
              ),
        cost:
          parent.cost +
          parent.scanEst * (child.startupCost + child.cost + child.scanEst),
        returnedRows: parent.returnedRows * child.selectivity,
        selectivity: child.selectivity * parent.selectivity,
        limit: parent.limit,
        fanout: parent.fanout,
      };
    } else {
      costEstimate = {
        startupCost: child.startupCost,
        scanEst:
          parent.limit === undefined
            ? parent.returnedRows * child.returnedRows
            : Math.min(
                parent.returnedRows * child.returnedRows,
                downstreamChildSelectivity === 0
                  ? 0
                  : parent.limit / downstreamChildSelectivity,
              ),
        cost:
          child.cost +
          child.scanEst * (parent.startupCost + parent.cost + parent.scanEst),
        // the child selectivity is not relevant here because it has already been taken into account via the flipping.
        // I.e., `child.returnedRows` is the estimated number of rows produced by the child _after_ taking filtering into account.
        returnedRows: parent.returnedRows * child.returnedRows,
        selectivity: parent.selectivity * child.selectivity,
        limit: parent.limit,
        fanout: parent.fanout,
      };
    }

    if (planDebugger) {
      planDebugger.log({
        type: 'node-cost',
        nodeType: 'join',
        node: this.getName(),
        branchPattern,
        downstreamChildSelectivity,
        costEstimate: omitFanout(costEstimate),
        joinType: this.#type,
      });
    }

    return costEstimate;
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
