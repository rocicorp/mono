import {assert} from '../../../shared/src/asserts.ts';
import type {Condition, Ordering} from '../../../zero-protocol/src/ast.ts';
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

/**
 * Represents a connection to a source (table scan).
 *
 * # Dual State Pattern
 * Like all planner nodes, PlannerConnection separates:
 * 1. immutable structure: Ordering, filters, cost model (set at construction)
 * 2. mutable state: Pinned status, constraints (mutated during planning)
 *
 * # Cost Estimation
 * The ordering and filters determine the initial cost. As planning progresses,
 * constraints from parent joins refine the cost estimate.
 *
 * # Constraint Flow
 * When a connection is pinned as the outer loop, it reveals constraints for
 * connected joins. These constraints propagate through the graph, allowing
 * other connections to update their cost estimates.
 *
 * Example:
 *
 * ```ts
 * builder.issue.whereExists('assignee', a => a.where('name', 'Alice'))
 * ```
 *
 * ```
 * [issue]  [assignee]
 *   |         |
 *   |         +-- where name = 'Alice'
 *    \        /
 *     \      /
 *      [join]
 *        |
 * ```
 *
 * - Initial state: Both connections have no constraints, costs are unconstrained
 * - If `issue` chosen first: Reveals constraint `assignee_id` for assignee connection
 * - If `assignee` chosen first: Reveals constraint `assignee_id` for issue connection
 * - Updated costs guide the next selection
 *
 * # Lifecycle
 * 1. Construct with immutable structure (ordering, filters, cost model)
 * 2. Wire to output node during graph construction
 * 3. Planning mutates pinned status and accumulates constraints
 * 4. reset() clears mutable state for replanning
 */
export class PlannerConnection {
  readonly kind = 'connection' as const;

  // ========================================================================
  // IMMUTABLE STRUCTURE (set during construction, never changes)
  // ========================================================================
  readonly #sort: Ordering;
  readonly #filters: Condition | undefined;
  readonly #model: ConnectionCostModel;
  readonly table: string;
  readonly name: string; // Human-readable name for debugging (defaults to table name)
  readonly #baseConstraints: PlannerConstraint | undefined; // Constraints from parent correlation
  readonly #baseLimit: number | undefined; // Original limit from query structure (never modified)
  readonly selectivity: number; // Fraction of rows passing filters (1.0 = no filtering)
  #output?: PlannerNode | undefined; // Set once during graph construction

  // ========================================================================
  // MUTABLE PLANNING STATE (changes during plan search)
  // ========================================================================
  /**
   * Current limit during planning. Can be cleared (set to undefined) when a
   * parent join is flipped, indicating this connection is now in an outer loop
   * and should not be limited by EXISTS semantics.
   */
  limit: number | undefined;

  /**
   * Constraints accumulated from parent joins during planning.
   * Key is a path through the graph (e.g., "0,1" for branch pattern [0,1]).
   *
   * Undefined constraints are possible when a FO converts to UFO and only
   * a single join in the UFO is flipped - other branches report undefined.
   */
  readonly #constraints: Map<string, PlannerConstraint | undefined>;

  readonly #isRoot: boolean;

  /**
   * Cached per-constraint costs to avoid redundant cost model calls.
   * Maps constraint key (branch pattern string) to computed cost.
   * Invalidated when constraints change.
   */
  #cachedConstraintCosts: Map<string, CostEstimate> = new Map();

  constructor(
    table: string,
    model: ConnectionCostModel,
    sort: Ordering,
    filters: Condition | undefined,
    isRoot: boolean,
    baseConstraints?: PlannerConstraint,
    limit?: number,
    name?: string,
  ) {
    this.table = table;
    this.name = name ?? table;
    this.#sort = sort;
    this.#filters = filters;
    this.#model = model;
    this.#baseConstraints = baseConstraints;
    this.#baseLimit = limit;
    this.limit = limit;
    this.#constraints = new Map();
    this.#isRoot = isRoot;

    // Compute selectivity for EXISTS child connections (baseLimit === 1)
    // Selectivity = fraction of rows that pass filters
    if (limit !== undefined && filters) {
      const costWithFilters = model(table, sort, filters, undefined);
      const costWithoutFilters = model(table, sort, undefined, undefined);
      this.selectivity =
        costWithoutFilters.rows > 0
          ? costWithFilters.rows / costWithoutFilters.rows
          : 1.0;
    } else {
      // Root connections or connections without filters
      this.selectivity = 1.0;
    }
  }

  setOutput(node: PlannerNode): void {
    this.#output = node;
  }

  get output(): PlannerNode {
    assert(this.#output !== undefined, 'Output not set');
    return this.#output;
  }

  closestJoinOrSource(): JoinOrConnection {
    return 'connection';
  }

  /**
   * Constraints are uniquely identified by their path through the
   * graph.
   *
   * FO represents all sub-joins as a single path.
   * UFO represents each sub-join as a separate path.
   * The first branch in a UFO will match the path of FO so no re-set needs to happen
   * when swapping from FO to UFO.
   *
   * FO swaps to UFO when a join inside FO-FI gets flipped.
   *
   * The max of the last element of the paths is the number of
   * root branches.
   */
  propagateConstraints(
    path: number[],
    c: PlannerConstraint | undefined,
    from?: PlannerNode,
    planDebugger?: PlanDebugger,
  ): void {
    const key = path.join(',');
    this.#constraints.set(key, c);
    // Constraints changed, invalidate cost caches
    this.#cachedConstraintCosts.clear();

    planDebugger?.log({
      type: 'node-constraint',
      nodeType: 'connection',
      node: this.name,
      branchPattern: path,
      constraint: c,
      from: from?.kind ?? 'unknown',
    });
  }

  estimateCost(
    downstreamChildSelectivity: number,
    branchPattern: number[],
    planDebugger?: PlanDebugger,
  ): CostEstimate {
    // Branch pattern specified - return cost for this specific branch
    const key = branchPattern.join(',');

    // Check per-constraint cache first
    let cost = this.#cachedConstraintCosts.get(key);
    if (cost !== undefined) {
      return cost;
    }

    // Cache miss - compute and cache
    const constraint = this.#constraints.get(key);
    // Merge base constraints with propagated constraints
    const mergedConstraint = mergeConstraints(
      this.#baseConstraints,
      constraint,
    );
    const {startupCost, fanout, rows} = this.#model(
      this.table,
      this.#sort,
      this.#filters,
      mergedConstraint,
    );
    cost = {
      startupCost,
      scanEst:
        this.limit === undefined
          ? rows
          : Math.min(rows, this.limit / downstreamChildSelectivity),
      cost: 0,
      returnedRows: rows,
      selectivity: this.selectivity,
      limit: this.limit,
      fanout,
    };
    this.#cachedConstraintCosts.set(key, cost);

    if (planDebugger) {
      planDebugger.log({
        type: 'node-cost',
        nodeType: 'connection',
        node: this.name,
        branchPattern,
        downstreamChildSelectivity,
        costEstimate: omitFanout(cost),
        filters: this.#filters,
        ordering: this.#sort,
      });
    }

    return cost;
  }

  /**
   * Remove the limit from this connection.
   * Called when a parent join is flipped, making this connection part of an
   * outer loop that should produce all rows rather than stopping at the limit.
   */
  unlimit(): void {
    if (this.#isRoot) {
      // We cannot unlimit root connections
      return;
    }
    if (this.limit !== undefined) {
      this.limit = undefined;
      // Limit changes do not impact connection costs.
      // Limit is taken into account at the join level.
      // Given that, we do not need to invalidate cost caches here.
    }
  }

  /**
   * Propagate unlimiting when a parent join is flipped.
   * For connections, we simply remove the limit.
   */
  propagateUnlimitFromFlippedJoin(): void {
    this.unlimit();
  }

  reset() {
    this.#constraints.clear();
    this.limit = this.#baseLimit;
    // Clear all cost caches
    this.#cachedConstraintCosts.clear();
  }

  /**
   * Capture constraint state for snapshotting.
   * Used by PlannerGraph to save/restore planning state.
   */
  captureConstraints(): Map<string, PlannerConstraint | undefined> {
    return new Map(this.#constraints);
  }

  /**
   * Restore constraint state from a snapshot.
   * Used by PlannerGraph to restore planning state.
   */
  restoreConstraints(
    constraints: Map<string, PlannerConstraint | undefined>,
  ): void {
    this.#constraints.clear();
    for (const [key, value] of constraints) {
      this.#constraints.set(key, value);
    }
    // Constraints changed, invalidate cost caches
    this.#cachedConstraintCosts.clear();
  }

  /** Get current constraints for debugging. */
  getConstraintsForDebug(): Record<string, PlannerConstraint | undefined> {
    const record: Record<string, PlannerConstraint | undefined> = {};
    for (const [key, value] of this.#constraints) {
      record[key] = value;
    }
    return record;
  }

  /** Get filters for debugging. */
  getFiltersForDebug(): Condition | undefined {
    return this.#filters;
  }

  /** Get sort/ordering for debugging. */
  getSortForDebug(): Ordering {
    return this.#sort;
  }

  /** Get estimated cost for each constraint branch. */
  getConstraintCostsForDebug(): Record<string, CostEstimate> {
    const record: Record<string, CostEstimate> = {};
    for (const [key, value] of this.#cachedConstraintCosts) {
      record[key] = value;
    }
    return record;
  }
}

type FanoutEst = {
  fanout: number;
  confidence: 'high' | 'med' | 'none';
};
export type FanoutCostModel = (columns: string[]) => FanoutEst;

export type CostModelCost = {
  startupCost: number;
  rows: number;
  fanout: FanoutCostModel;
};
export type ConnectionCostModel = (
  table: string,
  sort: Ordering,
  filters: Condition | undefined,
  constraint: PlannerConstraint | undefined,
) => CostModelCost;
