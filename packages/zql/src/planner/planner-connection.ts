import {assert} from '../../../shared/src/asserts.ts';
import type {Condition, Ordering} from '../../../zero-protocol/src/ast.ts';
import {
  mergeConstraints,
  type PlannerConstraint,
} from './planner-constraint.ts';
import type {PlanDebugger} from './planner-debug.ts';
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
  readonly filterSelectivity: number; // Fraction of rows passing filters (1.0 = no filtering)
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
   * Outer key: branch pattern (e.g., "0,1")
   * Inner key: source join ID (e.g., "join-5")
   * Value: constraint from that specific join
   *
   * This allows tracking which join contributed which constraint keys,
   * enabling per-join fanout factor lookups for semi-join selectivity.
   */
  readonly #constraints: Map<string, Map<string, PlannerConstraint>>;

  readonly #isRoot: boolean;

  /**
   * Cached per-constraint costs to avoid redundant cost model calls.
   * Maps constraint key (branch pattern string) to computed cost.
   * Invalidated when constraints change.
   */
  #cachedConstraintCosts: Map<string, CostEstimate> = new Map();

  /**
   * Fanout factors per source join ID.
   * Maps source join ID to fanout (average child rows per parent row).
   * Used for computing semi-join selectivity: 1 - (1 - filterSelectivity)^fanOut
   */
  #fanOuts: Map<string, number> = new Map();

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
      this.filterSelectivity =
        costWithoutFilters.rows > 0
          ? costWithFilters.rows / costWithoutFilters.rows
          : 1.0;
    } else {
      // Root connections or connections without filters
      this.filterSelectivity = 1.0;
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

    // Group constraint keys by their source join ID
    if (!c) {
      // Undefined constraint - can happen when FO → UFO and only some branches flip
      // Don't store anything for this branch pattern
      return;
    }

    // Get or create the inner map for this branch pattern
    let sourcesMap = this.#constraints.get(key);
    if (!sourcesMap) {
      sourcesMap = new Map();
      this.#constraints.set(key, sourcesMap);
    }

    // Group constraint columns by source join ID
    const bySource = new Map<string, PlannerConstraint>();
    for (const [col, metadata] of Object.entries(c)) {
      const sourceId = metadata.sourceJoinId ?? 'unknown';
      let sourceConstraint = bySource.get(sourceId);
      if (!sourceConstraint) {
        sourceConstraint = {};
        bySource.set(sourceId, sourceConstraint);
      }
      sourceConstraint[col] = metadata;
    }

    // Store each source's constraints separately
    for (const [sourceId, constraint] of bySource) {
      sourcesMap.set(sourceId, constraint);
    }

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
    const sourcesMap = this.#constraints.get(key);

    // Compute fanout for each source join and store it
    if (sourcesMap) {
      for (const [sourceId, sourceConstraint] of sourcesMap) {
        // Only compute fanout if we don't already have it for this source
        if (!this.#fanOuts.has(sourceId)) {
          // Merge base constraints with this source's constraint
          const constraintForFanout = mergeConstraints(
            this.#baseConstraints,
            sourceConstraint,
          );
          const {fanOut} = this.#model(
            this.table,
            this.#sort,
            this.#filters,
            constraintForFanout,
          );
          // Store fanout (default to 1.0 if undefined)
          this.#fanOuts.set(sourceId, fanOut ?? 1.0);
        }
      }
    }

    // Merge all constraints from all sources for this branch pattern
    let propagatedConstraint: PlannerConstraint | undefined = undefined;
    if (sourcesMap) {
      for (const sourceConstraint of sourcesMap.values()) {
        propagatedConstraint = mergeConstraints(
          propagatedConstraint,
          sourceConstraint,
        );
      }
    }

    // Merge base constraints with all propagated constraints
    const mergedConstraint = mergeConstraints(
      this.#baseConstraints,
      propagatedConstraint,
    );
    const {startupCost, rows} = this.#model(
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
      selectivity: this.filterSelectivity,
      limit: this.limit,
    };
    this.#cachedConstraintCosts.set(key, cost);

    planDebugger?.log({
      type: 'node-cost',
      nodeType: 'connection',
      node: this.name,
      branchPattern,
      downstreamChildSelectivity,
      costEstimate: cost,
      filters: this.#filters,
    });

    return cost;
  }

  /**
   * Get semi-join selectivity for a specific source join.
   * Semi-join selectivity represents the fraction of parent rows that have
   * at least one matching child row.
   *
   * Formula: 1 - (1 - filterSelectivity)^fanOut
   * Where:
   * - filterSelectivity: fraction of rows passing filters
   * - fanOut: average number of child rows per parent row
   *
   * Example:
   * - If filterSelectivity = 0.1 (10% of rows pass filters)
   * - And fanOut = 5 (average 5 child rows per parent)
   * - Then semiJoinSelectivity = 1 - (1 - 0.1)^5 = 1 - 0.9^5 ≈ 0.41
   *
   * This captures the fact that with multiple child rows, it's more likely
   * that at least one passes the filter.
   *
   * @param sourceJoinId - ID of the join whose fanout should be used
   * @returns Semi-join selectivity (0.0 to 1.0)
   */
  getSemiJoinSelectivity(sourceJoinId: string): number {
    const fanOut = this.#fanOuts.get(sourceJoinId) ?? 1.0;
    return 1 - Math.pow(1 - this.filterSelectivity, fanOut);
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
    // Clear fanout cache
    this.#fanOuts.clear();
  }

  /**
   * Capture constraint state for snapshotting.
   * Used by PlannerGraph to save/restore planning state.
   */
  captureConstraints(): Map<string, Map<string, PlannerConstraint>> {
    // Deep copy the nested map structure
    const snapshot = new Map<string, Map<string, PlannerConstraint>>();
    for (const [branchKey, sourcesMap] of this.#constraints) {
      snapshot.set(branchKey, new Map(sourcesMap));
    }
    return snapshot;
  }

  /**
   * Restore constraint state from a snapshot.
   * Used by PlannerGraph to restore planning state.
   */
  restoreConstraints(
    constraints: Map<string, Map<string, PlannerConstraint>>,
  ): void {
    this.#constraints.clear();
    for (const [branchKey, sourcesMap] of constraints) {
      this.#constraints.set(branchKey, new Map(sourcesMap));
    }
    // Constraints changed, invalidate cost caches
    this.#cachedConstraintCosts.clear();
  }

  /**
   * Get current constraints for debugging.
   * Returns a copy of the constraints map.
   */
  getConstraintsForDebug(): Map<string, Map<string, PlannerConstraint>> {
    return this.captureConstraints();
  }

  /**
   * Get constraints for a specific source join within a branch pattern.
   * Used for fanout factor lookups.
   */
  getConstraintsBySource(
    branchPattern: number[],
    sourceJoinId: string,
  ): PlannerConstraint | undefined {
    const key = branchPattern.join(',');
    const sourcesMap = this.#constraints.get(key);
    return sourcesMap?.get(sourceJoinId);
  }

  /**
   * Get filters for debugging.
   * Returns the filters applied to this connection.
   */
  getFiltersForDebug(): Condition | undefined {
    return this.#filters;
  }

  /**
   * Get estimated cost for each constraint branch.
   * Returns a map of constraint key to cost estimate.
   * Forces cost calculation if not already cached.
   */
  getConstraintCostsForDebug(): Map<string, CostEstimate> {
    // Return copy of cached costs
    return new Map(this.#cachedConstraintCosts);
  }
}

export type CostModelCost = {
  startupCost: number;
  rows: number;
  /**
   * Optional fanout factor for semi-join selectivity calculations.
   * Represents the average number of child rows per parent row for
   * the given constraint. Used to compute:
   * semiJoinSelectivity = 1 - (1 - filterSelectivity)^fanOut
   *
   * If undefined, defaults to 1.0 (assumes 1-to-1 relationship).
   */
  fanOut?: number | undefined;
};
export type ConnectionCostModel = (
  table: string,
  sort: Ordering,
  filters: Condition | undefined,
  constraint: PlannerConstraint | undefined,
) => CostModelCost;
