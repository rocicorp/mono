import {assert} from '../../../shared/src/asserts.ts';
import type {
  Condition,
  Ordering,
  SimpleCondition,
} from '../../../zero-protocol/src/ast.ts';
import type {NoSubqueryCondition} from '../builder/filter.ts';
import {
  mergeConstraints,
  type PlannerConstraint,
} from './planner-constraint.ts';
import type {PlanDebugger} from './planner-debug.ts';
import type {PlannerJoin} from './planner-join.ts';
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
 * # Pushed conditions
 * Correlated predicate pushdown copies a parent's condition on a correlation
 * column into the child (`pushed`). When the parent drives the join into the
 * child, the join binds that column to a parent row, so the parent's
 * condition implies the copy and the copy removes no rows. The connection
 * leaves such a copy out of the cost model call. Another join that binds the
 * column does not imply the copy, because its rows need not hold the parent's
 * condition. The connection also leaves every copy out of `selectivity`: a
 * semi-join always binds the column, and a flipped join gets the copy's
 * reduction through `returnedRows` instead.
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
  readonly #pushed: ReadonlySet<SimpleCondition> | undefined;
  /** `#filters` without the pushed conditions. */
  readonly #unpushedFilters: Condition | undefined;
  readonly #model: ConnectionCostModel;
  readonly table: string;
  readonly name: string; // Human-readable name for debugging (defaults to table name)
  readonly #baseConstraints: PlannerConstraint | undefined; // Constraints from parent correlation
  readonly #baseLimit: number | undefined; // Original limit from query structure (never modified)
  readonly selectivity: number; // Fraction of rows passing filters (1.0 = no filtering)
  #output?: PlannerNode | undefined; // Set once during graph construction
  /**
   * The join from the parent into this connection's EXISTS subquery. Set once
   * during graph construction. The pushed conditions are copies of that
   * parent's conditions. Undefined for a root connection, whose parent (if
   * it is a `related` subquery) always drives it.
   */
  #parentJoin: PlannerJoin | undefined;

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

  /**
   * Per-branch filters registered by `PlannerFilter` nodes (simple OR
   * branches) when the enclosing FanIn is in UFI mode. Key is the same
   * `branchPattern.join(',')` used by `#constraints`.
   *
   * These are AND-merged with `#filters` when calling the cost model so
   * the model sees the true filter that the runtime would push to the
   * source for this specific branch.
   */
  readonly #perBranchFilters: Map<string, NoSubqueryCondition> = new Map();

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
    pushed?: ReadonlySet<SimpleCondition>,
  ) {
    this.table = table;
    this.name = name ?? table;
    this.#sort = sort;
    this.#filters = filters;
    this.#pushed = pushed;
    this.#unpushedFilters = pushed
      ? withoutConjuncts(filters, c => pushed.has(c))
      : filters;
    this.#model = model;
    this.#baseConstraints = baseConstraints;
    this.#baseLimit = limit;
    this.limit = limit;
    this.#constraints = new Map();
    this.#isRoot = isRoot;

    this.selectivity = this.#computeSelectivity(this.#unpushedFilters);
  }

  setOutput(node: PlannerNode): void {
    this.#output = node;
  }

  get output(): PlannerNode {
    assert(this.#output !== undefined, 'Output not set');
    return this.#output;
  }

  setParentJoin(join: PlannerJoin): void {
    this.#parentJoin = join;
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

  /**
   * Register a per-branch filter for the given branch pattern. Called by
   * `PlannerFilter` (a simple OR branch) when the enclosing FanIn is in
   * UFI mode. The filter is AND-merged with `this.#filters` when the cost
   * model is invoked for this branch.
   */
  setPerBranchFilter(path: number[], filter: NoSubqueryCondition): void {
    this.#perBranchFilters.set(path.join(','), filter);
    // The cost depends on the filter, so invalidate caches.
    this.#cachedConstraintCosts.clear();
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
    // AND the per-branch filter (registered by a PlannerFilter for a
    // simple OR branch in UFI mode) with the connection-time filter so
    // the cost model sees the same effective filter the runtime applies.
    const perBranchFilter = this.#perBranchFilters.get(key);
    const {startupCost, fanout, rows} = this.#model(
      this.table,
      this.#sort,
      andFilters(this.#filtersFor(mergedConstraint), perBranchFilter),
      mergedConstraint,
    );
    const selectivity = perBranchFilter
      ? this.#computeSelectivity(
          andFilters(this.#unpushedFilters, perBranchFilter),
        )
      : this.selectivity;
    cost = {
      startupCost,
      scanEst:
        this.limit === undefined
          ? rows
          : Math.min(rows, this.limit / downstreamChildSelectivity),
      cost: 0,
      returnedRows: rows,
      selectivity,
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
   * `#filters` without the pushed conditions that `constraint` binds the
   * column of, when the parent drives the join into this connection. The
   * runtime still applies them, but they remove no rows.
   *
   * When that join is flipped, this connection is the outer loop, so every
   * pushed condition counts, even where another flipped join binds its
   * column.
   */
  #filtersFor(
    constraint: PlannerConstraint | undefined,
  ): Condition | undefined {
    const pushed = this.#pushed;
    if (
      !pushed ||
      !constraint ||
      this.#unpushedFilters === this.#filters ||
      this.#parentJoin?.type === 'flipped'
    ) {
      return this.#filters;
    }
    return withoutConjuncts(
      this.#filters,
      c =>
        pushed.has(c) &&
        c.left.type === 'column' &&
        Object.hasOwn(constraint, c.left.name),
    );
  }

  #computeSelectivity(filters: Condition | undefined): number {
    if (this.#baseLimit === undefined || filters === undefined) {
      return 1.0;
    }

    const costWithFilters = this.#model(
      this.table,
      this.#sort,
      filters,
      undefined,
    );
    const costWithoutFilters = this.#model(
      this.table,
      this.#sort,
      undefined,
      undefined,
    );
    return costWithoutFilters.rows > 0
      ? costWithFilters.rows / costWithoutFilters.rows
      : 1.0;
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
    this.#perBranchFilters.clear();
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
   *
   * Per-branch filters are not snapshotted — they are re-derived by the
   * subsequent `propagateConstraints` pass. Clearing them here ensures
   * that any entries left over from the previous iteration don't linger
   * in case the restored plan registers a different set.
   */
  restoreConstraints(
    constraints: Map<string, PlannerConstraint | undefined>,
  ): void {
    this.#constraints.clear();
    this.#perBranchFilters.clear();
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

function andFilters(
  a: Condition | undefined,
  b: Condition | undefined,
): Condition | undefined {
  return a && b ? {type: 'and', conditions: [a, b]} : (a ?? b);
}

/**
 * `c` without the simple conjuncts, including those of nested ANDs, for which
 * `remove` returns true. Returns `c` itself when it removes nothing.
 */
function withoutConjuncts(
  c: Condition | undefined,
  remove: (c: SimpleCondition) => boolean,
): Condition | undefined {
  switch (c?.type) {
    case 'simple':
      return remove(c) ? undefined : c;
    case 'and': {
      const conditions: Condition[] = [];
      for (const x of c.conditions) {
        const y = withoutConjuncts(x, remove);
        if (y !== undefined) {
          conditions.push(y);
        }
      }
      if (
        conditions.length === c.conditions.length &&
        conditions.every((x, i) => x === c.conditions[i])
      ) {
        return c;
      }
      if (conditions.length <= 1) {
        return conditions[0];
      }
      return {type: 'and', conditions};
    }
    default:
      return c;
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
