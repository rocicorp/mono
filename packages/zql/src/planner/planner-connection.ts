import {assert} from '../../../shared/src/asserts.ts';
import type {Condition, Ordering} from '../../../zero-protocol/src/ast.ts';
import type {PlannerConstraint} from './planner-constraint.ts';
import type {ConstraintPropagationType, PlannerNode} from './planner-node.ts';

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
  #output?: PlannerNode | undefined; // Set once during graph construction

  // ========================================================================
  // MUTABLE PLANNING STATE (changes during plan search)
  // ========================================================================
  pinned: boolean;

  /**
   * Constraints accumulated from parent joins during planning.
   * Key is a path through the graph (e.g., "0,1" for branch pattern [0,1]).
   *
   * Undefined constraints are possible when a FO converts to UFO and only
   * a single join in the UFO is flipped - other branches report undefined.
   */
  readonly #constraints: Map<string, PlannerConstraint | undefined>;

  /**
   * Cached cost result to avoid redundant cost model calls.
   * Invalidated when constraints change.
   */
  #cachedCost: number | undefined = undefined;
  #costDirty = true;

  constructor(
    table: string,
    model: ConnectionCostModel,
    sort: Ordering,
    filters: Condition | undefined,
  ) {
    this.pinned = false;
    this.table = table;
    this.#sort = sort;
    this.#filters = filters;
    this.#model = model;
    this.#constraints = new Map();
  }

  setOutput(node: PlannerNode): void {
    this.#output = node;
  }

  get output(): PlannerNode {
    assert(this.#output !== undefined, 'Output not set');
    return this.#output;
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
    from: ConstraintPropagationType,
  ): void {
    const key = path.join(',');
    this.#constraints.set(key, c);
    // Constraints changed, invalidate cost cache
    this.#costDirty = true;

    if (this.pinned) {
      assert(
        from === 'pinned' || from === 'terminus',
        'It should be impossible for a pinned connection to receive constraints from a non-pinned node',
      );
    }
    if (from === 'pinned') {
      this.pinned = true;
    }
  }

  estimateCost(): number {
    // Return cached cost if still valid
    if (!this.#costDirty && this.#cachedCost !== undefined) {
      return this.#cachedCost;
    }

    // Calculate fresh cost
    let total = 0;
    if (this.#constraints.size === 0) {
      total = this.#model(this.table, this.#sort, this.#filters, undefined);
    } else {
      for (const c of this.#constraints.values()) {
        total += this.#model(this.table, this.#sort, this.#filters, c);
      }
    }

    // Cache result and mark as clean
    this.#cachedCost = total;
    this.#costDirty = false;

    return total;
  }

  reset() {
    this.#constraints.clear();
    this.pinned = false;
    // Clear cost cache
    this.#cachedCost = undefined;
    this.#costDirty = true;
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
    // Constraints changed, invalidate cost cache
    this.#costDirty = true;
  }
}

export type ConnectionCostModel = (
  table: string,
  sort: Ordering,
  filters: Condition | undefined,
  constraint: PlannerConstraint | undefined,
) => number;
