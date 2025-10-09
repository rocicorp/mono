import type {Condition, Ordering} from '../../../zero-protocol/src/ast.ts';
import type {Constraint} from '../ivm/constraint.ts';
import type {PlannerNode} from './planner-node.ts';

/**
 * Represents a connection to a source.
 *
 * Connections have:
 * - ordering
 * - filters
 * - constraints
 *
 * The ordering and filters are used to determine the initial cost of the connection.
 *
 * Once the planner has decided on connection to be the outer loop of the query plan,
 * this will reveal constraints that can be sent to other connections.
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
 * `issue` and `assignee` start with no constraints.
 * Once the planner decides to make `issue` the outer loop, it will reveal
 * a constraint on `assignee` of `issue.assignee_id = assignee.id`.
 *
 * If the planner decides to make `assignee` the outer loop, it will reveal
 * a constraint on `issue` of `issue.assignee_id = assignee.id`.
 *
 * Constraints are propagated through the graph, sending them to their connections.
 * Connections can update their cost based on the constraints they have received.
 *
 * The planner will query all connections for their cost and pick the lowest cost connection
 * to be the next outer most loop.
 *
 * This process repeats until no more connections can be chosen. Either because
 * they have been chosen previously or the choices made in the past have pre-determined
 * the rest of the plan.
 *
 * E.g., if we choose `issue` as the outer loop, then `assignee` must be the inner loop
 * since only two connections are involved.
 */
export class PlannerConnection implements PlannerNode {
  pinned: boolean;
  readonly #sort: Ordering;
  readonly #filters: Condition | undefined;
  readonly #model: ConnectionCostModel;

  /**
   * Undefined constraints are possible to handle the case where
   * a FO gets converted to a UFO. If only a single join in the UFO is flipped,
   * all the other joins will report undefined constraints.
   */
  readonly #constraints: Map<string, Constraint | undefined>;

  constructor(
    model: ConnectionCostModel,
    sort: Ordering,
    filters: Condition | undefined,
  ) {
    this.pinned = false;
    this.#sort = sort;
    this.#filters = filters;
    this.#model = model;
    this.#constraints = new Map();
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
  propagateConstraints(path: number[], c: Constraint | undefined): void {
    const key = path.join(',');
    this.#constraints.set(key, c);
  }

  estimateCost(): number {
    if (this.#constraints.size === 0) {
      return this.#model(this.#sort, this.#filters, undefined);
    }

    let total = 0;
    for (const c of this.#constraints.values()) {
      total += this.#model(this.#sort, this.#filters, c);
    }
    return total;
  }

  reset() {
    this.#constraints.clear();
  }
}

export type ConnectionCostModel = (
  sort: Ordering,
  filters: Condition | undefined,
  constraint: Constraint | undefined,
) => number;
