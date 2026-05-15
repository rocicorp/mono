import {assert} from '../../../shared/src/asserts.ts';
import type {NoSubqueryCondition} from '../builder/filter.ts';
import type {PlannerConnection} from './planner-connection.ts';
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
 * Represents a simple (non-CSQ) branch of an OR in the planner graph.
 *
 * # Why this node exists
 * Without `PlannerFilter`, the planner would not see simple branches of an
 * OR at all — `processOr` historically only admitted subquery-bearing
 * branches into the graph. That meant the cost model never got to weigh
 * the per-branch filter that the runtime actually pushes down via
 * `FetchRequest.filter` for UFI/UFO branches.
 *
 * # What it does
 * - Stores the simple branch's `NoSubqueryCondition`.
 * - Forwards `propagateConstraints` to its input (typically a `PlannerFanOut`
 *   leading back to a `PlannerConnection`).
 * - When the enclosing FanIn is in `UFI` mode, registers the condition at
 *   the receiving `PlannerConnection` under the current `branchPattern` via
 *   `connection.setPerBranchFilter(...)`. This lets the cost model see the
 *   true per-branch filter at fetch time.
 * - In `FI` mode, registration is skipped — at runtime, FI/FO uses a
 *   single shared scan of the source for all branches, so per-branch filters
 *   would mis-credit the source scan cost.
 *
 * # Cost
 * Pass-through. The filter node itself adds no execution cost; the only
 * effect is at the connection layer (lower estimated rows).
 *
 * # Planner/runtime branch-count mismatch
 * For OR with multiple simple branches and a flipped CSQ (e.g.
 * `or(a=1, b=2, exists(...))`), the planner builds one `PlannerFilter` per
 * simple branch, but at runtime `applyFilterWithFlips` bundles all
 * non-flipped branches into a single source fetch with `(a=1 OR b=2)` —
 * one source scan, not N. In UFI mode the planner therefore models N
 * independent source scans against the runtime's 1, slightly inflating
 * the cost of OR-with-many-simples shapes. Common `or(simple, exists)`
 * shapes have only one simple branch and are unaffected.
 */
export class PlannerFilter {
  readonly kind = 'filter' as const;
  readonly #input: Exclude<PlannerNode, PlannerTerminus>;
  readonly #condition: NoSubqueryCondition | undefined;
  #output?: PlannerNode | undefined;

  constructor(
    input: Exclude<PlannerNode, PlannerTerminus>,
    condition: NoSubqueryCondition | undefined,
  ) {
    this.#input = input;
    this.#condition = condition;
  }

  setOutput(node: PlannerNode): void {
    this.#output = node;
  }

  get output(): PlannerNode {
    assert(this.#output !== undefined, 'Output not set');
    return this.#output;
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
      nodeType: 'filter',
      node: 'filter',
      branchPattern,
      constraint,
      from: from?.kind ?? 'unknown',
    });

    // Only register the per-branch filter when we're actually going to do
    // a per-branch fetch at runtime — i.e., when our enclosing FanIn is in
    // UFI mode. In FI mode, all branches share a single source scan, so
    // applying our filter at the source would drop rows that other
    // branches need.
    if (this.#condition && from?.kind === 'fan-in' && from.type === 'UFI') {
      const conn = findParentConnection(this.#input);
      if (conn) {
        conn.setPerBranchFilter(branchPattern, this.#condition);
      }
    }

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
    const cost = this.#input.estimateCost(
      downstreamChildSelectivity,
      branchPattern,
      planDebugger,
    );

    if (planDebugger) {
      planDebugger.log({
        type: 'node-cost',
        nodeType: 'filter',
        node: 'filter',
        branchPattern,
        downstreamChildSelectivity,
        costEstimate: omitFanout(cost),
      });
    }

    return cost;
  }

  /**
   * Forward unlimit propagation along the parent chain (toward the source).
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

  reset(): void {
    // No mutable structural state. Per-branch filters are re-registered on
    // every propagation pass, and PlannerConnection clears its
    // #perBranchFilters in its own reset().
  }

  /** For debugging. */
  getConditionForDebug(): NoSubqueryCondition | undefined {
    return this.#condition;
  }
}

/**
 * Walk the input chain (toward the source) looking for the first
 * `PlannerConnection`. The simple branch's filter is meant to land on the
 * connection that the FanOut wraps — for nested AST shapes, that may be
 * several joins above.
 */
function findParentConnection(
  node: Exclude<PlannerNode, PlannerTerminus>,
): PlannerConnection | undefined {
  switch (node.kind) {
    case 'connection':
      return node;
    case 'fan-out':
      return findParentConnection(node.input);
    case 'join':
      return findParentConnection(node.parent);
    case 'fan-in':
      // Reaching a FanIn while walking up means the simple branch sits
      // above a nested OR/UFI. The connection that matters for *this*
      // simple branch's filter is the one feeding the outer FanOut, but
      // from a nested FanIn we don't have a single input to walk through.
      // Be conservative: don't register, accept that deeply nested OR
      // shapes lose this optimization.
      return undefined;
    case 'filter':
      // Shouldn't occur in graphs built by `processOr` — a PlannerFilter's
      // input is always the FanOut it's a branch of, never another filter.
      return undefined;
  }
}
