import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import type {PlanDebugger} from './planner-debug.ts';
import type {PlannerConnection} from './planner-connection.ts';
import type {PlannerConstraint} from './planner-constraint.ts';
import type {PlannerFanIn} from './planner-fan-in.ts';
import type {PlannerFanOut} from './planner-fan-out.ts';
import type {PlannerJoin} from './planner-join.ts';
import {omitFanout} from './planner-node.ts';
import type {PlannerNode} from './planner-node.ts';
import {PlannerSource, type ConnectionCostModel} from './planner-source.ts';
import type {PlannerTerminus} from './planner-terminus.ts';

/**
 * Captured state of a plan for comparison and restoration.
 */
export type PlanState = {
  connections: Array<{limit: number | undefined}>;
  joins: Array<{type: 'semi' | 'flipped'}>;
  fanOuts: Array<{type: 'FO' | 'UFO'}>;
  fanIns: Array<{type: 'FI' | 'UFI'}>;
  connectionConstraints: Array<Map<string, PlannerConstraint | undefined>>;
};

/**
 * Maximum number of flippable joins to attempt exhaustive enumeration.
 * With n flippable joins, we explore 2^n plans.
 * 10 joins = 1024 plans (~100-200ms), 12 joins = 4096 plans (~400ms - 1 second)
 */
const MAX_FLIPPABLE_JOINS = 9;

/**
 * Cached information about FanOut→FanIn relationships.
 * Computed once during planning to avoid redundant BFS traversals.
 */
type FOFIInfo = {
  fi: PlannerFanIn | undefined;
  joinsBetween: PlannerJoin[];
};

export class PlannerGraph {
  // Sources indexed by table name
  readonly #sources = new Map<string, PlannerSource>();

  // The final output node where constraint propagation starts
  #terminus: PlannerTerminus | undefined = undefined;

  // Collections of nodes with mutable planning state
  joins: PlannerJoin[] = [];
  fanOuts: PlannerFanOut[] = [];
  fanIns: PlannerFanIn[] = [];
  connections: PlannerConnection[] = [];

  /**
   * Reset all planning state back to initial values for another planning pass.
   * Resets only mutable planning state - graph structure is unchanged.
   *
   * This allows replanning the same query graph with different strategies.
   */
  resetPlanningState() {
    for (const j of this.joins) j.reset();
    for (const fo of this.fanOuts) fo.reset();
    for (const fi of this.fanIns) fi.reset();
    for (const c of this.connections) c.reset();
  }

  /**
   * Create and register a source (table) in the graph.
   */
  addSource(name: string, model: ConnectionCostModel): PlannerSource {
    assert(
      !this.#sources.has(name),
      `Source ${name} already exists in the graph`,
    );
    const source = new PlannerSource(name, model);
    this.#sources.set(name, source);
    return source;
  }

  /**
   * Get a source by table name.
   */
  getSource(name: string): PlannerSource {
    const source = this.#sources.get(name);
    assert(source !== undefined, `Source ${name} not found in the graph`);
    return source;
  }

  /**
   * Check if a source exists by table name.
   */
  hasSource(name: string): boolean {
    return this.#sources.has(name);
  }

  /**
   * Set the terminus (final output) node of the graph.
   * Constraint propagation starts from this node.
   */
  setTerminus(terminus: PlannerTerminus): void {
    this.#terminus = terminus;
  }

  /**
   * Initiate constraint propagation from the terminus node.
   * This sends constraints up through the graph to update
   * connection cost estimates.
   */
  propagateConstraints(planDebugger?: PlanDebugger): void {
    assert(
      this.#terminus !== undefined,
      'Cannot propagate constraints without a terminus node',
    );
    this.#terminus.propagateConstraints(planDebugger);
  }

  /**
   * Calculate total cost of the current plan.
   * Total cost includes both startup cost (one-time, e.g., sorting) and running cost.
   */
  getTotalCost(planDebugger?: PlanDebugger): number {
    const estimate = must(this.#terminus).estimateCost(planDebugger);
    return estimate.cost + estimate.startupCost;
  }

  /**
   * Capture a lightweight snapshot of the current planning state.
   * Used for backtracking during multi-start greedy search.
   *
   * Captures mutable state including pinned flags, join types, and
   * constraint maps to avoid needing repropagation on restore.
   *
   * @returns A snapshot that can be restored via restorePlanningSnapshot()
   */
  capturePlanningSnapshot(): PlanState {
    return {
      connections: this.connections.map(c => ({
        limit: c.limit,
      })),
      joins: this.joins.map(j => ({type: j.type})),
      fanOuts: this.fanOuts.map(fo => ({type: fo.type})),
      fanIns: this.fanIns.map(fi => ({type: fi.type})),
      connectionConstraints: this.connections.map(c => c.captureConstraints()),
    };
  }

  /**
   * Restore planning state from a previously captured snapshot.
   * Used for backtracking when a planning attempt fails.
   *
   * Restores pinned flags, join types, and constraint maps, eliminating
   * the need for repropagation.
   *
   * @param state - Snapshot created by capturePlanningSnapshot()
   */
  restorePlanningSnapshot(state: PlanState): void {
    this.#validateSnapshotShape(state);
    this.#restoreConnections(state);
    this.#restoreJoins(state);
    this.#restoreFanNodes(state);
  }

  /**
   * Validate that snapshot shape matches current graph structure.
   */
  #validateSnapshotShape(state: PlanState): void {
    assert(
      this.connections.length === state.connections.length,
      'Plan state mismatch: connections',
    );
    assert(
      this.joins.length === state.joins.length,
      'Plan state mismatch: joins',
    );
    assert(
      this.fanOuts.length === state.fanOuts.length,
      'Plan state mismatch: fanOuts',
    );
    assert(
      this.fanIns.length === state.fanIns.length,
      'Plan state mismatch: fanIns',
    );
    assert(
      this.connections.length === state.connectionConstraints.length,
      'Plan state mismatch: connectionConstraints',
    );
  }

  /**
   * Restore connection pinned flags, limits, and constraint maps.
   */
  #restoreConnections(state: PlanState): void {
    for (let i = 0; i < this.connections.length; i++) {
      this.connections[i].limit = state.connections[i].limit;
      this.connections[i].restoreConstraints(state.connectionConstraints[i]);
    }
  }

  /**
   * Restore join types and pinned flags.
   */
  #restoreJoins(state: PlanState): void {
    for (let i = 0; i < this.joins.length; i++) {
      const join = this.joins[i];
      const targetState = state.joins[i];

      // Reset to initial state first
      join.reset();

      // Apply target state
      if (targetState.type === 'flipped' && join.type !== 'flipped') {
        join.flip();
      }
      assert(
        targetState.type === join.type,
        'join is not in the correct state after reset',
      );
    }
  }

  /**
   * Restore FanOut and FanIn types.
   */
  #restoreFanNodes(state: PlanState): void {
    for (let i = 0; i < this.fanOuts.length; i++) {
      const fo = this.fanOuts[i];
      const targetType = state.fanOuts[i].type;
      if (targetType === 'UFO' && fo.type === 'FO') {
        fo.convertToUFO();
      }
    }

    for (let i = 0; i < this.fanIns.length; i++) {
      const fi = this.fanIns[i];
      const targetType = state.fanIns[i].type;
      if (targetType === 'UFI' && fi.type === 'FI') {
        fi.convertToUFI();
      }
    }
  }

  /**
   * Main planning algorithm using exhaustive join flip enumeration.
   *
   * Enumerates all possible flip patterns for flippable joins (2^n for n flippable joins).
   * Each pattern represents a different query execution plan. We evaluate the cost of each
   * plan and select the one with the lowest cost.
   *
   * Connections are used only for cost estimation - the flip patterns determine the plan.
   * FanOut/FanIn states (FO/UFO and FI/UFI) are automatically derived from join flip states.
   *
   * @param planDebugger - Optional debugger to receive structured events during planning
   * @param lc - Optional logger for warnings
   */
  plan(planDebugger?: PlanDebugger, lc?: LogContext): void {
    // Get all flippable joins
    const flippableJoins = this.joins.filter(j => j.isFlippable());

    // Too many flippable joins - skip optimization and run as-is
    if (flippableJoins.length > MAX_FLIPPABLE_JOINS) {
      lc?.warn?.(
        `Query has ${flippableJoins.length} EXISTS checks which would require ` +
          `${2 ** flippableJoins.length} plan evaluations. Skipping optimization.`,
      );
      return;
    }

    // Build FO→FI cache once to avoid redundant BFS traversals in each iteration
    const fofiCache = buildFOFICache(this);

    const numPatterns =
      flippableJoins.length === 0 ? 0 : 2 ** flippableJoins.length;
    let bestCost = Infinity;
    let bestPlan: PlanState | undefined = undefined;
    let bestAttemptNumber = -1;

    // Enumerate all flip patterns
    for (let pattern = 0; pattern < numPatterns; pattern++) {
      // Reset to initial state
      this.resetPlanningState();

      if (planDebugger) {
        planDebugger.log({
          type: 'attempt-start',
          attemptNumber: pattern,
          totalAttempts: numPatterns,
        });
      }

      // Apply flip pattern (treat pattern as bitmask)
      // Bit i set to 1 means flip join i
      for (let i = 0; i < flippableJoins.length; i++) {
        if (pattern & (1 << i)) {
          flippableJoins[i].flip();
        }
      }

      // Derive FO/UFO and FI/UFI states from join flip states
      checkAndConvertFOFI(fofiCache);

      // Propagate unlimiting for flipped joins
      propagateUnlimitForFlippedJoins(this);

      // Propagate constraints through the graph
      this.propagateConstraints(planDebugger);

      if (planDebugger) {
        planDebugger.log({
          type: 'constraints-propagated',
          attemptNumber: pattern,
          connectionConstraints: this.connections.map(c => {
            const constraintCosts = c.getConstraintCostsForDebug();
            const constraintCostsWithoutFanout: Record<
              string,
              Omit<(typeof constraintCosts)[string], 'fanout'>
            > = {};
            for (const [key, cost] of Object.entries(constraintCosts)) {
              constraintCostsWithoutFanout[key] = omitFanout(cost);
            }
            return {
              connection: c.name,
              constraints: c.getConstraintsForDebug(),
              constraintCosts: constraintCostsWithoutFanout,
            };
          }),
        });
      }

      // Evaluate this plan
      const totalCost = this.getTotalCost(planDebugger);

      if (planDebugger) {
        planDebugger.log({
          type: 'plan-complete',
          attemptNumber: pattern,
          totalCost,
          flipPattern: pattern, // Bitmask of which joins are flipped
          planSnapshot: this.capturePlanningSnapshot(),
          joinStates: this.joins.map(j => {
            const info = j.getDebugInfo();
            return {
              join: info.name,
              type: info.type,
            };
          }),
        });
      }

      // Track best plan
      if (totalCost < bestCost) {
        bestCost = totalCost;
        bestPlan = this.capturePlanningSnapshot();
        bestAttemptNumber = pattern;
      }
    }

    // Restore best plan
    if (bestPlan) {
      this.restorePlanningSnapshot(bestPlan);
      // Propagate constraints to ensure all derived state is consistent
      this.propagateConstraints(planDebugger);

      if (planDebugger) {
        planDebugger.log({
          type: 'best-plan-selected',
          bestAttemptNumber,
          totalCost: bestCost,
          flipPattern: bestAttemptNumber, // The best attempt number is also the flip pattern
          joinStates: this.joins.map(j => ({
            join: j.getName(),
            type: j.type,
          })),
        });
      }
    } else {
      assert(
        numPatterns === 0,
        'no plan was found but flippable joins did exist!',
      );
    }
  }
}

/**
 * Build cache of FO→FI relationships and joins between them.
 * Called once at the start of planning to avoid redundant BFS traversals.
 */
function buildFOFICache(graph: PlannerGraph): Map<PlannerFanOut, FOFIInfo> {
  const cache = new Map<PlannerFanOut, FOFIInfo>();

  for (const fo of graph.fanOuts) {
    const info = findFIAndJoins(fo);
    cache.set(fo, info);
  }

  return cache;
}

/**
 * Check if any joins downstream of a FanOut (before reaching FanIn) are flipped.
 * If so, convert the FO to UFO and the FI to UFI.
 *
 * This must be called after join flipping and before propagateConstraints.
 */
function checkAndConvertFOFI(fofiCache: Map<PlannerFanOut, FOFIInfo>): void {
  for (const [fo, info] of fofiCache) {
    const hasFlippedJoin = info.joinsBetween.some(j => j.type === 'flipped');
    if (info.fi && hasFlippedJoin) {
      fo.convertToUFO();
      info.fi.convertToUFI();
    }
  }
}

/**
 * Traverse from a FanOut through its outputs to find the corresponding FanIn
 * and collect all joins along the way.
 */
function findFIAndJoins(fo: PlannerFanOut): FOFIInfo {
  const joinsBetween: PlannerJoin[] = [];
  let fi: PlannerFanIn | undefined = undefined;

  // BFS through FO outputs to find FI and collect joins
  const queue: PlannerNode[] = [...fo.outputs];
  const visited = new Set<PlannerNode>();

  while (queue.length > 0) {
    const node = must(queue.shift());
    if (visited.has(node)) continue;
    visited.add(node);

    switch (node.kind) {
      case 'join':
        joinsBetween.push(node);
        queue.push(node.output);
        break;
      case 'fan-out':
        // Nested FO - traverse its outputs
        queue.push(...node.outputs);
        break;
      case 'fan-in':
        // Found the FI - this is the boundary, don't traverse further
        fi = node;
        break;
      case 'connection':
        // Shouldn't happen in a well-formed graph
        break;
      case 'terminus':
        // Reached the end without finding FI
        break;
    }
  }

  return {fi, joinsBetween};
}

/**
 * Propagate unlimiting to all flipped joins in the graph.
 * When a join is flipped, its child becomes the outer loop and should no longer
 * be limited by EXISTS semantics.
 *
 * This must be called after join flipping and before propagateConstraints.
 */
function propagateUnlimitForFlippedJoins(graph: PlannerGraph): void {
  for (const join of graph.joins) {
    if (join.type === 'flipped') {
      join.propagateUnlimit();
    }
  }
}
