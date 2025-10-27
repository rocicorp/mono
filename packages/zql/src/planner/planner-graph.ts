import {assert} from '../../../shared/src/asserts.ts';
import {UnflippableJoinError, type PlannerJoin} from './planner-join.ts';
import type {PlannerFanOut} from './planner-fan-out.ts';
import type {PlannerFanIn} from './planner-fan-in.ts';
import type {PlannerConnection} from './planner-connection.ts';
import type {PlannerTerminus} from './planner-terminus.ts';
import type {CostEstimate, PlannerNode} from './planner-node.ts';
import {PlannerSource, type ConnectionCostModel} from './planner-source.ts';
import type {PlannerConstraint} from './planner-constraint.ts';
import {must} from '../../../shared/src/must.ts';
import type {PlanDebugger} from './planner-debug.ts';

/**
 * Captured state of a plan for comparison and restoration.
 */
type PlanState = {
  connections: Array<{pinned: boolean; limit: number | undefined}>;
  joins: Array<{type: 'semi' | 'flipped'; pinned: boolean}>;
  fanOuts: Array<{type: 'FO' | 'UFO'}>;
  fanIns: Array<{type: 'FI' | 'UFI'}>;
  connectionConstraints: Array<Map<string, PlannerConstraint | undefined>>;
};

/**
 * Maximum number of different starting connections to try during multi-start search.
 * Higher values explore more of the search space but take longer.
 */
const MAX_PLANNING_ATTEMPTS = 6;

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
   * Get all connections that haven't been pinned yet.
   * These are candidates for selection in the next planning iteration.
   */
  getUnpinnedConnections(): PlannerConnection[] {
    return this.connections.filter(c => !c.pinned);
  }

  /**
   * Trigger cost estimation on all unpinned connections and return
   * them sorted by cost (lowest first).
   *
   * This should be called after constraint propagation so connections
   * have up-to-date constraint information.
   */
  getUnpinnedConnectionCosts(): Array<{
    connection: PlannerConnection;
    cost: number;
  }> {
    const unpinned = this.getUnpinnedConnections();
    const costs = unpinned.map(connection => ({
      connection,
      // Pass undefined to get sum of all branch costs
      cost: connection.estimateCost(undefined).runningCost,
    }));

    // Sort by cost ascending (lowest cost first)
    costs.sort((a, b) => a.cost - b.cost);

    return costs;
  }

  /**
   * Initiate constraint propagation from the terminus node.
   * This sends constraints up through the graph to update
   * connection cost estimates.
   */
  propagateConstraints(): void {
    assert(
      this.#terminus !== undefined,
      'Cannot propagate constraints without a terminus node',
    );
    this.#terminus.propagateConstraints();
  }

  /**
   * Check if all connections have been pinned (planning is complete).
   */
  hasPlan(): boolean {
    return this.connections.every(c => c.pinned);
  }

  /**
   * Calculate total cost of the current plan.
   */
  getTotalCost(): number {
    return must(this.#terminus).estimateCost().runningCost;
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
        pinned: c.pinned,
        limit: c.limit,
      })),
      joins: this.joins.map(j => ({type: j.type, pinned: j.pinned})),
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
   * Collect cost estimates from all nodes in the graph for debugging.
   */
  #collectNodeCosts(): Array<{
    node: string;
    nodeType: 'connection' | 'join' | 'fan-out' | 'fan-in' | 'terminus';
    costEstimate: CostEstimate;
  }> {
    const costs: Array<{
      node: string;
      nodeType: 'connection' | 'join' | 'fan-out' | 'fan-in' | 'terminus';
      costEstimate: CostEstimate;
    }> = [];

    // Collect connection costs
    for (const c of this.connections) {
      costs.push({
        node: c.name,
        nodeType: 'connection',
        costEstimate: c.estimateCost(undefined),
      });
    }

    // Collect join costs
    for (const j of this.joins) {
      costs.push({
        node: j.getName(),
        nodeType: 'join',
        costEstimate: j.estimateCost(undefined),
      });
    }

    // Collect fan-out costs
    for (const fo of this.fanOuts) {
      costs.push({
        node: 'FO',
        nodeType: 'fan-out',
        costEstimate: fo.estimateCost(undefined),
      });
    }

    // Collect fan-in costs
    for (const fi of this.fanIns) {
      costs.push({
        node: 'FI',
        nodeType: 'fan-in',
        costEstimate: fi.estimateCost(undefined),
      });
    }

    return costs;
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
      this.connections[i].pinned = state.connections[i].pinned;
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
      if (targetState.type === 'flipped') {
        join.flip();
      }
      if (targetState.pinned) {
        join.pin();
      }
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
   * Main planning algorithm using multi-start greedy search.
   *
   * Tries up to min(connections.length, MAX_PLANNING_ATTEMPTS) different starting connections.
   * For iteration i, picks costs[i].connection as the root, then continues
   * with greedy selection of lowest-cost connections.
   *
   * Returns the best plan found across all attempts.
   *
   * @param planDebugger - Optional debugger to receive structured events during planning
   */
  plan(planDebugger?: PlanDebugger): void {
    const numAttempts = Math.min(
      this.connections.length,
      MAX_PLANNING_ATTEMPTS,
    );
    let bestCost = Infinity;
    let bestPlan: PlanState | undefined = undefined;
    let bestAttemptNumber = -1;

    for (let i = 0; i < numAttempts; i++) {
      // Reset to initial state
      this.resetPlanningState();

      if (planDebugger) {
        planDebugger.log({
          type: 'attempt-start',
          attemptNumber: i,
          totalAttempts: numAttempts,
        });
      }

      // Get initial costs (no propagation yet)
      let costs = this.getUnpinnedConnectionCosts();
      if (i >= costs.length) break;

      if (planDebugger) {
        planDebugger.log({
          type: 'connection-costs',
          attemptNumber: i,
          costs: costs.map(c => ({
            connection: c.connection.name,
            cost: c.cost,
            costEstimate: c.connection.estimateCost(undefined),
            pinned: c.connection.pinned,
            constraints: c.connection.getConstraintsForDebug(),
            constraintCosts: c.connection.getConstraintCostsForDebug(),
          })),
        });
      }

      // Try to pick costs[i] as root for this attempt
      try {
        let connection = costs[i].connection;
        connection.pinned = true; // Pin FIRST

        if (planDebugger) {
          planDebugger.log({
            type: 'connection-selected',
            attemptNumber: i,
            connection: connection.name,
            cost: costs[i].cost,
            isRoot: true,
          });
        }

        pinAndMaybeFlipJoins(connection); // Then flip/pin joins - might throw
        checkAndConvertFOFI(this); // Convert FO/FI to UFO/UFI if joins flipped
        propagateUnlimitForFlippedJoins(this); // Unlimit children of flipped joins
        this.propagateConstraints(); // Then propagate

        if (planDebugger) {
          planDebugger.log({
            type: 'constraints-propagated',
            attemptNumber: i,
            connectionConstraints: this.connections.map(c => ({
              connection: c.name,
              constraints: c.getConstraintsForDebug(),
              constraintCosts: c.getConstraintCostsForDebug(),
            })),
          });
        }

        // Continue with greedy selection
        while (!this.hasPlan()) {
          costs = this.getUnpinnedConnectionCosts();
          if (costs.length === 0) break;

          if (planDebugger) {
            planDebugger.log({
              type: 'connection-costs',
              attemptNumber: i,
              costs: costs.map(c => ({
                connection: c.connection.name,
                cost: c.cost,
                costEstimate: c.connection.estimateCost(undefined),
                pinned: c.connection.pinned,
                constraints: c.connection.getConstraintsForDebug(),
                constraintCosts: c.connection.getConstraintCostsForDebug(),
              })),
            });
          }

          // Try connections in order until one works
          let success = false;
          for (const {connection} of costs) {
            // Save state before attempting this connection
            const stateBeforeAttempt = this.capturePlanningSnapshot();

            try {
              connection.pinned = true; // Pin FIRST

              if (planDebugger) {
                planDebugger.log({
                  type: 'connection-selected',
                  attemptNumber: i,
                  connection: connection.name,
                  cost: connection.estimateCost(undefined).runningCost,
                  isRoot: false,
                });
              }

              pinAndMaybeFlipJoins(connection); // Then flip/pin joins - might throw
              checkAndConvertFOFI(this); // Convert FO/FI to UFO/UFI if joins flipped
              propagateUnlimitForFlippedJoins(this); // Unlimit children of flipped joins
              success = true;
              break; // Success, exit the inner loop
            } catch (e) {
              if (e instanceof UnflippableJoinError) {
                // Restore to state before this attempt
                this.restorePlanningSnapshot(stateBeforeAttempt);
                // Try next connection
                continue;
              }
              throw e; // Re-throw other errors
            }
          }

          if (!success) {
            // No connection could be pinned, this plan attempt failed
            if (planDebugger) {
              planDebugger.log({
                type: 'plan-failed',
                attemptNumber: i,
                reason:
                  'No connection could be pinned (all attempts led to unflippable joins)',
              });
            }
            break;
          }

          // Only propagate after successful connection selection
          this.propagateConstraints();

          if (planDebugger) {
            planDebugger.log({
              type: 'constraints-propagated',
              attemptNumber: i,
              connectionConstraints: this.connections.map(c => ({
                connection: c.name,
                constraints: c.getConstraintsForDebug(),
                constraintCosts: c.getConstraintCostsForDebug(),
              })),
            });
          }
        }

        // Evaluate this plan (if complete)
        if (this.hasPlan()) {
          const totalCost = this.getTotalCost();

          if (planDebugger) {
            planDebugger.log({
              type: 'plan-complete',
              attemptNumber: i,
              totalCost,
              nodeCosts: this.#collectNodeCosts(),
              joinStates: this.joins.map(j => {
                const info = j.getDebugInfo();
                return {
                  join: info.name,
                  type: info.type,
                  pinned: info.pinned,
                };
              }),
            });
          }

          if (totalCost < bestCost) {
            bestCost = totalCost;
            bestPlan = this.capturePlanningSnapshot();
            bestAttemptNumber = i;
          }
        }
      } catch (e) {
        if (e instanceof UnflippableJoinError) {
          // This root connection led to an unreachable path, try next root
          if (planDebugger) {
            planDebugger.log({
              type: 'plan-failed',
              attemptNumber: i,
              reason: `Root connection led to unflippable join: ${e.message}`,
            });
          }
          continue;
        }
        throw e; // Re-throw other errors
      }
    }

    // Restore best plan
    if (bestPlan) {
      this.restorePlanningSnapshot(bestPlan);
      // Propagate constraints to ensure all derived state is consistent.
      // While we restore constraint maps from the snapshot, propagation
      // ensures FanOut/FanIn states and any derived values are correct.
      this.propagateConstraints();

      if (planDebugger) {
        planDebugger.log({
          type: 'best-plan-selected',
          bestAttemptNumber,
          totalCost: bestCost,
          joinStates: this.joins.map(j => ({
            join: j.getName(),
            type: j.type,
          })),
        });
      }
    }
  }
}

/**
 * Traverse from a connection through the graph, pinning and flipping joins as needed.
 *
 * When a connection is selected, we traverse downstream and:
 * - Pin all joins on the path
 * - Flip joins where the connection is the child input
 *
 * This ensures the selected connection runs in the outer loop.
 * FO/FI conversion to UFO/UFI is handled separately by checkAndConvertFOFI.
 */
function traverseAndPin(from: PlannerNode, node: PlannerNode): void {
  switch (node.kind) {
    case 'join':
      if (node.pinned) {
        // Already pinned, nothing to do
        // downstream must also be pinned so stop traversal
        return;
      }

      node.flipIfNeeded(from);
      node.pin();
      traverseAndPin(node, node.output);
      return;
    case 'fan-out':
      for (const output of node.outputs) {
        // fan-out will always be the parent input to its outputs
        // so it will never cause a flip but it will pin them
        traverseAndPin(node, output);
      }
      return;
    case 'fan-in':
      traverseAndPin(node, node.output);
      return;
    case 'terminus':
      return;
    case 'connection':
      throw new Error('a connection cannot flow to another connection');
  }
}

/**
 * Check if any joins downstream of a FanOut (before reaching FanIn) are flipped.
 * If so, convert the FO to UFO and the FI to UFI.
 *
 * This must be called after pinAndMaybeFlipJoins and before propagateConstraints.
 */
function checkAndConvertFOFI(graph: PlannerGraph): void {
  for (const fo of graph.fanOuts) {
    const {fi, hasFlippedJoin} = findFIAndCheckFlips(fo);
    if (fi && hasFlippedJoin) {
      fo.convertToUFO();
      fi.convertToUFI();
    }
  }
}

/**
 * Traverse from a FanOut through its outputs to find the corresponding FanIn,
 * checking if any joins along the way are flipped.
 */
function findFIAndCheckFlips(fo: PlannerFanOut): {
  fi: PlannerFanIn | undefined;
  hasFlippedJoin: boolean;
} {
  let hasFlippedJoin = false;
  let fi: PlannerFanIn | undefined = undefined;

  // BFS through FO outputs to find FI
  const queue: PlannerNode[] = [...fo.outputs];
  const visited = new Set<PlannerNode>();

  while (queue.length > 0) {
    const node = must(queue.shift());
    if (visited.has(node)) continue;
    visited.add(node);

    switch (node.kind) {
      case 'join':
        if (node.type === 'flipped') {
          hasFlippedJoin = true;
        }
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

  return {fi, hasFlippedJoin};
}

export function pinAndMaybeFlipJoins(connection: PlannerConnection): void {
  traverseAndPin(connection, connection.output);
}

/**
 * Propagate unlimiting to all flipped joins in the graph.
 * When a join is flipped, its child becomes the outer loop and should no longer
 * be limited by EXISTS semantics.
 *
 * This must be called after pinAndMaybeFlipJoins and before propagateConstraints.
 */
function propagateUnlimitForFlippedJoins(graph: PlannerGraph): void {
  for (const join of graph.joins) {
    if (join.type === 'flipped') {
      join.propagateUnlimit();
    }
  }
}
