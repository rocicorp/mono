import {assert} from '../../../shared/src/asserts.ts';
import {UnflippableJoinError, type PlannerJoin} from './planner-join.ts';
import type {PlannerFanOut} from './planner-fan-out.ts';
import type {PlannerFanIn} from './planner-fan-in.ts';
import type {PlannerConnection} from './planner-connection.ts';
import type {PlannerTerminus} from './planner-terminus.ts';
import type {PlannerNode} from './planner-node.ts';
import {PlannerSource, type ConnectionCostModel} from './planner-source.ts';
import type {PlannerConstraint} from './planner-constraint.ts';
import {must} from '../../../shared/src/must.ts';
import type {
  PlannerDebugDelegate,
  ConnectionCostDetail,
} from './planner-debug-delegate.ts';

/**
 * Captured state of a plan for comparison and restoration.
 */
type PlanState = {
  connections: Array<{pinned: boolean}>;
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
  // ========================================================================
  // GRAPH STRUCTURE (immutable after construction)
  // ========================================================================

  // Sources indexed by table name
  readonly #sources = new Map<string, PlannerSource>();

  // The final output node where constraint propagation starts
  #terminus: PlannerTerminus | undefined = undefined;

  // ========================================================================
  // PLANNING STATE (mutable during search)
  // ========================================================================

  // Collections of nodes with mutable planning state
  joins: PlannerJoin[] = [];
  fanOuts: PlannerFanOut[] = [];
  fanIns: PlannerFanIn[] = [];
  connections: PlannerConnection[] = [];

  // ========================================================================
  // Graph Construction (structural operations)
  // ========================================================================

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

  // ========================================================================
  // Planning Algorithm (state mutation operations)
  // ========================================================================

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
  estimateCosts(): Array<{connection: PlannerConnection; cost: number}> {
    const unpinned = this.getUnpinnedConnections();
    const costs = unpinned.map(connection => ({
      connection,
      cost: connection.estimateCost(),
    }));

    // Sort by cost ascending (lowest cost first)
    costs.sort((a, b) => a.cost - b.cost);

    return costs;
  }

  /**
   * Get detailed connection cost information for debugging.
   * Includes per-branch-pattern cost breakdown.
   */
  getConnectionCostDetails(): ConnectionCostDetail[] {
    const unpinned = this.getUnpinnedConnections();
    const details = unpinned.map(connection => {
      const {branchCosts, branchConstraints} =
        connection.getBranchCostDetails();
      return {
        connection,
        totalCost: connection.estimateCost(),
        branchCosts,
        branchConstraints,
      };
    });

    // Sort by total cost ascending (lowest cost first)
    details.sort((a, b) => a.totalCost - b.totalCost);

    return details;
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

  // ========================================================================
  // Inspection & Debug
  // ========================================================================

  /**
   * Check if all connections have been pinned (planning is complete).
   */
  hasPlan(): boolean {
    return this.connections.every(c => c.pinned);
  }

  /**
   * Get a summary of the current planning state for debugging.
   * Uses single-pass iteration for efficiency.
   */
  getPlanSummary(): {
    totalConnections: number;
    pinnedConnections: number;
    unpinnedConnections: number;
    totalJoins: number;
    pinnedJoins: number;
    flippedJoins: number;
  } {
    let pinnedConnections = 0;
    for (const c of this.connections) {
      if (c.pinned) pinnedConnections++;
    }

    let pinnedJoins = 0;
    let flippedJoins = 0;
    for (const j of this.joins) {
      if (j.pinned) pinnedJoins++;
      if (j.type === 'flipped') flippedJoins++;
    }

    return {
      totalConnections: this.connections.length,
      pinnedConnections,
      unpinnedConnections: this.connections.length - pinnedConnections,
      totalJoins: this.joins.length,
      pinnedJoins,
      flippedJoins,
    };
  }

  /**
   * Calculate total cost of the current plan by multiplying connection costs.
   * Uses log/exp to avoid numerical overflow: exp(Σlog(costs)) = Π(costs)
   *
   * This is correct for nested loop joins where inner connections execute
   * once per outer connection row, making costs multiplicative not additive.
   *
   * Relies on connection-level cost caching to avoid redundant calculations.
   */
  getTotalCost(): number {
    return must(this.#terminus).estimateCost();
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
      connections: this.connections.map(c => ({pinned: c.pinned})),
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
   * Restore connection pinned flags and constraint maps.
   */
  #restoreConnections(state: PlanState): void {
    for (let i = 0; i < this.connections.length; i++) {
      this.connections[i].pinned = state.connections[i].pinned;
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
   * @param debug - If true, logs debug information to stderr (deprecated, use delegate instead)
   * @param delegate - Optional debug delegate to trace planning decisions
   */
  plan(debug = false, delegate?: PlannerDebugDelegate): void {
    const numAttempts = Math.min(
      this.connections.length,
      MAX_PLANNING_ATTEMPTS,
    );
    let bestCost = Infinity;
    let bestPlan: PlanState | undefined = undefined;

    /* eslint-disable no-console */
    for (let i = 0; i < numAttempts; i++) {
      // Reset to initial state
      this.resetPlanningState();

      // Get initial costs (no propagation yet)
      let costs = this.estimateCosts();
      if (i >= costs.length) break;

      const startConnection = costs[i].connection;
      const startCost = costs[i].cost;

      // Delegate: onAttemptStart
      delegate?.onAttemptStart?.(i, startConnection, startCost);

      if (debug) {
        console.error(
          `\n--- Attempt ${i + 1}: Starting with connection at index ${i} (cost: ${startCost}) ---`,
        );
      }

      // Try to pick costs[i] as root for this attempt
      try {
        startConnection.pinned = true; // Pin FIRST
        const flippedJoins = pinAndMaybeFlipJoins(startConnection); // Then flip/pin joins - might throw
        this.propagateConstraints(); // Then propagate

        // Delegate: onConnectionPinned for initial connection
        delegate?.onConnectionPinned?.(
          startConnection,
          startCost,
          flippedJoins,
        );

        let step = 1;
        // Continue with greedy selection
        while (!this.hasPlan()) {
          costs = this.estimateCosts();
          if (costs.length === 0) break;

          // Delegate: onConnectionCosts
          if (delegate?.onConnectionCosts) {
            const costDetails = this.getConnectionCostDetails();
            delegate.onConnectionCosts(step, costDetails);
          }

          if (debug) {
            console.error(
              `  Step ${step}: Available connections: ${costs.length}, costs: [${costs.map(c => c.cost).join(', ')}]`,
            );
          }

          // Try connections in order until one works
          let success = false;
          let pickedCost = 0;
          let pickedConnection: PlannerConnection | undefined;
          let pickedFlippedJoins: PlannerJoin[] = [];

          for (const {connection, cost} of costs) {
            // Save state before attempting this connection
            const stateBeforeAttempt = this.capturePlanningSnapshot();

            try {
              connection.pinned = true; // Pin FIRST
              const flipped = pinAndMaybeFlipJoins(connection); // Then flip/pin joins - might throw
              pickedCost = cost;
              pickedConnection = connection;
              pickedFlippedJoins = flipped;
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
            if (debug)
              console.error(
                `  Step ${step}: No connection could be pinned - plan failed`,
              );
            delegate?.onAttemptFailed?.(i, 'No connection could be pinned');
            break;
          }

          // Only propagate after successful connection selection
          this.propagateConstraints();

          // Delegate: onConnectionPinned
          if (pickedConnection) {
            delegate?.onConnectionPinned?.(
              pickedConnection,
              pickedCost,
              pickedFlippedJoins,
            );
          }

          if (debug) {
            console.error(
              `  Step ${step}: Picked connection with cost ${pickedCost}`,
            );
          }
          step++;
        }

        // Evaluate this plan (if complete)
        if (this.hasPlan()) {
          const totalCost = this.getTotalCost();
          const summary = this.getPlanSummary();

          // Delegate: onAttemptComplete
          delegate?.onAttemptComplete?.(i, totalCost, summary);

          if (debug)
            console.error(
              `  Attempt ${i + 1}: Complete! Total cost: ${totalCost}`,
            );

          if (totalCost < bestCost) {
            bestCost = totalCost;
            bestPlan = this.capturePlanningSnapshot();

            // Delegate: onBestPlanFound
            delegate?.onBestPlanFound?.(i, totalCost, summary);

            if (debug) console.error(`  *** New best plan found! ***`);
          }
        }
      } catch (e) {
        if (e instanceof UnflippableJoinError) {
          // This root connection led to an unreachable path, try next root
          if (debug)
            console.error(`  Attempt ${i + 1}: Failed with unflippable join`);
          delegate?.onAttemptFailed?.(i, 'Unflippable join encountered');
          continue;
        }
        throw e; // Re-throw other errors
      }
    }
    /* eslint-enable no-console */

    // Restore best plan
    if (bestPlan) {
      this.restorePlanningSnapshot(bestPlan);
      // Propagate constraints to ensure all derived state is consistent.
      // While we restore constraint maps from the snapshot, propagation
      // ensures FanOut/FanIn states and any derived values are correct.
      this.propagateConstraints();
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
 *
 * @param flippedJoins - Array to collect joins that were flipped during traversal
 */
function traverseAndPin(
  from: PlannerNode,
  node: PlannerNode,
  flippedJoins: PlannerJoin[],
): void {
  switch (node.kind) {
    case 'join':
      if (node.pinned) {
        // Already pinned, nothing to do
        // downstream must also be pinned so stop traversal
        return;
      }

      const typeBefore = node.type;
      node.flipIfNeeded(from);
      node.pin();
      // If type changed from 'semi' to 'flipped', this join was flipped
      if (typeBefore === 'semi' && node.type === 'flipped') {
        flippedJoins.push(node);
      }
      traverseAndPin(node, node.output, flippedJoins);
      return;
    case 'fan-out':
      for (const output of node.outputs) {
        // fan-out will always be the parent input to its outputs
        // so it will never cause a flip but it will pin them
        traverseAndPin(node, output, flippedJoins);
      }
      return;
    case 'fan-in':
      traverseAndPin(node, node.output, flippedJoins);
      return;
    case 'terminus':
      return;
    case 'connection':
      throw new Error('a connection cannot flow to another connection');
  }
}

export function pinAndMaybeFlipJoins(
  connection: PlannerConnection,
): PlannerJoin[] {
  const flippedJoins: PlannerJoin[] = [];
  traverseAndPin(connection, connection.output, flippedJoins);
  return flippedJoins;
}
