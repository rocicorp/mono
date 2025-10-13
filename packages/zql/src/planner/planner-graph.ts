import {assert} from '../../../shared/src/asserts.ts';
import type {PlannerJoin} from './planner-join.ts';
import type {PlannerFanOut} from './planner-fan-out.ts';
import type {PlannerFanIn} from './planner-fan-in.ts';
import type {PlannerConnection} from './planner-connection.ts';
import type {PlannerTerminus} from './planner-terminus.ts';
import type {PlannerNode} from './planner-node.ts';
import {PlannerSource, type ConnectionCostModel} from './planner-source.ts';

/**
 * Error thrown when attempting to flip a non-flippable join.
 * This indicates that a connection path is unreachable from the current starting point.
 */
export class UnflippableJoinError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'UnflippableJoinError';
  }
}

/**
 * Captured state of a plan for comparison and restoration.
 */
type PlanState = {
  connections: Array<{pinned: boolean}>;
  joins: Array<{type: 'left' | 'flipped'; pinned: boolean}>;
  fanOuts: Array<{type: 'FO' | 'UFO'}>;
  fanIns: Array<{type: 'FI' | 'UFI'}>;
};

/**
 * Central orchestrator for query plan optimization.
 *
 * ARCHITECTURE:
 * This class manages two distinct concerns:
 *
 * 1. GRAPH STRUCTURE (immutable after construction):
 *    - Nodes: Sources, Connections, Joins, FanOut/FanIn, Terminus
 *    - Edges: How nodes connect to each other
 *    - Built once by planner-builder.ts, never modified during planning
 *
 * 2. PLANNING STATE (mutable during search):
 *    - Which connections are pinned
 *    - Which joins are flipped
 *    - What constraints flow where
 *    - Mutated during plan() as we search for optimal plan
 *
 * LIFECYCLE:
 * 1. Build: Construct immutable graph structure via planner-builder
 * 2. Plan: Run plan() which mutates planning state via multi-start greedy search
 * 3. Read: Extract results via joins[].type, connections[].pinned, etc.
 * 4. Reset: Call resetPlanningState() to clear mutable state for replanning
 *
 * MUTATION MODEL:
 * The graph structure never changes, but planning state is mutated in-place
 * for performance. Use capturePlanningSnapshot() and restorePlanningSnapshot()
 * for backtracking during search.
 */
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
   */
  getPlanSummary(): {
    totalConnections: number;
    pinnedConnections: number;
    unpinnedConnections: number;
    totalJoins: number;
    pinnedJoins: number;
    flippedJoins: number;
  } {
    return {
      totalConnections: this.connections.length,
      pinnedConnections: this.connections.filter(c => c.pinned).length,
      unpinnedConnections: this.connections.filter(c => !c.pinned).length,
      totalJoins: this.joins.length,
      pinnedJoins: this.joins.filter(j => j.pinned).length,
      flippedJoins: this.joins.filter(j => j.type === 'flipped').length,
    };
  }

  /**
   * Calculate total cost of the current plan by multiplying connection costs.
   * Uses log/exp to avoid numerical overflow: exp(Σlog(costs)) = Π(costs)
   *
   * This is correct for nested loop joins where inner connections execute
   * once per outer connection row, making costs multiplicative not additive.
   */
  getTotalCost(): number {
    let logSum = 0;
    for (const connection of this.connections) {
      const cost = connection.estimateCost();
      logSum += Math.log(cost);
    }
    return Math.exp(logSum);
  }

  /**
   * Capture a lightweight snapshot of the current planning state.
   * Used for backtracking during multi-start greedy search.
   *
   * Only captures mutable state (pinned flags, join types), not structure.
   *
   * @returns A snapshot that can be restored via restorePlanningSnapshot()
   */
  capturePlanningSnapshot(): PlanState {
    return {
      connections: this.connections.map(c => ({pinned: c.pinned})),
      joins: this.joins.map(j => ({type: j.type, pinned: j.pinned})),
      fanOuts: this.fanOuts.map(fo => ({type: fo.type})),
      fanIns: this.fanIns.map(fi => ({type: fi.type})),
    };
  }

  /**
   * Restore planning state from a previously captured snapshot.
   * Used for backtracking when a planning attempt fails.
   *
   * @param state - Snapshot created by capturePlanningSnapshot()
   */
  restorePlanningSnapshot(state: PlanState): void {
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

    for (let i = 0; i < this.connections.length; i++) {
      this.connections[i].pinned = state.connections[i].pinned;
    }

    // Need to restore joins by calling flip() or reset() to get to correct state
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
   * Tries up to min(connections.length, 6) different starting connections.
   * For iteration i, picks costs[i].connection as the root, then continues
   * with greedy selection of lowest-cost connections.
   *
   * Returns the best plan found across all attempts.
   */
  plan(debug = false): void {
    const numAttempts = Math.min(this.connections.length, 6);
    let bestCost = Infinity;
    let bestPlan: PlanState | undefined = undefined;

    /* eslint-disable no-console */
    for (let i = 0; i < numAttempts; i++) {
      // Reset to initial state
      this.resetPlanningState();

      // Get initial costs (no propagation yet)
      let costs = this.estimateCosts();
      if (i >= costs.length) break;

      if (debug) {
        console.error(
          `\n--- Attempt ${i + 1}: Starting with connection at index ${i} (cost: ${costs[i].cost}) ---`,
        );
      }

      // Try to pick costs[i] as root for this attempt
      try {
        let connection = costs[i].connection;
        connection.pinned = true; // Pin FIRST
        pinAndMaybeFlipJoins(connection); // Then flip/pin joins - might throw
        this.propagateConstraints(); // Then propagate

        let step = 1;
        // Continue with greedy selection
        while (!this.hasPlan()) {
          costs = this.estimateCosts();
          if (costs.length === 0) break;

          if (debug) {
            console.error(
              `  Step ${step}: Available connections: ${costs.length}, costs: [${costs.map(c => c.cost).join(', ')}]`,
            );
          }

          // Try connections in order until one works
          let success = false;
          for (const {connection, cost} of costs) {
            // Save state before attempting this connection
            const stateBeforeAttempt = this.capturePlanningSnapshot();

            try {
              connection.pinned = true; // Pin FIRST
              pinAndMaybeFlipJoins(connection); // Then flip/pin joins - might throw
              this.propagateConstraints(); // Then propagate
              if (debug) {
                console.error(
                  `  Step ${step}: Picked connection with cost ${cost}`,
                );
              }
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
            break;
          }
          step++;
        }

        // Evaluate this plan (if complete)
        if (this.hasPlan()) {
          const totalCost = this.getTotalCost();
          if (debug)
            console.error(
              `  Attempt ${i + 1}: Complete! Total cost: ${totalCost}`,
            );
          if (totalCost < bestCost) {
            bestCost = totalCost;
            bestPlan = this.capturePlanningSnapshot();
            if (debug) console.error(`  *** New best plan found! ***`);
          }
        }
      } catch (e) {
        if (e instanceof UnflippableJoinError) {
          // This root connection led to an unreachable path, try next root
          if (debug)
            console.error(`  Attempt ${i + 1}: Failed with unflippable join`);
          continue;
        }
        throw e; // Re-throw other errors
      }
    }
    /* eslint-enable no-console */

    // Restore best plan
    if (bestPlan) {
      this.restorePlanningSnapshot(bestPlan);
      // After restoring the plan structure, we need to propagate constraints
      // to repopulate the connection constraints based on the restored plan
      this.propagateConstraints();
    }
  }
}

export function pinAndMaybeFlipJoins(connection: PlannerConnection): void {
  function traverse(from: PlannerNode, node: PlannerNode) {
    switch (node.kind) {
      case 'join':
        if (node.pinned) {
          // Already pinned, nothing to do
          // downstream must also be pinned so stop traversal
          return;
        }

        node.maybeFlip(from);
        node.pin();
        traverse(node, node.output);
        return;
      case 'fan-out':
        for (const output of node.outputs) {
          // fan-out will always be the parent input to its outputs
          // so it will never cause a flip
          // but it will pin them.
          // We do not technically have to pin all outputs
          //
          traverse(node, output);
        }
        return;
      case 'fan-in':
        traverse(node, node.output);
        return;
      case 'terminus':
        return;
      case 'connection':
        throw new Error('a connection cannot flow to another connection');
    }
  }

  traverse(connection, connection.output);
}
