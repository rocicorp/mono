import {assert} from '../../../shared/src/asserts.ts';
import type {PlannerJoin} from './planner-join.ts';
import type {PlannerFanOut} from './planner-fan-out.ts';
import type {PlannerFanIn} from './planner-fan-in.ts';
import type {PlannerConnection} from './planner-connection.ts';
import type {PlannerTerminus} from './planner-terminus.ts';
import type {PlannerNode} from './planner-node.ts';
import {PlannerSource, type ConnectionCostModel} from './planner-source.ts';

/**
 * Captured state of a plan for comparison and restoration.
 */
type PlanState = {
  connections: Array<{pinned: boolean}>;
  joins: Array<{type: 'left' | 'flipped'; pinned: boolean}>;
  fanOuts: Array<{type: 'FO' | 'UFO'}>;
  fanIns: Array<{type: 'FI' | 'UFI'}>;
};

export class PlannerGraph {
  // Collections of nodes participating in planning/reset lifecycle
  joins: PlannerJoin[] = [];
  fanOuts: PlannerFanOut[] = [];
  fanIns: PlannerFanIn[] = [];
  connections: PlannerConnection[] = [];

  // The final output node where constraint propagation starts
  #terminus: PlannerTerminus | undefined = undefined;

  // Sources indexed by table name
  readonly #sources = new Map<string, PlannerSource>();

  /**
   * Reset the graph back to an initial state for another planning pass.
   * Resets only nodes that have runtime-mutable state.
   */
  reset() {
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

  // ========================================================================
  // Planning Algorithm
  // ========================================================================

  /**
   * Calculate total cost of the current plan by summing all connection costs.
   */
  getTotalCost(): number {
    let total = 0;
    for (const connection of this.connections) {
      total += connection.estimateCost();
    }
    return total;
  }

  /**
   * Capture the current plan state for later restoration.
   */
  savePlan(): PlanState {
    return {
      connections: this.connections.map(c => ({pinned: c.pinned})),
      joins: this.joins.map(j => ({type: j.type, pinned: j.pinned})),
      fanOuts: this.fanOuts.map(fo => ({type: fo.type})),
      fanIns: this.fanIns.map(fi => ({type: fi.type})),
    };
  }

  /**
   * Restore a previously saved plan state.
   */
  restorePlan(state: PlanState): void {
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
  plan(): void {
    const numAttempts = Math.min(this.connections.length, 6);
    let bestCost = Infinity;
    let bestPlan: PlanState | undefined = undefined;

    for (let i = 0; i < numAttempts; i++) {
      // Reset to initial state
      this.reset();

      // Get initial costs (no propagation yet)
      let costs = this.estimateCosts();
      if (i >= costs.length) break;

      // Pick costs[i] as root for this attempt
      let connection = costs[i].connection;
      connection.pinned = true; // Pin FIRST
      pinAndMaybeFlipJoins(connection); // Then flip/pin joins
      this.propagateConstraints(); // Then propagate

      // Continue with greedy selection
      while (!this.hasPlan()) {
        costs = this.estimateCosts();
        if (costs.length === 0) break;

        connection = costs[0].connection; // Always pick lowest cost
        connection.pinned = true; // Pin FIRST
        pinAndMaybeFlipJoins(connection); // Then flip/pin joins
        this.propagateConstraints(); // Then propagate
      }

      // Evaluate this plan
      const totalCost = this.getTotalCost();
      if (totalCost < bestCost) {
        bestCost = totalCost;
        bestPlan = this.savePlan();
      }
    }

    // Restore best plan
    if (bestPlan) {
      this.restorePlan(bestPlan);
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
