import type {PlannerConnection} from './planner-connection.ts';
import type {PlannerJoin} from './planner-join.ts';
import type {PlannerConstraint} from './planner-constraint.ts';

/**
 * Summary of the current planning state.
 */
export type PlanSummary = {
  totalConnections: number;
  pinnedConnections: number;
  unpinnedConnections: number;
  totalJoins: number;
  pinnedJoins: number;
  flippedJoins: number;
};

/**
 * Connection cost details including per-branch-pattern breakdown.
 */
export type ConnectionCostDetail = {
  connection: PlannerConnection;
  totalCost: number;
  /** Map of branch pattern (e.g., "0,1") to individual cost for that pattern */
  branchCosts: Map<string, number>;
  /** Map of branch pattern to the constraint used for that pattern */
  branchConstraints: Map<string, PlannerConstraint | undefined>;
};

/**
 * Debug delegate interface for tracing planner decisions.
 *
 * All methods are optional. Implement only the events you want to observe.
 *
 * The planner will call these methods at key decision points during planning,
 * allowing you to trace, visualize, or record the planning process.
 */
export interface PlannerDebugDelegate {
  /**
   * Called at the start of each planning attempt.
   *
   * @param attempt - Zero-based attempt number
   * @param connection - The connection being tried as the root for this attempt
   * @param cost - The initial cost of this connection
   */
  onAttemptStart?(
    attempt: number,
    connection: PlannerConnection,
    cost: number,
  ): void;

  /**
   * Called after estimating connection costs (during greedy selection).
   *
   * Provides detailed breakdown of costs including per-branch-pattern information
   * for connections involved in OR queries (FanOut/FanIn structures).
   *
   * @param step - The step number within the current attempt (0 = initial, 1+ = greedy steps)
   * @param costs - Array of connection cost details, sorted by total cost (lowest first)
   */
  onConnectionCosts?(step: number, costs: ConnectionCostDetail[]): void;

  /**
   * Called when a connection is successfully pinned.
   *
   * @param connection - The connection that was pinned
   * @param cost - The cost of the connection at the time it was pinned
   * @param flippedJoins - Array of joins that were flipped as a result of pinning this connection
   */
  onConnectionPinned?(
    connection: PlannerConnection,
    cost: number,
    flippedJoins: PlannerJoin[],
  ): void;

  /**
   * Called when a planning attempt completes successfully.
   *
   * @param attempt - Zero-based attempt number
   * @param totalCost - The total cost of the completed plan
   * @param summary - Summary of the plan state
   */
  onAttemptComplete?(
    attempt: number,
    totalCost: number,
    summary: PlanSummary,
  ): void;

  /**
   * Called when a new best plan is found.
   *
   * @param attempt - Zero-based attempt number that produced this plan
   * @param cost - The cost of the new best plan
   * @param summary - Summary of the plan state
   */
  onBestPlanFound?(attempt: number, cost: number, summary: PlanSummary): void;

  /**
   * Called when a planning attempt fails.
   *
   * @param attempt - Zero-based attempt number
   * @param reason - Description of why the attempt failed
   */
  onAttemptFailed?(attempt: number, reason: string): void;
}
