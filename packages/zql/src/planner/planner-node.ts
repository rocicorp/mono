import type {FanoutCostModel, PlannerConnection} from './planner-connection.ts';
import type {PlannerFanIn} from './planner-fan-in.ts';
import type {PlannerFanOut} from './planner-fan-out.ts';
import type {PlannerJoin} from './planner-join.ts';
import type {PlannerTerminus} from './planner-terminus.ts';

/**
 * Union of all node types that can appear in the planner graph.
 * All nodes follow the dual-state pattern described above.
 */
export type PlannerNode =
  | PlannerJoin
  | PlannerConnection
  | PlannerFanOut
  | PlannerFanIn
  | PlannerTerminus;

export type CostEstimate = {
  startupCost: number;
  scanEst: number;

  /**
   * The cumulative cost to run the pipeline so far.
   *
   * In a semi-join, each row output by the parent is multiplied by the cost to evaluate the child.
   * In a flipped join, each row output by the child is multiplied by the cost to evaluate the parent.
   *
   * "each row output by the parent" is determined by the downstreamChildSelectivity parameter in combination
   * with the limit or the rows output by the parent node.
   *
   * We pull on the parent and stop when hitting a limit or exhausting rows.
   */
  cost: number;

  /**
   * The number of rows output from a node.
   * - For a connection, this is the estimated number of rows returned by the source query.
   * - For a semi-join, this is the estimated number of rows that pass the semi-join filter.
   * - For a flipped join, this is the estimated number of rows that match all child rows.
   * - For fan-in, this is the sum of the rows from each input.
   * - For fan-out, this is the rows from its input.
   */
  returnedRows: number;

  /**
   * The selectivity of the node.
   * For a connection, this is the fraction of rows passing filters (1.0 = no filtering).
   * For joins, this is the fraction of parent rows that match child rows.
   * For fan-in, this is the probability of a match in any branch, assuming independent events.
   * For fan-out, this is the selectivity of its input.
   */
  selectivity: number;
  limit: number | undefined;

  fanout: FanoutCostModel;
};

/**
 * Omit the fanout function from a cost estimate for serialization.
 */
export function omitFanout(cost: CostEstimate): Omit<CostEstimate, 'fanout'> {
  const {fanout: _, ...rest} = cost;
  return rest;
}

export type NodeType = PlannerNode['kind'];

export type JoinOrConnection = 'join' | 'connection';

export type JoinType = PlannerJoin['type'];
