import type {PlannerConnection} from './planner-connection.ts';
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
  baseCardinality: number;
  runningCost: number;
  selectivity: number;
  limit: number | undefined;
};

export type NodeType = PlannerNode['kind'];

export type JoinOrConnection = 'join' | 'connection';

export type JoinType = PlannerJoin['type'];
