import type {PlannerConnection} from './planner-connection.ts';
import type {PlannerFanIn} from './planner-fan-in.ts';
import type {PlannerFanOut} from './planner-fan-out.ts';
import type {PlannerJoin} from './planner-join.ts';
import type {PlannerTerminus} from './planner-terminus.ts';

export type FromType = 'pinned' | 'unpinned' | 'terminus';

export type PlannerNode =
  | PlannerJoin
  | PlannerConnection
  | PlannerFanOut
  | PlannerFanIn
  | PlannerTerminus;
