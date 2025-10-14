/**
 * Planner Node Types and Architecture
 *
 * All planner nodes follow a DUAL-STATE PATTERN that separates concerns:
 *
 * 1. STRUCTURAL STATE (Immutable):
 *    - Node connections (inputs/outputs in the graph)
 *    - Configuration (filters, constraints, cost models)
 *    - Created once during graph construction by planner-builder
 *    - Never modified during planning algorithm execution
 *
 * 2. PLANNING STATE (Mutable):
 *    - Pinned status (has this node been locked into the plan?)
 *    - Join type (left vs. flipped)
 *    - Accumulated constraints (what do we know from parent joins?)
 *    - Modified by PlannerGraph.plan() during multi-start greedy search
 *    - Can be reset via node.reset() to clear for replanning
 *
 * This separation enables:
 * - Fast planning: Mutate state in-place without copying structure
 * - Multi-start search: Reset state and try different starting points
 * - Backtracking: Capture/restore state snapshots when attempts fail
 *
 * CONSTRAINT PROPAGATION:
 * During planning, constraints flow through the graph via propagateConstraints().
 * The FromType parameter indicates the source:
 * - 'pinned': From a pinned node (locks downstream nodes)
 * - 'unpinned': From an unpinned node (doesn't lock downstream)
 * - 'terminus': From the final output node (starts propagation)
 */
import type {PlannerConnection} from './planner-connection.ts';
import type {PlannerFanIn} from './planner-fan-in.ts';
import type {PlannerFanOut} from './planner-fan-out.ts';
import type {PlannerJoin} from './planner-join.ts';
import type {PlannerTerminus} from './planner-terminus.ts';

/**
 * Indicates where a constraint propagation came from.
 * Determines whether downstream nodes get pinned by the propagation.
 */
export type FromType = 'pinned' | 'unpinned' | 'terminus';

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
