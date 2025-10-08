import type {AST} from '../../../zero-protocol/src/ast.ts';
import {PlannerGraph} from './planner-graph.ts';

export function buildPlanGraph(_ast: AST): PlannerGraph {
  return new PlannerGraph();
}
