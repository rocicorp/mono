import type {AST} from '../../../zero-protocol/src/ast.ts';
import {PlannerGraph} from './planner-graph.ts';

/**
 * A query can have many sub-queries.
 *
 * Each sub-query, that cannot be hoisted up into a join, is planned independently.
 * To say it another way: each sub-query created by a `related` call is planned independently.
 * Sub-queries created by `whereExists` calls, no matter how nested, are planned as part of the main query.
 */
export type Plans = {
  plan: PlannerGraph;
  subPlans: {[key: string]: Plans};
};

/**
 * This is analogous to the pipeline builder but instead of creating
 * a graph of dataflow operators it creates a graph of planner nodes.
 *
 * Planning involves walking over this graph to determine the best
 * way to execute the query.
 */
export function buildPlanGraph(_ast: AST): Plans {
  return {
    plan: new PlannerGraph(),
    subPlans: {},
  };
}
