import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import type {
  AST,
  Condition,
  Conjunction,
  CorrelatedSubqueryCondition,
  Disjunction,
} from '../../../zero-protocol/src/ast.ts';
import {planIdSymbol} from '../../../zero-protocol/src/ast.ts';
import type {ConnectionCostModel} from './planner-connection.ts';
import type {PlannerConstraint} from './planner-constraint.ts';
import {PlannerFanIn} from './planner-fan-in.ts';
import {PlannerFanOut} from './planner-fan-out.ts';
import {PlannerGraph} from './planner-graph.ts';
import {PlannerJoin} from './planner-join.ts';
import type {PlannerNode} from './planner-node.ts';
import {PlannerTerminus} from './planner-terminus.ts';

/**
 * Helper to wire an output to a node, handling the different node types.
 * PlannerFanOut uses addOutput(), others use setOutput().
 */
function wireOutput(from: PlannerNode, to: PlannerNode): void {
  switch (from.kind) {
    case 'connection':
    case 'join':
    case 'fan-in':
      from.setOutput(to);
      break;
    case 'fan-out':
      from.addOutput(to);
      break;
    case 'terminus':
      assert(false, 'Terminus nodes cannot have outputs');
  }
}

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
 *
 * @param baseConstraints - Constraints from parent correlation (for related subqueries)
 */
export function buildPlanGraph(
  ast: AST,
  model: ConnectionCostModel,
  baseConstraints?: PlannerConstraint,
): Plans {
  const graph = new PlannerGraph();
  let nextPlanId = 0;

  // Create source for the main table
  const source = graph.addSource(ast.table, model);

  // Create the main connection with ordering and filters
  const connection = source.connect(
    ast.orderBy ?? [],
    ast.where,
    baseConstraints,
  );
  graph.connections.push(connection);

  // Process WHERE clause to build joins
  let end: PlannerNode = connection;
  if (ast.where) {
    end = processCondition(
      ast.where,
      end,
      graph,
      model,
      ast.table,
      () => nextPlanId++,
    );
  }

  // Create terminus and wire the end node to it
  const terminus = new PlannerTerminus(end);
  wireOutput(end, terminus);
  graph.setTerminus(terminus);

  // Build subplans for 'related' queries
  const subPlans: {[key: string]: Plans} = {};
  if (ast.related) {
    for (const csq of ast.related) {
      const alias = must(
        csq.subquery.alias,
        'Related subquery must have alias',
      );
      // Extract base constraints from the correlation for the child table
      const childConstraints = extractConstraint(
        csq.correlation.childField,
        csq.subquery.table,
      );
      subPlans[alias] = buildPlanGraph(csq.subquery, model, childConstraints);
    }
  }

  return {plan: graph, subPlans};
}

/**
 * Process a condition and build the appropriate planner nodes.
 */
function processCondition(
  condition: Condition,
  input: PlannerNode,
  graph: PlannerGraph,
  model: ConnectionCostModel,
  parentTable: string,
  getPlanId: () => number,
): PlannerNode {
  switch (condition.type) {
    case 'simple':
      // Simple conditions don't create joins, they're just filters in connections
      return input;

    case 'and':
      return processAnd(condition, input, graph, model, parentTable, getPlanId);

    case 'or':
      return processOr(condition, input, graph, model, parentTable, getPlanId);

    case 'correlatedSubquery':
      return processCorrelatedSubquery(
        condition,
        input,
        graph,
        model,
        parentTable,
        getPlanId,
      );
  }
}

/**
 * Process AND condition - creates sequential joins.
 */
function processAnd(
  condition: Conjunction,
  input: PlannerNode,
  graph: PlannerGraph,
  model: ConnectionCostModel,
  parentTable: string,
  getPlanId: () => number,
): PlannerNode {
  let end = input;
  for (const subCondition of condition.conditions) {
    end = processCondition(
      subCondition,
      end,
      graph,
      model,
      parentTable,
      getPlanId,
    );
  }
  return end;
}

/**
 * Process OR condition - creates FanOut -> joins -> FanIn.
 * Only creates fan structure if there are correlated subqueries.
 */
function processOr(
  condition: Disjunction,
  input: PlannerNode,
  graph: PlannerGraph,
  model: ConnectionCostModel,
  parentTable: string,
  getPlanId: () => number,
): PlannerNode {
  // Separate subquery conditions from simple conditions
  const subqueryConditions = condition.conditions.filter(
    c => c.type === 'correlatedSubquery' || hasCorrelatedSubquery(c),
  );

  // If no subqueries, no planning needed (just filters)
  if (subqueryConditions.length === 0) {
    return input;
  }

  // Create FanOut
  const fanOut = new PlannerFanOut(input);
  graph.fanOuts.push(fanOut);
  wireOutput(input, fanOut);

  // Process each branch
  const branches: PlannerNode[] = [];
  for (const subCondition of subqueryConditions) {
    const branch = processCondition(
      subCondition,
      fanOut,
      graph,
      model,
      parentTable,
      getPlanId,
    );
    branches.push(branch);
    fanOut.addOutput(branch);
  }

  // Create FanIn
  const fanIn = new PlannerFanIn(branches);
  graph.fanIns.push(fanIn);
  for (const branch of branches) {
    wireOutput(branch, fanIn);
  }

  return fanIn;
}

/**
 * Process a correlated subquery (EXISTS/NOT EXISTS) - creates Connection + Join.
 */
function processCorrelatedSubquery(
  condition: CorrelatedSubqueryCondition,
  input: PlannerNode,
  graph: PlannerGraph,
  model: ConnectionCostModel,
  parentTable: string,
  getPlanId: () => number,
): PlannerNode {
  const {related} = condition;
  const childTable = related.subquery.table;

  // Create source for child table if not exists
  const childSource = graph.hasSource(childTable)
    ? graph.getSource(childTable)
    : graph.addSource(childTable, model);

  // Create connection for child
  const childConnection = childSource.connect(
    related.subquery.orderBy ?? [],
    related.subquery.where,
  );
  graph.connections.push(childConnection);

  // Process nested WHERE clause in child
  let childEnd: PlannerNode = childConnection;
  if (related.subquery.where) {
    childEnd = processCondition(
      related.subquery.where,
      childEnd,
      graph,
      model,
      childTable,
      getPlanId,
    );
  }

  // Extract constraints from correlation
  const parentConstraint = extractConstraint(
    related.correlation.parentField,
    parentTable,
  );
  const childConstraint = extractConstraint(
    related.correlation.childField,
    childTable,
  );

  // Determine if join can be flipped (NOT EXISTS cannot be flipped)
  const flippable = condition.op !== 'NOT EXISTS';

  // Generate plan ID and attach to both AST and planner join
  const planId = getPlanId();
  condition[planIdSymbol] = planId;

  // Create join
  const join = new PlannerJoin(
    input,
    childEnd,
    parentConstraint,
    childConstraint,
    flippable,
    planId,
  );
  graph.joins.push(join);

  // Wire up inputs to join
  wireOutput(input, join);
  wireOutput(childEnd, join);

  return join;
}

/**
 * Check if a condition contains a correlated subquery at any level.
 */
function hasCorrelatedSubquery(condition: Condition): boolean {
  if (condition.type === 'correlatedSubquery') {
    return true;
  }
  if (condition.type === 'and' || condition.type === 'or') {
    return condition.conditions.some(hasCorrelatedSubquery);
  }
  return false;
}

/**
 * Extract constraint from correlation fields.
 * Creates a mapping from field names to their types (simplified for now).
 */
function extractConstraint(
  fields: readonly string[],
  _tableName: string,
): PlannerConstraint {
  // For now, create a simple constraint with unknown types
  // In a full implementation, this would look up actual column types
  const constraint: PlannerConstraint = {};
  for (const field of fields) {
    constraint[field] = undefined;
  }
  return constraint;
}

/**
 * Recursively plan all graphs in a Plans tree.
 * Uses post-order traversal so subqueries are planned before their parents.
 *
 * @param plans - The Plans tree to execute planning on
 */
function planRecursively(plans: Plans): void {
  // Plan subqueries first (post-order traversal)
  for (const subPlan of Object.values(plans.subPlans)) {
    planRecursively(subPlan);
  }

  // Then plan this graph
  plans.plan.plan();
}

/**
 * Plan a query and return an optimized AST with flip flags applied.
 * This is the main entrypoint for query planning.
 *
 * Orchestrates:
 * 1. Build plan graphs for query and all related subqueries
 * 2. Execute planning algorithm recursively (children before parents)
 * 3. Apply computed plans back to AST by marking flipped joins
 *
 * @param ast - The input AST to plan
 * @param model - The cost model for connection estimation
 * @returns Optimized AST with flip: true on joins that should use FlippedJoin
 */
export function planQuery(ast: AST, model: ConnectionCostModel): AST {
  // Build plan graphs recursively
  const plans = buildPlanGraph(ast, model);

  // Execute planning algorithm recursively (post-order)
  planRecursively(plans);

  // Apply plans to AST
  return applyPlansToAST(ast, plans);
}

/**
 * Apply plans recursively to an AST by marking flipped joins.
 * Handles main plan and all subPlans for 'related' queries.
 *
 * @param ast - The AST to modify
 * @param plans - The Plans tree containing plan graphs
 * @returns The modified AST with flip flags applied
 */
function applyPlansToAST(ast: AST, plans: Plans): AST {
  // Build set of flipped join plan IDs for O(1) lookup
  const flippedIds = new Set<number>();
  for (const join of plans.plan.joins) {
    if (join.type === 'flipped' && join.planId !== undefined) {
      flippedIds.add(join.planId);
    }
  }

  // Single-pass traversal to apply flip flags
  const applyToCondition = (condition: Condition): Condition => {
    if (condition.type === 'simple') {
      return condition;
    }

    if (condition.type === 'correlatedSubquery') {
      const planId = (condition as unknown as Record<symbol, number>)[
        planIdSymbol
      ];
      const shouldFlip = planId !== undefined && flippedIds.has(planId);

      // Recursively process nested WHERE clauses in the subquery
      // Note: .related() subqueries are handled separately via subPlans,
      // but nested .whereExists() subqueries are part of this same plan
      return {
        ...condition,
        flip: shouldFlip ? true : condition.flip,
        related: {
          ...condition.related,
          subquery: {
            ...condition.related.subquery,
            where: condition.related.subquery.where
              ? applyToCondition(condition.related.subquery.where)
              : undefined,
          },
        },
      };
    }

    // Handle 'and' and 'or'
    return {
      ...condition,
      conditions: condition.conditions.map(applyToCondition),
    };
  };

  return {
    ...ast,
    where: ast.where ? applyToCondition(ast.where) : undefined,
    related: ast.related?.map(csq => {
      const alias = must(
        csq.subquery.alias,
        'Related subquery must have alias',
      );
      const subPlan = plans.subPlans[alias];
      return {
        ...csq,
        subquery: subPlan
          ? applyPlansToAST(csq.subquery, subPlan)
          : csq.subquery,
      };
    }),
  };
}
