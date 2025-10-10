import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import type {
  AST,
  Condition,
  Conjunction,
  CorrelatedSubqueryCondition,
  Disjunction,
} from '../../../zero-protocol/src/ast.ts';
import type {ValueType} from '../../../zero-protocol/src/client-schema.ts';
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
 */
export function buildPlanGraph(ast: AST, model: ConnectionCostModel): Plans {
  const graph = new PlannerGraph();

  // Create source for the main table
  const source = graph.addSource(ast.table, model);

  // Create the main connection with ordering and filters
  const connection = source.connect(ast.orderBy ?? [], ast.where);
  graph.connections.push(connection);

  // Process WHERE clause to build joins
  let end: PlannerNode = connection;
  if (ast.where) {
    end = processCondition(ast.where, end, graph, model, ast.table);
  }

  // Create terminus
  const terminus = new PlannerTerminus(end);
  graph.setTerminus(terminus);

  // Build subplans for 'related' queries
  const subPlans: {[key: string]: Plans} = {};
  if (ast.related) {
    for (const csq of ast.related) {
      const alias = must(
        csq.subquery.alias,
        'Related subquery must have alias',
      );
      subPlans[alias] = buildPlanGraph(csq.subquery, model);
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
): PlannerNode {
  switch (condition.type) {
    case 'simple':
      // Simple conditions don't create joins, they're just filters in connections
      return input;

    case 'and':
      return processAnd(condition, input, graph, model, parentTable);

    case 'or':
      return processOr(condition, input, graph, model, parentTable);

    case 'correlatedSubquery':
      return processCorrelatedSubquery(
        condition,
        input,
        graph,
        model,
        parentTable,
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
): PlannerNode {
  let end = input;
  for (const subCondition of condition.conditions) {
    end = processCondition(subCondition, end, graph, model, parentTable);
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
): PlannerNode {
  const {related} = condition;
  const childTable = related.subquery.table;

  // Create source for child table if not exists
  let childSource = graph.getSource(childTable);
  if (!childSource) {
    childSource = graph.addSource(childTable, model);
  }

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

  // Create join
  const join = new PlannerJoin(
    input,
    childEnd,
    parentConstraint,
    childConstraint,
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
    // Using 'string' as placeholder type - real implementation would
    // look up actual column type from schema
    constraint[field] = 'string' as ValueType;
  }
  return constraint;
}
