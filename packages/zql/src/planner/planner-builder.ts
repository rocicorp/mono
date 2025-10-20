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

export type Plans = {
  plan: PlannerGraph;
  subPlans: {[key: string]: Plans};
};

export function buildPlanGraph(
  ast: AST,
  model: ConnectionCostModel,
  baseConstraints?: PlannerConstraint,
): Plans {
  const graph = new PlannerGraph();
  let nextPlanId = 0;

  const source = graph.addSource(ast.table, model);
  const connection = source.connect(
    ast.orderBy ?? [],
    ast.where,
    baseConstraints,
  );
  graph.connections.push(connection);

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

  const terminus = new PlannerTerminus(end);
  wireOutput(end, terminus);
  graph.setTerminus(terminus);

  const subPlans: {[key: string]: Plans} = {};
  if (ast.related) {
    for (const csq of ast.related) {
      const alias = must(
        csq.subquery.alias,
        'Related subquery must have alias',
      );
      const childConstraints = extractConstraint(
        csq.correlation.childField,
        csq.subquery.table,
      );
      subPlans[alias] = buildPlanGraph(csq.subquery, model, childConstraints);
    }
  }

  return {plan: graph, subPlans};
}

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

function processOr(
  condition: Disjunction,
  input: PlannerNode,
  graph: PlannerGraph,
  model: ConnectionCostModel,
  parentTable: string,
  getPlanId: () => number,
): PlannerNode {
  const subqueryConditions = condition.conditions.filter(
    c => c.type === 'correlatedSubquery' || hasCorrelatedSubquery(c),
  );

  if (subqueryConditions.length === 0) {
    return input;
  }

  const fanOut = new PlannerFanOut(input);
  graph.fanOuts.push(fanOut);
  wireOutput(input, fanOut);

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

  const fanIn = new PlannerFanIn(branches);
  graph.fanIns.push(fanIn);
  for (const branch of branches) {
    wireOutput(branch, fanIn);
  }

  return fanIn;
}

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

  const childSource = graph.hasSource(childTable)
    ? graph.getSource(childTable)
    : graph.addSource(childTable, model);

  const childConnection = childSource.connect(
    related.subquery.orderBy ?? [],
    related.subquery.where,
  );
  graph.connections.push(childConnection);

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

  const parentConstraint = extractConstraint(
    related.correlation.parentField,
    parentTable,
  );
  const childConstraint = extractConstraint(
    related.correlation.childField,
    childTable,
  );

  const planId = getPlanId();
  condition[planIdSymbol] = planId;

  const join = new PlannerJoin(
    input,
    childEnd,
    parentConstraint,
    childConstraint,
    condition.op !== 'NOT EXISTS',
    planId,
  );
  graph.joins.push(join);

  wireOutput(input, join);
  wireOutput(childEnd, join);

  return join;
}

function hasCorrelatedSubquery(condition: Condition): boolean {
  if (condition.type === 'correlatedSubquery') {
    return true;
  }
  if (condition.type === 'and' || condition.type === 'or') {
    return condition.conditions.some(hasCorrelatedSubquery);
  }
  return false;
}

function extractConstraint(
  fields: readonly string[],
  _tableName: string,
): PlannerConstraint {
  return Object.fromEntries(fields.map(field => [field, undefined]));
}

function planRecursively(plans: Plans): void {
  for (const subPlan of Object.values(plans.subPlans)) {
    planRecursively(subPlan);
  }
  plans.plan.plan();
}

export function planQuery(ast: AST, model: ConnectionCostModel): AST {
  const plans = buildPlanGraph(ast, model);
  planRecursively(plans);
  return applyPlansToAST(ast, plans);
}

function applyToCondition(
  condition: Condition,
  flippedIds: Set<number>,
): Condition {
  if (condition.type === 'simple') {
    return condition;
  }

  if (condition.type === 'correlatedSubquery') {
    const planId = (condition as unknown as Record<symbol, number>)[
      planIdSymbol
    ];
    const shouldFlip = planId !== undefined && flippedIds.has(planId);

    return {
      ...condition,
      flip: shouldFlip ? true : condition.flip,
      related: {
        ...condition.related,
        subquery: {
          ...condition.related.subquery,
          where: condition.related.subquery.where
            ? applyToCondition(condition.related.subquery.where, flippedIds)
            : undefined,
        },
      },
    };
  }

  return {
    ...condition,
    conditions: condition.conditions.map(c => applyToCondition(c, flippedIds)),
  };
}

function applyPlansToAST(ast: AST, plans: Plans): AST {
  const flippedIds = new Set<number>();
  for (const join of plans.plan.joins) {
    if (join.type === 'flipped' && join.planId !== undefined) {
      flippedIds.add(join.planId);
    }
  }

  return {
    ...ast,
    where: ast.where ? applyToCondition(ast.where, flippedIds) : undefined,
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
