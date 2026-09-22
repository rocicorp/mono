import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import type {
  AST,
  Condition,
  Conjunction,
  CorrelatedSubqueryCondition,
  Disjunction,
  SimpleCondition,
} from '../../../zero-protocol/src/ast.ts';
import {planIdSymbol} from '../../../zero-protocol/src/ast.ts';
import {transformFilters} from '../builder/filter.ts';
import type {ConnectionCostModel} from './planner-connection.ts';
import type {PlannerConstraint} from './planner-constraint.ts';
import type {PlanDebugger} from './planner-debug.ts';
import {PlannerFanIn} from './planner-fan-in.ts';
import {PlannerFanOut} from './planner-fan-out.ts';
import {PlannerFilter} from './planner-filter.ts';
import {PlannerGraph} from './planner-graph.ts';
import {PlannerJoin} from './planner-join.ts';
import type {PlannerNode} from './planner-node.ts';
import {PlannerTerminus} from './planner-terminus.ts';

function wireOutput(from: PlannerNode, to: PlannerNode): void {
  switch (from.kind) {
    case 'connection':
    case 'join':
    case 'fan-in':
    case 'filter':
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

/**
 * @param pushed The conditions that correlated predicate pushdown copied into
 * a child (see `pushDownCorrelatedPredicates`).
 */
export function buildPlanGraph(
  ast: AST,
  model: ConnectionCostModel,
  isRoot: boolean,
  baseConstraints?: PlannerConstraint,
  pushed?: ReadonlySet<SimpleCondition>,
): Plans {
  const graph = new PlannerGraph();
  let nextPlanId = 0;

  const source = graph.addSource(ast.table, model);
  const connection = source.connect(
    ast.orderBy ?? [],
    ast.where,
    isRoot,
    baseConstraints,
    ast.limit,
    pushed,
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
      pushed,
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
      subPlans[alias] = buildPlanGraph(
        csq.subquery,
        model,
        true,
        childConstraints,
        pushed,
      );
    }
  }

  return {plan: graph, subPlans};
}

function processCondition(
  condition: Condition,
  input: Exclude<PlannerNode, PlannerTerminus>,
  graph: PlannerGraph,
  model: ConnectionCostModel,
  parentTable: string,
  getPlanId: () => number,
  pushed: ReadonlySet<SimpleCondition> | undefined,
): Exclude<PlannerNode, PlannerTerminus> {
  switch (condition.type) {
    case 'simple':
      return input;
    case 'and':
      return processAnd(
        condition,
        input,
        graph,
        model,
        parentTable,
        getPlanId,
        pushed,
      );
    case 'or':
      return processOr(
        condition,
        input,
        graph,
        model,
        parentTable,
        getPlanId,
        pushed,
      );
    case 'correlatedSubquery':
      return processCorrelatedSubquery(
        condition,
        input,
        graph,
        model,
        parentTable,
        getPlanId,
        pushed,
      );
  }
}

function processAnd(
  condition: Conjunction,
  input: Exclude<PlannerNode, PlannerTerminus>,
  graph: PlannerGraph,
  model: ConnectionCostModel,
  parentTable: string,
  getPlanId: () => number,
  pushed: ReadonlySet<SimpleCondition> | undefined,
): Exclude<PlannerNode, PlannerTerminus> {
  let end = input;
  for (const subCondition of condition.conditions) {
    end = processCondition(
      subCondition,
      end,
      graph,
      model,
      parentTable,
      getPlanId,
      pushed,
    );
  }
  return end;
}

function processOr(
  condition: Disjunction,
  input: Exclude<PlannerNode, PlannerTerminus>,
  graph: PlannerGraph,
  model: ConnectionCostModel,
  parentTable: string,
  getPlanId: () => number,
  pushed: ReadonlySet<SimpleCondition> | undefined,
): Exclude<PlannerNode, PlannerTerminus> {
  // Skip building fan structure when no branch contains a CSQ. The runtime
  // collapses such ORs to a single Filter node, so the planner has nothing
  // to choose between.
  const hasAnySubquery = condition.conditions.some(
    c => c.type === 'correlatedSubquery' || hasCorrelatedSubquery(c),
  );
  if (!hasAnySubquery) {
    return input;
  }

  const fanOut = new PlannerFanOut(input);
  graph.fanOuts.push(fanOut);
  wireOutput(input, fanOut);

  const branches: Exclude<PlannerNode, PlannerTerminus>[] = [];
  for (const subCondition of condition.conditions) {
    let branch: Exclude<PlannerNode, PlannerTerminus>;
    if (
      subCondition.type === 'correlatedSubquery' ||
      hasCorrelatedSubquery(subCondition)
    ) {
      branch = processCondition(
        subCondition,
        fanOut,
        graph,
        model,
        parentTable,
        getPlanId,
        pushed,
      );
    } else {
      // Simple OR branch: wrap in a PlannerFilter so its filter can be
      // registered at the receiving connection on a per-branch basis when
      // the FanIn is in UFI mode.
      const transformed = transformFilters(subCondition).filters;
      const filter = new PlannerFilter(fanOut, transformed);
      graph.filters.push(filter);
      wireOutput(fanOut, filter);
      branch = filter;
    }
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
  input: Exclude<PlannerNode, PlannerTerminus>,
  graph: PlannerGraph,
  model: ConnectionCostModel,
  parentTable: string,
  getPlanId: () => number,
  pushed: ReadonlySet<SimpleCondition> | undefined,
): Exclude<PlannerNode, PlannerTerminus> {
  const {related} = condition;
  const childTable = related.subquery.table;

  const childSource = graph.hasSource(childTable)
    ? graph.getSource(childTable)
    : graph.addSource(childTable, model);

  const childConnection = childSource.connect(
    related.subquery.orderBy ?? [],
    related.subquery.where,
    false,
    undefined, // no base constraints for EXISTS/NOT EXISTS
    condition.op === 'EXISTS' ? 1 : undefined,
    pushed,
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
      pushed,
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

  // Determine flippability and initial type based on flip flag and operator
  const isNotExists = condition.op === 'NOT EXISTS';
  const manualFlip = condition.flip;

  let flippable: boolean;
  let initialType: 'semi' | 'flipped';

  if (isNotExists) {
    // NOT EXISTS joins can never be flipped
    flippable = false;
    initialType = 'semi';
  } else if (manualFlip === true) {
    // User explicitly requested flip=true: start flipped, don't allow planner to change
    flippable = false;
    initialType = 'flipped';
  } else if (manualFlip === false) {
    // User explicitly requested flip=false: start semi, don't allow planner to change
    flippable = false;
    initialType = 'semi';
  } else {
    // flip is undefined: planner can decide
    flippable = true;
    initialType = 'semi';
  }

  const join = new PlannerJoin(
    input,
    childEnd,
    parentConstraint,
    childConstraint,
    flippable,
    planId,
    initialType,
  );
  graph.joins.push(join);
  childConnection.setParentJoin(join);

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
  // simple conditions don't contain correlated subqueries
  return false;
}

function extractConstraint(
  fields: readonly string[],
  _tableName: string,
): PlannerConstraint {
  return Object.fromEntries(fields.map(field => [field, undefined]));
}

function planRecursively(
  plans: Plans,
  planDebugger?: PlanDebugger,
  lc?: LogContext,
): void {
  for (const subPlan of Object.values(plans.subPlans)) {
    planRecursively(subPlan, planDebugger, lc);
  }
  plans.plan.plan(planDebugger, lc);
}

/**
 * @param pushed The conditions that correlated predicate pushdown copied into
 * a child (see `pushDownCorrelatedPredicates`). The planner counts each one
 * only where it removes rows.
 */
export function planQuery(
  ast: AST,
  model: ConnectionCostModel,
  planDebugger?: PlanDebugger,
  lc?: LogContext,
  pushed?: ReadonlySet<SimpleCondition>,
): AST {
  const plans = buildPlanGraph(ast, model, true, undefined, pushed);
  planRecursively(plans, planDebugger, lc);
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
      flip: shouldFlip,
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

export function applyPlansToAST(ast: AST, plans: Plans): AST {
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
