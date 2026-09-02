import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import type {
  AST,
  Condition,
  Conjunction,
  CorrelatedSubqueryCondition,
  Disjunction,
} from '../../../zero-protocol/src/ast.ts';
import type {ConnectionCostModel} from './planner-connection.ts';
import type {PlannerConstraint} from './planner-constraint.ts';
import type {PlanDebugger} from './planner-debug.ts';
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
  isRoot: boolean,
  baseConstraints?: PlannerConstraint,
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
      subPlans[alias] = buildPlanGraph(
        csq.subquery,
        model,
        true,
        childConstraints,
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
): Exclude<PlannerNode, PlannerTerminus> {
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
  input: Exclude<PlannerNode, PlannerTerminus>,
  graph: PlannerGraph,
  model: ConnectionCostModel,
  parentTable: string,
  getPlanId: () => number,
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
): Exclude<PlannerNode, PlannerTerminus> {
  const subqueryConditions = condition.conditions.filter(
    c => c.type === 'correlatedSubquery' || hasCorrelatedSubquery(c),
  );

  if (subqueryConditions.length === 0) {
    return input;
  }

  const fanOut = new PlannerFanOut(input);
  graph.fanOuts.push(fanOut);
  wireOutput(input, fanOut);

  const branches: Exclude<PlannerNode, PlannerTerminus>[] = [];
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
  input: Exclude<PlannerNode, PlannerTerminus>,
  graph: PlannerGraph,
  model: ConnectionCostModel,
  parentTable: string,
  getPlanId: () => number,
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
 * The planner's join flip choices for one AST, in a form that outlives the
 * planner graph.
 *
 * `flips` is indexed by plan ID: the position of a correlated subquery
 * condition in the depth first walk of this AST's own `where` clause.
 * `related` holds the blueprint of each planned related subquery, keyed by
 * alias.
 *
 * A blueprint is deeply frozen and holds no reference to an AST, a
 * {@link PlannerGraph}, a cost model, or any IVM object, so it can be cached
 * and reapplied to any structurally identical AST to produce a fresh planned
 * AST. Every stateful operator and storage object is still built from scratch
 * by the builder.
 */
export type FlipBlueprint = {
  readonly flips: readonly boolean[];
  readonly related: {readonly [alias: string]: FlipBlueprint};
};

/**
 * Plans `ast` and returns only the resulting flip decisions. Callers that want
 * a planned AST should use {@link planQuery}; this entry point exists so that
 * hosts can cache the decisions across structurally identical queries.
 */
export function planQueryBlueprint(
  ast: AST,
  model: ConnectionCostModel,
  planDebugger?: PlanDebugger,
  lc?: LogContext,
): FlipBlueprint {
  const plans = buildPlanGraph(ast, model, true);
  planRecursively(plans, planDebugger, lc);
  return blueprintOf(plans);
}

export function planQuery(
  ast: AST,
  model: ConnectionCostModel,
  planDebugger?: PlanDebugger,
  lc?: LogContext,
): AST {
  return applyFlipBlueprint(
    ast,
    planQueryBlueprint(ast, model, planDebugger, lc),
  );
}

function blueprintOf(plans: Plans): FlipBlueprint {
  const flips: boolean[] = new Array(plans.plan.joins.length).fill(false);
  for (const join of plans.plan.joins) {
    flips[join.planId] = join.type === 'flipped';
  }

  const related: {[alias: string]: FlipBlueprint} = {};
  for (const [alias, subPlan] of Object.entries(plans.subPlans)) {
    related[alias] = blueprintOf(subPlan);
  }

  return Object.freeze({
    flips: Object.freeze(flips),
    related: Object.freeze(related),
  });
}

/**
 * Rebuilds `condition` with the blueprint's flip choices applied.
 *
 * Plan IDs are not stored on the AST. They are re-derived by walking the
 * condition tree in the same order {@link buildPlanGraph} assigns them, i.e.
 * depth first, with a correlated subquery numbered after the conditions of its
 * own subquery. Conditions that {@link processOr} filters out contain no
 * correlated subquery and so consume no IDs, which keeps both walks in step.
 */
function applyToCondition(
  condition: Condition,
  flips: readonly boolean[],
  getPlanId: () => number,
): Condition {
  if (condition.type === 'simple') {
    return condition;
  }

  if (condition.type === 'correlatedSubquery') {
    const {subquery} = condition.related;
    const where = subquery.where
      ? applyToCondition(subquery.where, flips, getPlanId)
      : undefined;

    return {
      ...condition,
      flip: flips[getPlanId()],
      related: {
        ...condition.related,
        subquery: {
          ...subquery,
          where,
        },
      },
    };
  }

  return {
    ...condition,
    conditions: condition.conditions.map(c =>
      applyToCondition(c, flips, getPlanId),
    ),
  };
}

/**
 * Stamps `blueprint`'s decisions onto a fresh copy of `ast`. `ast` is not
 * modified, and no part of `blueprint` ends up in the result.
 */
export function applyFlipBlueprint(ast: AST, blueprint: FlipBlueprint): AST {
  let nextPlanId = 0;
  return {
    ...ast,
    where: ast.where
      ? applyToCondition(ast.where, blueprint.flips, () => nextPlanId++)
      : undefined,
    related: ast.related?.map(csq => {
      const alias = must(
        csq.subquery.alias,
        'Related subquery must have alias',
      );
      const subBlueprint = blueprint.related[alias];
      return {
        ...csq,
        subquery: subBlueprint
          ? applyFlipBlueprint(csq.subquery, subBlueprint)
          : csq.subquery,
      };
    }),
  };
}

export function applyPlansToAST(ast: AST, plans: Plans): AST {
  return applyFlipBlueprint(ast, blueprintOf(plans));
}
