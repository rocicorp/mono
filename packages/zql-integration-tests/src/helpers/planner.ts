import {mapAST} from '../../../zero-protocol/src/ast.ts';
import type {NameMapper} from '../../../zero-types/src/name-mapper.ts';
import {
  buildPlanGraph,
  type Plans,
} from '../../../zql/src/planner/planner-builder.ts';
import type {ConnectionCostModel} from '../../../zql/src/planner/planner-connection.ts';
import type {AnyQuery} from '../../../zql/src/query/query-impl.ts';
import type {PlanDebugger} from '../../../zql/src/planner/planner-debug.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';

export function makeGetPlanAST(
  mapper: NameMapper,
  costModel: ConnectionCostModel,
) {
  return (q: AnyQuery, planDebugger?: PlanDebugger) => {
    const ast = mapAST(q.ast, mapper);
    const plans = buildPlanGraph(ast, costModel);
    planRecursively(plans, planDebugger);
    return applyPlansToAST(ast, plans);
  };
}

function planRecursively(plans: Plans, planDebugger?: PlanDebugger): void {
  for (const subPlan of Object.values(plans.subPlans)) {
    planRecursively(subPlan, planDebugger);
  }
  plans.plan.plan(planDebugger);
}

function applyPlansToAST(ast: AST, plans: Plans): AST {
  const flippedIds = new Set<number>();
  for (const join of plans.plan.joins) {
    if (join.type === 'flipped' && join.planId !== undefined) {
      flippedIds.add(join.planId);
    }
  }

  return applyToCondition(ast, flippedIds, plans);
}

function applyToCondition(
  ast: AST,
  flippedIds: Set<number>,
  plans: Plans,
): AST {
  const planIdSymbol = Symbol.for('planId');

  function processCondition(
    condition: any,
  ): any {
    if (!condition) return condition;

    if (condition.type === 'simple') {
      return condition;
    }

    if (condition.type === 'correlatedSubquery') {
      const planId = condition[planIdSymbol];
      const shouldFlip = planId !== undefined && flippedIds.has(planId);

      return {
        ...condition,
        flip: shouldFlip ? true : condition.flip,
        related: {
          ...condition.related,
          subquery: {
            ...condition.related.subquery,
            where: condition.related.subquery.where
              ? processCondition(condition.related.subquery.where)
              : undefined,
          },
        },
      };
    }

    if (condition.type === 'and' || condition.type === 'or') {
      return {
        ...condition,
        conditions: condition.conditions.map(processCondition),
      };
    }

    return condition;
  }

  return {
    ...ast,
    where: ast.where ? processCondition(ast.where) : undefined,
    related: ast.related?.map(csq => {
      const alias = csq.subquery.alias;
      if (!alias) return csq;

      const subPlan = plans.subPlans[alias];
      return {
        ...csq,
        subquery: subPlan
          ? applyToCondition(csq.subquery, flippedIds, subPlan)
          : csq.subquery,
      };
    }),
  };
}

// oxlint-disable-next-line no-explicit-any
export function pick(node: any, path: (string | number)[]) {
  let cur = node;
  for (const p of path) {
    cur = cur[p];
    if (cur === undefined) return undefined;
  }
  return cur;
}
