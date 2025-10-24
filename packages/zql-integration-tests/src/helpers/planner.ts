import {mapAST} from '../../../zero-protocol/src/ast.ts';
import type {NameMapper} from '../../../zero-types/src/name-mapper.ts';
import {planQuery} from '../../../zql/src/planner/planner-builder.ts';
import type {ConnectionCostModel} from '../../../zql/src/planner/planner-connection.ts';
import type {AnyQuery} from '../../../zql/src/query/query-impl.ts';
import type {PlanDebugger} from '../../../zql/src/planner/planner-debug.ts';

export function makeGetPlanAST(
  mapper: NameMapper,
  costModel: ConnectionCostModel,
) {
  return (q: AnyQuery, planDebugger?: PlanDebugger) =>
    planQuery(mapAST(q.ast, mapper), costModel, planDebugger);
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
