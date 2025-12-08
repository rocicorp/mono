import {mapAST} from '../../../zero-protocol/src/ast.ts';
import type {NameMapper} from '../../../zero-types/src/name-mapper.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {planQuery} from '../../../zql/src/planner/planner-builder.ts';
import type {ConnectionCostModel} from '../../../zql/src/planner/planner-connection.ts';
import type {PlanDebugger} from '../../../zql/src/planner/planner-debug.ts';
import {completeOrdering} from '../../../zql/src/query/complete-ordering.ts';
import type {AnyQuery} from '../../../zql/src/query/query-builder.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';

export function makeGetPlanAST(
  schema: Schema,
  mapper: NameMapper,
  costModel: ConnectionCostModel,
) {
  return (q: AnyQuery, planDebugger?: PlanDebugger) => {
    const ast = asQueryInternals(q).ast;
    return planQuery(
      mapAST(
        completeOrdering(ast, tableName => schema.tables[tableName].primaryKey),
        mapper,
      ),
      costModel,
      planDebugger,
    );
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
