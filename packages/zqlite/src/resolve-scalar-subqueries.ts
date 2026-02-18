import type {
  AST,
  Condition,
  LiteralValue,
  ScalarSubqueryCondition,
  SimpleCondition,
} from '../../zero-protocol/src/ast.ts';
import type {PrimaryKey} from '../../zero-protocol/src/primary-key.ts';

type TableSpecWithUniqueKeys = {
  tableSpec: {
    uniqueKeys: PrimaryKey[];
  };
};

export type CompanionSubquery = {
  /** The original scalar subquery AST (the subquery table query). */
  ast: AST;
  /** The field in the subquery row whose value was resolved. */
  childField: string;
  /** The resolved value, `null` if a row matched but the field was `NULL`,
   * or `undefined` if no row matched. */
  resolvedValue: LiteralValue | null | undefined;
};

export type ResolveResult = {
  ast: AST;
  companions: CompanionSubquery[];
};

/**
 * Callback that executes a scalar subquery and returns the value of
 * `childField` from the (at most one) matching row, or `undefined`
 * if no rows match.
 */
export type ScalarExecutor = (
  subqueryAST: AST,
  childField: string,
) => LiteralValue | null | undefined;

/**
 * Resolves "simple" scalar subqueries by calling the provided executor
 * and replacing them with literal conditions. A scalar subquery is simple
 * when all columns of at least one unique index on the subquery table are
 * equality-constrained by literal values in the subquery's WHERE clause
 * (using only AND conjunctions).
 *
 * Non-simple scalar subqueries are left untouched for the existing
 * EXISTS rewrite in buildPipelineInternal.
 *
 * Returns the resolved AST and a list of companion subquery ASTs whose
 * rows need to be synced to the client for the EXISTS rewrite to work.
 */
export function resolveSimpleScalarSubqueries(
  ast: AST,
  tableSpecs: Map<string, TableSpecWithUniqueKeys>,
  execute: ScalarExecutor,
): ResolveResult {
  const companions: CompanionSubquery[] = [];
  const resolved = resolveASTRecursive(ast, tableSpecs, execute, companions);
  return {ast: resolved, companions};
}

function resolveASTRecursive(
  ast: AST,
  tableSpecs: Map<string, TableSpecWithUniqueKeys>,
  execute: ScalarExecutor,
  companions: CompanionSubquery[],
): AST {
  const where = ast.where
    ? resolveCondition(ast.where, tableSpecs, execute, companions)
    : undefined;

  const related = ast.related?.map(r => ({
    ...r,
    subquery: resolveASTRecursive(r.subquery, tableSpecs, execute, companions),
  }));

  if (where !== ast.where || related !== ast.related) {
    return {...ast, where, related};
  }
  return ast;
}

function resolveCondition(
  condition: Condition,
  tableSpecs: Map<string, TableSpecWithUniqueKeys>,
  execute: ScalarExecutor,
  companions: CompanionSubquery[],
): Condition {
  switch (condition.type) {
    case 'scalarSubquery':
      return resolveScalarSubquery(condition, tableSpecs, execute, companions);
    case 'and':
    case 'or': {
      const resolved = condition.conditions.map(c =>
        resolveCondition(c, tableSpecs, execute, companions),
      );
      if (resolved.every((c, i) => c === condition.conditions[i])) {
        return condition;
      }
      return {type: condition.type, conditions: resolved};
    }
    default:
      return condition;
  }
}

function resolveScalarSubquery(
  condition: ScalarSubqueryCondition,
  tableSpecs: Map<string, TableSpecWithUniqueKeys>,
  execute: ScalarExecutor,
  companions: CompanionSubquery[],
): Condition {
  // Recursively resolve any scalar subqueries nested in the
  // subquery's own WHERE (and related) before evaluating this one.
  const subquery = resolveASTRecursive(
    condition.subquery,
    tableSpecs,
    execute,
    companions,
  );

  if (!isSimpleSubquery(subquery, tableSpecs)) {
    // Return with the (possibly partially-resolved) subquery.
    if (subquery !== condition.subquery) {
      return {...condition, subquery};
    }
    return condition;
  }

  const value = execute(subquery, condition.childField);

  // Record the companion subquery AST so its rows are synced to the client.
  // The client rewrites scalar subqueries to EXISTS and needs those rows.
  companions.push({
    ast: subquery,
    childField: condition.childField,
    resolvedValue: value,
  });

  if (value === undefined || value === null) {
    // No rows or NULL value — both x = NULL and x != NULL are false in SQL
    return ALWAYS_FALSE;
  }

  return {
    type: 'simple',
    op: condition.op,
    left: {type: 'column', name: condition.parentField},
    right: {type: 'literal', value},
  } satisfies SimpleCondition;
}

const ALWAYS_FALSE: SimpleCondition = {
  type: 'simple',
  op: '=',
  left: {type: 'literal', value: 1},
  right: {type: 'literal', value: 0},
};

/**
 * Checks if the subquery has at least one unique index whose columns
 * are all equality-constrained by literal values.
 */
export function isSimpleSubquery(
  subquery: AST,
  tableSpecs: Map<string, TableSpecWithUniqueKeys>,
): boolean {
  const spec = tableSpecs.get(subquery.table);
  if (!spec) {
    return false;
  }

  if (!subquery.where) {
    return false;
  }

  const constraints = extractLiteralEqualityConstraints(subquery.where);
  if (constraints.size === 0) {
    return false;
  }

  const {uniqueKeys} = spec.tableSpec;
  return uniqueKeys.some(key => key.every(col => constraints.has(col)));
}

/**
 * Extracts column=literal equality constraints from a condition tree,
 * only following AND conjunctions (not OR).
 */
export function extractLiteralEqualityConstraints(
  condition: Condition,
): Map<string, LiteralValue> {
  const constraints = new Map<string, LiteralValue>();
  collectConstraints(condition, constraints);
  return constraints;
}

function collectConstraints(
  condition: Condition,
  constraints: Map<string, LiteralValue>,
): void {
  switch (condition.type) {
    case 'simple':
      if (condition.op === '=') {
        if (
          condition.left.type === 'column' &&
          condition.right.type === 'literal'
        ) {
          constraints.set(condition.left.name, condition.right.value);
        }
      }
      break;
    case 'and':
      for (const c of condition.conditions) {
        collectConstraints(c, constraints);
      }
      break;
    // OR, correlatedSubquery, scalarSubquery — don't contribute constraints
    default:
      break;
  }
}
