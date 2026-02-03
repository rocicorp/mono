import type {
  AST,
  Condition,
  LiteralValue,
  ScalarSubqueryCondition,
  SimpleCondition,
} from '../../../../zero-protocol/src/ast.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import type {LiteAndZqlSpec} from '../../db/specs.ts';

/**
 * Resolves "simple" scalar subqueries by executing them against SQLite
 * and replacing them with literal conditions. A scalar subquery is simple
 * when all columns of at least one unique index on the subquery table are
 * equality-constrained by literal values in the subquery's WHERE clause
 * (using only AND conjunctions).
 *
 * Non-simple scalar subqueries are left untouched for the existing
 * EXISTS rewrite in buildPipelineInternal.
 */
export function resolveSimpleScalarSubqueries(
  ast: AST,
  tableSpecs: Map<string, LiteAndZqlSpec>,
  db: Database,
): AST {
  return resolveASTRecursive(ast, tableSpecs, db);
}

function resolveASTRecursive(
  ast: AST,
  tableSpecs: Map<string, LiteAndZqlSpec>,
  db: Database,
): AST {
  const where = ast.where
    ? resolveCondition(ast.where, tableSpecs, db)
    : undefined;

  const related = ast.related?.map(r => ({
    ...r,
    subquery: resolveASTRecursive(r.subquery, tableSpecs, db),
  }));

  if (where !== ast.where || related !== ast.related) {
    return {...ast, where, related};
  }
  return ast;
}

function resolveCondition(
  condition: Condition,
  tableSpecs: Map<string, LiteAndZqlSpec>,
  db: Database,
): Condition {
  switch (condition.type) {
    case 'scalarSubquery':
      return resolveScalarSubquery(condition, tableSpecs, db);
    case 'and':
    case 'or': {
      const resolved = condition.conditions.map(c =>
        resolveCondition(c, tableSpecs, db),
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
  tableSpecs: Map<string, LiteAndZqlSpec>,
  db: Database,
): Condition {
  // Only handle single-column field and column references
  if (condition.field.length > 1 || condition.column.length > 1) {
    return condition;
  }

  const subquery = condition.subquery;
  if (!isSimpleSubquery(subquery, tableSpecs)) {
    return condition;
  }

  const value = executeScalarSubquery(subquery, condition.column[0], db);

  if (value === undefined || value === null) {
    // No rows or NULL value — both x = NULL and x != NULL are false in SQL
    return ALWAYS_FALSE;
  }

  return {
    type: 'simple',
    op: condition.op,
    left: {type: 'column', name: condition.field[0]},
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
  tableSpecs: Map<string, LiteAndZqlSpec>,
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

/**
 * Executes a scalar subquery against SQLite, returning the value of the
 * specified column from the first matching row, or undefined if no rows match.
 */
function executeScalarSubquery(
  subquery: AST,
  column: string,
  db: Database,
): LiteralValue | undefined {
  const params: LiteralValue[] = [];
  const whereSQL = subquery.where
    ? conditionToSQL(subquery.where, params)
    : '1';
  const sql = `SELECT "${column}" FROM "${subquery.table}" WHERE ${whereSQL} LIMIT 1`;
  const row = db.prepare(sql).get<Record<string, LiteralValue>>(...params);
  if (!row) {
    return undefined;
  }
  return row[column];
}

/**
 * Translates an AST Condition to a SQL WHERE clause string.
 * Only handles simple, and, or conditions — scalar subquery and
 * correlated subquery conditions within a subquery's WHERE would
 * make it non-simple, so they are not expected here.
 */
export function conditionToSQL(
  condition: Condition,
  params: LiteralValue[],
): string {
  switch (condition.type) {
    case 'simple': {
      const left = valueToSQL(condition.left, params);
      const right = valueToSQL(condition.right, params);
      if (
        condition.op === 'IN' ||
        condition.op === 'NOT IN'
      ) {
        // IN/NOT IN with array literal
        if (
          condition.right.type === 'literal' &&
          Array.isArray(condition.right.value)
        ) {
          const placeholders = condition.right.value
            .map(() => '?')
            .join(', ');
          // The array values were already pushed in valueToSQL, but we
          // need to redo this properly. Remove the last param (the array)
          // and add individual values.
          params.pop();
          for (const v of condition.right.value) {
            params.push(v);
          }
          return `${left} ${condition.op} (${placeholders})`;
        }
        return `${left} ${condition.op} (${right})`;
      }
      if (condition.op === 'IS' || condition.op === 'IS NOT') {
        return `${left} ${condition.op} ${right}`;
      }
      return `${left} ${condition.op} ${right}`;
    }
    case 'and':
      return condition.conditions
        .map(c => `(${conditionToSQL(c, params)})`)
        .join(' AND ');
    case 'or':
      return condition.conditions
        .map(c => `(${conditionToSQL(c, params)})`)
        .join(' OR ');
    case 'correlatedSubquery':
    case 'scalarSubquery':
      throw new Error(
        `Unexpected ${condition.type} condition in simple subquery WHERE`,
      );
    default:
      throw new Error(`Unknown condition type: ${(condition as Condition).type}`);
  }
}

function valueToSQL(
  value: {type: string; name?: string; value?: LiteralValue},
  params: LiteralValue[],
): string {
  if (value.type === 'column') {
    return `"${value.name}"`;
  }
  if (value.type === 'literal') {
    if (value.value === null) {
      return 'NULL';
    }
    if (Array.isArray(value.value)) {
      // Array values are handled specially in the IN clause
      params.push(value.value as LiteralValue);
      return '?';
    }
    params.push(value.value as LiteralValue);
    return '?';
  }
  // static parameters — shouldn't appear in simple subqueries
  throw new Error(`Unexpected value type: ${value.type}`);
}
