import type {
  AST,
  Condition,
  CorrelatedSubqueryCondition,
  LiteralValue,
  SimpleCondition,
} from '../../zero-protocol/src/ast.ts';
import type {PrimaryKey} from '../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../zero-schema/src/table-schema.ts';

type TableSpecWithUniqueKeys = {
  tableSpec: {
    uniqueKeys: PrimaryKey[];
  };
  zqlSpec: Record<string, SchemaValue>;
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

/**
 * A `scalar` hint that could not be honored: the subquery was not provably
 * limited to one row, so the gate was left as a plain EXISTS.
 */
export type IgnoredScalarHint = {
  /** The subquery's table. */
  table: string;
  /** The unique keys that were available to pin it. */
  uniqueKeys: readonly PrimaryKey[];
};

export type ResolveResult = {
  ast: AST;
  companions: CompanionSubquery[];
  /**
   * Hints the caller may want to surface: each one is a `{scalar: true}` the
   * author asked for and did not get. See {@link IgnoredScalarHint}.
   */
  ignoredScalarHints: IgnoredScalarHint[];
};

/** Accumulators threaded through the recursion. */
type Out = {
  companions: CompanionSubquery[];
  ignoredScalarHints: IgnoredScalarHint[];
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
 * The enclosing query a correlated subquery is being resolved in: its table,
 * and the `column = literal` equalities its WHERE imposes on *every* row it
 * returns (top-level AND conjuncts only — see
 * {@link extractLiteralEqualityConstraints}).
 */
type ParentScope = {
  table: string;
  literals: ReadonlyMap<string, LiteralValue>;
};

/**
 * Resolves "simple" scalar subqueries by calling the provided executor
 * and replacing them with literal conditions. A scalar subquery is simple
 * when all columns of at least one unique index on the subquery table are
 * equality-constrained by literal values in the subquery's WHERE clause
 * (using only AND conjunctions), counting a literal the enclosing query
 * pushes through the correlation (see {@link pinWithParentLiteral}).
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
  const out: Out = {companions: [], ignoredScalarHints: []};
  const resolved = resolveASTRecursive(ast, tableSpecs, execute, out);
  return {ast: resolved, ...out};
}

function resolveASTRecursive(
  ast: AST,
  tableSpecs: Map<string, TableSpecWithUniqueKeys>,
  execute: ScalarExecutor,
  out: Out,
): AST {
  const where = ast.where
    ? resolveCondition(
        ast.where,
        // Derived from the *unresolved* WHERE, so which literals are usable
        // does not depend on the order gates happen to be resolved in.
        {
          table: ast.table,
          literals: extractLiteralEqualityConstraints(ast.where),
        },
        tableSpecs,
        execute,
        out,
      )
    : undefined;

  const related = ast.related?.map(r => ({
    ...r,
    subquery: resolveASTRecursive(r.subquery, tableSpecs, execute, out),
  }));

  if (where !== ast.where || related !== ast.related) {
    return {...ast, where, related};
  }
  return ast;
}

function resolveCondition(
  condition: Condition,
  parent: ParentScope,
  tableSpecs: Map<string, TableSpecWithUniqueKeys>,
  execute: ScalarExecutor,
  out: Out,
): Condition {
  switch (condition.type) {
    case 'correlatedSubquery':
      if (condition.scalar) {
        return resolveScalarSubquery(
          condition,
          parent,
          tableSpecs,
          execute,
          out,
        );
      }
      // Non-scalar correlated subquery: recurse into its subquery
      {
        const resolvedSubquery = resolveASTRecursive(
          condition.related.subquery,
          tableSpecs,
          execute,
          out,
        );
        if (resolvedSubquery !== condition.related.subquery) {
          return {
            ...condition,
            related: {...condition.related, subquery: resolvedSubquery},
          };
        }
        return condition;
      }
    case 'and':
    case 'or': {
      const resolved = condition.conditions.map(c =>
        resolveCondition(c, parent, tableSpecs, execute, out),
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

/**
 * Pushes a parent-side literal through the join correlation, when doing so is
 * what pins the subquery to at most one row.
 *
 * The rewrite is sound because the literal and the gate are conjoined:
 *
 * ```
 *   parent.pf = L AND EXISTS(child WHERE child.cf = parent.pf AND P)
 * ≡ parent.pf = L AND EXISTS(child WHERE child.cf = parent.pf AND child.cf = L AND P)
 * ```
 *
 * Every parent row that survives the conjunction has `pf = L`, so every child
 * row satisfying the correlation for such a parent has `cf = pf = L` and the
 * added conjunct discards nothing. Parent rows with a different `pf` — and
 * parent rows with `pf` NULL, which `=` never matches — are dropped by the
 * literal conjunct itself, so the gate's value for them is never observed.
 * That also covers `NOT EXISTS`: the two gates agree pointwise wherever the
 * conjunction can be true, so their negations do too.
 *
 * The literal must therefore come from a conjunct that applies to every row
 * the enclosing query returns. `extractLiteralEqualityConstraints` follows
 * only `and` nodes down from the WHERE root, so a literal under an `or`, one
 * inside a nested correlated subquery (including a `NOT EXISTS`), or one in
 * any position other than `column = literal` never reaches here.
 *
 * Returns the subquery with `cf = L` spliced in, or `undefined` when the
 * rewrite does not apply or does not pin the subquery to one row.
 */
function pinWithParentLiteral(
  condition: CorrelatedSubqueryCondition,
  parent: ParentScope,
  tableSpecs: Map<string, TableSpecWithUniqueKeys>,
): AST | undefined {
  const {correlation, subquery} = condition.related;
  // The resolved gate names a single parent column, so a compound correlation
  // cannot be answered by one scalar value.
  if (
    correlation.parentField.length !== 1 ||
    correlation.childField.length !== 1
  ) {
    return undefined;
  }
  const parentField = correlation.parentField[0];
  const childField = correlation.childField[0];

  const value = parent.literals.get(parentField);
  if (value === undefined) {
    return undefined;
  }
  // Nothing to push through: the subquery already pins itself.
  if (isSimpleSubquery(subquery, tableSpecs)) {
    return undefined;
  }

  // The correlation binds the parent's value using the *child* column's
  // declared type (`constraintsToSQL` → `toSQLiteType(value, columns[cf].type)`),
  // while a literal `=` binds using the *literal's own* JS type
  // (`valuePositionToSQL` → `toSQLiteType(value, getJsType(value))`), and the
  // in-memory overlay and filter paths compare the decoded row value with
  // `===`. Those three agree only when the parent column, the child column and
  // the literal all carry the same primitive type. Requiring `typeof value` to
  // equal the declared type is what excludes `json` columns — encoded with
  // `JSON.stringify` on one path but not the other — along with array literals
  // and `null` literals, since `typeof` yields neither 'json' nor 'null'.
  const parentType = tableSpecs.get(parent.table)?.zqlSpec[parentField]?.type;
  const childType = tableSpecs.get(subquery.table)?.zqlSpec[childField]?.type;
  if (
    parentType === undefined ||
    parentType !== childType ||
    parentType !== typeof value
  ) {
    return undefined;
  }

  const pin: SimpleCondition = {
    type: 'simple',
    op: '=',
    left: {type: 'column', name: childField},
    right: {type: 'literal', value},
  };
  const pinned: AST = {
    ...subquery,
    where: subquery.where
      ? {type: 'and', conditions: [pin, subquery.where]}
      : pin,
  };
  // The same at-most-one-row test the in-subquery literal path uses; the
  // pushed literal only widens the set of constraints it gets to see.
  return isSimpleSubquery(pinned, tableSpecs) ? pinned : undefined;
}

function resolveScalarSubquery(
  condition: CorrelatedSubqueryCondition,
  parent: ParentScope,
  tableSpecs: Map<string, TableSpecWithUniqueKeys>,
  execute: ScalarExecutor,
  out: Out,
): Condition {
  const parentField = condition.related.correlation.parentField[0];
  const childField = condition.related.correlation.childField[0];

  // Recursively resolve any scalar subqueries nested in the
  // subquery's own WHERE (and related) before evaluating this one.
  const subquery = resolveASTRecursive(
    pinWithParentLiteral(condition, parent, tableSpecs) ??
      condition.related.subquery,
    tableSpecs,
    execute,
    out,
  );

  if (!isSimpleSubquery(subquery, tableSpecs)) {
    // The author asked for a scalar rewrite and is not getting one. Record it
    // so the caller can say so — silently falling back is the whole reason the
    // hint is easy to get wrong.
    out.ignoredScalarHints.push({
      table: subquery.table,
      uniqueKeys: tableSpecs.get(subquery.table)?.tableSpec.uniqueKeys ?? [],
    });
    // Return with the (possibly partially-resolved) subquery.
    if (subquery !== condition.related.subquery) {
      return {
        ...condition,
        related: {...condition.related, subquery},
      };
    }
    return condition;
  }

  const value = execute(subquery, childField);

  // Record the companion subquery AST so its rows are synced to the client.
  // The client rewrites scalar subqueries to EXISTS and needs those rows.
  out.companions.push({
    ast: subquery,
    childField,
    resolvedValue: value,
  });

  if (value === undefined || value === null) {
    // No row matched, or the matched row's `childField` is NULL (which can
    // never satisfy the correlation, since `parentField = NULL` is never
    // true). Either way the correlated EXISTS is false for *every* parent
    // row, so the gate collapses to a constant: false for EXISTS, and its
    // negation — true — for NOT EXISTS.
    return condition.op === 'EXISTS' ? ALWAYS_FALSE : ALWAYS_TRUE;
  }

  const op = condition.op === 'EXISTS' ? '=' : 'IS NOT';
  return {
    type: 'simple',
    op,
    left: {type: 'column', name: parentField},
    right: {type: 'literal', value},
  } satisfies SimpleCondition;
}

const ALWAYS_FALSE: SimpleCondition = {
  type: 'simple',
  op: '=',
  left: {type: 'literal', value: 1},
  right: {type: 'literal', value: 0},
};

const ALWAYS_TRUE: SimpleCondition = {
  type: 'simple',
  op: '=',
  left: {type: 'literal', value: 1},
  right: {type: 'literal', value: 1},
};

/**
 * Checks if the subquery is guaranteed to return at most one deterministic row.
 *
 * This is true when all columns of at least one unique index on the subquery
 * table are equality-constrained by literal values in the WHERE clause
 * (using only AND conjunctions).
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
    // OR, correlatedSubquery (non-scalar) — don't contribute constraints
    default:
      break;
  }
}
