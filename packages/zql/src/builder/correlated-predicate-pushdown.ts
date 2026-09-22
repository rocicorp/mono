import {deepEqual} from '../../../shared/src/json.ts';
import type {
  AST,
  Condition,
  Conjunction,
  CorrelatedSubquery,
  SimpleCondition,
} from '../../../zero-protocol/src/ast.ts';
import type {SchemaValue} from '../../../zero-types/src/schema-value.ts';
import {simplifyCondition} from '../query/expression.ts';

/**
 * The longest `IN` list that is copied into a child. `IN` compiles to
 * `IN (SELECT value FROM json_each(?))`, and every per-parent fetch of the
 * child carries the whole list.
 */
export const MAX_PUSHED_IN_VALUES = 32;

type ColumnCondition = SimpleCondition & {
  readonly left: {readonly type: 'column'; readonly name: string};
};

/**
 * Copies a parent's condition on a correlation column into the child
 * subquery, renamed to the matching child column. For example,
 * `user.where('user_id', X).related('reading')` becomes
 * `user.where('user_id', X).related('reading', r => r.where('user_id', X))`.
 *
 * The result is the same. Only child rows that cannot join to a parent that
 * matters are removed. But a push that climbs the tree then fetches parents
 * with SQL that includes the ancestor's condition, and a change to a child row
 * that cannot match stops at the child's source.
 *
 * Conditions are copied only when they are true for every parent row where
 * the edge can affect the result:
 * - for `related`, the top-level AND conjuncts of the parent's `where`;
 * - for EXISTS and NOT EXISTS, the conjuncts of every AND on the path from
 *   the root of `where` to the subquery condition. An OR adds no facts.
 *
 * A condition is copied when it is `column = literal`, `column IS literal`, or
 * `column IN literal-list` (at most {@link MAX_PUSHED_IN_VALUES} values), the
 * column is a `parentField` of the correlation, and the parent and child
 * columns have the same Zero type, which is not `json`.
 *
 * A condition is not copied when the child already has the same condition as
 * a conjunct. So the pass does not change an AST that it already pushed.
 *
 * Does not change its input. Returns the same object for every part of the
 * AST that it does not change.
 *
 * @param pushed When given, receives each child conjunct that is a copy of a
 * parent condition: the copies that the pass adds, and the child's own
 * conjuncts that equal a copy. The parent's condition implies each of them
 * wherever the join binds its column, so the planner ignores them there.
 */
export function pushDownCorrelatedPredicates(
  ast: AST,
  columnsOf: (table: string) => Record<string, SchemaValue>,
  pushed?: Set<SimpleCondition> | undefined,
): AST {
  function visit(ast: AST): AST {
    const where = ast.where && visitCondition(ast.where, ast.table, []);
    const facts = where ? simpleConjuncts(where) : [];
    const related =
      ast.related &&
      mapShared(ast.related, csq => {
        const subquery = visit(pushInto(csq, facts, ast.table));
        return subquery === csq.subquery ? csq : {...csq, subquery};
      });
    if (where === ast.where && related === ast.related) {
      return ast;
    }
    return {...ast, where, related};
  }

  function visitCondition(
    c: Condition,
    table: string,
    facts: readonly SimpleCondition[],
  ): Condition {
    switch (c.type) {
      case 'simple':
        return c;
      case 'and':
        return visitConjunction(c, table, [...facts, ...simpleConjuncts(c)]);
      case 'or': {
        const conditions = mapShared(c.conditions, x =>
          visitCondition(x, table, facts),
        );
        return conditions === c.conditions ? c : {...c, conditions};
      }
      case 'correlatedSubquery': {
        const subquery = visit(pushInto(c.related, facts, table));
        return subquery === c.related.subquery
          ? c
          : {...c, related: {...c.related, subquery}};
      }
    }
  }

  // `facts` already holds the simple conjuncts of `c` and of the ANDs nested
  // directly in it, so nested ANDs must not add them again.
  function visitConjunction(
    c: Conjunction,
    table: string,
    facts: readonly SimpleCondition[],
  ): Conjunction {
    const conditions = mapShared(c.conditions, x =>
      x.type === 'and'
        ? visitConjunction(x, table, facts)
        : visitCondition(x, table, facts),
    );
    return conditions === c.conditions ? c : {...c, conditions};
  }

  function pushInto(
    csq: CorrelatedSubquery,
    facts: readonly SimpleCondition[],
    parentTable: string,
  ): AST {
    const {parentField, childField} = csq.correlation;
    const {subquery} = csq;
    const conjuncts = subquery.where ? simpleConjuncts(subquery.where) : [];
    const added: SimpleCondition[] = [];
    for (const fact of facts) {
      if (!isPushable(fact)) {
        continue;
      }
      for (let i = 0; i < parentField.length; i++) {
        if (
          parentField[i] === fact.left.name &&
          haveSameScalarType(
            columnsOf(parentTable)[parentField[i]],
            columnsOf(subquery.table)[childField[i]],
          )
        ) {
          const copy: SimpleCondition = {
            ...fact,
            left: {type: 'column', name: childField[i]},
          };
          const same = conjuncts.find(c => isSameCondition(c, copy));
          if (same) {
            pushed?.add(same);
          } else {
            conjuncts.push(copy);
            added.push(copy);
            pushed?.add(copy);
          }
        }
      }
    }
    if (added.length === 0) {
      return subquery;
    }
    return {
      ...subquery,
      where: simplifyCondition({
        type: 'and',
        conditions: subquery.where ? [subquery.where, ...added] : added,
      }),
    };
  }

  return visit(ast);
}

/**
 * The simple conditions that are conjuncts of `c`, including those of ANDs
 * nested in ANDs.
 */
function simpleConjuncts(c: Condition): SimpleCondition[] {
  switch (c.type) {
    case 'simple':
      return [c];
    case 'and':
      return c.conditions.flatMap(simpleConjuncts);
    default:
      return [];
  }
}

function isPushable(c: SimpleCondition): c is ColumnCondition {
  if (c.left.type !== 'column' || c.right.type !== 'literal') {
    return false;
  }
  switch (c.op) {
    case '=':
    case 'IS':
      return true;
    case 'IN':
      return (
        Array.isArray(c.right.value) &&
        c.right.value.length <= MAX_PUSHED_IN_VALUES
      );
    default:
      return false;
  }
}

function isSameCondition(a: SimpleCondition, b: SimpleCondition): boolean {
  return (
    a.op === b.op &&
    a.left.type === 'column' &&
    b.left.type === 'column' &&
    a.left.name === b.left.name &&
    a.right.type === 'literal' &&
    b.right.type === 'literal' &&
    deepEqual(a.right.value, b.right.value)
  );
}

function haveSameScalarType(
  parent: SchemaValue | undefined,
  child: SchemaValue | undefined,
): boolean {
  return (
    parent !== undefined &&
    child !== undefined &&
    parent.type === child.type &&
    parent.type !== 'json'
  );
}

/**
 * Like `Array.prototype.map`, but returns `arr` itself when `f` returns every
 * element unchanged.
 */
function mapShared<T>(arr: readonly T[], f: (x: T) => T): readonly T[] {
  let result: T[] | undefined;
  for (let i = 0; i < arr.length; i++) {
    const x = f(arr[i]);
    if (result === undefined && x !== arr[i]) {
      result = arr.slice(0, i);
    }
    result?.push(x);
  }
  return result ?? arr;
}
