/* oxlint-disable @typescript-eslint/no-explicit-any */
import {must} from '../../../shared/src/must.ts';
import {
  toStaticParam,
  SUBQ_PREFIX,
  type AST,
  type Condition,
  type LiteralValue,
  type Parameter,
  type SimpleOperator,
} from '../../../zero-protocol/src/ast.ts';
import type {Schema as ZeroSchema} from '../../../zero-types/src/schema.ts';
import {asQueryInternals} from './query-internals.ts';
import type {
  AvailableRelationships,
  DestTableName,
  ExistsOptions,
  GetFilterType,
  NoCompoundTypeSelector,
  PullTableSchema,
  Query,
} from './query.ts';

export type ParameterReference = {
  [toStaticParam](): Parameter;
};

export const toScalarRef = Symbol('toScalarRef');

export type ScalarReference = {
  readonly [toScalarRef]: {
    readonly ast: AST;
    readonly column: string;
  };
};

export function isScalarReference(value: unknown): value is ScalarReference {
  return (
    value !== null &&
    value !== undefined &&
    typeof value === 'object' &&
    toScalarRef in (value as Record<symbol, unknown>)
  );
}

/**
 * A factory function that creates a condition. This is used to create
 * complex conditions that can be passed to the `where` method of a query.
 *
 * @example
 *
 * ```ts
 * const condition: ExpressionFactory<User> = ({and, cmp, or}) =>
 *   and(
 *     cmp('name', '=', 'Alice'),
 *     or(cmp('age', '>', 18), cmp('isStudent', '=', true)),
 *   );
 *
 * const query = z.query.user.where(condition);
 * ```
 */
export interface ExpressionFactory<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends ZeroSchema,
> {
  (eb: ExpressionBuilder<TTable, TSchema>): Condition;
}

export class ExpressionBuilder<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends ZeroSchema,
> {
  readonly #exists: (
    relationship: string,
    cb?: (query: Query<TTable, TSchema>) => Query<TTable, TSchema, any>,
    options?: ExistsOptions,
  ) => Condition;

  constructor(
    exists: (
      relationship: string,
      cb?: (query: Query<TTable, TSchema>) => Query<TTable, TSchema, any>,
      options?: ExistsOptions,
    ) => Condition,
  ) {
    this.#exists = exists;
    this.exists = this.exists.bind(this);
    this.scalar = this.scalar.bind(this);
  }

  get eb() {
    return this;
  }

  cmp<
    TSelector extends NoCompoundTypeSelector<PullTableSchema<TTable, TSchema>>,
  >(field: TSelector, op: '=' | 'IS NOT', value: ScalarReference): Condition;
  cmp<
    TSelector extends NoCompoundTypeSelector<PullTableSchema<TTable, TSchema>>,
  >(field: TSelector, value: ScalarReference): Condition;
  cmp<
    TSelector extends NoCompoundTypeSelector<PullTableSchema<TTable, TSchema>>,
    TOperator extends SimpleOperator,
  >(
    field: TSelector,
    op: TOperator,
    value:
      | GetFilterType<PullTableSchema<TTable, TSchema>, TSelector, TOperator>
      | ParameterReference
      | undefined,
  ): Condition;
  cmp<
    TSelector extends NoCompoundTypeSelector<PullTableSchema<TTable, TSchema>>,
  >(
    field: TSelector,
    value:
      | GetFilterType<PullTableSchema<TTable, TSchema>, TSelector, '='>
      | ParameterReference
      | undefined,
  ): Condition;
  cmp(
    field: string,
    opOrValue:
      | SimpleOperator
      | ParameterReference
      | ScalarReference
      | LiteralValue
      | undefined,
    value?: ParameterReference | ScalarReference | LiteralValue | undefined,
  ): Condition {
    if (arguments.length === 2) {
      return cmp(field, opOrValue);
    }
    return cmp(field, opOrValue, value);
  }

  cmpLit(
    left: ParameterReference | LiteralValue | undefined,
    op: SimpleOperator,
    right: ParameterReference | LiteralValue | undefined,
  ): Condition {
    return {
      type: 'simple',
      left: isParameterReference(left)
        ? left[toStaticParam]()
        : {type: 'literal', value: left ?? null},
      right: isParameterReference(right)
        ? right[toStaticParam]()
        : {type: 'literal', value: right ?? null},
      op,
    };
  }

  and = and;
  or = or;
  not = not;

  exists = <TRelationship extends AvailableRelationships<TTable, TSchema>>(
    relationship: TRelationship,
    cb?: (
      query: Query<DestTableName<TTable, TSchema, TRelationship>, TSchema>,
    ) => Query<any, TSchema>,
    options?: ExistsOptions,
  ): Condition => this.#exists(relationship, cb, options);

  /**
   * EXPERIMENTAL. Use at your own risk.
   * Currently this has no benefit over `whereExists`
   * In future versions, scalar subqueries will unlock query optimizations
   * that are not possible with `whereExists`
   */
  scalar = (
    query: Query<string, any, any>,
    column: string,
  ): ScalarReference => {
    const qi = asQueryInternals(query);
    return {
      [toScalarRef]: {
        ast: {...qi.ast, limit: 1},
        column,
      },
    };
  };
}

export function and(...conditions: (Condition | undefined)[]): Condition {
  const expressions = filterTrue(filterUndefined(conditions));

  if (expressions.length === 1) {
    return expressions[0];
  }

  if (expressions.some(isAlwaysFalse)) {
    return FALSE;
  }

  return {type: 'and', conditions: expressions};
}

export function or(...conditions: (Condition | undefined)[]): Condition {
  const expressions = filterFalse(filterUndefined(conditions));

  if (expressions.length === 1) {
    return expressions[0];
  }

  if (expressions.some(isAlwaysTrue)) {
    return TRUE;
  }

  return {type: 'or', conditions: expressions};
}

export function not(expression: Condition): Condition {
  switch (expression.type) {
    case 'and':
      return {
        type: 'or',
        conditions: expression.conditions.map(not),
      };
    case 'or':
      return {
        type: 'and',
        conditions: expression.conditions.map(not),
      };
    case 'correlatedSubquery':
      return {
        type: 'correlatedSubquery',
        related: expression.related,
        op: negateOperator(expression.op),
      };
    case 'scalarSubquery':
      return {
        ...expression,
        op: expression.op === '=' ? 'IS NOT' : '=',
      };
    case 'simple':
      return {
        type: 'simple',
        op: negateOperator(expression.op),
        left: expression.left,
        right: expression.right,
      };
  }
}

export function cmp(
  field: string,
  opOrValue:
    | SimpleOperator
    | ParameterReference
    | ScalarReference
    | LiteralValue
    | undefined,
  value?: ParameterReference | ScalarReference | LiteralValue | undefined,
): Condition {
  let op: SimpleOperator;
  let actualValue:
    | ParameterReference
    | ScalarReference
    | LiteralValue
    | undefined;

  if (arguments.length === 2) {
    // 2-arg form: cmp(field, value) - defaults to '=' operator
    actualValue = opOrValue as
      | ParameterReference
      | ScalarReference
      | LiteralValue
      | undefined;
    op = '=';
  } else {
    // 3-arg form: cmp(field, op, value)
    op = opOrValue as SimpleOperator;
    actualValue = value;
  }

  if (isScalarReference(actualValue)) {
    if (op !== '=' && op !== 'IS NOT') {
      throw new Error(
        `Scalar subqueries only support '=' and 'IS NOT' operators, got '${op}'`,
      );
    }
    const subqueryAst = actualValue[toScalarRef].ast;
    return {
      type: 'scalarSubquery',
      op,
      parentField: field,
      childField: actualValue[toScalarRef].column,
      subquery: {
        ...subqueryAst,
        alias: subqueryAst.alias ?? `${SUBQ_PREFIX}scalar_${subqueryAst.table}`,
      },
    };
  }

  return {
    type: 'simple',
    left: {type: 'column', name: field},
    right: isParameterReference(actualValue)
      ? actualValue[toStaticParam]()
      : {type: 'literal', value: actualValue ?? null},
    op,
  };
}

function isParameterReference(
  value: ParameterReference | LiteralValue | null | undefined,
): value is ParameterReference {
  return (
    value !== null &&
    value !== undefined &&
    typeof value === 'object' &&
    (value as any)[toStaticParam]
  );
}

export const TRUE: Condition = {
  type: 'and',
  conditions: [],
};

const FALSE: Condition = {
  type: 'or',
  conditions: [],
};

function isAlwaysTrue(condition: Condition): boolean {
  return condition.type === 'and' && condition.conditions.length === 0;
}

function isAlwaysFalse(condition: Condition): boolean {
  return condition.type === 'or' && condition.conditions.length === 0;
}

export function simplifyCondition(c: Condition): Condition {
  if (
    c.type === 'simple' ||
    c.type === 'correlatedSubquery' ||
    c.type === 'scalarSubquery'
  ) {
    return c;
  }
  if (c.conditions.length === 1) {
    return simplifyCondition(c.conditions[0]);
  }
  const conditions = flatten(c.type, c.conditions.map(simplifyCondition));
  if (c.type === 'and' && conditions.some(isAlwaysFalse)) {
    return FALSE;
  }
  if (c.type === 'or' && conditions.some(isAlwaysTrue)) {
    return TRUE;
  }
  return {
    type: c.type,
    conditions,
  };
}

export function flatten(
  type: 'and' | 'or',
  conditions: readonly Condition[],
): Condition[] {
  const flattened: Condition[] = [];
  for (const c of conditions) {
    if (c.type === type) {
      flattened.push(...c.conditions);
    } else {
      flattened.push(c);
    }
  }

  return flattened;
}

const negateSimpleOperatorMap = {
  ['=']: '!=',
  ['!=']: '=',
  ['<']: '>=',
  ['>']: '<=',
  ['>=']: '<',
  ['<=']: '>',
  ['IN']: 'NOT IN',
  ['NOT IN']: 'IN',
  ['LIKE']: 'NOT LIKE',
  ['NOT LIKE']: 'LIKE',
  ['ILIKE']: 'NOT ILIKE',
  ['NOT ILIKE']: 'ILIKE',
  ['IS']: 'IS NOT',
  ['IS NOT']: 'IS',
} as const;

const negateOperatorMap = {
  ...negateSimpleOperatorMap,
  ['EXISTS']: 'NOT EXISTS',
  ['NOT EXISTS']: 'EXISTS',
} as const;

export function negateOperator<OP extends keyof typeof negateOperatorMap>(
  op: OP,
): (typeof negateOperatorMap)[OP] {
  return must(negateOperatorMap[op]);
}

function filterUndefined<T>(array: (T | undefined)[]): T[] {
  return array.filter(e => e !== undefined);
}

function filterTrue(conditions: Condition[]): Condition[] {
  return conditions.filter(c => !isAlwaysTrue(c));
}

function filterFalse(conditions: Condition[]): Condition[] {
  return conditions.filter(c => !isAlwaysFalse(c));
}
