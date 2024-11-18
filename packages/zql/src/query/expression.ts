/* eslint-disable @typescript-eslint/no-explicit-any */
import {must} from '../../../shared/src/must.js';
import type {Condition, LiteralValue} from '../../../zero-protocol/src/ast.js';
import type {
  PullSchemaForRelationship,
  TableSchema,
} from '../../../zero-schema/src/table-schema.js';
import type {
  DefaultQueryResultRow,
  GetFieldTypeNoUndefined,
  NoJsonSelector,
  Operator,
  Parameter,
  Query,
  QueryType,
} from './query.js';

import type {Parameter as ASTParameter} from '../../../zero-protocol/src/ast.js';

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
export interface ExpressionFactory<TSchema extends TableSchema> {
  (eb: ExpressionBuilder<TSchema>): Condition;
}

export class ExpressionBuilder<TSchema extends TableSchema> {
  readonly #exists: (
    relationship: string,
    cb?: (
      query: Query<TableSchema, QueryType>,
    ) => Query<TableSchema, QueryType>,
  ) => Condition;

  constructor(
    exists: (
      relationship: string,
      cb?: (
        query: Query<TableSchema, QueryType>,
      ) => Query<TableSchema, QueryType>,
    ) => Condition,
  ) {
    this.#exists = exists;
    this.exists = this.exists.bind(this);
  }

  get eb() {
    return this;
  }

  cmp<
    TSelector extends NoJsonSelector<TSchema>,
    TOperator extends Operator,
    TParamAnchor = never,
    TParamField extends keyof TParamAnchor = never,
    TParamTypeBound extends GetFieldTypeNoUndefined<
      TSchema,
      TSelector,
      TOperator
    > = never,
  >(
    field: TSelector,
    op: TOperator,
    value:
      | GetFieldTypeNoUndefined<TSchema, TSelector, TOperator>
      | Parameter<TParamAnchor, TParamField, TParamTypeBound>,
  ): Condition;
  cmp<
    TSelector extends NoJsonSelector<TSchema>,
    TParamAnchor = never,
    TParamField extends keyof TParamAnchor = never,
    TParamTypeBound extends GetFieldTypeNoUndefined<
      TSchema,
      TSelector,
      '='
    > = never,
  >(
    field: TSelector,
    value:
      | GetFieldTypeNoUndefined<TSchema, TSelector, '='>
      | Parameter<TParamAnchor, TParamField, TParamTypeBound>,
  ): Condition;
  cmp(
    field: string,
    opOrValue: Operator | ASTParameter | LiteralValue,
    value?: ASTParameter | LiteralValue,
  ): Condition {
    return cmp(field, opOrValue, value);
  }

  and = and;
  or = or;
  not = not;

  exists(relationship: keyof TSchema['relationships']): Condition;
  exists<TRelationship extends keyof TSchema['relationships'] & string>(
    relationship: TRelationship,
    cb: (
      query: Query<
        PullSchemaForRelationship<TSchema, TRelationship>,
        DefaultQueryResultRow<PullSchemaForRelationship<TSchema, TRelationship>>
      >,
    ) => Query<TableSchema, QueryType>,
  ): Condition;
  exists(
    relationship: string,
    cb?: (
      query: Query<TableSchema, QueryType>,
    ) => Query<TableSchema, QueryType>,
  ): Condition {
    return this.#exists(relationship, cb);
  }
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
  opOrValue: Operator | ASTParameter | LiteralValue,
  value?: ASTParameter | LiteralValue,
): Condition {
  let op: Operator;
  if (value === undefined) {
    value = opOrValue;
    op = '=';
  } else {
    op = opOrValue as Operator;
  }

  return {
    type: 'simple',
    left: {type: 'column', name: field},
    right: isParameter(value) ? value : {type: 'literal', value},
    op,
  };
}

function isParameter(
  value: ASTParameter | LiteralValue,
): value is ASTParameter {
  return (
    typeof value === 'object' && (value as {type: string})?.type === 'static'
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

export function flatten(
  type: 'and' | 'or',
  conditions: Condition[],
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

function negateOperator<OP extends keyof typeof negateOperatorMap>(
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
