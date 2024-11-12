/* eslint-disable @typescript-eslint/no-explicit-any */
import type {
  Condition,
  SimpleOperator,
  ValuePosition,
} from '../../../zero-protocol/src/ast.js';
import type {TableSchema} from '../../../zero-schema/src/table-schema.js';
import type {
  GetFieldTypeNoNullOrUndefined,
  NoJsonSelector,
  Operator,
  Parameter,
} from './query.js';

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

export interface ExpressionBuilder<TSchema extends TableSchema> {
  readonly eb: ExpressionBuilder<TSchema>;

  cmp<
    TSelector extends NoJsonSelector<TSchema>,
    TOperator extends Operator,
    TParamAnchor = never,
    TParamField extends keyof TParamAnchor = never,
    TParamTypeBound extends GetFieldTypeNoNullOrUndefined<
      TSchema,
      TSelector,
      TOperator
    > = never,
  >(
    field: TSelector,
    op: TOperator,
    value:
      | GetFieldTypeNoNullOrUndefined<TSchema, TSelector, TOperator>
      | Parameter<TParamAnchor, TParamField, TParamTypeBound>,
  ): Condition;

  cmp<
    TSelector extends NoJsonSelector<TSchema>,
    TParamAnchor = never,
    TParamField extends keyof TParamAnchor = never,
    TParamTypeBound extends GetFieldTypeNoNullOrUndefined<
      TSchema,
      TSelector,
      '='
    > = never,
  >(
    field: TSelector,
    value:
      | GetFieldTypeNoNullOrUndefined<TSchema, TSelector, '='>
      | Parameter<TParamAnchor, TParamField, TParamTypeBound>,
  ): Condition;

  and(...conditions: (Condition | undefined)[]): Condition;
  or(...conditions: (Condition | undefined)[]): Condition;
  not(condition: Condition): Condition;
}

export const expressionBuilder: ExpressionBuilder<TableSchema> = Object.freeze({
  get eb() {
    return this;
  },
  cmp,
  and,
  or,
  not,
});

export function cmp(
  field: string,
  opOrValue: Operator | ValuePosition,
  value?: ValuePosition,
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
    field,
    op,
    value,
  };
}

export function and(...conditions: (Condition | undefined)[]): Condition {
  const expressions = filterUndefined(conditions);

  if (expressions.length === 1) {
    return expressions[0];
  }

  // If any internal conditions are `or` then we distribute `or` over the `and`.
  // This allows the graph and pipeline builder to remain simple and not have to deal with
  // nested conditions.
  // In other words, conditions are in [DNF](https://en.wikipedia.org/wiki/Disjunctive_normal_form).
  const ands = expressions.flatMap(c => {
    if (c.type === 'and') {
      return c.conditions;
    }
    if (c.type === 'simple') {
      return [c];
    }
    return [];
  });
  const ors = expressions.filter(c => c.type === 'or');

  if (ors.length === 0) {
    return {type: 'and', conditions: ands};
  }

  const flatOrs = flatten('or', ors);
  const flatAnds = flatten('and', ands);

  return {
    type: 'or',
    conditions: flatOrs.map(part => ({
      type: 'and',
      conditions: [
        ...(part.type === 'and' ? part.conditions : [part]),
        ...flatAnds,
      ],
    })),
  };
}

export function or(...conditions: (Condition | undefined)[]): Condition {
  const expressions = filterUndefined(conditions);
  if (expressions.length === 0) {
    return {type: 'or', conditions: []};
  }
  if (expressions.length === 1) {
    return expressions[0];
  }
  return {type: 'or', conditions: flatten('or', expressions)};
}

function not(expression: Condition): Condition {
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
    default:
      return {
        type: 'simple',
        op: negateOperator(expression.op),
        field: expression.field,
        value: expression.value,
      };
  }
}

function flatten(type: 'and' | 'or', conditions: Condition[]): Condition[] {
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

function negateOperator(op: SimpleOperator): SimpleOperator {
  switch (op) {
    case '=':
      return '!=';
    case '!=':
      return '=';
    case '<':
      return '>=';
    case '>':
      return '<=';
    case '>=':
      return '<';
    case '<=':
      return '>';
    case 'IN':
      return 'NOT IN';
    case 'NOT IN':
      return 'IN';
    case 'LIKE':
      return 'NOT LIKE';
    case 'NOT LIKE':
      return 'LIKE';
    case 'ILIKE':
      return 'NOT ILIKE';
    case 'NOT ILIKE':
      return 'ILIKE';
  }
}

function filterUndefined<T>(array: (T | undefined)[]): T[] {
  return array.filter(e => e !== undefined);
}
