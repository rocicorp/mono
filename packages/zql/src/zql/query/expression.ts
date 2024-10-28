/* eslint-disable @typescript-eslint/no-explicit-any */
import type {
  SimpleOperator,
  ValuePosition,
} from '../../../../zero-protocol/src/ast.js';
import type {
  GetFieldTypeNoNullOrUndefined,
  Operator,
  Parameter,
  Selector,
} from './query.js';
import type {TableSchema} from './schema.js';

export type GenericCondition<TSchema extends TableSchema> =
  | {
      type: 'and';
      conditions: GenericCondition<TSchema>[];
    }
  | {
      type: 'or';
      conditions: GenericCondition<TSchema>[];
    }
  | {
      type: 'simple';
      op: SimpleOperator;
      field: Selector<TSchema>;
      value: ValuePosition;
    };

export function cmp<
  TSchema extends TableSchema,
  TSelector extends Selector<TSchema>,
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
): GenericCondition<TSchema>;
export function cmp<
  TSchema extends TableSchema,
  TSelector extends Selector<TSchema>,
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
): GenericCondition<TSchema>;
export function cmp(
  field: string,
  opOrValue:
    | Operator
    | GetFieldTypeNoNullOrUndefined<any, any, any>
    | Parameter<any, any, any>,
  value?:
    | GetFieldTypeNoNullOrUndefined<any, any, any>
    | Parameter<any, any, any>,
): GenericCondition<any>;
export function cmp(
  field: string,
  opOrValue:
    | Operator
    | GetFieldTypeNoNullOrUndefined<any, any, any>
    | Parameter<any, any, any>,
  value?:
    | GetFieldTypeNoNullOrUndefined<any, any, any>
    | Parameter<any, any, any>,
): GenericCondition<any> {
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

export function and<TSchema extends TableSchema>(
  ...conditions: GenericCondition<TSchema>[]
): GenericCondition<TSchema> {
  return flatten('and', conditions);
}

export function or<TSchema extends TableSchema>(
  ...conditions: GenericCondition<TSchema>[]
): GenericCondition<TSchema> {
  return flatten('or', conditions);
}

export function not<TSchema extends TableSchema>(
  expr: GenericCondition<TSchema>,
): GenericCondition<TSchema> {
  switch (expr.type) {
    case 'and':
      return {
        type: 'or',
        conditions: expr.conditions.map(not),
      };
    case 'or':
      return {
        type: 'and',
        conditions: expr.conditions.map(not),
      };
    default:
      return {
        type: 'simple',
        op: negateOperator(expr.op),
        field: expr.field,
        value: expr.value,
      };
  }
}

function flatten<TSchema extends TableSchema>(
  type: 'and' | 'or',
  conditions: GenericCondition<TSchema>[],
): GenericCondition<TSchema> {
  const flattened: GenericCondition<TSchema>[] = [];
  for (const c of conditions) {
    if (c.type === type) {
      flattened.push(...c.conditions);
    } else {
      flattened.push(c);
    }
  }

  return {type, conditions: flattened};
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
