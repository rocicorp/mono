/* eslint-disable @typescript-eslint/no-explicit-any */
import type {
  SimpleOperator,
  ValuePosition,
} from '../../../../zero-protocol/src/ast.js';
import type {
  GetFieldTypeNoNullOrUndefined,
  NoJsonSelector,
  Operator,
  Parameter,
  Selector,
} from './query.js';
import type {TableSchema} from './schema.js';

export type GenericCondition<TSchema extends TableSchema> =
  | GenericConjunction<TSchema>
  | GenericDisjunction<TSchema>
  | {
      type: 'simple';
      op: SimpleOperator;
      field: Selector<TSchema>;
      value: ValuePosition;
    };

type GenericConjunction<TSchema extends TableSchema> = {
  type: 'and';
  conditions: readonly GenericCondition<TSchema>[];
};

type GenericDisjunction<TSchema extends TableSchema> = {
  type: 'or';
  conditions: readonly GenericCondition<TSchema>[];
};

export function cmp<
  TSchema extends TableSchema,
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
): GenericCondition<TSchema>;
export function cmp<
  TSchema extends TableSchema,
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
    value: value as ValuePosition,
  };
}

export function and<TSchema extends TableSchema>(
  ...conditions: GenericCondition<TSchema>[]
): GenericCondition<TSchema> {
  // If any internal conditions are `or` then we distribute `or` over the `and`.
  // This allows the graph and pipeline builder to remain simple and not have to deal with
  // nested conditions.
  // In other words, conditions are in [DNF](https://en.wikipedia.org/wiki/Disjunctive_normal_form).
  const ands: GenericCondition<TSchema>[] = conditions.flatMap(c => {
    if (c.type === 'and') {
      return c.conditions;
    } else if (c.type === 'simple') {
      return [c];
    }
    return [];
  });
  const ors: GenericCondition<TSchema>[] = conditions.filter(
    c => c.type === 'or',
  );

  if (ors.length === 0) {
    return {type: 'and', conditions: ands};
  }

  const flatOrs = flatten('or', ors);
  const flatAnds = flatten('and', ands);

  return {
    type: 'or',
    conditions: flatOrs.conditions.map(part => ({
      type: 'and',
      conditions: [
        ...(part.type === 'and' ? part.conditions : [part]),
        ...flatAnds.conditions,
      ],
    })),
  };
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

function flatten<TSchema extends TableSchema, TConnector extends 'and' | 'or'>(
  type: TConnector,
  conditions: GenericCondition<TSchema>[],
): TConnector extends 'and'
  ? GenericConjunction<TSchema>
  : GenericDisjunction<TSchema> {
  const flattened: GenericCondition<TSchema>[] = [];
  for (const c of conditions) {
    if (c.type === type) {
      flattened.push(...c.conditions);
    } else {
      flattened.push(c);
    }
  }

  return {
    type,
    conditions: flattened,
  } satisfies GenericCondition<TSchema> as any;
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
