/* oxlint-disable @typescript-eslint/no-explicit-any */
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import {
  toStaticParam,
  type ColumnReference,
  type Condition,
  type JsonPathReference,
  type LiteralValue,
  type Parameter,
  type SimpleOperator,
} from '../../../zero-protocol/src/ast.ts';
import type {SchemaValueToTSType} from '../../../zero-types/src/schema-value.ts';
import type {Schema as ZeroSchema} from '../../../zero-types/src/schema.ts';
import type {
  AvailableRelationships,
  DestTableName,
  ExistsOptions,
  GetFilterType,
  GetFilterTypeFromTSType,
  JsonSelectors,
  NoCompoundTypeSelector,
  PullTableSchema,
  Query,
  ValidJsonPath,
  ValueAtPath,
} from './query.ts';

export type ParameterReference = {
  [toStaticParam](): Parameter;
};

const toColumnRef = Symbol();
declare const columnRefBrand: unique symbol;

/**
 * A reference to a value *inside* a `json()` column, produced by
 * {@link ExpressionBuilder.json} and accepted by `cmp` in the left position.
 *
 * It carries, as phantom type parameters (never present at runtime), the json
 * column's resolved TypeScript type (`TColumnType`) and the path (`P`). `json`
 * resolves `TColumnType` from the schema at the (concrete) call site, so `cmp`
 * walks the path over a *concrete* type rather than a deferred schema lookup —
 * a deferred path-walk in `cmp`'s signature is too complex for TypeScript's
 * structural comparison of `ExpressionBuilder` and breaks it wholesale.
 */
export type ColumnRef<
  TColumnType = unknown,
  P extends readonly (string | number)[] = readonly (string | number)[],
> = {
  [toColumnRef](): ColumnReference | JsonPathReference;
  // Phantom brand. Declared as a *method* (not a property) so the type
  // parameters are bivariant: `json`'s return type embeds a schema-derived
  // `TColumnType`, and a covariant brand would make a concrete-schema
  // `ColumnRef` unassignable to a generic-schema one, breaking
  // `ExpressionBuilder` assignability across the query builder. `cmp` still
  // infers `TColumnType`/`P` from this position.
  [columnRefBrand]?(brand: [TColumnType, P]): void;
};

function isColumnRef(v: unknown): v is ColumnRef {
  return typeof v === 'object' && v !== null && toColumnRef in v;
}

function makeColumnRef(
  name: string,
  path: readonly (string | number)[],
): ColumnRef {
  const column: ColumnReference = {type: 'column', name};
  const ref: ColumnReference | JsonPathReference =
    path.length > 0 ? {type: 'json', value: column, path} : column;
  return {[toColumnRef]: () => ref};
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
  }

  get eb() {
    return this;
  }

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
  cmp<
    TColumnType,
    const P extends readonly (string | number)[],
    TOperator extends SimpleOperator,
  >(
    ref: ColumnRef<TColumnType, P>,
    op: TOperator,
    value:
      | GetFilterTypeFromTSType<ValueAtPath<TColumnType, P>, TOperator>
      | ParameterReference
      | undefined,
  ): Condition;
  cmp<TColumnType, const P extends readonly (string | number)[]>(
    ref: ColumnRef<TColumnType, P>,
    value:
      | GetFilterTypeFromTSType<ValueAtPath<TColumnType, P>, '='>
      | ParameterReference
      | undefined,
  ): Condition;
  // Implementation signature — intentionally loose. The typed contract is
  // defined by the overloads above; this only needs to subsume them.
  cmp(
    field: string | ColumnRef,
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    ...args: any[]
  ): Condition {
    if (args.length === 1) {
      return cmp(field, args[0]);
    }
    return cmp(field, args[0], args[1]);
  }

  /**
   * References a value inside a `json()` column for use in `cmp`. Path
   * segments are object keys or array indices, applied left-to-right.
   *
   * @example
   * ```ts
   * z.query.issue.where(({cmp, json}) =>
   *   cmp(json('metadata', 'priority'), '=', 'high'));
   * ```
   */
  json<
    TColumn extends JsonSelectors<PullTableSchema<TTable, TSchema>> & string,
    const P extends readonly (string | number)[],
  >(
    column: TColumn,
    // `P` is inferred from the args; intersecting with `ValidJsonPath` rejects
    // an out-of-shape segment at its position (and drives autocomplete).
    ...path: P &
      ValidJsonPath<
        SchemaValueToTSType<
          PullTableSchema<TTable, TSchema>['columns'][TColumn]
        >,
        P
      >
  ): ColumnRef<
    SchemaValueToTSType<PullTableSchema<TTable, TSchema>['columns'][TColumn]>,
    P
  > {
    return makeColumnRef(column, path) as ColumnRef<
      SchemaValueToTSType<PullTableSchema<TTable, TSchema>['columns'][TColumn]>,
      P
    >;
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
        ...(expression.flip !== undefined ? {flip: expression.flip} : {}),
        ...(expression.scalar !== undefined ? {scalar: expression.scalar} : {}),
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
  field: string | ColumnRef,
  opOrValue:
    | SimpleOperator
    | ParameterReference
    | ReadonlyJSONValue
    | undefined,
  value?: ParameterReference | ReadonlyJSONValue,
): Condition {
  let op: SimpleOperator;
  let actualValue: ParameterReference | ReadonlyJSONValue | undefined;

  if (arguments.length === 2) {
    // 2-arg form: cmp(field, value) - defaults to '=' operator
    actualValue = opOrValue as
      | ParameterReference
      | ReadonlyJSONValue
      | undefined;
    op = '=';
  } else {
    // 3-arg form: cmp(field, op, value)
    op = opOrValue as SimpleOperator;
    actualValue = value;
  }

  return {
    type: 'simple',
    left: isColumnRef(field)
      ? field[toColumnRef]()
      : {type: 'column', name: field},
    right: isParameterReference(actualValue)
      ? actualValue[toStaticParam]()
      : {type: 'literal', value: (actualValue ?? null) as LiteralValue},
    op,
  };
}

function isParameterReference(
  value: ParameterReference | ReadonlyJSONValue | null | undefined,
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
  if (c.type === 'simple' || c.type === 'correlatedSubquery') {
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
