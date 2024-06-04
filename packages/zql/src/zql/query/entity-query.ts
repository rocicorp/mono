import {must} from 'shared/src/must.js';
import type {
  AST,
  Selector as ASTSelector,
  Aggregation,
  Condition,
  EqualityOps,
  HavingCondition,
  InOps,
  LikeOps,
  OrderOps,
  Ordering,
  Primitive,
  PrimitiveArray,
  SetOps,
  SimpleOperator,
} from '../ast/ast.js';
import type {Context} from '../context/context.js';
import {Misuse} from '../error/misuse.js';
import type {Entity} from '../schema/entity-schema.js';
import {AggArray, Aggregate, Max, Min, isAggregate} from './agg.js';
import {Statement} from './statement.js';

type NotUndefined<T> = Exclude<T, undefined>;
type WeakKey = object;

export type ValueAsOperatorInput<
  V,
  Op extends SimpleOperator,
> = Op extends InOps
  ? NotUndefined<V>[]
  : Op extends LikeOps
  ? V extends string | undefined
    ? NotUndefined<V>
    : never
  : Op extends OrderOps
  ? V extends boolean | undefined
    ? never
    : NotUndefined<V>
  : Op extends EqualityOps
  ? NotUndefined<V>
  : Op extends SetOps
  ? NotUndefined<V>[]
  : never;

export type FieldAsOperatorInput<
  F extends FromSet,
  S extends SimpleSelector<F>,
  Op extends SimpleOperator,
> = S extends `${infer T}.${infer K}`
  ? ValueAsOperatorInput<F[T][K], Op>
  : ValueAsOperatorInput<ExtractNestedTypeByName<F, S>, Op>;

export type FromSet = {
  [tableOrAlias: string]: Entity;
};

type NestedKeys<T> = {
  [K in keyof T]: string & keyof T[K];
}[keyof T];

type ObjectHasSingleProperty<T> = {
  [K in keyof T]: Exclude<keyof Omit<T, K>, K>;
}[keyof T];

type QualifiedSelector<F extends FromSet> = {
  [K in keyof F]: `${string & K}.${string & keyof NotUndefined<F[K]>}`;
}[keyof F];

type SimpleSelector<F extends FromSet> =
  | QualifiedSelector<F>
  | (ObjectHasSingleProperty<F> extends never ? NestedKeys<F> : never);

type Selector<F extends FromSet> =
  | {
      [K in keyof F]:
        | `${string & K}.${string & keyof NotUndefined<F[K]>}`
        | `${string & K}.*`;
    }[keyof F]
  | (ObjectHasSingleProperty<F> extends never ? NestedKeys<F> : never)
  | '*';

type AggregateResult<
  Selection extends string,
  From extends FromSet,
  Alias extends string,
  Value,
> = Selection extends `${infer Table}.${string}`
  ? ObjectHasSingleProperty<From> extends never
    ? {
        [K in Alias]: Value;
      }
    : {
        [K in Table]: {
          [K in Alias]: Value;
        };
      }
  : {
      [K in Alias]: Value;
    };

type ExtractAggregatePiece<From extends FromSet, K extends Aggregator<From>> =
  // array aggregation
  K extends AggArray<infer Selection, infer Alias>
    ? Selection extends `${infer Table}.*`
      ? {
          [K in Alias]: From[Table][];
        }
      : {
          [K in Alias]: ExtractFieldValue<
            From,
            Selection extends SimpleSelector<From> ? Selection : never
          >[];
        }
    : K extends
        | Min<infer Selection, infer Alias>
        | Max<infer Selection, infer Alias>
    ? {
        [K in Alias]: ExtractFieldValue<
          From,
          Selection extends SimpleSelector<From> ? Selection : never
        >;
      }
    : // all other aggregate functions
    K extends Aggregate<infer Selection, infer Alias>
    ? AggregateResult<Selection, From, Alias, number>
    : never;

type ExtractFieldPiece<From extends FromSet, Selection extends Selector<From>> =
  // 'table.*'
  Selection extends `${infer Table}.*`
    ? Table extends keyof From
      ? ObjectHasSingleProperty<From> extends never
        ? From[Table]
        : {[K in Table]: From[Table]}
      : never
    : // 'table.column'
    Selection extends `${infer Table}.${infer Column}`
    ? ObjectHasSingleProperty<From> extends never
      ? {
          [K in Column]: ExtractFieldValue<From, Selection>;
        }
      : {
          [K in Table]: undefined extends From[Table]
            ?
                | {
                    [K in Column]: ExtractFieldValue<From, Selection>;
                  }
                | undefined
            : {
                [K in Column]: ExtractFieldValue<From, Selection>;
              };
        }
    : // '*'
    Selection extends '*'
    ? ObjectHasSingleProperty<From> extends never
      ? From[keyof From]
      : From
    : // 'column' -- we're pre-validated that the object has a single property at this point
      {
        [P in string & Selection]: ExtractNestedTypeByName<
          From,
          string & Selection
        >;
      };

type ExtractNestedTypeByName<T, S extends string> = {
  [K in keyof T]: S extends keyof T[K] ? T[K][S] : never;
}[keyof T];

type ExtractFieldValue<
  F extends FromSet,
  S extends SimpleSelector<F>,
> = S extends `${infer T}.${infer K}`
  ? NotUndefined<F[T]>[K]
  : ExtractNestedTypeByName<F, S>;

type MergeRecords<T> = T extends Record<string, unknown>
  ? {
      [K in keyof T]: MergeRecords<T[K]>;
    }
  : T;

type CombineFromSets<A, B> = MergeRecords<A & B>;

type CombineSelections<
  From extends FromSet,
  Selections extends (Selector<From> | Aggregator<From>)[],
> = Selections extends [infer First, ...infer Rest]
  ? First extends Selector<From>
    ? CombineFromSets<
        CombineSelections<
          From,
          Rest extends (Selector<From> | Aggregator<From>)[] ? Rest : []
        >,
        ExtractFieldPiece<From, First>
      >
    : First extends Aggregator<From>
    ? CombineFromSets<
        CombineSelections<
          From,
          Rest extends (Selector<From> | Aggregator<From>)[] ? Rest : []
        >,
        ExtractAggregatePiece<From, First>
      >
    : never
  : unknown;

type Aggregator<From extends FromSet> =
  | Aggregate<SimpleSelector<From>, string>
  | AggArray<Selector<From>, string>;

/**
 * Have you ever noticed that when you hover over Types in TypeScript, it shows
 * Pick<Omit<T, K>, K>? Rather than the final object structure after picking and omitting?
 * Or any time you use a type alias.
 *
 * MakeHumanReadable collapses the type aliases into their final form.
 */
// eslint-disable-next-line @typescript-eslint/ban-types
export type MakeHumanReadable<T> = {} & {
  readonly [P in keyof T]: T[P] extends string ? T[P] : MakeHumanReadable<T[P]>;
};

export type WhereCondition<From extends FromSet> =
  | {
      type: 'conjunction';
      op: 'AND' | 'OR';
      conditions: WhereCondition<From>[];
    }
  | SimpleCondition<From, SimpleSelector<From>, SimpleOperator>;

type SimpleCondition<
  From extends FromSet,
  Selector extends SimpleSelector<From>,
  Op extends SimpleOperator,
> = {
  type: 'simple';
  op: SimpleOperator;
  field: SimpleSelector<From>;
  value: {
    type: 'value';
    value: FieldAsOperatorInput<From, Selector, Op>;
  };
};

export class EntityQuery<From extends FromSet, Return = []> {
  readonly #ast: AST;
  readonly #name: string;
  readonly #context: Context;

  constructor(context: Context, tableName: string, ast?: AST) {
    this.#ast = ast ?? {
      table: tableName,
    };
    this.#name = tableName;
    this.#context = context;

    // TODO(arv): Guard this with TESTING once we have the infrastructure.
    astWeakMap.set(this, this.#ast);
  }

  select<Fields extends (Selector<From> | Aggregator<From>)[]>(
    ...x: Fields
  ): EntityQuery<From, CombineSelections<From, Fields>[]> {
    const seen = new Set(this.#ast.select?.map(s => s[1]));
    const aggregate: Aggregation[] = [];
    const ast = this.#ast;
    const select = ast.select ? [...ast.select] : [];

    for (const more of x) {
      if (!isAggregate(more)) {
        if (seen.has(more)) {
          continue;
        }
        seen.add(more);
        select.push([qualifySelector(ast, more), more]);

        continue;
      }
      aggregate.push({
        field:
          more.field !== undefined
            ? qualifySelector(ast, more.field)
            : undefined,
        alias: more.alias,
        aggregate: more.aggregate,
      });
    }

    return new EntityQuery<From, CombineSelections<From, Fields>[]>(
      this.#context,
      this.#name,
      {
        ...ast,
        select,
        aggregate,
      },
    );
  }

  // AFAICT `EntityQuery` would need to carry its table name in a third generic parameter
  // in order for us to be able make `Alias` optional. Seems doable.
  join<OtherFromSet extends FromSet, OtherReturn, Alias extends string>(
    other: EntityQuery<OtherFromSet, OtherReturn>,
    alias: Alias,
    thisField: SimpleSelector<From>,
    otherField: SimpleSelector<OtherFromSet>,
  ): EntityQuery<
    CombineFromSets<
      From,
      {
        [K in Alias]: OtherFromSet[keyof OtherFromSet];
      }
    >,
    Return
  > {
    return new EntityQuery(this.#context, this.#name, {
      ...this.#ast,
      joins: [
        ...(this.#ast.joins ?? []),
        {
          type: 'inner',
          other: other.#ast,
          as: alias,
          on: [
            qualifySelector(this.#ast, thisField),
            qualifySelector(other.#ast, otherField, alias),
          ],
        },
      ],
    });
  }

  leftJoin<OtherFromSet extends FromSet, OtherReturn, Alias extends string>(
    other: EntityQuery<OtherFromSet, OtherReturn>,
    alias: Alias,
    thisField: SimpleSelector<From>,
    otherField: SimpleSelector<OtherFromSet>,
  ): EntityQuery<
    CombineFromSets<
      From,
      {
        [K in Alias]?: OtherFromSet[keyof OtherFromSet] | undefined;
      }
    >,
    Return
  > {
    return new EntityQuery(this.#context, this.#name, {
      ...this.#ast,
      joins: [
        ...(this.#ast.joins ?? []),
        {
          type: 'left',
          other: other.#ast,
          as: alias,
          on: [
            qualifySelector(this.#ast, thisField),
            qualifySelector(other.#ast, otherField, alias),
          ],
        },
      ],
    });
  }

  groupBy<Fields extends SimpleSelector<From>[]>(...x: Fields) {
    return new EntityQuery<From, Return>(this.#context, this.#name, {
      ...this.#ast,
      groupBy: x.map(x => qualifySelector(this.#ast, x)),
    });
  }

  distinct<Field extends SimpleSelector<From>>(field: Field) {
    return new EntityQuery<From, Return>(this.#context, this.#name, {
      ...this.#ast,
      distinct: qualifySelector(this.#ast, field),
    });
  }

  where(expr: WhereCondition<From>): EntityQuery<From, Return>;
  where<K extends SimpleSelector<From>, Op extends SimpleOperator>(
    field: K,
    op: Op,
    value: FieldAsOperatorInput<From, K, Op>,
  ): EntityQuery<From, Return>;
  where<K extends SimpleSelector<From>, Op extends SimpleOperator>(
    exprOrField: K | WhereCondition<From>,
    op?: Op,
    value?: FieldAsOperatorInput<From, K, Op>,
  ): EntityQuery<From, Return> {
    return this.#whereOrHaving('where', exprOrField, op, value);
  }

  having(expr: WhereCondition<From>): EntityQuery<From, Return>;
  having<
    K extends
      | SimpleSelector<From>
      | keyof (Return extends Array<unknown> ? Return[number] : never),
    Op extends SimpleOperator,
  >(
    field: K,
    op: Op,
    value: K extends SimpleSelector<From>
      ? FieldAsOperatorInput<From, K, Op>
      : Return extends Array<unknown>
      ? K extends keyof Return[number]
        ? Return[number][K]
        : never
      : never,
  ): EntityQuery<From, Return>;
  having<K extends SimpleSelector<From>, Op extends SimpleOperator>(
    exprOrField: K | WhereCondition<From>,
    op?: Op,
    value?: FieldAsOperatorInput<From, K, Op>,
  ): EntityQuery<From, Return> {
    return this.#whereOrHaving('having', exprOrField, op, value);
  }

  #whereOrHaving<K extends SimpleSelector<From>, Op extends SimpleOperator>(
    whereOrHaving: 'where' | 'having',
    exprOrField: K | WhereCondition<From>,
    op?: Op,
    value?: FieldAsOperatorInput<From, K, Op>,
  ) {
    let expr: WhereCondition<From>;
    if (typeof exprOrField === 'string') {
      expr = exp(exprOrField, op!, value!);
    } else {
      expr = exprOrField;
    }

    let cond: Condition | HavingCondition;
    if (whereOrHaving === 'where') {
      // HAVING operates on the result of the query
      // so it does not qualify its accessors.
      cond = qualify(this.#ast, expr);
    } else {
      cond = qualifyHaving(expr);
    }

    const existingWhereOrHaving = this.#ast[whereOrHaving];
    if (existingWhereOrHaving) {
      if (existingWhereOrHaving.op === 'AND') {
        const {conditions} = existingWhereOrHaving;
        cond = flattenCondition('AND', [...conditions, cond]);
      } else {
        if (whereOrHaving === 'where') {
          cond = {
            type: 'conjunction',
            op: 'AND',
            conditions: [existingWhereOrHaving as Condition, cond as Condition],
          };
        } else {
          cond = {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              existingWhereOrHaving as HavingCondition,
              cond as HavingCondition,
            ],
          };
        }
      }
    }

    return new EntityQuery<From, Return>(this.#context, this.#name, {
      ...this.#ast,
      // Can't use satisfies here because WhereCondition is recursive.
      // Tests ensure that the expected AST output satisfies the Condition
      // type.
      [whereOrHaving]: cond,
    });
  }

  limit(n: number) {
    if (this.#ast.limit !== undefined) {
      throw new Misuse('Limit already set');
    }

    return new EntityQuery<From, Return>(this.#context, this.#name, {
      ...this.#ast,
      limit: n,
    });
  }

  asc(...x: SimpleSelector<From>[]) {
    return new EntityQuery<From, Return>(this.#context, this.#name, {
      ...this.#ast,
      orderBy: x.map(x => [qualifySelector(this.#ast, x), 'asc']),
    });
  }

  desc(...x: SimpleSelector<From>[]) {
    return new EntityQuery<From, Return>(this.#context, this.#name, {
      ...this.#ast,
      orderBy: x.map(x => [qualifySelector(this.#ast, x), 'desc']),
    });
  }

  prepare(): Statement<Return> {
    return new Statement<Return>(this.#context, {
      ...this.#ast,
      orderBy: makeOrderingDeterministic(this.#ast, this.#ast.orderBy),
    });
  }

  toString() {
    return JSON.stringify(this.#ast, null, 2);
  }
}

const astWeakMap = new WeakMap<WeakKey, AST>();

export function astForTesting(q: WeakKey): AST {
  return must(astWeakMap.get(q));
}

type ArrayOfAtLeastTwo<T> = [T, T, ...T[]];

export function or<F extends FromSet>(
  ...conditions: ArrayOfAtLeastTwo<WhereCondition<F>>
): WhereCondition<F> {
  return flatten('OR', conditions);
}

export function and<F extends FromSet>(
  ...conditions: ArrayOfAtLeastTwo<WhereCondition<F>>
): WhereCondition<F> {
  return flatten('AND', conditions);
}

function flatten<F extends FromSet>(
  op: 'AND' | 'OR',
  conditions: WhereCondition<F>[],
): WhereCondition<F> {
  const flattened: WhereCondition<F>[] = [];
  for (const c of conditions) {
    if (c.op === op) {
      flattened.push(...c.conditions);
    } else {
      flattened.push(c);
    }
  }

  return {type: 'conjunction', op, conditions: flattened};
}

function flattenCondition<T extends Condition | HavingCondition>(
  op: 'AND' | 'OR',
  conditions: T[],
): T {
  const flattened: T[] = [];
  for (const c of conditions) {
    if (c.op === op) {
      flattened.push(...(c.conditions as T[]));
    } else {
      flattened.push(c);
    }
  }

  return {type: 'conjunction', op, conditions: flattened} as unknown as T;
}

export function exp<
  From extends FromSet,
  Selector extends SimpleSelector<From>,
  Op extends SimpleOperator,
>(
  field: Selector,
  op: Op,
  value: FieldAsOperatorInput<From, Selector, Op>,
): WhereCondition<From> {
  return {
    type: 'simple',
    op,
    field,
    value: {
      type: 'value',
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      value: value as any, // TODO
    },
  };
}

export function not<From extends FromSet>(
  expr: WhereCondition<From>,
): WhereCondition<From> {
  switch (expr.op) {
    case 'AND':
      return {
        type: 'conjunction',
        op: 'OR',
        conditions: expr.conditions.map(not),
      };
    case 'OR':
      return {
        type: 'conjunction',
        op: 'AND',
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
    case 'INTERSECTS':
      return 'DISJOINT';
    case 'DISJOINT':
      return 'INTERSECTS';
    case 'SUPERSET':
      return 'SUBSET';
    case 'SUBSET':
      return 'SUPERSET';
    case 'CONGRUENT':
      return 'INCONGRUENT';
    case 'INCONGRUENT':
      return 'CONGRUENT';
  }
}

export function qualify<F extends FromSet>(
  ast: AST,
  expr: WhereCondition<F>,
): Condition {
  if (expr.type === 'simple') {
    return {
      type: expr.type,
      op: expr.op,
      field: qualifySelector(ast, expr.field),
      value: {
        type: expr.value.type,
        value: expr.value.value as Primitive | PrimitiveArray,
      },
    };
  }
  return {
    ...expr,
    conditions: expr.conditions.map(c => qualify(ast, c)),
  };
}

export function qualifyHaving<F extends FromSet>(
  expr: WhereCondition<F>,
): HavingCondition {
  if (expr.type === 'simple') {
    return {
      type: expr.type,
      op: expr.op,
      field: expr.field.includes('.')
        ? (expr.field.split('.') as unknown as ASTSelector)
        : [null, expr.field],
      value: {
        type: expr.value.type,
        value: expr.value.value as Primitive | PrimitiveArray,
      },
    };
  }
  return {
    ...expr,
    conditions: expr.conditions.map(c => qualifyHaving(c)),
  };
}

function qualifySelector(
  ast: AST,
  selector: string,
  alias?: string | undefined,
): ASTSelector {
  // if there are joins then the type system
  // will have ensured that the selector is already qualified
  if (
    (ast.joins !== undefined && ast.joins.length > 0) ||
    (alias && selector.startsWith(alias + '.')) ||
    (alias === undefined && selector.startsWith(ast.table + '.'))
  ) {
    return selector.split('.') as unknown as ASTSelector;
  }

  return [alias ?? ast.table, selector];
}

// Primary keys of all joined tables _must_ exist in the `orderBy` to ensure we have
// a deterministic ordering. As in, both client and server agree and we're not relying on undocumented
// behavior with respect to how to order identical (wrt ordering) items.
//
// This also facilitates cursoring as the cursor will always point to a unique item.
//
// Because we do this it is also important to ensure we are only creating sources in a new order based on
// the first key and not all the keys in `orderBy`. To prevent an explosion of sources
// while also giving us a perf gain. If/when we allow cleaning up of new orderings we can revisit that choice.
export function makeOrderingDeterministic(
  ast: AST,
  order: Ordering | undefined,
): Ordering {
  const requiredOrderFields =
    getRequiredOrderFieldsForDeterministicOrdering(ast);

  if (order) {
    for (const [selector] of order) {
      const key = selector.join('.');
      requiredOrderFields.delete(key);
    }
  } else {
    requiredOrderFields.delete(`${ast.table}.id`);
    order = [[[ast.table, 'id'], 'asc']];
  }

  if (requiredOrderFields.size === 0) {
    return order;
  }

  const defaultDirection = order[0][1];
  const newOrder = [...order];
  for (const selector of requiredOrderFields.values()) {
    newOrder.push([selector, defaultDirection]);
  }

  return newOrder;
}

function getRequiredOrderFieldsForDeterministicOrdering(
  ast: AST,
): Map<string, ASTSelector> {
  const ret = new Map<string, ASTSelector>();

  // If a group-by was used, we just need to append the group-by fields to the order-by.
  if (ast.groupBy !== undefined) {
    for (const group of ast.groupBy) {
      ret.set(group.join('.'), group);
    }
    return ret;
  }

  // TODO(mlaw): this'll change with compound primary keys
  ret.set(ast.table + '.id', [ast.table, 'id']);

  const {joins} = ast;
  if (joins) {
    for (const join of joins) {
      ret.set(join.as + '.id', [join.as, 'id']);
    }
  }

  return ret;
}
