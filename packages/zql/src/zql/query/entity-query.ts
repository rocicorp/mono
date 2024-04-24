import {must} from 'shared/src/must.js';
import type {
  AST,
  Aggregation,
  Condition,
  EqualityOps,
  InOps,
  LikeOps,
  OrderOps,
  SimpleOperator,
  SetOps,
} from '../ast/ast.js';
import type {Context} from '../context/context.js';
import {Misuse} from '../error/misuse.js';
import type {EntitySchema} from '../schema/entity-schema.js';
import {AggArray, Aggregate, Max, Min, isAggregate} from './agg.js';
import {Statement} from './statement.js';

type NotUndefined<T> = Exclude<T, undefined>;

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
  [tableOrAlias: string]: EntitySchema;
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
      : AggregateResult<
          Selection,
          From,
          Alias,
          ExtractFieldValue<
            From,
            Selection extends SimpleSelector<From> ? Selection : never
          >[]
        >
    : K extends
        | Min<infer Selection, infer Alias>
        | Max<infer Selection, infer Alias>
    ? AggregateResult<
        Selection,
        From,
        Alias,
        ExtractFieldValue<
          From,
          Selection extends SimpleSelector<From> ? Selection : never
        >
      >
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

type CombineSelections<
  From extends FromSet,
  Selections extends (Selector<From> | Aggregator<From>)[],
> = Selections extends [infer First, ...infer Rest]
  ? First extends Selector<From>
    ? CombineSelections<
        From,
        Rest extends (Selector<From> | Aggregator<From>)[] ? Rest : []
      > &
        ExtractFieldPiece<From, First>
    : First extends Aggregator<From>
    ? CombineSelections<
        From,
        Rest extends (Selector<From> | Aggregator<From>)[] ? Rest : []
      > &
        ExtractAggregatePiece<From, First>
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
      op: 'AND' | 'OR';
      conditions: WhereCondition<From>[];
    }
  | SimpleCondition<From, SimpleSelector<From>, SimpleOperator>;

type SimpleCondition<
  From extends FromSet,
  Selector extends SimpleSelector<From>,
  Op extends SimpleOperator,
> = {
  op: SimpleOperator;
  field: SimpleSelector<From>;
  value: {
    type: 'literal';
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
      orderBy: [['id'], 'asc'],
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
    const select = [...(this.#ast.select ?? [])];
    for (const more of x) {
      if (!isAggregate(more)) {
        if (seen.has(more)) {
          continue;
        }
        seen.add(more);
        select.push([more, more]);

        continue;
      }
      aggregate.push(more);
    }

    return new EntityQuery<From, CombineSelections<From, Fields>[]>(
      this.#context,
      this.#name,
      {
        ...this.#ast,
        select: [...select],
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
    From & {
      [K in Alias]: OtherFromSet[keyof OtherFromSet];
    },
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
          on: [thisField, otherField],
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
    From & {
      [K in Alias]?: OtherFromSet[keyof OtherFromSet] | undefined;
    },
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
          on: [thisField, otherField],
        },
      ],
    });
  }

  groupBy<Fields extends SimpleSelector<From>[]>(...x: Fields) {
    return new EntityQuery<From, Return>(this.#context, this.#name, {
      ...this.#ast,
      groupBy: x as string[],
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
  having<K extends SimpleSelector<From>, Op extends SimpleOperator>(
    field: K,
    op: Op,
    value: FieldAsOperatorInput<From, K, Op>,
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

    let cond: WhereCondition<From>;
    const exitingWhereOrHaving = this.#ast[whereOrHaving] as
      | WhereCondition<From>
      | undefined;
    if (!exitingWhereOrHaving) {
      cond = expr;
    } else if (exitingWhereOrHaving.op === 'AND') {
      const {conditions} = exitingWhereOrHaving;
      cond = flatten('AND', [...conditions, expr]);
    } else {
      cond = {
        op: 'AND',
        conditions: [exitingWhereOrHaving, expr],
      };
    }

    return new EntityQuery<From, Return>(this.#context, this.#name, {
      ...this.#ast,
      [whereOrHaving]: cond as Condition,
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
    if (!x.includes('id')) {
      x.push('id');
    }

    return new EntityQuery<From, Return>(this.#context, this.#name, {
      ...this.#ast,
      orderBy: [x, 'asc'],
    });
  }

  desc(...x: SimpleSelector<From>[]) {
    if (!x.includes('id')) {
      x.push('id');
    }

    return new EntityQuery<From, Return>(this.#context, this.#name, {
      ...this.#ast,
      orderBy: [x, 'desc'],
    });
  }

  prepare(): Statement<Return> {
    return new Statement<Return>(this.#context, this.#ast);
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

  return {op, conditions: flattened};
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
    op,
    field,
    value: {
      type: 'literal',
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
        op: 'OR',
        conditions: expr.conditions.map(not),
      };
    case 'OR':
      return {
        op: 'AND',
        conditions: expr.conditions.map(not),
      };
    default:
      return {
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
