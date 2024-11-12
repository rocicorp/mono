/**
 * Wire-format representation of the zql AST interface.
 *
 * `v.Type<...>` types are explicitly declared to facilitate Typescript verification
 * that the schemas satisfy the zql type definitions. (Incidentally, explicit types
 * are also required for recursive schema definitions.)
 */

import {compareUTF8} from 'compare-utf8';
import {must} from '../../shared/src/must.js';
import * as v from '../../shared/src/valita.js';
import {defined} from '../../shared/src/arrays.js';
import {rowSchema, type Row} from './data.js';

export const selectorSchema = v.string();

const orderingElementSchema = v.readonly(
  v.tuple([selectorSchema, v.union(v.literal('asc'), v.literal('desc'))]),
);

export const orderingSchema = v.readonlyArray(orderingElementSchema);

export const primitiveSchema = v.union(
  v.string(),
  v.number(),
  v.boolean(),
  v.null(),
);

export const equalityOpsSchema = v.union(v.literal('='), v.literal('!='));

export const orderOpsSchema = v.union(
  v.literal('<'),
  v.literal('>'),
  v.literal('<='),
  v.literal('>='),
);

export const likeOpsSchema = v.union(
  v.literal('LIKE'),
  v.literal('NOT LIKE'),
  v.literal('ILIKE'),
  v.literal('NOT ILIKE'),
);

export const inOpsSchema = v.union(v.literal('IN'), v.literal('NOT IN'));

export const simpleOperatorSchema = v.union(
  equalityOpsSchema,
  orderOpsSchema,
  likeOpsSchema,
  inOpsSchema,
);

export const simpleConditionSchema = v.object({
  type: v.literal('simple'),
  op: simpleOperatorSchema,
  field: selectorSchema,
  value: v.union(
    v.string(),
    v.number(),
    v.boolean(),
    v.readonlyArray(v.union(v.string(), v.number(), v.boolean())),
    v.object({
      type: v.literal('static'),
      anchor: v.union(v.literal('authData'), v.literal('preMutationRow')),
      field: v.string(),
    }),
  ),
});

export const correlatedSubqueryConditionOperatorSchema = v.union(
  v.literal('EXISTS'),
  v.literal('NOT EXISTS'),
);

export const correlatedSubqueryConditionSchema = v.readonlyObject({
  type: v.literal('correlatedSubquery'),
  related: v.lazy(() => correlatedSubquerySchema),
  op: correlatedSubqueryConditionOperatorSchema,
});

export const conditionSchema = v.union(
  simpleConditionSchema,
  v.lazy(() => conjunctionSchema),
  v.lazy(() => disjunctionSchema),
  correlatedSubqueryConditionSchema,
);

const conjunctionSchema: v.Type<Conjunction> = v.readonlyObject({
  type: v.literal('and'),
  conditions: v.readonlyArray(conditionSchema),
});

const disjunctionSchema: v.Type<Disjunction> = v.readonlyObject({
  type: v.literal('or'),
  conditions: v.readonlyArray(conditionSchema),
});

// Split out so that its inferred type can be checked against
// Omit<CorrelatedSubquery, 'correlation'> in ast-type-test.ts.
// The mutually-recursive reference of the 'other' field to astSchema
// is the only thing added in v.lazy.  The v.lazy is necessary due to the
// mutually-recursive types, but v.lazy prevents inference of the resulting
// type.
export const correlatedSubquerySchemaOmitSubquery = v.readonlyObject({
  correlation: v.object({
    parentField: v.string(),
    childField: v.string(),
    op: v.literal('='),
  }),
  hidden: v.boolean().optional(),
});

export const correlatedSubquerySchema: v.Type<CorrelatedSubquery> =
  correlatedSubquerySchemaOmitSubquery.extend({
    subquery: v.lazy(() => astSchema),
  });

export const astSchema = v.object({
  schema: v.string().optional(),
  table: v.string(),
  alias: v.string().optional(),
  where: conditionSchema.optional(),
  related: v.readonlyArray(correlatedSubquerySchema).optional(),
  limit: v.number().optional(),
  orderBy: orderingSchema.optional(),
  start: v
    .object({
      row: rowSchema,
      exclusive: v.boolean(),
    })
    .optional(),
});

export type Bound = {
  row: Row;
  exclusive: boolean;
};

/**
 * As in SQL you can have multiple orderings. We don't currently
 * support ordering on anything other than the root query.
 */
export type OrderPart = readonly [field: string, direction: 'asc' | 'desc'];
export type Ordering = readonly OrderPart[];

export type SimpleOperator = EqualityOps | OrderOps | LikeOps | InOps;
export type EqualityOps = '=' | '!=';
export type OrderOps = '<' | '>' | '<=' | '>=';
export type LikeOps = 'LIKE' | 'NOT LIKE' | 'ILIKE' | 'NOT ILIKE';
export type InOps = 'IN' | 'NOT IN';

export type AST = {
  readonly schema?: string | undefined;
  readonly table: string;

  // A query would be aliased if the AST is a subquery.
  // e.g., when two subqueries select from the same table
  // they need an alias to differentiate them.
  // `SELECT
  //   [SELECT * FROM issue WHERE issue.id = outer.parentId] AS parent
  //   [SELECT * FROM issue WHERE issue.parentId = outer.id] AS children
  //  FROM issue as outer`
  readonly alias?: string | undefined;

  // `select` is missing given we return all columns for now.

  // The PipelineBuilder will pick what to use to correlate
  // a subquery with a parent query. It can choose something from the
  // where conditions or choose the _first_ `related` entry.
  // Choosing the first `related` entry is almost always the best choice if
  // one exists.
  readonly where?: Condition | undefined;

  readonly related?: readonly CorrelatedSubquery[] | undefined;
  readonly start?: Bound | undefined;
  readonly limit?: number | undefined;
  readonly orderBy?: Ordering | undefined;
};

export type CorrelatedSubquery = {
  /**
   * Only equality correlations are supported for now.
   * E.g., direct foreign key relationships.
   */
  readonly correlation: {
    readonly parentField: string;
    readonly childField: string;
    readonly op: '=';
  };
  readonly subquery: AST;
  // If a hop in the subquery chain should be hidden from the output view.
  // A common example is junction edges. The query API provides the illusion
  // that they don't exist: `issue.related('labels')` instead of `issue.related('issue_labels').related('labels')`.
  // To maintain this illusion, the junction edge should be hidden.
  // When `hidden` is set to true, this hop will not be included in the output view
  // but its children will be.
  readonly hidden?: boolean | undefined;
};

export type ValuePosition = LiteralValue | Parameter;

export type LiteralValue =
  | string
  | number
  | boolean
  | ReadonlyArray<string | number | boolean>;

/**
 * Starting only with SimpleCondition for now.
 * ivm1 supports Conjunctions and Disjunctions.
 * We'll support them in the future.
 */
export type Condition =
  | SimpleCondition
  | Conjunction
  | Disjunction
  | CorrelatedSubqueryCondition;

export type SimpleCondition = {
  type: 'simple';
  op: SimpleOperator;

  /**
   * Not a path yet as we're currently not allowing
   * comparisons across tables. This will need to
   * be a path through the tree in the near future.
   */
  field: string;

  /**
   * `null` is absent since we do not have an `IS` or `IS NOT`
   * operator defined and `null != null` in SQL.
   */
  value: ValuePosition;
};

export type Conjunction = {
  type: 'and';
  conditions: readonly Condition[];
};

export type Disjunction = {
  type: 'or';
  conditions: readonly Condition[];
};

export type CorrelatedSubqueryCondition = {
  type: 'correlatedSubquery';
  related: CorrelatedSubquery;
  op: CorrelatedSubqueryConditionOperator;
};

export type CorrelatedSubqueryConditionOperator = 'EXISTS' | 'NOT EXISTS';

/**
 * A parameter is a value that is not known at the time the query is written
 * and is resolved at runtime.
 *
 * StaticParameters refer to something provided by the caller.
 * StaticParameters are injected when the query pipeline is built from the AST
 * and do not change for the life of that pipeline.
 *
 * An example StaticParameter is the current authentication data.
 * When a user is authenticated, queries on the server have access
 * to the user's authentication data in order to evaluate authorization rules.
 * Authentication data doesn't change over the life of a query as a change
 * in auth data would represent a log-in / log-out of the user.
 *
 * AncestorParameters refer to rows encountered while running the query.
 * They are used by subqueries to refer to rows emitted by parent queries.
 */
export type Parameter = StaticParameter;
type StaticParameter = {
  type: 'static';
  // The "namespace" of the injected parameter.
  // Write authorization will send the value of a row
  // prior to the mutation being run (preMutationRow).
  // Read and write authorization will both send the
  // current authentication data (authData).
  anchor: 'authData' | 'preMutationRow';
  field: string;
};

const normalizeCache = new WeakMap<AST, Required<AST>>();
export function normalizeAST(ast: AST): Required<AST> {
  const cached = normalizeCache.get(ast);
  if (cached) {
    return cached;
  }
  const where = flattened(ast.where);
  const normalized = {
    schema: ast.schema,
    table: ast.table,
    alias: ast.alias,
    where: where ? sortedWhere(where) : undefined,
    related: ast.related
      ? sortedRelated(
          ast.related.map(
            r =>
              ({
                correlation: {
                  parentField: r.correlation.parentField,
                  childField: r.correlation.childField,
                  op: r.correlation.op,
                },
                hidden: r.hidden,
                subquery: normalizeAST(r.subquery),
              }) satisfies Required<CorrelatedSubquery>,
          ),
        )
      : undefined,
    start: ast.start,
    limit: ast.limit,
    orderBy: ast.orderBy,
  };

  normalizeCache.set(ast, normalized);
  return normalized;
}

function sortedWhere(where: Condition): Condition {
  if (where.type === 'simple' || where.type === 'correlatedSubquery') {
    return where;
  }
  return {
    type: where.type,
    conditions: where.conditions.map(w => sortedWhere(w)).sort(cmpCondition),
  };
}

function sortedRelated(
  related: CorrelatedSubquery[],
): readonly CorrelatedSubquery[] {
  return related.sort(cmpRelated);
}

function cmpCondition(a: Condition, b: Condition): number {
  if (a.type === 'simple') {
    if (b.type !== 'simple') {
      return -1; // Order SimpleConditions first to simplify logic for invalidation filtering.
    }
    return (
      compareUTF8MaybeNull(a.field, b.field) ||
      compareUTF8MaybeNull(a.op, b.op) ||
      // Comparing the same field with the same op more than once doesn't make logical
      // sense, but is technically possible. Assume the values are of the same type and
      // sort by their String forms.
      compareUTF8MaybeNull(String(a.value), String(b.value))
    );
  }

  if (b.type === 'simple') {
    return 1; // Order SimpleConditions first to simplify logic for invalidation filtering.
  }

  if (a.type === 'correlatedSubquery') {
    if (b.type !== 'correlatedSubquery') {
      return -1; // Order subquery before conjuctions/disjuctions
    }
    return cmpRelated(a.related, b.related) || compareUTF8MaybeNull(a.op, b.op);
  }
  if (b.type === 'correlatedSubquery') {
    return -1; // Order correlatedSubquery before conjuctions/disjuctions
  }

  const val = compareUTF8MaybeNull(a.type, b.type);
  if (val !== 0) {
    return val;
  }
  for (
    let l = 0, r = 0;
    l < a.conditions.length && r < b.conditions.length;
    l++, r++
  ) {
    const val = cmpCondition(a.conditions[l], b.conditions[r]);
    if (val !== 0) {
      return val;
    }
  }
  // prefixes first
  return a.conditions.length - b.conditions.length;
}

function cmpRelated(a: CorrelatedSubquery, b: CorrelatedSubquery): number {
  return compareUTF8(must(a.subquery.alias), must(b.subquery.alias));
}

/**
 * Returns a flattened version of the Conditions in which nested Conjunctions with
 * the same operation ('AND' or 'OR') are flattened to the same level. e.g.
 *
 * ```
 * ((a AND b) AND (c AND (d OR (e OR f)))) -> (a AND b AND c AND (d OR e OR f))
 * ```
 *
 * Also flattens singleton Conjunctions regardless of operator, and removes
 * empty Conjunctions.
 */
function flattened<T extends Condition>(cond: T | undefined): T | undefined {
  if (cond === undefined) {
    return undefined;
  }
  if (cond.type === 'simple' || cond.type === 'correlatedSubquery') {
    return cond;
  }
  const conditions = defined(
    cond.conditions.flatMap(c =>
      c.type === cond.type ? c.conditions.map(c => flattened(c)) : flattened(c),
    ),
  );

  switch (conditions.length) {
    case 0:
      return undefined;
    case 1:
      return conditions[0] as T;
    default:
      return {
        type: cond.type,
        conditions,
      } as unknown as T;
  }
}

function compareUTF8MaybeNull(a: string | null, b: string | null): number {
  if (a !== null && b !== null) {
    return compareUTF8(a, b);
  }
  if (b !== null) {
    return -1;
  }
  if (a !== null) {
    return 1;
  }
  return 0;
}
