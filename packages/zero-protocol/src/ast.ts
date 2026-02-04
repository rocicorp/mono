/**
 * Wire-format representation of the zql AST interface.
 *
 * `v.Type<...>` types are explicitly declared to facilitate Typescript verification
 * that the schemas satisfy the zql type definitions. (Incidentally, explicit types
 * are also required for recursive schema definitions.)
 */

import {compareUTF8} from 'compare-utf8';
import {defined} from '../../shared/src/arrays.ts';
import {assert} from '../../shared/src/asserts.ts';
import {must} from '../../shared/src/must.ts';
import * as v from '../../shared/src/valita.ts';
import type {NameMapper} from '../../zero-types/src/name-mapper.ts';
import {rowSchema, type Row} from './data.ts';

export const SUBQ_PREFIX = 'zsubq_';

export const selectorSchema = v.string();
export const toStaticParam = Symbol();
export const planIdSymbol = Symbol('planId');

const orderingElementSchema = v.readonly(
  v.tuple([selectorSchema, v.literalUnion('asc', 'desc')]),
);

export const orderingSchema = v.readonlyArray(orderingElementSchema);
export type System = 'permissions' | 'client' | 'test';

export const primitiveSchema = v.union(
  v.string(),
  v.number(),
  v.boolean(),
  v.null(),
);

export const equalityOpsSchema = v.literalUnion('=', '!=', 'IS', 'IS NOT');

export const orderOpsSchema = v.literalUnion('<', '>', '<=', '>=');

export const likeOpsSchema = v.literalUnion(
  'LIKE',
  'NOT LIKE',
  'ILIKE',
  'NOT ILIKE',
);

export const inOpsSchema = v.literalUnion('IN', 'NOT IN');

export const simpleOperatorSchema = v.union(
  equalityOpsSchema,
  orderOpsSchema,
  likeOpsSchema,
  inOpsSchema,
);

const literalReferenceSchema: v.Type<LiteralReference> = v.readonlyObject({
  type: v.literal('literal'),
  value: v.union(
    v.string(),
    v.number(),
    v.boolean(),
    v.null(),
    v.readonlyArray(v.union(v.string(), v.number(), v.boolean())),
  ),
});
const columnReferenceSchema: v.Type<ColumnReference> = v.readonlyObject({
  type: v.literal('column'),
  name: v.string(),
});

/**
 * A parameter is a value that is not known at the time the query is written
 * and is resolved at runtime.
 *
 * Static parameters refer to something provided by the caller.
 * Static parameters are injected when the query pipeline is built from the AST
 * and do not change for the life of that pipeline.
 *
 * An example static parameter is the current authentication data.
 * When a user is authenticated, queries on the server have access
 * to the user's authentication data in order to evaluate authorization rules.
 * Authentication data doesn't change over the life of a query as a change
 * in auth data would represent a log-in / log-out of the user.
 *
 * AncestorParameters refer to rows encountered while running the query.
 * They are used by subqueries to refer to rows emitted by parent queries.
 */
const parameterReferenceSchema = v.readonlyObject({
  type: v.literal('static'),
  // The "namespace" of the injected parameter.
  // Write authorization will send the value of a row
  // prior to the mutation being run (preMutationRow).
  // Read and write authorization will both send the
  // current authentication data (authData).
  anchor: v.literalUnion('authData', 'preMutationRow'),
  field: v.union(v.string(), v.array(v.string())),
});

const conditionValueSchema = v.union(
  literalReferenceSchema,
  columnReferenceSchema,
  parameterReferenceSchema,
);

export type Parameter = v.Infer<typeof parameterReferenceSchema>;

export const simpleConditionSchema: v.Type<SimpleCondition> = v.readonlyObject({
  type: v.literal('simple'),
  op: simpleOperatorSchema,
  left: conditionValueSchema,
  right: v.union(parameterReferenceSchema, literalReferenceSchema),
});

type ConditionValue = v.Infer<typeof conditionValueSchema>;

export const correlatedSubqueryConditionOperatorSchema: v.Type<CorrelatedSubqueryConditionOperator> =
  v.literalUnion('EXISTS', 'NOT EXISTS');

export const correlatedSubqueryConditionSchema: v.Type<CorrelatedSubqueryCondition> =
  v.readonlyObject({
    type: v.literal('correlatedSubquery'),
    related: v.lazy(() => correlatedSubquerySchema),
    op: correlatedSubqueryConditionOperatorSchema,
    flip: v.boolean().optional(),
  });

export const scalarSubqueryConditionSchema: v.Type<ScalarSubqueryCondition> =
  v.readonlyObject({
    type: v.literal('scalarSubquery'),
    op: v.literalUnion('=', 'IS NOT'),
    parentField: v.string(),
    subquery: v.lazy(() => astSchema),
    childField: v.string(),
  });

export const conditionSchema: v.Type<Condition> = v.union(
  simpleConditionSchema,
  v.lazy(() => conjunctionSchema),
  v.lazy(() => disjunctionSchema),
  correlatedSubqueryConditionSchema,
  scalarSubqueryConditionSchema,
);

const conjunctionSchema: v.Type<Conjunction> = v.readonlyObject({
  type: v.literal('and'),
  conditions: v.readonlyArray(conditionSchema),
});

const disjunctionSchema: v.Type<Disjunction> = v.readonlyObject({
  type: v.literal('or'),
  conditions: v.readonlyArray(conditionSchema),
});

export type CompoundKey = readonly [string, ...string[]];

function mustCompoundKey(field: readonly string[]): CompoundKey {
  assert(Array.isArray(field) && field.length >= 1);
  return field as unknown as CompoundKey;
}

export const compoundKeySchema: v.Type<CompoundKey> = v.readonly(
  v.tuple([v.string()]).concat(v.array(v.string())),
);

const correlationSchema = v.readonlyObject({
  parentField: compoundKeySchema,
  childField: compoundKeySchema,
});

// Split out so that its inferred type can be checked against
// Omit<CorrelatedSubquery, 'correlation'> in ast-type-test.ts.
// The mutually-recursive reference of the 'other' field to astSchema
// is the only thing added in v.lazy.  The v.lazy is necessary due to the
// mutually-recursive types, but v.lazy prevents inference of the resulting
// type.
export const correlatedSubquerySchemaOmitSubquery = v.readonlyObject({
  correlation: correlationSchema,
  hidden: v.boolean().optional(),
  system: v.literalUnion('permissions', 'client', 'test').optional(),
});

export const correlatedSubquerySchema: v.Type<CorrelatedSubquery> =
  correlatedSubquerySchemaOmitSubquery.extend({
    subquery: v.lazy(() => astSchema),
  });

export const astSchema: v.Type<AST> = v.readonlyObject({
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
export type EqualityOps = '=' | '!=' | 'IS' | 'IS NOT';
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

export type Correlation = {
  readonly parentField: CompoundKey;
  readonly childField: CompoundKey;
};

export type CorrelatedSubquery = {
  /**
   * Only equality correlation are supported for now.
   * E.g., direct foreign key relationships.
   */
  readonly correlation: Correlation;
  readonly subquery: AST;
  readonly system?: System | undefined;
  // If a hop in the subquery chain should be hidden from the output view.
  // A common example is junction edges. The query API provides the illusion
  // that they don't exist: `issue.related('labels')` instead of `issue.related('issue_labels').related('labels')`.
  // To maintain this illusion, the junction edge should be hidden.
  // When `hidden` is set to true, this hop will not be included in the output view
  // but its children will be.
  readonly hidden?: boolean | undefined;
};

export type ValuePosition = LiteralReference | Parameter | ColumnReference;

export type ColumnReference = {
  readonly type: 'column';
  /**
   * Not a path yet as we're currently not allowing
   * comparisons across tables. This will need to
   * be a path through the tree in the near future.
   */
  readonly name: string;
};

export type LiteralReference = {
  readonly type: 'literal';
  readonly value: LiteralValue;
};

export type LiteralValue =
  | string
  | number
  | boolean
  | null
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
  | CorrelatedSubqueryCondition
  | ScalarSubqueryCondition;

export type SimpleCondition = {
  readonly type: 'simple';
  readonly op: SimpleOperator;
  readonly left: ValuePosition;

  /**
   * `null` is absent since we do not have an `IS` or `IS NOT`
   * operator defined and `null != null` in SQL.
   */
  readonly right: Exclude<ValuePosition, ColumnReference>;
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
  flip?: boolean | undefined;
  [planIdSymbol]?: number | undefined;
};

export type CorrelatedSubqueryConditionOperator = 'EXISTS' | 'NOT EXISTS';

export type ScalarSubqueryCondition = {
  type: 'scalarSubquery';
  op: '=' | 'IS NOT';
  parentField: string;
  subquery: AST;
  childField: string;
};

interface ASTTransform {
  tableName(orig: string): string;
  columnName(origTable: string, origColumn: string): string;
  related(subqueries: CorrelatedSubquery[]): readonly CorrelatedSubquery[];
  where(cond: Condition): Condition | undefined;
  // conjunction or disjunction, called when traversing the return value of where()
  conditions(conds: Condition[]): readonly Condition[];
}

function transformAST(ast: AST, transform: ASTTransform): Required<AST> {
  // Name mapping functions (e.g. to server names)
  const {tableName, columnName} = transform;
  const colName = (c: string) => columnName(ast.table, c);
  const key = (table: string, k: CompoundKey) => {
    const serverKey = k.map(col => columnName(table, col));
    return mustCompoundKey(serverKey);
  };

  const where = ast.where ? transform.where(ast.where) : undefined;
  const transformed = {
    schema: ast.schema,
    table: tableName(ast.table),
    alias: ast.alias,
    where: where ? transformWhere(where, ast.table, transform) : undefined,
    related: ast.related
      ? transform.related(
          ast.related.map(
            r =>
              ({
                correlation: {
                  parentField: key(ast.table, r.correlation.parentField),
                  childField: key(r.subquery.table, r.correlation.childField),
                },
                hidden: r.hidden,
                subquery: transformAST(r.subquery, transform),
                system: r.system,
              }) satisfies Required<CorrelatedSubquery>,
          ),
        )
      : undefined,
    start: ast.start
      ? {
          ...ast.start,
          row: Object.fromEntries(
            Object.entries(ast.start.row).map(([col, val]) => [
              colName(col),
              val,
            ]),
          ),
        }
      : undefined,
    limit: ast.limit,
    orderBy: ast.orderBy?.map(([col, dir]) => [colName(col), dir] as const),
  };

  return transformed;
}

function transformWhere(
  where: Condition,
  table: string,
  transform: ASTTransform,
): Condition {
  // Name mapping functions (e.g. to server names)
  const {columnName} = transform;
  const condValue = (c: ConditionValue) =>
    c.type !== 'column' ? c : {...c, name: columnName(table, c.name)};
  const key = (table: string, k: CompoundKey) => {
    const serverKey = k.map(col => columnName(table, col));
    return mustCompoundKey(serverKey);
  };

  if (where.type === 'simple') {
    return {...where, left: condValue(where.left)};
  } else if (where.type === 'correlatedSubquery') {
    const {correlation, subquery} = where.related;
    return {
      ...where,
      related: {
        ...where.related,
        correlation: {
          parentField: key(table, correlation.parentField),
          childField: key(subquery.table, correlation.childField),
        },
        subquery: transformAST(subquery, transform),
      },
    };
  } else if (where.type === 'scalarSubquery') {
    return {
      ...where,
      parentField: columnName(table, where.parentField),
      childField: columnName(where.subquery.table, where.childField),
      subquery: transformAST(where.subquery, transform),
    };
  }

  return {
    type: where.type,
    conditions: transform.conditions(
      where.conditions.map(c => transformWhere(c, table, transform)),
    ),
  };
}

const normalizeCache = new WeakMap<AST, Required<AST>>();

const NORMALIZE_TRANSFORM: ASTTransform = {
  tableName: t => t,
  columnName: (_, c) => c,
  related: sortedRelated,
  where: flattened,
  conditions: c => c.sort(cmpCondition),
};

export function normalizeAST(ast: AST): Required<AST> {
  let normalized = normalizeCache.get(ast);
  if (!normalized) {
    normalized = transformAST(ast, NORMALIZE_TRANSFORM);
    normalizeCache.set(ast, normalized);
  }
  return normalized;
}

export function mapAST(ast: AST, mapper: NameMapper) {
  return transformAST(ast, {
    tableName: table => mapper.tableName(table),
    columnName: (table, col) => mapper.columnName(table, col),
    related: r => r,
    where: w => w,
    conditions: c => c,
  });
}

export function mapCondition(
  cond: Condition,
  table: string,
  mapper: NameMapper,
) {
  return transformWhere(cond, table, {
    tableName: table => mapper.tableName(table),
    columnName: (table, col) => mapper.columnName(table, col),
    related: r => r,
    where: w => w,
    conditions: c => c,
  });
}

function sortedRelated(
  related: CorrelatedSubquery[],
): readonly CorrelatedSubquery[] {
  return related.sort(cmpRelated);
}

function cmpCondition(a: Condition, b: Condition): number {
  if (a.type === 'simple') {
    if (b.type !== 'simple') {
      return -1; // Order SimpleConditions first
    }

    return (
      compareValuePosition(a.left, b.left) ||
      compareUTF8MaybeNull(a.op, b.op) ||
      compareValuePosition(a.right, b.right)
    );
  }

  if (b.type === 'simple') {
    return 1; // Order SimpleConditions first
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

  if (a.type === 'scalarSubquery') {
    if (b.type !== 'scalarSubquery') {
      return -1; // Order scalarSubquery before conjunctions/disjunctions
    }
    return (
      compareUTF8(a.parentField, b.parentField) ||
      compareUTF8(a.childField, b.childField) ||
      compareUTF8MaybeNull(a.op, b.op)
    );
  }
  if (b.type === 'scalarSubquery') {
    return 1;
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

function compareValuePosition(a: ValuePosition, b: ValuePosition): number {
  if (a.type !== b.type) {
    return compareUTF8(a.type, b.type);
  }
  switch (a.type) {
    case 'literal':
      assert(b.type === 'literal');
      return compareUTF8(String(a.value), String(b.value));
    case 'column':
      assert(b.type === 'column');
      return compareUTF8(a.name, b.name);
    case 'static':
      throw new Error(
        'Static parameters should be resolved before normalization',
      );
  }
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
function flattened(cond: Condition): Condition | undefined {
  if (
    cond.type === 'simple' ||
    cond.type === 'correlatedSubquery' ||
    cond.type === 'scalarSubquery'
  ) {
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
      return conditions[0];
    default:
      return {
        type: cond.type,
        conditions,
      };
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
