/**
 * Wire-format representation of the zql AST interface.
 *
 * `v.Type<...>` types are explicitly declared to facilitate Typescript verification
 * that the schemas satisfy the zql type definitions. (Incidentally, explicit types
 * are also required for recursive schema definitions.)
 */

import {compareUTF8} from 'compare-utf8';
import {assert} from '../../shared/src/asserts.ts';
import {getOrInsertComputed} from '../../shared/src/map.ts';
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
 * Upper bound for an array-index segment. SQLite parses a `$[i]` index as an
 * unsigned 32-bit integer (larger values silently wrap), and the Postgres `->`
 * operator takes an `int4`, so anything above this is not a usable index on
 * every engine.
 */
export const MAX_JSON_PATH_INDEX = 2 ** 31 - 1;

/**
 * A numeric JSON path segment is an array index and must be an integer in
 * `[0, MAX_JSON_PATH_INDEX]`. Negative indices are rejected because the engines
 * disagree on them (Postgres counts from the end, while JavaScript and SQLite
 * yield null); larger values are rejected because SQLite wraps them modulo
 * 2^32 (silently addressing another element) and Postgres cannot take them as
 * an `int4` operand.
 */
export function isValidJsonPathIndex(index: number): boolean {
  return Number.isInteger(index) && index >= 0 && index <= MAX_JSON_PATH_INDEX;
}

/**
 * The JS type a JSON leaf must have to be compared against `literal` —
 * `'string' | 'number' | 'boolean'` — or `undefined` for `null` and for an
 * empty list. For an `IN`/`NOT IN` list, the type of its first element (lists
 * are homogeneous: enforced by the builder's types, by `cmp()` and at the
 * wire). Shared by the in-memory predicate, the SQLite pushdown and the
 * Postgres compiler so the type-strict rule cannot drift between engines.
 */
export function jsonLiteralType(
  literal: LiteralValue,
): 'string' | 'number' | 'boolean' | undefined {
  const v = Array.isArray(literal) ? literal[0] : literal;
  switch (typeof v) {
    case 'string':
      return 'string';
    case 'number':
      return 'number';
    case 'boolean':
      return 'boolean';
    default:
      return undefined;
  }
}

const negatedOperators: ReadonlySet<SimpleOperator> = new Set([
  '!=',
  'NOT LIKE',
  'NOT ILIKE',
  'NOT IN',
]);

/**
 * The operators under which a JSON leaf of a *different* type than the literal
 * is a match (it is "not equal"); under every other operator a mismatch is a
 * non-match. `IS NOT` is not listed: `IS`/`IS NOT` are null-safe equality and
 * already yield the strict answer.
 */
export function isNegatedOperator(op: SimpleOperator): boolean {
  return negatedOperators.has(op);
}

const likeOperators: ReadonlySet<SimpleOperator> = new Set([
  'LIKE',
  'NOT LIKE',
  'ILIKE',
  'NOT ILIKE',
]);

/**
 * The pattern-matching operators, which compare text and therefore require a
 * string leaf whatever the literal's type.
 */
export function isLikeOperator(op: SimpleOperator): boolean {
  return likeOperators.has(op);
}

/**
 * Formats a JSON path reference for debug/display output, e.g.
 * `metadata.tags[0]`. Shared by the builder's filter names, the planner debug
 * output and test stringifiers so they cannot drift.
 */
export function formatJsonPathReference(ref: JsonPathReference): string {
  return `${ref.value.name}${ref.path
    .map(s => (typeof s === 'number' ? `[${s}]` : `.${s}`))
    .join('')}`;
}

const jsonPathReferenceSchema: v.Type<JsonPathReference> = v.readonlyObject({
  type: v.literal('json'),
  value: columnReferenceSchema,
  path: v
    .readonlyArray(
      v.union(
        v.string(),
        v
          .number()
          .assert(
            isValidJsonPathIndex,
            'expected a non-negative integer array index',
          ),
      ),
    )
    // `json(col, ...path)` types the path as non-empty; reject an empty one at
    // the wire too, since the compilers address the last segment and a bare
    // column is not a JSON leaf.
    .assert(path => path.length > 0, 'expected a non-empty JSON path'),
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
  jsonPathReferenceSchema,
  parameterReferenceSchema,
);

export type Parameter = v.Infer<typeof parameterReferenceSchema>;

/**
 * The literal compared against a JSON path leaf has the shape its operator
 * needs: `IN`/`NOT IN` take a list (or `null`, the explicitly supported
 * constant-false case) and every other operator a scalar. The builder's types
 * guarantee this for a literal, but a parameter is bound to whatever the
 * anchor holds; without this check a scalar bound to `IN` trips the in-memory
 * predicate's array assertion and a list bound to `=` the Postgres compiler's
 * plural assertion. A plain column's literal is not checked here (its shape
 * was never validated at the wire, and changing that is out of scope).
 */
function hasJsonLeafLiteralShape(c: SimpleCondition): boolean {
  if (c.left.type !== 'json' || c.right.type !== 'literal') {
    return true;
  }
  const isList = Array.isArray(c.right.value);
  return c.op === 'IN' || c.op === 'NOT IN'
    ? isList || c.right.value === null
    : !isList;
}

/**
 * An `IN`/`NOT IN` list against a JSON path leaf is homogeneous: the engines
 * compare the leaf against the type of the list's first element (see
 * `jsonLiteralType`), and a mixed list would cast-fail on Postgres while
 * silently dropping elements elsewhere. `cmp()` rejects one for a `json()`
 * reference; this rejects it at the wire so a hand-built AST cannot bypass
 * that. A plain column's list is not restricted (its type is the column's).
 */
function hasHomogeneousJsonInList(c: SimpleCondition): boolean {
  if (
    c.left.type !== 'json' ||
    (c.op !== 'IN' && c.op !== 'NOT IN') ||
    c.right.type !== 'literal'
  ) {
    return true;
  }
  const {value} = c.right;
  if (!Array.isArray(value)) {
    return true;
  }
  const list: readonly (string | number | boolean)[] = value;
  return list.every(e => typeof e === typeof list[0]);
}

export const simpleConditionSchema: v.Type<SimpleCondition> = v
  .readonlyObject({
    type: v.literal('simple'),
    op: simpleOperatorSchema,
    left: conditionValueSchema,
    right: v.union(parameterReferenceSchema, literalReferenceSchema),
  })
  .assert(
    hasJsonLeafLiteralShape,
    'expected a list for IN/NOT IN against a JSON path and a scalar for other operators',
  )
  .assert(
    hasHomogeneousJsonInList,
    'expected a list of values of one type for IN against a JSON path',
  );

type ConditionValue = v.Infer<typeof conditionValueSchema>;

export const correlatedSubqueryConditionOperatorSchema: v.Type<CorrelatedSubqueryConditionOperator> =
  v.literalUnion('EXISTS', 'NOT EXISTS');

export const correlatedSubqueryConditionSchema: v.Type<CorrelatedSubqueryCondition> =
  v.readonlyObject({
    type: v.literal('correlatedSubquery'),
    related: v.lazy(() => correlatedSubquerySchema),
    op: correlatedSubqueryConditionOperatorSchema,
    flip: v.boolean().optional(),
    scalar: v.boolean().optional(),
  });

export const conditionSchema: v.Type<Condition> = v.union(
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

export type CompoundKey = readonly [string, ...string[]];

function mustCompoundKey(field: readonly string[]): CompoundKey {
  assert(
    Array.isArray(field) && field.length >= 1,
    'Expected non-empty array for compound key',
  );
  return field as unknown as CompoundKey;
}

export const compoundKeySchema: v.Type<CompoundKey> = v.readonly(
  // oxlint-disable-next-line e18e/prefer-spread-syntax
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

export type ValuePosition =
  | LiteralReference
  | Parameter
  | ColumnReference
  | JsonPathReference;

export type ColumnReference = {
  readonly type: 'column';
  /**
   * The name of the column on the current table.
   *
   * Note: this is *not* a path through the relationship tree. Cross-table
   * comparisons are not yet supported and would be modeled separately.
   */
  readonly name: string;
};

/**
 * A reference to a value *inside* a `json()` column, produced by `eb.json()`.
 *
 * Wraps a {@link ColumnReference} and navigates into its JSON value via `path`:
 * segments are applied left-to-right as object keys (string) and array indices
 * (number). A `JsonPathReference` is allowed wherever a {@link ColumnReference} is
 * (currently only the left operand of a filter predicate).
 *
 * Evaluation semantics — implemented identically by the in-memory predicate
 * (`zql/src/builder/filter.ts`), the SQLite pushdown (`zqlite/src/query-builder.ts`)
 * and the Postgres compiler (`z2s/src/compiler.ts`); the engine comments refer
 * here rather than restating the rules:
 *
 * - **Segments are strict.** A number segment indexes an array and a string
 *   segment reads an own key of an object. Any other step — a segment of the
 *   wrong kind, a missing key, a scalar or null intermediate — yields null.
 * - **Missing and JSON null collapse to null:** a non-match for every value
 *   operator, a match for `IS NULL`.
 * - **Comparison is type-strict.** A leaf whose JSON type differs from the
 *   literal's is never equal: a non-match for a positive operator, a match for a
 *   negated one (`!=`, `NOT IN`, `NOT LIKE`, `NOT ILIKE`), and never an error.
 *   Only scalar leaves are comparable.
 *
 * A path references a value inside a single column; it never crosses tables.
 * Because a path-extracted value is not a stored/indexed column, it can never
 * be a primary-key or join constraint key (see {@link extractColumn}, which
 * excludes it from constraint/PK lookups).
 */
export type JsonPathReference = {
  readonly type: 'json';
  readonly value: ColumnReference;
  readonly path: readonly (string | number)[];
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
  | CorrelatedSubqueryCondition;

export type SimpleCondition = {
  readonly type: 'simple';
  readonly op: SimpleOperator;
  readonly left: ValuePosition;

  /**
   * `null` is absent since we do not have an `IS` or `IS NOT`
   * operator defined and `null != null` in SQL.
   */
  readonly right: Exclude<ValuePosition, ColumnReference | JsonPathReference>;
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
  scalar?: boolean | undefined;
  [planIdSymbol]?: number | undefined;
};

export type CorrelatedSubqueryConditionOperator = 'EXISTS' | 'NOT EXISTS';

interface ASTTransform {
  tableName(orig: string): string;
  columnName(origTable: string, origColumn: string): string;
  // The column wrapped by a JSON path reference. A mapper that knows column
  // types rejects a column that is not a json column here.
  jsonColumnName(origTable: string, origColumn: string): string;
}

function transformAST(ast: AST, transform: ASTTransform): Required<AST> {
  // Name mapping functions (e.g. to server names)
  const {tableName, columnName} = transform;
  const colName = (c: string) => columnName(ast.table, c);
  const key = (table: string, k: CompoundKey) => {
    const serverKey = k.map(col => columnName(table, col));
    return mustCompoundKey(serverKey);
  };

  const transformed = {
    schema: ast.schema,
    table: tableName(ast.table),
    alias: ast.alias,
    where: ast.where
      ? transformWhere(ast.where, ast.table, transform)
      : undefined,
    related: ast.related?.map(
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
  const {columnName, jsonColumnName} = transform;
  const condValue = (c: ConditionValue): ConditionValue => {
    switch (c.type) {
      case 'column':
        return {...c, name: columnName(table, c.name)};
      case 'json':
        // The column name maps client->server (and is validated to be a json
        // column); the path is data and is preserved as-is.
        return {
          ...c,
          value: {...c.value, name: jsonColumnName(table, c.value.name)},
        };
      default:
        return c;
    }
  };
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
  }

  return {
    type: where.type,
    conditions: where.conditions.map(c => transformWhere(c, table, transform)),
  };
}

declare const normalizedTag: unique symbol;

/**
 * An {@link AST} in its normalized form: `related` is sorted, `where` is
 * flattened and sorted, and every field is present, in a fixed order. Two ASTs
 * that describe the same query have the same JSON encoding, and thus the same
 * hash, once normalized.
 *
 * The tag only exists in the type, so the only way to get one is from
 * {@link normalizeAST}, {@link normalizedAST} or {@link tableAST}. Spreading
 * one keeps the tag, which is what lets a query builder derive a normalized
 * AST from another one by replacing a field it knows to be safe.
 */
export type NormalizedAST = Required<AST> & {readonly [normalizedTag]: true};

function asNormalized(ast: Required<AST>): NormalizedAST {
  return ast as NormalizedAST;
}

const normalizeCache = new WeakMap<AST, NormalizedAST>();

export function normalizeAST(ast: AST): NormalizedAST {
  return getOrInsertComputed(normalizeCache, ast, normalizeASTUncached);
}

function normalizeASTUncached(ast: AST): NormalizedAST {
  // normalizedAST() normalizes a single level, so normalize the subqueries
  // first.
  const {where, related} = ast;
  return normalizedAST({
    ...ast,
    where: where && normalizeSubqueries(where),
    related: related?.map(r =>
      normalizedRelated({...r, subquery: normalizeAST(r.subquery)}),
    ),
  });
}

/**
 * Normalizes the ASTs of the correlated subqueries of `cond`. The condition
 * itself is normalized by {@link normalizeCondition}.
 */
function normalizeSubqueries(cond: Condition): Condition {
  switch (cond.type) {
    case 'simple':
      return cond;
    case 'correlatedSubquery':
      return {
        ...cond,
        related: {
          ...cond.related,
          subquery: normalizeAST(cond.related.subquery),
        },
      };
    default:
      return {
        type: cond.type,
        conditions: cond.conditions.map(normalizeSubqueries),
      };
  }
}

/**
 * Normalizes a single level of an AST, assuming that the ASTs of its
 * subqueries (i.e. those in `related` and in correlated subquery conditions)
 * are already normalized and that the `related` entries have the shape that
 * {@link normalizedRelated} gives them.
 *
 * This lets a query builder keep its AST normalized as it builds it up
 * instead of normalizing the whole tree afterwards. A builder that knows
 * which of the rules a step can break at all is better off applying just
 * those (see {@link tableAST}, {@link insertRelated} and
 * {@link normalizeCondition}).
 */
export function normalizedAST(ast: AST): NormalizedAST {
  const {schema, table, alias, where, related, start, limit, orderBy} = ast;
  // Every field is written, in the same order, so that all normalized ASTs
  // have the same shape (one hidden class) and the same JSON encoding (and
  // thus the same hash) no matter how they were put together. The fields that
  // are undefined are dropped by JSON.stringify() anyway.
  return asNormalized({
    schema,
    table,
    alias,
    where: where === undefined ? undefined : normalizeCondition(where),
    related: related === undefined ? undefined : normalizeRelated(related),
    start,
    limit,
    orderBy,
  });
}

/**
 * The normalized AST that selects a whole table. Spreading it (or any other
 * normalized AST) and replacing a field keeps the field order, since a
 * normalized AST has every field.
 */
export function tableAST(
  table: string,
  alias?: string | undefined,
): NormalizedAST {
  return asNormalized({
    schema: undefined,
    table,
    alias,
    where: undefined,
    related: undefined,
    start: undefined,
    limit: undefined,
    orderBy: undefined,
  });
}

/**
 * Adds `subquery` to the `related` of a normalized AST, at the position that
 * keeps it sorted.
 */
export function insertRelated(
  related: readonly CorrelatedSubquery[] | undefined,
  subquery: CorrelatedSubquery,
): readonly CorrelatedSubquery[] {
  const normalized = normalizedRelated(subquery);
  if (related === undefined) {
    return [normalized];
  }
  // Sorting is stable, so an entry goes after the ones it compares equal to.
  let i = related.length;
  while (i > 0 && cmpRelated(related[i - 1], normalized) > 0) {
    i--;
  }
  return related.toSpliced(i, 0, normalized);
}

// Conjunctions and disjunctions that are known to be normalized. The query
// builder rebuilds its AST for every step, so this keeps the conditions that
// did not change from being normalized over and over again.
const normalizedConditions = new WeakSet<Condition>();

function normalizeRelated(
  related: readonly CorrelatedSubquery[],
): readonly CorrelatedSubquery[] {
  for (let i = 1; i < related.length; i++) {
    if (cmpRelated(related[i - 1], related[i]) > 0) {
      return related.toSorted(cmpRelated);
    }
  }
  return related;
}

/**
 * Gives a correlated subquery the shape that a normalized AST has: every
 * field, in a fixed order (see {@link normalizedAST}). Its `subquery` must
 * already be normalized.
 */
export function normalizedRelated(
  related: CorrelatedSubquery,
): Required<CorrelatedSubquery> {
  const {correlation, hidden, subquery, system} = related;
  return {
    correlation: {
      parentField: correlation.parentField,
      childField: correlation.childField,
    },
    hidden,
    subquery,
    system,
  };
}

function normalizeValuePosition<T extends ValuePosition>(v: T): T {
  switch (v.type) {
    case 'literal':
      return {type: 'literal', value: v.value} as T;
    case 'column':
      return {type: 'column', name: v.name} as T;
    case 'json':
      return {
        type: 'json',
        value: normalizeValuePosition(v.value),
        path: v.path,
      } as T;
    default:
      return {type: 'static', anchor: v.anchor, field: v.field} as T;
  }
}

/**
 * Normalizes a condition, assuming that the ASTs of any correlated subqueries
 * it contains are already normalized.
 *
 * Like {@link normalizedAST}, this rebuilds every node with its fields in a
 * fixed order, so that structurally equal conditions have the same JSON
 * encoding (and thus the same hash) no matter how they were put together.
 * That includes a round trip through Postgres, whose jsonb type reorders
 * object keys: without the rebuild, every query record reloaded from the CVR
 * re-hashed to a different transformationHash than was stored, making it look
 * changed and re-execute on every view-syncer restart.
 */
export function normalizeCondition(cond: Condition): Condition {
  if (normalizedConditions.has(cond)) {
    return cond;
  }
  if (cond.type === 'simple') {
    const normalized: Condition = {
      type: 'simple',
      op: cond.op,
      left: normalizeValuePosition(cond.left),
      right: normalizeValuePosition(cond.right),
    };
    normalizedConditions.add(normalized);
    return normalized;
  }
  if (cond.type === 'correlatedSubquery') {
    const normalized: Condition = {
      type: 'correlatedSubquery',
      related: normalizedRelated(cond.related),
      op: cond.op,
      flip: cond.flip,
      scalar: cond.scalar,
    };
    normalizedConditions.add(normalized);
    return normalized;
  }

  // Flatten the conditions of nested conjunctions of the same type and sort
  // them. The nested conditions are normalized (and thus flattened and sorted)
  // first.
  const conditions: Condition[] = [];
  for (const c of cond.conditions) {
    const n = normalizeCondition(c);
    if (n.type === cond.type) {
      conditions.push(...n.conditions);
    } else {
      conditions.push(n);
    }
  }

  // A singleton conjunction is the same as the condition itself. Empty
  // conjunctions, on the other hand, are meaningful: an empty 'and' is always
  // true and an empty 'or' is always false.
  if (conditions.length === 1) {
    return conditions[0];
  }
  conditions.sort(cmpCondition);
  const normalized: Condition = {type: cond.type, conditions};
  normalizedConditions.add(normalized);
  return normalized;
}

/**
 * Maps the table and column names of `ast`. The result is not a
 * {@link NormalizedAST} even if `ast` was normalized: the names it sorts by
 * are the ones that changed.
 */
export function mapAST(ast: AST, mapper: NameMapper) {
  return transformAST(ast, {
    tableName: table => mapper.tableName(table),
    columnName: (table, col) => mapper.columnName(table, col),
    jsonColumnName: (table, col) => mapper.jsonColumnName(table, col),
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
    jsonColumnName: (table, col) => mapper.jsonColumnName(table, col),
  });
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
    return (
      cmpRelated(a.related, b.related) ||
      compareUTF8MaybeNull(a.op, b.op) ||
      cmpOptionalBool(a.flip, b.flip) ||
      cmpOptionalBool(a.scalar, b.scalar)
    );
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

function compareValuePosition(a: ValuePosition, b: ValuePosition): number {
  if (a.type !== b.type) {
    return compareUTF8(a.type, b.type);
  }
  switch (a.type) {
    case 'literal':
      assert(b.type === 'literal', 'Expected literal type for comparison');
      return cmpLiteralValue(a.value, b.value);
    case 'column':
      assert(b.type === 'column', 'Expected column type for comparison');
      return compareUTF8(a.name, b.name);
    case 'json':
      assert(b.type === 'json', 'Expected json type for comparison');
      return (
        compareValuePosition(a.value, b.value) ||
        compareUTF8(JSON.stringify(a.path), JSON.stringify(b.path))
      );
    case 'static':
      assert(b.type === 'static', 'Expected static type for comparison');
      // A field is a string or a path of strings, which cmpLiteralValue()
      // tells apart.
      return (
        compareUTF8(a.anchor, b.anchor) || cmpLiteralValue(a.field, b.field)
      );
  }
}

// null < boolean < number < string < array. Ordering values of different
// types by their string form would make distinct values compare equal (['a',
// 'b'] and 'a,b', or 1 and '1'), which would leave their order up to the
// order they were written in, and with it the hash of the query.
const LITERAL_VALUE_TYPES = ['object', 'boolean', 'number', 'string'];

function literalValueType(value: LiteralValue): number {
  return Array.isArray(value)
    ? LITERAL_VALUE_TYPES.length
    : LITERAL_VALUE_TYPES.indexOf(typeof value);
}

function cmpLiteralValue(a: LiteralValue, b: LiteralValue): number {
  const type = literalValueType(a) - literalValueType(b);
  if (type !== 0) {
    return type;
  }

  if (Array.isArray(a)) {
    assert(Array.isArray(b), 'Expected array for comparison');
    for (let i = 0; i < a.length && i < b.length; i++) {
      const val = cmpLiteralValue(a[i], b[i]);
      if (val !== 0) {
        return val;
      }
    }
    // prefixes first
    return a.length - b.length;
  }

  switch (typeof a) {
    case 'string':
      assert(typeof b === 'string', 'Expected string for comparison');
      return compareUTF8(a, b);
    case 'number':
      assert(typeof b === 'number', 'Expected number for comparison');
      return a < b ? -1 : a > b ? 1 : 0;
    case 'boolean':
      assert(typeof b === 'boolean', 'Expected boolean for comparison');
      return Number(a) - Number(b);
    default:
      return 0; // null
  }
}

function cmpRelated(a: CorrelatedSubquery, b: CorrelatedSubquery): number {
  return compareUTF8(must(a.subquery.alias), must(b.subquery.alias));
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

function cmpOptionalBool(
  a: boolean | undefined,
  b: boolean | undefined,
): number {
  // undefined < false < true
  const toNum = (v: boolean | undefined) => (v === undefined ? 0 : v ? 2 : 1);
  return toNum(a) - toNum(b);
}
