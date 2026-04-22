/**
 * Wire-format representation of the zql AST interface.
 *
 * `v.Type<...>` types are explicitly declared to facilitate Typescript verification
 * that the schemas satisfy the zql type definitions. (Incidentally, explicit types
 * are also required for recursive schema definitions.)
 */

import {compareUTF8} from 'compare-utf8';
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
  | CorrelatedSubqueryCondition;

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
  scalar?: boolean | undefined;
  [planIdSymbol]?: number | undefined;
};

export type CorrelatedSubqueryConditionOperator = 'EXISTS' | 'NOT EXISTS';

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
  where: simplifyCondition,
  conditions: c => c,
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
      return compareUTF8(String(a.value), String(b.value));
    case 'column':
      assert(b.type === 'column', 'Expected column type for comparison');
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
 * Canonicalizes a boolean condition tree: flattens nested same-type compounds,
 * sorts siblings, dedups, consolidates `col = x OR col = y` to `col IN (...)`,
 * applies absorption, and unwraps empty/singleton compounds. Idempotent.
 */
export function simplifyCondition(cond: Condition): Condition | undefined {
  if (cond.type === 'simple' || cond.type === 'correlatedSubquery') {
    return cond;
  }

  // Bottom-up: simplify each child first, then flatten same-type children
  // into this level.
  const simplifiedChildren: Condition[] = [];
  for (const c of cond.conditions) {
    const s = simplifyCondition(c);
    if (s === undefined) {
      continue;
    }
    if (s.type === cond.type) {
      simplifiedChildren.push(...s.conditions);
    } else {
      simplifiedChildren.push(s);
    }
  }

  // Dedup via canonical-key Set (O(s) rather than sort+adjacent scan).
  let siblingKeys = new Set<string>();
  let children: Condition[] = [];
  for (const c of simplifiedChildren) {
    const k = conditionKey(c);
    if (!siblingKeys.has(k)) {
      siblingKeys.add(k);
      children.push(c);
    }
  }

  children.sort(cmpCondition);

  if (cond.type === 'or') {
    const consolidated = consolidateEqualsToIn(children);
    if (consolidated) {
      children = consolidated;
      children.sort(cmpCondition);
      siblingKeys = new Set<string>();
      for (const c of children) siblingKeys.add(conditionKey(c));
    }
  }

  const absorbed = absorb(cond.type, children, siblingKeys);
  if (absorbed) {
    children = absorbed;
  }

  switch (children.length) {
    case 0:
      return undefined;
    case 1:
      return children[0];
    default:
      return {
        type: cond.type,
        conditions: children,
      };
  }
}

// Only `col = <non-null literal>` and `col IN [literal, ...]` branches are
// eligible; `col = NULL` uses `IS` in ZQL and is intentionally skipped.
// Returns undefined if no consolidation was applied.
function consolidateEqualsToIn(
  conds: readonly Condition[],
): Condition[] | undefined {
  type Group = {
    values: (string | number | boolean)[];
    indices: number[];
  };
  const byColumn = new Map<string, Group>();

  for (let i = 0; i < conds.length; i++) {
    const c = conds[i];
    if (c.type !== 'simple') continue;
    if (c.left.type !== 'column') continue;
    if (c.right.type !== 'literal') continue;

    const right = c.right.value;
    const isArr = Array.isArray(right);
    if (c.op === '=') {
      if (right === null || isArr) continue;
    } else if (c.op === 'IN') {
      if (!isArr) continue;
    } else {
      continue;
    }

    let group = byColumn.get(c.left.name);
    if (!group) {
      group = {values: [], indices: []};
      byColumn.set(c.left.name, group);
    }
    if (isArr) {
      group.values.push(...(right as readonly (string | number | boolean)[]));
    } else {
      group.values.push(right as string | number | boolean);
    }
    group.indices.push(i);
  }

  // Find groups that actually need consolidation (more than one eligible
  // branch for the same column).
  const replacements = new Map<number, Condition | null>();
  for (const [colName, group] of byColumn) {
    if (group.indices.length < 2) continue;

    const uniqueSorted = dedupAndSortLiterals(group.values);
    const replacement: SimpleCondition = {
      type: 'simple',
      op: 'IN',
      left: {type: 'column', name: colName},
      right: {type: 'literal', value: uniqueSorted},
    };
    replacements.set(group.indices[0], replacement);
    for (let k = 1; k < group.indices.length; k++) {
      replacements.set(group.indices[k], null);
    }
  }

  if (replacements.size === 0) {
    return undefined;
  }

  const result: Condition[] = [];
  for (let i = 0; i < conds.length; i++) {
    const r = replacements.get(i);
    if (r === undefined) {
      result.push(conds[i]);
    } else if (r !== null) {
      result.push(r);
    }
  }
  return result;
}

// Sort by string coercion for deterministic order across mixed-type arrays.
function dedupAndSortLiterals(
  values: readonly (string | number | boolean)[],
): readonly (string | number | boolean)[] {
  const unique = [...new Set(values)];
  unique.sort((a, b) => compareUTF8(String(a), String(b)));
  return unique;
}

// Returns undefined if nothing was absorbed. Linear in total children size:
// for each opposite-type compound child, we hash its sub-condition keys and
// look them up in the pre-built sibling key set.
function absorb(
  parentType: 'and' | 'or',
  children: readonly Condition[],
  siblingKeys: ReadonlySet<string>,
): Condition[] | undefined {
  const oppositeType = parentType === 'and' ? 'or' : 'and';
  let result: Condition[] | undefined;
  for (let i = 0; i < children.length; i++) {
    const child = children[i];
    let drop = false;
    if (child.type === oppositeType) {
      for (const sub of child.conditions) {
        if (siblingKeys.has(conditionKey(sub))) {
          drop = true;
          break;
        }
      }
    }
    if (drop) {
      if (!result) result = children.slice(0, i);
    } else if (result) {
      result.push(child);
    }
  }
  return result;
}

// Canonical structural key. Stable for any two conditions that are
// structurally equal after simplification (children are already sorted at
// compound nodes). JSON.stringify on strings/literals handles escaping so
// separators inside values can't collide with delimiters.
function conditionKey(c: Condition): string {
  switch (c.type) {
    case 'simple':
      return `s|${valueKey(c.left)}|${c.op}|${valueKey(c.right)}`;
    case 'correlatedSubquery':
      return `cs|${JSON.stringify(c.related.subquery.alias ?? '')}|${c.op}|${c.flip ?? 0}|${c.scalar ?? 0}`;
    case 'and':
    case 'or':
      return `${c.type}[${c.conditions.map(conditionKey).join(',')}]`;
  }
}

function valueKey(v: ValuePosition): string {
  switch (v.type) {
    case 'column':
      return `col|${JSON.stringify(v.name)}`;
    case 'literal':
      return `lit|${JSON.stringify(v.value)}`;
    case 'static':
      return `stc|${v.anchor}|${JSON.stringify(v.field)}`;
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

function cmpOptionalBool(
  a: boolean | undefined,
  b: boolean | undefined,
): number {
  // undefined < false < true
  const toNum = (v: boolean | undefined) => (v === undefined ? 0 : v ? 2 : 1);
  return toNum(a) - toNum(b);
}
