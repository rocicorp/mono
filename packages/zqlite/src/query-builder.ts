import type {SQLQuery} from '@databases/sql';
import {assert, unreachable} from '../../shared/src/asserts.ts';
import {
  isLikeOperator,
  isNegatedOperator,
  jsonLiteralType,
  type Condition,
  type JsonPathReference,
  type Ordering,
  type SimpleCondition,
  type ValuePosition,
} from '../../zero-protocol/src/ast.ts';
import type {
  SchemaValue,
  ValueType,
} from '../../zero-schema/src/table-schema.ts';
import type {Constraint} from '../../zql/src/ivm/constraint.ts';
import type {MultiConstraint, Start} from '../../zql/src/ivm/operator.ts';
import {sql} from './internal/sql.ts';

/**
 * Condition type without correlated subqueries.
 * This matches the output of transformFilters from zql/builder/filter.ts
 */
export type NoSubqueryCondition = Exclude<
  Condition,
  {type: 'correlatedSubquery'}
>;

export function buildSelectQuery(
  tableName: string,
  columns: Record<string, SchemaValue>,
  constraint: Constraint | undefined,
  filters: NoSubqueryCondition | undefined,
  order: Ordering | undefined,
  reverse: boolean | undefined,
  start: Start | undefined,
  multiConstraints?: readonly MultiConstraint[] | undefined,
  fetchFilters?: NoSubqueryCondition | undefined,
) {
  let query = sql`SELECT ${sql.join(
    Object.keys(columns).map(c => sql.ident(c)),
    sql`,`,
  )} FROM ${sql.ident(tableName)}`;
  const constraints: SQLQuery[] = constraintsToSQL(constraint, columns);

  if (multiConstraints) {
    for (const mc of multiConstraints) {
      if (mc.length > 0) {
        constraints.push(multiConstraintToSQL(mc, columns));
      }
    }
  }

  if (start) {
    assert(order !== undefined, 'start requires ordering');
    constraints.push(gatherStartConstraints(start, reverse, order, columns));
  }

  if (filters) {
    constraints.push(filtersToSQL(filters));
  }

  if (fetchFilters) {
    constraints.push(filtersToSQL(fetchFilters));
  }

  if (constraints.length > 0) {
    query = sql`${query} WHERE ${sql.join(constraints, sql` AND `)}`;
  }

  if (order && order.length > 0) {
    return sql`${query} ${orderByToSQL(order, !!reverse)}`;
  }
  return query;
}

export function constraintsToSQL(
  constraint: Constraint | undefined,
  columns: Record<string, SchemaValue>,
) {
  if (!constraint) {
    return [];
  }

  const constraints: SQLQuery[] = [];
  for (const [key, value] of Object.entries(constraint)) {
    constraints.push(
      sql`${sql.ident(key)} = ${toSQLiteType(value, columns[key].type)}`,
    );
  }

  return constraints;
}

/**
 * Builds a single batched IN clause from a `MultiConstraint`. All entries
 * are assumed to share the same shape (the keys of the first entry);
 * FlippedJoin derives them from the same parentKey for all children.
 *
 * Single-column form: `col IN (?, ?, ?)`
 * Compound form:      `(a, b) IN (VALUES (?, ?), (?, ?), …)`
 *
 * NOTE: SQLite optimizes `col IN (literal-list)` using the column's index;
 * verified via EXPLAIN QUERY PLAN — see query-builder.test.ts.
 */
export function multiConstraintToSQL(
  multiConstraint: MultiConstraint,
  columns: Record<string, SchemaValue>,
): SQLQuery {
  assert(multiConstraint.length > 0, 'multiConstraint must be non-empty');
  // All entries share the same keys; pull the column list from the first.
  const keys = Object.keys(multiConstraint[0]);
  assert(keys.length > 0, 'multiConstraint entries must have at least one key');
  // Subsequent entries must share the first entry's shape — the SQL form
  // is `(col_a, col_b, …) IN VALUES (…)`, with one binding per key per
  // entry. Heterogeneous keys would silently produce incorrect bindings.
  for (let i = 1; i < multiConstraint.length; i++) {
    const entry = multiConstraint[i];
    assert(
      Object.keys(entry).length === keys.length && keys.every(k => k in entry),
      () =>
        `multiConstraint entries must share the same keys (entry 0: [${keys.join(
          ',',
        )}], entry ${i}: [${Object.keys(entry).join(',')}])`,
    );
  }

  if (keys.length === 1) {
    const key = keys[0];
    const colType = columns[key].type;
    return sql`${sql.ident(key)} IN (${sql.join(
      multiConstraint.map(c => sql`${toSQLiteType(c[key], colType)}`),
      sql`,`,
    )})`;
  }

  // Compound: `(col_a, col_b, …) IN (VALUES (?, ?, …), …)`
  const colList = sql`(${sql.join(
    keys.map(k => sql.ident(k)),
    sql`,`,
  )})`;
  const rows = multiConstraint.map(
    c =>
      sql`(${sql.join(
        keys.map(k => sql`${toSQLiteType(c[k], columns[k].type)}`),
        sql`,`,
      )})`,
  );
  return sql`${colList} IN (VALUES ${sql.join(rows, sql`,`)})`;
}

export function orderByToSQL(order: Ordering, reverse: boolean): SQLQuery {
  if (reverse) {
    return sql`ORDER BY ${sql.join(
      order.map(
        s =>
          sql`${sql.ident(s[0])} ${sql.__dangerous__rawValue(
            s[1] === 'asc' ? 'desc' : 'asc',
          )}`,
      ),
      sql`, `,
    )}`;
  } else {
    return sql`ORDER BY ${sql.join(
      order.map(
        s => sql`${sql.ident(s[0])} ${sql.__dangerous__rawValue(s[1])}`,
      ),
      sql`, `,
    )}`;
  }
}

/**
 * Converts filters (conditions) to SQL WHERE clause.
 * This applies all filters present in the AST for a query to the source.
 */
export function filtersToSQL(filters: NoSubqueryCondition): SQLQuery {
  switch (filters.type) {
    case 'simple':
      return simpleConditionToSQL(filters);
    case 'and':
      return filters.conditions.length > 0
        ? sql`(${sql.join(
            filters.conditions.map(condition =>
              filtersToSQL(condition as NoSubqueryCondition),
            ),
            sql` AND `,
          )})`
        : sql`TRUE`;
    case 'or':
      return filters.conditions.length > 0
        ? sql`(${sql.join(
            filters.conditions.map(condition =>
              filtersToSQL(condition as NoSubqueryCondition),
            ),
            sql` OR `,
          )})`
        : sql`FALSE`;
  }
}

function simpleConditionToSQL(filter: SimpleCondition): SQLQuery {
  if (filter.left.type === 'json') {
    const json = jsonPathConditionToSQL(filter, filter.left);
    if (json) {
      return json;
    }
  }
  return comparisonToSQL(filter, valuePositionToSQL(filter.left));
}

/**
 * Renders `filter`'s operator and right operand against an already-rendered
 * left operand.
 */
function comparisonToSQL(filter: SimpleCondition, left: SQLQuery): SQLQuery {
  const {op} = filter;
  if (op === 'IN' || op === 'NOT IN') {
    switch (filter.right.type) {
      case 'literal':
        return sql`${left} ${sql.__dangerous__rawValue(
          filter.op,
        )} (SELECT value FROM json_each(${JSON.stringify(
          filter.right.value,
        )}))`;
      case 'static':
        throw new Error(
          'Static parameters must be replaced before conversion to SQL',
        );
    }
  }
  if (
    op === 'LIKE' ||
    op === 'NOT LIKE' ||
    op === 'ILIKE' ||
    op === 'NOT ILIKE'
  ) {
    return likeConditionToSQL(filter, left);
  }

  if (
    (op === 'IS' || op === 'IS NOT') &&
    filter.right.type === 'literal' &&
    filter.right.value === null
  ) {
    return sql`${left} ${sql.__dangerous__rawValue(op)} NULL`;
  }

  return sql`${left} ${sql.__dangerous__rawValue(
    filter.op,
  )} ${valuePositionToSQL(filter.right)}`;
}

function likeConditionToSQL(filter: SimpleCondition, left: SQLQuery): SQLQuery {
  const {op} = filter;
  // Mirror Postgres pattern-matching semantics:
  //  * LIKE is case-sensitive. The replica connection runs with
  //    `PRAGMA case_sensitive_like = ON` (see db.ts), so the bare LIKE
  //    operator is case-sensitive.
  //  * ILIKE is case-insensitive. We lower() both operands using the
  //    Unicode-aware lower() that @rocicorp/zero-sqlite3 provides via ICU,
  //    mirroring the toLowerCase() used by the in-memory IVM matcher
  //    (see zql/src/builder/like.ts).
  //  * Backslash is the default escape character in Postgres and in the IVM
  //    matcher, but SQLite has no default, so we specify `ESCAPE '\'`
  //    explicitly. The SQL literal '\' is a single backslash (SQLite does not
  //    process backslash escapes inside string literals).
  const caseInsensitive = op === 'ILIKE' || op === 'NOT ILIKE';
  const negated = op === 'NOT LIKE' || op === 'NOT ILIKE';
  const likeOp = sql.__dangerous__rawValue(negated ? 'NOT LIKE' : 'LIKE');

  const right = valuePositionToSQL(filter.right);
  if (caseInsensitive) {
    return sql`lower(${left}) ${likeOp} lower(${right}) ESCAPE '\\'`;
  }
  return sql`${left} ${likeOp} ${right} ESCAPE '\\'`;
}

/** The extraction of a JSON path leaf (the path string is a bound parameter). */
function jsonExtract(ref: JsonPathReference): SQLQuery {
  return sql`json_extract(${sql.ident(ref.value.name)}, ${jsonPathExpr(
    ref.path,
  )})`;
}

/** The SQLite `json_type()` names a leaf may have for a JSON literal type. */
function jsonTypeNames(
  t: 'string' | 'number' | 'boolean',
): readonly [string, ...string[]] {
  switch (t) {
    case 'string':
      return ['text'];
    case 'number':
      return ['integer', 'real'];
    case 'boolean':
      return ['true', 'false'];
  }
}

/**
 * Compiles a comparison whose left operand is a JSON path, type-strictly, per
 * the semantics documented on {@link JsonPathReference} (shared with the
 * in-memory predicate and the Postgres compiler). Returns `undefined` where
 * the generic rendering already agrees with the predicate.
 *
 * A bare `json_extract` would not be strict: SQLite returns booleans as 1/0 (so
 * `true` = 1), orders any TEXT above any number (`'n/a' > 5`), and coerces
 * numbers for LIKE. Gating the extraction on `json_type()` makes a mismatched
 * leaf NULL, which a positive comparison excludes. Negated operators get an
 * explicit CASE because NULL would exclude there too, where a mismatch must
 * match: `json_type()` is NULL for a missing key and `'null'` for a JSON null,
 * so one `COALESCE` covers the null guard without a second extraction.
 *
 * `IN`/`NOT IN` with a `null` literal is constant-false (as in the predicate),
 * and an empty `NOT IN` matches every non-null leaf (bare `NULL NOT IN ()` is
 * TRUE).
 */
function jsonPathConditionToSQL(
  filter: SimpleCondition,
  left: JsonPathReference,
): SQLQuery | undefined {
  const {op, right} = filter;
  if (right.type !== 'literal') {
    return undefined;
  }
  if ((op === 'IN' || op === 'NOT IN') && right.value === null) {
    return sql`FALSE`;
  }
  const raw = jsonExtract(left);
  let cond = filter;
  let t = jsonLiteralType(right.value);
  if (
    isLikeOperator(op) &&
    right.value !== null &&
    !Array.isArray(right.value)
  ) {
    // The LIKE family compares text, so the leaf must be a string whatever
    // the literal's type, and the pattern is the literal's text form — as in
    // the in-memory predicate (`String(pattern)`) and the Postgres compiler.
    // Gating on the literal's type instead would let SQLite's text coercion
    // match `3 LIKE 3` on a numeric leaf that the predicate rejects.
    t = 'string';
    cond = {...filter, right: {type: 'literal', value: String(right.value)}};
  }
  if (t === undefined) {
    // Only an empty list reaches here (a null literal has no type either, but
    // the generic NULL comparison is already constant-false for it).
    return op === 'NOT IN' ? sql`${raw} IS NOT NULL` : undefined;
  }
  const col = sql.ident(left.value.name);
  const path = jsonPathExpr(left.path);
  const types = jsonTypeNames(t);
  if (!isNegatedOperator(op)) {
    const typeList = sql.join(
      types.map(x => sql`${x}`),
      sql`, `,
    );
    return comparisonToSQL(
      cond,
      sql`(CASE WHEN json_type(${col}, ${path}) IN (${typeList}) THEN ${raw} END)`,
    );
  }
  // One WHEN per accepted type name (a number leaf is 'integer' or 'real');
  // only one branch runs per row.
  const cmp = comparisonToSQL(cond, raw);
  const whens = sql.join(
    types.map(x => sql`WHEN ${x} THEN ${cmp}`),
    sql` `,
  );
  return sql`(CASE COALESCE(json_type(${col}, ${path}), 'null') WHEN 'null' THEN 0 ${whens} ELSE 1 END)`;
}

/**
 * Builds a SQLite JSON path string (e.g. `$.a.b[0]`) from a path of object
 * keys and array indices. Object keys are emitted as JSON string literals
 * (`JSON.stringify`): SQLite parses a double-quoted path label with JSON
 * escapes, so `"` and `\` inside a key must be backslash-escaped — SQL-style
 * `""` doubling is not understood and yields NULL or a "bad JSON path" error.
 */
function jsonPathExpr(path: readonly (string | number)[]): string {
  let s = '$';
  for (const seg of path) {
    s += typeof seg === 'number' ? `[${seg}]` : `.${JSON.stringify(seg)}`;
  }
  return s;
}

function valuePositionToSQL(value: ValuePosition): SQLQuery {
  switch (value.type) {
    case 'column':
      return sql.ident(value.name);
    case 'json':
      return jsonExtract(value);
    case 'literal':
      return sql`${toSQLiteType(value.value, getJsType(value.value))}`;
    case 'static':
      throw new Error(
        'Static parameters must be replaced before conversion to SQL',
      );
    default:
      unreachable(value);
  }
}

function getJsType(value: unknown): ValueType {
  if (value === null) {
    return 'null';
  }
  return typeof value === 'string'
    ? 'string'
    : typeof value === 'number'
      ? 'number'
      : typeof value === 'boolean'
        ? 'boolean'
        : 'json';
}

export function toSQLiteType(v: unknown, type: ValueType): unknown {
  switch (type) {
    case 'boolean':
      return v === null ? null : v ? 1 : 0;
    case 'number':
    case 'string':
    case 'null':
      return v;
    case 'json':
      return JSON.stringify(v);
  }
}

function nullableAwareEquality(
  field: string,
  value: unknown,
  columnType: SchemaValue,
): SQLQuery {
  if (value === null) {
    // A NULL bound value proves the column is nullable regardless of the
    // column metadata, and `=` never matches NULL — `IS` selects the NULL
    // tie-break group a cursor anchored on a NULL value needs.
    return sql`${sql.ident(field)} IS NULL`;
  }
  // Use = instead of IS for non-nullable columns to enable better
  // index usage in SQLite.
  return columnType.optional === true
    ? sql`${sql.ident(field)} IS ${value}`
    : sql`${sql.ident(field)} = ${value}`;
}

function nullableAwareRangeComparison(
  field: string,
  value: unknown,
  operator: '>' | '<',
  columnType: SchemaValue,
): SQLQuery {
  if (value === null) {
    return operator === '>' ? sql`${sql.ident(field)} IS NOT NULL` : sql`FALSE`;
  }

  // For non-nullable columns, skip IS NULL checks to avoid breaking
  // SQLite's MULTI-INDEX OR optimization, which falls back to a full
  // table scan when any OR branch involves NULL.
  // See: https://github.com/rocicorp/mono/pull/5542
  const comparison = sql`${sql.ident(field)} ${sql.__dangerous__rawValue(
    operator,
  )} ${value}`;
  if (columnType.optional !== true) {
    return comparison;
  }

  // The bound is non-NULL here. NULLs sort before every non-NULL value, so
  // `>` already excludes them and needs no guard, while `<` must admit the
  // NULL group explicitly — a bare `col < ?` would silently drop NULL rows
  // from a backward walk.
  return operator === '>'
    ? comparison
    : sql`(${sql.ident(field)} IS NULL OR ${comparison})`;
}

function sargableLeadingStartBound(
  field: string,
  value: unknown,
  operator: '>' | '<',
  columnType: SchemaValue,
): SQLQuery | undefined {
  // A NULL bound value proves the column is nullable regardless of the
  // column metadata, and a bare range bound is not sound there: `col >= NULL`
  // is never true, so instead of being redundant it would annihilate the
  // whole start constraint. A nullable column also cannot use a `<` bound,
  // because the start constraint must retain the NULL group. For `>`, NULLs
  // sort before the non-NULL bound, so `col >= value` remains sound.
  if (value === null || (columnType.optional === true && operator === '<')) {
    return undefined;
  }

  const inclusiveOperator = operator === '>' ? '>=' : '<=';
  return sql`${sql.ident(field)} ${sql.__dangerous__rawValue(
    inclusiveOperator,
  )} ${value}`;
}

/**
 * The ordering could be complex such as:
 * `ORDER BY a ASC, b DESC, c ASC`
 *
 * In those cases, we need to encode the constraints as various
 * `OR` clauses.
 *
 * E.g.,
 *
 * to get the row after (a = 1, b = 2, c = 3) would be:
 *
 * `WHERE a > 1 OR (a = 1 AND b < 2) OR (a = 1 AND b = 2 AND c > 3)`
 *
 * - after vs before flips the comparison operators.
 * - inclusive adds a final `OR` clause for the exact match.
 */
function gatherStartConstraints(
  start: Start,
  reverse: boolean | undefined,
  order: Ordering,
  columnTypes: Record<string, SchemaValue>,
): SQLQuery {
  const constraints: SQLQuery[] = [];
  const {row: from, basis} = start;
  let leadingBound: SQLQuery | undefined;

  for (let i = 0; i < order.length; i++) {
    const group: SQLQuery[] = [];
    const [iField, iDirection] = order[i];
    for (let j = 0; j <= i; j++) {
      if (j === i) {
        const columnType = columnTypes[iField];
        const constraintValue = toSQLiteType(
          from[iField] ?? null,
          columnType.type,
        );
        const operator =
          iDirection === 'asc' ? (reverse ? '<' : '>') : reverse ? '>' : '<';
        if (i === 0) {
          leadingBound = sargableLeadingStartBound(
            iField,
            constraintValue,
            operator,
            columnType,
          );
        }
        group.push(
          nullableAwareRangeComparison(
            iField,
            constraintValue,
            operator,
            columnType,
          ),
        );
      } else {
        const [jField] = order[j];
        const columnType = columnTypes[jField];
        const value = toSQLiteType(from[jField] ?? null, columnType.type);
        group.push(nullableAwareEquality(jField, value, columnType));
      }
    }
    constraints.push(sql`(${sql.join(group, sql` AND `)})`);
  }

  if (basis === 'at') {
    constraints.push(
      sql`(${sql.join(
        order.map(([field]) => {
          const columnType = columnTypes[field];
          const value = toSQLiteType(from[field] ?? null, columnType.type);
          return nullableAwareEquality(field, value, columnType);
        }),
        sql` AND `,
      )})`,
    );
  }

  const lexicographicStart = sql`(${sql.join(constraints, sql` OR `)})`;
  return leadingBound === undefined
    ? lexicographicStart
    : sql`(${leadingBound} AND ${lexicographicStart})`;
}
