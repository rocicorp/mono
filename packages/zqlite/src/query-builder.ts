import type {SQLQuery} from '@databases/sql';
import {assert} from '../../shared/src/asserts.ts';
import type {
  Condition,
  Ordering,
  SimpleCondition,
  ValuePosition,
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

/**
 * Builds the SELECTs for a fetch. Running them in order and concatenating
 * their rows gives the fetch's rows, in order.
 *
 * There is a second SELECT only when the fetch starts at a non-NULL value of a
 * nullable leading sort column and walks towards its NULL group. The first
 * SELECT then leaves that group out so that SQLite can seek it (see
 * `gatherStartConstraints`), and the second fetches the group. NULLs sort
 * before every other value, so the group follows all of the first SELECT's
 * rows, and a fetch that stops early never needs to run the second.
 */
export function buildSelectQueries(
  tableName: string,
  columns: Record<string, SchemaValue>,
  constraint: Constraint | undefined,
  filters: NoSubqueryCondition | undefined,
  order: Ordering | undefined,
  reverse: boolean | undefined,
  start: Start | undefined,
  multiConstraints?: readonly MultiConstraint[] | undefined,
  fetchFilters?: NoSubqueryCondition | undefined,
): [SQLQuery] | [SQLQuery, SQLQuery] {
  const select = sql`SELECT ${sql.join(
    Object.keys(columns).map(c => sql.ident(c)),
    sql`,`,
  )} FROM ${sql.ident(tableName)}`;
  const leading: SQLQuery[] = constraintsToSQL(constraint, columns);

  if (multiConstraints) {
    for (const mc of multiConstraints) {
      if (mc.length > 0) {
        leading.push(multiConstraintToSQL(mc, columns));
      }
    }
  }

  const trailing: SQLQuery[] = [];
  if (filters) {
    trailing.push(filtersToSQL(filters));
  }

  if (fetchFilters) {
    trailing.push(filtersToSQL(fetchFilters));
  }

  const orderBy =
    order && order.length > 0 ? orderByToSQL(order, !!reverse) : undefined;
  const selectWhere = (constraints: SQLQuery[]) => {
    const query =
      constraints.length > 0
        ? sql`${select} WHERE ${sql.join(constraints, sql` AND `)}`
        : select;
    return orderBy ? sql`${query} ${orderBy}` : query;
  };

  if (!start) {
    return [selectWhere([...leading, ...trailing])];
  }

  assert(order !== undefined, 'start requires ordering');
  const {constraint: startConstraint, excludesNullGroup} =
    gatherStartConstraints(start, reverse, order, columns);
  const query = selectWhere([...leading, startConstraint, ...trailing]);
  const [leadingField] = order[0];
  if (
    !excludesNullGroup ||
    // The rest of the fetch would reject every row of the NULL group anyway.
    constraintsRejectNull(constraint, multiConstraints, leadingField) ||
    rejectsNull(filters, leadingField) ||
    rejectsNull(fetchFilters, leadingField)
  ) {
    return [query];
  }
  return [
    query,
    selectWhere([
      ...leading,
      sql`${sql.ident(leadingField)} IS NULL`,
      ...trailing,
    ]),
  ];
}

/**
 * {@link buildSelectQueries} for a fetch that needs only one SELECT, such as
 * one without a start row.
 */
export function buildSelectQuery(
  ...args: Parameters<typeof buildSelectQueries>
): SQLQuery {
  const [query, nullGroup] = buildSelectQueries(...args);
  assert(
    nullGroup === undefined,
    'The fetch needs a second SELECT for its NULL group; use buildSelectQueries',
  );
  return query;
}

/**
 * Whether `condition` is never true for a row whose `field` is NULL. Only a
 * comparison of `field` itself, alone or ANDed with other conditions, is
 * recognized. Every operator yields NULL for a NULL operand except `IS`,
 * `IS NOT` and `NOT IN`, which is true for an empty list.
 */
function rejectsNull(
  condition: NoSubqueryCondition | undefined,
  field: string,
): boolean {
  switch (condition?.type) {
    case undefined:
    case 'or':
      return false;
    case 'and':
      return condition.conditions.some(c =>
        rejectsNull(c as NoSubqueryCondition, field),
      );
    case 'simple': {
      const {op, left, right} = condition;
      if (left.type !== 'column' || left.name !== field) {
        return false;
      }
      switch (op) {
        case 'IS':
          return right.type === 'literal' && right.value !== null;
        case 'IS NOT':
          return right.type === 'literal' && right.value === null;
        case 'NOT IN':
          return false;
        default:
          return true;
      }
    }
  }
}

/**
 * Whether the constraints of the fetch are never true for a row whose `field`
 * is NULL. {@link constraintsToSQL} compares the field with `=` and
 * {@link multiConstraintToSQL} with `IN`, neither of which is ever true for a
 * NULL operand, so the field being constrained at all is enough.
 */
function constraintsRejectNull(
  constraint: Constraint | undefined,
  multiConstraints: readonly MultiConstraint[] | undefined,
  field: string,
): boolean {
  if (constraint && field in constraint) {
    return true;
  }
  // `multiConstraintToSQL` takes its key list from the first entry and asserts
  // that the rest match it.
  return multiConstraints?.some(mc => mc.length > 0 && field in mc[0]) === true;
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
  const {op} = filter;
  if (op === 'IN' || op === 'NOT IN') {
    switch (filter.right.type) {
      case 'literal':
        return sql`${valuePositionToSQL(
          filter.left,
        )} ${sql.__dangerous__rawValue(
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
    return likeConditionToSQL(filter);
  }

  if (
    (op === 'IS' || op === 'IS NOT') &&
    filter.right.type === 'literal' &&
    filter.right.value === null
  ) {
    return sql`${valuePositionToSQL(filter.left)} ${sql.__dangerous__rawValue(
      op,
    )} NULL`;
  }

  return sql`${valuePositionToSQL(filter.left)} ${sql.__dangerous__rawValue(
    filter.op,
  )} ${valuePositionToSQL(filter.right)}`;
}

function likeConditionToSQL(filter: SimpleCondition): SQLQuery {
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

  const left = valuePositionToSQL(filter.left);
  const right = valuePositionToSQL(filter.right);
  if (caseInsensitive) {
    return sql`lower(${left}) ${likeOp} lower(${right}) ESCAPE '\\'`;
  }
  return sql`${left} ${likeOp} ${right} ESCAPE '\\'`;
}

function valuePositionToSQL(value: ValuePosition): SQLQuery {
  switch (value.type) {
    case 'column':
      return sql.ident(value.name);
    case 'literal':
      return sql`${toSQLiteType(value.value, getJsType(value.value))}`;
    case 'static':
      throw new Error(
        'Static parameters must be replaced before conversion to SQL',
      );
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
  /**
   * Whether an entailed bound ANDed in front of the disjunction already
   * excludes the column's NULL group, which a second SELECT then fetches (see
   * {@link gatherStartConstraints}).
   */
  nullGroupExcluded: boolean,
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
  // from a backward walk. Unless the NULL group is excluded and fetched on its
  // own: the guard is dead then, and a NULL inside an OR is what costs SQLite
  // the MULTI-INDEX OR optimization in the first place.
  return operator === '>' || nullGroupExcluded
    ? comparison
    : sql`(${sql.ident(field)} IS NULL OR ${comparison})`;
}

type SargableLeadingStartBound = {
  readonly bound: SQLQuery;
  /**
   * Whether `bound` excludes the column's NULL group, which belongs to the
   * rows the start walks over and must then be fetched separately.
   */
  readonly excludesNullGroup: boolean;
};

function sargableLeadingStartBound(
  field: string,
  value: unknown,
  operator: '>' | '<',
  columnType: SchemaValue,
): SargableLeadingStartBound | undefined {
  // A NULL bound value proves the column is nullable regardless of the
  // column metadata, and a bare range bound is not sound there: `col >= NULL`
  // is never true, so instead of being redundant it would annihilate the
  // whole start constraint.
  if (value === null) {
    return undefined;
  }

  const inclusiveOperator = operator === '>' ? '>=' : '<=';
  return {
    bound: sql`${sql.ident(field)} ${sql.__dangerous__rawValue(
      inclusiveOperator,
    )} ${value}`,
    // For `>`, NULLs sort before the non-NULL bound, so `col >= value` remains
    // sound on a nullable column. For `<`, the rows before the bound include
    // the whole NULL group, which `col <= value` drops. SQLite cannot seek
    // `col IS NULL OR col <= value` as one index range, so the caller fetches
    // that group separately.
    excludesNullGroup: columnType.optional === true && operator === '<',
  };
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
 *
 * SQLite cannot seek an index with that disjunction, so a redundant, entailed
 * bound on the leading column is ANDed in front of it (e.g. `a >= 1 AND ...`).
 *
 * When the leading column is nullable and the walk heads towards lower values
 * (`<`), the rows the start walks over include the whole NULL group, which
 * that bound drops. `excludesNullGroup` is set then, and the group is left out
 * of the disjunction too — every row the returned constraint admits has
 * `a <= ?`, so it and `a IS NULL` partition those rows, and the caller fetches
 * the NULL group with a second SELECT.
 */
function gatherStartConstraints(
  start: Start,
  reverse: boolean | undefined,
  order: Ordering,
  columnTypes: Record<string, SchemaValue>,
): {readonly constraint: SQLQuery; readonly excludesNullGroup: boolean} {
  const constraints: SQLQuery[] = [];
  const {row: from, basis} = start;
  let leadingBound: SargableLeadingStartBound | undefined;

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
        let nullGroupExcluded = false;
        if (i === 0) {
          leadingBound = sargableLeadingStartBound(
            iField,
            constraintValue,
            operator,
            columnType,
          );
          nullGroupExcluded = leadingBound?.excludesNullGroup === true;
        }
        group.push(
          nullableAwareRangeComparison(
            iField,
            constraintValue,
            operator,
            columnType,
            nullGroupExcluded,
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
  if (leadingBound === undefined) {
    return {constraint: lexicographicStart, excludesNullGroup: false};
  }
  return {
    constraint: sql`(${leadingBound.bound} AND ${lexicographicStart})`,
    excludesNullGroup: leadingBound.excludesNullGroup,
  };
}
