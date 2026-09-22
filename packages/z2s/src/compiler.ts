import type {SQLQuery} from '@databases/sql';
import {last, zip} from '../../shared/src/arrays.ts';
import {assert, unreachable} from '../../shared/src/asserts.ts';
import {
  parse as parseBigIntJson,
  type JSONValue as BigIntJSONValue,
} from '../../shared/src/bigint-json.ts';
import {hasOwn} from '../../shared/src/has-own.ts';
import {type JSONValue} from '../../shared/src/json.ts';
import {must} from '../../shared/src/must.ts';
import {
  isNegatedOperator,
  jsonLiteralType,
  type AST,
  type Bound,
  type Condition,
  type CorrelatedSubquery,
  type CorrelatedSubqueryCondition,
  type Correlation,
  type JsonPathReference,
  type LiteralReference,
  type Ordering,
  type SimpleCondition,
  type ValuePosition,
} from '../../zero-protocol/src/ast.ts';
import {
  clientToServer,
  type NameMapper,
} from '../../zero-schema/src/name-mapper.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {ServerSchema} from '../../zero-types/src/server-schema.ts';
import type {Format} from '../../zql/src/ivm/view.ts';
import {completeOrdering} from '../../zql/src/query/complete-ordering.ts';
import {
  pgTypeForLiteralType,
  sql,
  sqlConvertColumnArg,
  sqlConvertPluralLiteralArg,
  sqlConvertSingularLiteralArg,
  type PluralLiteralType,
} from './sql.ts';

type Table = {
  zql: string;
  alias: string;
};

type QualifiedColumn = {
  table: Table;
  zql: string;
};

type ServerSpec = {
  schema: ServerSchema;
  // maps zql names to server names
  mapper: NameMapper;
};

export type Spec = {
  server: ServerSpec;
  zql: Schema['tables'];
  aliasCount: number;
};

const ZQL_RESULT_KEY = 'zql_result';
const ZQL_RESULT_KEY_IDENT = sql.ident(ZQL_RESULT_KEY);

const ZQL_RESULT_TABLE_KEY = 'zql_root';
const ZQL_RESULT_TABLE_IDENT = sql.ident(ZQL_RESULT_TABLE_KEY);

export function compile(
  serverSchema: ServerSchema,
  zqlSchema: Schema,
  ast: AST,
  format?: Format,
): SQLQuery {
  ast = completeOrdering(
    ast,
    tableName => zqlSchema.tables[tableName].primaryKey,
  );
  const spec: Spec = {
    aliasCount: 0,
    server: {
      schema: serverSchema,
      mapper: clientToServer(zqlSchema.tables),
    },
    zql: zqlSchema.tables,
  };
  return sql`SELECT 
    ${toJSON(ZQL_RESULT_TABLE_KEY, format?.singular)}::text AS ${ZQL_RESULT_KEY_IDENT}
    FROM (${select(spec, ast, format)}) ${ZQL_RESULT_TABLE_IDENT}`;
}

function select(
  spec: Spec,
  ast: AST,
  format: Format | undefined,
  correlate?: (childTable: Table) => SQLQuery,
): SQLQuery {
  const table = makeTable(spec, ast.table);
  const selectionSet = related(spec, ast.related ?? [], format, table);
  const tableSchema = spec.zql[ast.table];
  const usedAliases = new Set<string>(
    ast.related?.map(r => r.subquery.alias ?? ''),
  );
  for (const column of Object.keys(tableSchema.columns)) {
    if (!usedAliases.has(column)) {
      selectionSet.push(
        selectIdent(spec.server, {
          table,
          zql: column,
        }),
      );
    }
  }

  let appliedWhere = false;
  function maybeWhere(test: unknown) {
    if (!test) {
      return sql``;
    }

    const ret = appliedWhere ? sql`AND` : sql`WHERE`;
    appliedWhere = true;
    return ret;
  }

  return sql`SELECT ${sql.join(selectionSet, ',')}
    FROM ${fromIdent(spec.server, table)}
    ${maybeWhere(ast.where)} ${where(spec, ast.where, table)}${
      ast.start
        ? sql`
    ${maybeWhere(ast.start)} ${start(spec, ast.start, ast.orderBy, table)}`
        : sql``
    }
    ${maybeWhere(correlate)} ${correlate ? correlate(table) : sql``}
    ${orderBy(spec, ast.orderBy, table)}
    ${limit(ast.limit, format?.singular)}`;
}

export function limit(
  limit: number | undefined,
  singular: boolean | undefined,
): SQLQuery {
  if (limit === 0) {
    return sql`LIMIT 0`;
  }
  if (singular) {
    return sql`LIMIT 1`;
  }
  if (limit === undefined) {
    return sql``;
  }
  return sql`LIMIT ${sqlConvertSingularLiteralArg(limit)}`;
}

function makeTable(spec: Spec, zql: string, alias?: string): Table {
  alias = alias ?? zql + '_' + spec.aliasCount++;
  return {
    zql,
    alias,
  };
}

export function orderBy(
  spec: Spec,
  orderBy: Ordering | undefined,
  table: Table,
): SQLQuery {
  if (!orderBy) {
    return sql``;
  }
  return sql`ORDER BY ${sql.join(
    orderBy.map(([col, dir]) =>
      dir === 'asc'
        ? // Oh postgres. The table must be referred to by client name but the column by server name.
          // E.g., `SELECT server_col as client_col FROM server_table as client_table ORDER BY client_Table.server_col`
          sql`${colIdent(spec.server, {
            table,
            zql: col,
          })} ASC NULLS FIRST`
        : sql`${colIdent(spec.server, {
            table,
            zql: col,
          })} DESC NULLS LAST`,
    ),
    ', ',
  )}`;
}

export function start(
  spec: Spec,
  bound: Bound | undefined,
  orderBy: Ordering | undefined,
  table: Table,
): SQLQuery {
  if (!bound) {
    return sql``;
  }
  assert(
    orderBy !== undefined && orderBy.length > 0,
    'start requires ordering',
  );

  const constraints: SQLQuery[] = [];
  for (let i = 0; i < orderBy.length; i++) {
    const group: SQLQuery[] = [];
    const [iField, iDirection] = orderBy[i];
    for (let j = 0; j <= i; j++) {
      const [field] = orderBy[j];
      if (j === i) {
        group.push(
          startRangeComparison(
            spec,
            table,
            iField,
            bound.row[iField] ?? null,
            iDirection === 'asc' ? '>' : '<',
          ),
        );
      } else {
        group.push(startEquality(spec, table, field, bound.row[field] ?? null));
      }
    }
    constraints.push(sql`(${sql.join(group, ' AND ')})`);
  }

  if (!bound.exclusive) {
    constraints.push(
      sql`(${sql.join(
        orderBy.map(([field]) =>
          startEquality(spec, table, field, bound.row[field] ?? null),
        ),
        ' AND ',
      )})`,
    );
  }

  return sql`(${sql.join(constraints, ' OR ')})`;
}

function related(
  spec: Spec,
  relationships: readonly CorrelatedSubquery[],
  format: Format | undefined,
  parentTable: Table,
): SQLQuery[] {
  return relationships.map(relationship =>
    relationshipSubquery(
      spec,
      relationship,
      format?.relationships[must(relationship.subquery.alias)],
      parentTable,
    ),
  );
}

function relationshipSubquery(
  spec: Spec,
  relationship: CorrelatedSubquery,
  format: Format | undefined,
  parentTable: Table,
): SQLQuery {
  const innerAlias = `inner_${relationship.subquery.alias}`;
  if (relationship.hidden) {
    const {join, participatingTables} = makeJunctionJoin(spec, relationship);
    const lastTable = must(last(participatingTables)).table;

    assert(
      relationship.subquery.related,
      'hidden relationship must be a junction',
    );
    const nestedAst = relationship.subquery.related[0].subquery;
    const selectionSet = related(
      spec,
      nestedAst.related ?? [],
      format,
      lastTable,
    );
    const tableSchema = spec.zql[nestedAst.table];
    for (const column of Object.keys(tableSchema.columns)) {
      selectionSet.push(
        selectIdent(spec.server, {
          table: lastTable,
          zql: column,
        }),
      );
    }

    return sql`(
        SELECT ${toJSON(innerAlias, format?.singular)} FROM (SELECT ${sql.join(
          selectionSet,
          ',',
        )} FROM ${join} WHERE (${makeCorrelator(
          spec,
          relationship.correlation.parentField.map(f => ({
            table: parentTable,
            zql: f,
          })),
          relationship.correlation.childField,
        )(participatingTables[0].table)}) ${
          nestedAst.where
            ? sql`AND ${where(spec, nestedAst.where, lastTable)}`
            : sql``
        }${
          nestedAst.start
            ? sql` AND ${start(
                spec,
                nestedAst.start,
                nestedAst.orderBy,
                lastTable,
              )}`
            : sql``
        } ${orderBy(spec, nestedAst.orderBy, lastTable)} ${limit(
          last(participatingTables)?.limit,
          format?.singular,
        )} ) ${sql.ident(innerAlias)}
      ) as ${sql.ident(relationship.subquery.alias)}`;
  }

  return sql`(
      SELECT ${toJSON(innerAlias, format?.singular)} FROM (${select(
        spec,
        relationship.subquery,
        format,
        makeCorrelator(
          spec,
          relationship.correlation.parentField.map(f => ({
            table: parentTable,
            zql: f,
          })),
          relationship.correlation.childField,
        ),
      )}) ${sql.ident(innerAlias)}
    ) as ${sql.ident(relationship.subquery.alias)}`;
}

function where(
  spec: Spec,
  condition: Condition | undefined,
  table: Table,
): SQLQuery {
  if (!condition) {
    return sql``;
  }

  switch (condition.type) {
    case 'and':
      return sql`(${sql.join(
        condition.conditions.map(c => where(spec, c, table)),
        ' AND ',
      )})`;
    case 'or':
      return sql`(${sql.join(
        condition.conditions.map(c => where(spec, c, table)),
        ' OR ',
      )})`;
    case 'correlatedSubquery':
      // `scalar` is deliberately ignored here. It is a planner hint for the
      // IVM engines, which have no cost-based planner and benefit from
      // pre-resolving an at-most-one-row subquery to a literal comparison
      // (see zqlite's `resolveSimpleScalarSubqueries`). Postgres decorrelates
      // EXISTS into a semi-join on its own, so the hint buys nothing here —
      // and honoring it as `parentField = (SELECT childField … LIMIT 1)` is
      // only sound when the subquery matches at most one row *globally*.
      // Applying it unconditionally silently dropped rows whenever the
      // subquery was not pinned to a unique key.
      return exists(spec, condition, table);
    case 'simple':
      return simple(spec, condition, table);
  }
}

function exists(
  spec: Spec,
  condition: CorrelatedSubqueryCondition,
  parentTable: Table,
): SQLQuery {
  switch (condition.op) {
    case 'EXISTS':
      return sql`EXISTS (${select(
        spec,
        condition.related.subquery,
        undefined,
        makeCorrelator(
          spec,
          condition.related.correlation.parentField.map(f => ({
            table: parentTable,
            zql: f,
          })),
          condition.related.correlation.childField,
        ),
      )})`;
    case 'NOT EXISTS':
      return sql`NOT EXISTS (${select(
        spec,
        condition.related.subquery,
        undefined,
        makeCorrelator(
          spec,
          condition.related.correlation.parentField.map(f => ({
            table: parentTable,
            zql: f,
          })),
          condition.related.correlation.childField,
        ),
      )})`;
  }
}

export function makeCorrelator(
  spec: Spec,
  parentFields: readonly QualifiedColumn[],
  childZqlFields: readonly string[],
): (childTable: Table) => SQLQuery {
  return (childTable: Table) => {
    const childFields = childZqlFields.map(zqlField => ({
      table: childTable,
      zql: zqlField,
    }));
    return sql.join(
      zip(parentFields, childFields).map(
        ([parentColumn, childColumn]) =>
          sql`${colIdent(spec.server, parentColumn)} = ${colIdent(
            spec.server,
            childColumn,
          )}`,
      ),
      ' AND ',
    );
  };
}

export function simple(
  spec: Spec,
  condition: SimpleCondition,
  table: Table,
): SQLQuery {
  if (condition.left.type === 'json') {
    const special = jsonPathCondition(spec, condition.left, condition, table);
    if (special) {
      return special;
    }
  }
  switch (condition.op) {
    case '!=':
    case '<':
    case '<=':
    case '=':
    case '>':
    case '>=':
    case 'ILIKE':
    case 'LIKE':
    case 'NOT ILIKE':
    case 'NOT LIKE':
      return sql`${valueComparison(
        spec,
        condition.left,
        table,
        condition.right,
        false,
      )} ${sql.__dangerous__rawValue(condition.op)} ${valueComparison(
        spec,
        condition.right,
        table,
        condition.left,
        false,
      )}`;
    case 'NOT IN':
    case 'IN':
      return any(spec, condition, table);
    case 'IS':
    case 'IS NOT':
      return distinctFrom(spec, condition, table);
  }
}

export function any(
  spec: Spec,
  condition: SimpleCondition,
  table: Table,
): SQLQuery {
  return sql`${condition.op === 'NOT IN' ? sql`NOT` : sql``}
    (
      ${valueComparison(spec, condition.left, table, condition.right, false)} = ANY 
      (${valueComparison(spec, condition.right, table, condition.left, true)})
    )`;
}

export function distinctFrom(
  spec: Spec,
  condition: SimpleCondition,
  table: Table,
): SQLQuery {
  return sql`${valueComparison(spec, condition.left, table, condition.right, false)} ${
    condition.op === 'IS' ? sql`IS NOT DISTINCT FROM` : sql`IS DISTINCT FROM`
  } ${valueComparison(spec, condition.right, table, condition.left, false)}`;
}

function startEquality(
  spec: Spec,
  table: Table,
  field: string,
  value: unknown,
): SQLQuery {
  return sql`${colIdent(spec.server, {
    table,
    zql: field,
  })} IS NOT DISTINCT FROM ${startValue(spec, table, field, value)}`;
}

function startRangeComparison(
  spec: Spec,
  table: Table,
  field: string,
  value: unknown,
  operator: '>' | '<',
): SQLQuery {
  const column = colIdent(spec.server, {table, zql: field});
  if (value === null) {
    return operator === '>' ? sql`${column} IS NOT NULL` : sql`FALSE`;
  }
  const typedValue = startValue(spec, table, field, value);
  return operator === '>'
    ? sql`${column} > ${typedValue}`
    : sql`(${column} IS NULL OR ${column} < ${typedValue})`;
}

function startValue(
  spec: Spec,
  table: Table,
  field: string,
  value: unknown,
): SQLQuery {
  return sqlConvertColumnArg(
    getServerColumn(spec.server, table, field),
    value,
    false,
    true,
  );
}

function valueComparison(
  spec: Spec,
  valuePos: ValuePosition,
  table: Table,
  otherValuePos: ValuePosition,
  plural: boolean,
): SQLQuery {
  const valuePosType = valuePos.type;
  switch (valuePosType) {
    case 'column': {
      const qualified: QualifiedColumn = {
        table,
        zql: valuePos.name,
      };
      return colIdent(spec.server, qualified);
    }
    case 'json':
      return jsonPathLeaf(spec, valuePos, table, otherValuePos);
    case 'literal':
      return literalValueComparison(
        spec,
        valuePos,
        table,
        otherValuePos,
        plural,
      );
    case 'static':
      throw new Error(
        'Static parameters must be bound to a value before compiling to SQL',
      );
    default:
      unreachable(valuePosType);
  }
}

function literalValueComparison(
  spec: Spec,
  valuePos: LiteralReference,
  table: Table,
  otherValuePos: ValuePosition,
  plural: boolean,
): SQLQuery {
  const otherType = otherValuePos.type;
  switch (otherType) {
    case 'column':
      return sqlConvertColumnArg(
        getServerColumn(spec.server, table, otherValuePos.name),
        valuePos.value,
        plural,
        true,
      );
    case 'json': {
      // The other side is a JSON path leaf, which is dynamically typed. Render
      // this literal by its OWN JS type so its cast matches the leaf's cast in
      // `jsonPathLeaf` (`leafType`). LiteralValue is primitives or a
      // homogeneous primitive list (enforced at the wire and by the builder).
      assert(
        plural === Array.isArray(valuePos.value),
        'Expected plural flag to match whether value is an array',
      );
      const {value} = valuePos;
      if (Array.isArray(value)) {
        return sqlConvertPluralLiteralArg(
          jsonLiteralType(value) ?? 'string',
          value as PluralLiteralType[],
        );
      }
      // `Array.isArray` does not narrow a `readonly` array out of the union.
      return sqlConvertSingularLiteralArg(
        value as string | number | boolean | null,
      );
    }
    case 'literal': {
      assert(
        plural === Array.isArray(valuePos.value),
        'Expected plural flag to match whether value is an array',
      );
      if (Array.isArray(valuePos.value)) {
        if (valuePos.value.length > 0) {
          // If the array is non-empty base its type on its first
          // element
          return sqlConvertPluralLiteralArg(
            typeof valuePos.value[0] as PluralLiteralType,
            valuePos.value as PluralLiteralType[],
          );
        }
        // If the array is empty, base its type on the other value
        // position's type (as long as the other value position is non-null,
        // cannot have a null[]).
        if (otherValuePos.value !== null) {
          return sqlConvertPluralLiteralArg(
            typeof otherValuePos.value as PluralLiteralType,
            [],
          );
        }
        // If the other value position is null, it can be compared to any
        // type of empty array, chose 'string' arbitrarily.
        return sqlConvertPluralLiteralArg('string', []);
      }
      if (
        typeof valuePos.value === 'string' ||
        typeof valuePos.value === 'number' ||
        typeof valuePos.value === 'boolean'
      ) {
        return sqlConvertSingularLiteralArg(valuePos.value);
      }
      throw new Error(`Literal of unexpected type: ${typeof valuePos.value}`);
    }
    case 'static':
      throw new Error(
        'Static parameters must be bound to a value before compiling to SQL',
      );
    default:
      unreachable(otherType);
  }
}

/**
 * The two SQL pieces every JSON path comparison is built from: the leaf's text
 * (`raw`, SQL NULL for a missing key or a JSON null) and its JSON type
 * (`jsonType`, via `jsonb_typeof`).
 *
 * Navigation chains `->` with a *typed* operand per segment — a text operand
 * for an object key, an integer operand for an array index — so Postgres
 * applies the same strict rule as the in-memory reader and SQLite: a string
 * segment never indexes an array and a number never reads an object key. (A
 * `#>>` text[] path would instead resolve a digit string by the container's
 * runtime type, including `'-1'` from the end.) The last step uses `->>` for
 * the text form.
 *
 * The base is the column as jsonb: a `json` column is cast, and a Postgres
 * array column — which zero maps to a `json()` column and the replica stores
 * as JSON text — is converted with `to_jsonb`.
 */
function jsonPathParts(
  spec: Spec,
  ref: JsonPathReference,
  table: Table,
): {value: SQLQuery; raw: SQLQuery; jsonType: SQLQuery} {
  const {name} = ref.value;
  const col = colIdent(spec.server, {table, zql: name});
  const {type, isArray} = getServerColumn(spec.server, table, name);
  assert(
    isArray || type === 'json' || type === 'jsonb',
    () => `JSON path on non-JSON column "${name}" of type ${type}`,
  );
  let obj: SQLQuery = isArray
    ? sql`to_jsonb(${col})`
    : type === 'json'
      ? sql`${col}::jsonb`
      : col;
  const operand = (seg: string | number): SQLQuery =>
    typeof seg === 'number'
      ? // A validated non-negative int32 (see isValidJsonPathIndex), so it is
        // safe to inline; `->` needs an integer operand to index an array.
        sql.__dangerous__rawValue(String(seg))
      : sqlConvertSingularLiteralArg(seg);
  const last = ref.path.length - 1;
  for (let i = 0; i < last; i++) {
    obj = sql`(${obj} -> ${operand(ref.path[i])})`;
  }
  const leaf = operand(ref.path[last]);
  return {
    // The leaf as jsonb; `raw` is its text form.
    value: sql`(${obj} -> ${leaf})`,
    raw: sql`(${obj} ->> ${leaf})`,
    jsonType: sql`jsonb_typeof(${obj} -> ${leaf})`,
  };
}

/**
 * The JS type the leaf must have to be compared against the literal on the
 * other side (`jsonLiteralType`, shared across engines) — also the
 * `jsonb_typeof` name and the key into `pgTypeForLiteralType` for the cast, so
 * the leaf's cast and the literal's own cast cannot drift. `undefined` for a
 * `null` literal (`IS NULL`: a missing key and a JSON null must both read as
 * SQL NULL), an empty list, or a non-literal.
 */
function leafType(other: ValuePosition): PluralLiteralType | undefined {
  return other.type === 'literal' ? jsonLiteralType(other.value) : undefined;
}

/**
 * Compiles a JSON path reference to its text form, cast to the comparison type
 * derived from the literal on the other side (`leafType`) so it lines up with
 * that literal's own cast.
 *
 * `->>` maps **both** a missing key and a JSON `null` to SQL `NULL` — matching
 * the SQLite `json_extract` pushdown and the in-memory predicate, so `IS NULL`
 * agrees across all three.
 *
 * The cast is gated on the leaf's JSON type (`CASE WHEN jsonb_typeof(...)`), so a
 * leaf of a different type than the literal is SQL NULL, i.e. a non-match. This
 * is the type-strict comparison documented on {@link JsonPathReference}: `->>`
 * renders a numeric `42` as the text `'42'`, so an ungated text comparison
 * would wrongly match it against the string `'42'`. For `number`/`boolean` the
 * gate is also what keeps the cast from throwing: Postgres errors when casting
 * non-conforming text (a string `"n/a"`, an object's JSON text) to
 * `double precision`/`boolean`, which would fail the whole query on one
 * mismatched row. Negated operators need a different form — see
 * `jsonPathCondition`.
 */
function jsonPathLeaf(
  spec: Spec,
  ref: JsonPathReference,
  table: Table,
  other: ValuePosition,
): SQLQuery {
  const {raw, jsonType} = jsonPathParts(spec, ref, table);
  const t = leafType(other);
  if (t === undefined) {
    return raw;
  }
  return sql`(CASE WHEN ${jsonType} = ${sqlConvertSingularLiteralArg(
    t,
  )} THEN ${raw}::${sql.__dangerous__rawValue(pgTypeForLiteralType(t))} END)`;
}

/**
 * The comparisons on a JSON path left operand that cannot go through the
 * generic operator rendering with the gated leaf (`jsonPathLeaf`); returns
 * `undefined` for the ones that can.
 *
 * - `IN`/`NOT IN` with a `null` literal is constant-false, as in the
 *   in-memory predicate (the generic forms would assert, or match every
 *   non-null leaf).
 * - `=`/`IN` against a string or boolean literal compare the leaf *as jsonb*:
 *   jsonb equality is type-strict by itself (`"42"` never equals `42`, a JSON
 *   `null` never equals a string) and a missing key is SQL NULL, so the
 *   `jsonb_typeof` gate is unnecessary — and without the `CASE` around the
 *   extraction, an expression index on `(col -> 'key')` can serve the
 *   predicate. Numbers keep the gated `double precision` form: jsonb compares
 *   numbers exactly (`numeric`), which would diverge from the client's and
 *   SQLite's IEEE-754 equality above 2^53 or beyond double precision.
 * - An empty `NOT IN` list matches every non-null leaf: SQL's
 *   `NOT (x = ANY('{}'))` is TRUE even for a NULL leaf, whereas the
 *   predicate's null guard excludes it.
 * - The negated operators (`!=`, `NOT LIKE`, `NOT ILIKE`, `NOT IN`): the gate
 *   makes a mismatched leaf SQL NULL, which a positive comparison correctly
 *   excludes — but NULL also excludes under a negated operator, where a
 *   mismatch must *match* (JS `42 !== '42'` is true). `jsonb_typeof` is NULL
 *   for a missing key and `'null'` for a JSON null, so one `COALESCE` covers
 *   the null guard without a second extraction: null/missing leaf → false,
 *   same type → the real (negated) comparison, other type → true.
 */
function jsonPathCondition(
  spec: Spec,
  left: JsonPathReference,
  condition: SimpleCondition,
  table: Table,
): SQLQuery | undefined {
  const {op, right} = condition;
  if (
    (op === 'IN' || op === 'NOT IN') &&
    right.type === 'literal' &&
    right.value === null
  ) {
    return sql`false`;
  }
  if ((op === '=' || op === 'IN') && right.type === 'literal') {
    const t = leafType(right);
    if (t === 'string' || t === 'boolean') {
      const {value} = jsonPathParts(spec, left, table);
      const lit = valueComparison(spec, right, table, left, op === 'IN');
      return op === 'IN'
        ? sql`${value} = ANY (ARRAY(SELECT to_jsonb(v) FROM unnest(${lit}) AS v))`
        : sql`${value} = to_jsonb(${lit})`;
    }
  }
  if (!isNegatedOperator(op)) {
    return undefined;
  }
  const t = leafType(right);
  const {raw, jsonType} = jsonPathParts(spec, left, table);
  if (t === undefined) {
    // Only an empty NOT IN list reaches here: `!=`/`NOT LIKE` against null
    // are constant-false through the generic NULL comparison.
    return op === 'NOT IN' ? sql`${raw} IS NOT NULL` : undefined;
  }
  const leaf = sql`${raw}::${sql.__dangerous__rawValue(pgTypeForLiteralType(t))}`;
  const plural = op === 'NOT IN';
  const lit = valueComparison(spec, right, table, left, plural);
  const cmp = plural
    ? sql`NOT (${leaf} = ANY (${lit}))`
    : sql`${leaf} ${sql.__dangerous__rawValue(op)} ${lit}`;
  return sql`(CASE COALESCE(${jsonType}, 'null') WHEN 'null' THEN false WHEN ${sqlConvertSingularLiteralArg(
    t,
  )} THEN ${cmp} ELSE true END)`;
}

export function makeJunctionJoin(
  spec: Spec,
  relationship: CorrelatedSubquery,
): {
  join: SQLQuery;
  participatingTables: ReturnType<typeof pullTablesForJunction>;
} {
  const participatingTables = pullTablesForJunction(spec, relationship);
  const joins: SQLQuery[] = [];

  for (const {table} of participatingTables) {
    if (joins.length === 0) {
      joins.push(fromIdent(spec.server, table));
      continue;
    }
    joins.push(
      sql` JOIN ${fromIdent(spec.server, table)} ON ${makeCorrelator(
        spec,
        participatingTables[joins.length].correlation.parentField.map(f => ({
          table: participatingTables[joins.length - 1].table,
          zql: f,
        })),
        participatingTables[joins.length].correlation.childField,
      )(participatingTables[joins.length].table)}`,
    );
  }

  return {
    join: sql`${sql.join(joins, '')}`,
    participatingTables,
    // lastTable: participatingTables[participatingTables.length - 1].table,
    // lastLimit: participatingTables[participatingTables.length - 1].limit,
  };
}

export function pullTablesForJunction(
  spec: Spec,
  relationship: CorrelatedSubquery,
): [
  {
    table: Table;
    correlation: Correlation;
    limit: number | undefined;
  },
  {table: Table; correlation: Correlation; limit: number | undefined},
] {
  assert(
    relationship.subquery.related?.length === 1,
    'Too many related tables for a junction edge',
  );
  const otherRelationship = relationship.subquery.related[0];
  assert(
    !otherRelationship.hidden,
    'Expected junction edge relationship to not be hidden',
  );
  return [
    {
      table: makeTable(spec, relationship.subquery.table),
      correlation: relationship.correlation,
      limit: relationship.subquery.limit,
    },
    {
      table: makeTable(spec, otherRelationship.subquery.table),
      correlation: otherRelationship.correlation,
      limit: otherRelationship.subquery.limit,
    },
  ];
}

function toJSON(table: string, singular = false): SQLQuery {
  return sql`${
    singular ? sql`` : sql`COALESCE(json_agg`
  }(row_to_json(${sql.ident(table)}))${singular ? sql`` : sql`, '[]'::json)`}`;
}

function selectIdent(server: ServerSpec, column: QualifiedColumn): SQLQuery {
  const serverColumnSchema =
    server.schema[server.mapper.tableName(column.table.zql)][
      server.mapper.columnName(column.table.zql, column.zql)
    ];
  const serverType = serverColumnSchema.type;
  if (!serverColumnSchema.isEnum) {
    let needsNormalization = false;
    switch (serverType) {
      case 'timestamptz':
      // @ts-expect-error Fallthrough intended
      case 'timetz':
        needsNormalization = true;
      // fallthrough

      case 'date':
      case 'time':
      case 'time without time zone':
      case 'time with time zone':
      case 'timestamp':
      case 'timestamp without time zone':
      case 'timestamp with time zone': {
        // EXTRACT(EPOCH FROM timetz) can be negative when the UTC offset is
        // positive (e.g. 01:00+02 = 23:00 UTC prev day = -3600s). Wrap with
        // modular arithmetic to normalize to 0..86400000.
        const toMs = (epochExpr: SQLQuery): SQLQuery =>
          needsNormalization
            ? sql`((${epochExpr})::bigint + 86400000) % 86400000`
            : epochExpr;

        if (serverColumnSchema.isArray) {
          const col = colIdent(server, column);
          return sql`CASE WHEN ${col} IS NULL THEN NULL ELSE ARRAY(SELECT ${toMs(
            sql`EXTRACT(EPOCH FROM unnest(${col})) * 1000`,
          )}) END as ${sql.ident(column.zql)}`;
        }

        return sql`${toMs(
          sql`EXTRACT(EPOCH FROM ${colIdent(server, column)}) * 1000`,
        )} as ${sql.ident(column.zql)}`;
      }
    }
  }

  return sql`${colIdent(server, column)} as ${sql.ident(column.zql)}`;
}

function colIdent(server: ServerSpec, column: QualifiedColumn) {
  return sql.ident(
    column.table.alias,
    server.mapper.columnName(column.table.zql, column.zql),
  );
}

function fromIdent(server: ServerSpec, table: Table) {
  return sql`${sql.ident(server.mapper.tableName(table.zql))} AS ${sql.ident(table.alias)}`;
}

function getServerColumn(spec: ServerSpec, table: Table, zqlColumn: string) {
  return spec.schema[spec.mapper.tableName(table.zql)][
    spec.mapper.columnName(table.zql, zqlColumn)
  ];
}

// oxlint-disable-next-line @typescript-eslint/no-explicit-any
export function extractZqlResult(pgResult: Array<any>): JSONValue {
  const bigIntJson: BigIntJSONValue = parseBigIntJson(
    pgResult[0][ZQL_RESULT_KEY],
  );
  assertJSONValue(bigIntJson);
  return bigIntJson;
}

function assertJSONValue(v: BigIntJSONValue): asserts v is JSONValue {
  const path = findPathToBigInt(v);
  if (path) {
    throw new Error(`Value exceeds safe Number range. ${path}`);
  }
}

function findPathToBigInt(v: BigIntJSONValue): string | undefined {
  const typeOfV = typeof v;
  switch (typeOfV) {
    case 'bigint':
      return ` = ${v}`;
    case 'object': {
      if (v === null) {
        return;
      }
      if (Array.isArray(v)) {
        for (let i = 0; i < v.length; i++) {
          const path = findPathToBigInt(v[i]);
          if (path) {
            return `[${i}]${path}`;
          }
        }
        return undefined;
      }

      const o = v as Record<string, BigIntJSONValue>;
      for (const k in o) {
        if (hasOwn(o, k)) {
          const path = findPathToBigInt(o[k]);
          if (path) {
            return `['${k}']${path}`;
          }
        }
      }
      return undefined;
    }
    case 'number':
      return undefined;
    case 'boolean':
      return undefined;
    default:
      return undefined;
  }
}
