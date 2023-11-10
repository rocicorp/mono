import {unreachable} from 'shared/src/asserts.js';
import type * as v from 'shared/src/valita.js';

/**
 * Interfaces to access (a subset of) the SQL API supported by Workers Analytics Engine:
 * https://developers.cloudflare.com/analytics/analytics-engine/sql-reference/#select-statement
 * with valitas-schema assisted type-safety.
 */

// Generic Schema to represent SELECT'ed aliases and their types.
export type SelectSchema = Record<string, v.Type<string | number | Date>>;
// Maps an alias to its defining expression in a SELECT statement.
// Selections that are not aliased can be omitted.
// https://developers.cloudflare.com/analytics/analytics-engine/sql-reference/#select-clause
export type Expressions<T extends SelectSchema> = {
  [Alias in keyof T]?: string;
};

export type SelectClause<T extends SelectSchema> = {
  schema: v.ObjectType<T>;
  expr: Expressions<T>;
};

export interface Selectable {
  select<T extends SelectSchema>(clause: SelectClause<T>): Where<T>;
}

// https://developers.cloudflare.com/analytics/analytics-engine/sql-reference/#comparison-operators
export type Comparison =
  | '='
  | '!='
  | '<>'
  | '<='
  | '>='
  | '<'
  | '>'
  | 'IN'
  | 'NOT IN';

// https://developers.cloudflare.com/analytics/analytics-engine/sql-reference/#where-clause
export interface Where<T extends SelectSchema> extends GroupBy<T> {
  // WHERE comparisons of selected columns. The most common case.
  where<Alias extends keyof T>(
    column: Alias,
    cmp: Comparison,
    val: v.Infer<T[Alias]>,
  ): WhereBuilder<T>;

  // WHERE comparisons of backing columns that were not selected.
  // The is useful, for example, for `timestamp` restrictions of aggregation queries.
  where<Alias extends keyof BackingSchema>(
    column: Alias,
    cmp: Comparison,
    val: BackingSchema[Alias],
  ): WhereBuilder<T>;
}

export interface WhereBuilder<T extends SelectSchema> extends GroupBy<T> {
  // AND comparisons of selected columns. The most common case.
  and<Alias extends keyof T>(
    column: Alias,
    cmp: Comparison,
    val: v.Infer<T[Alias]>,
  ): WhereBuilder<T>;

  // AND comparisons of backing columns that were not selected.
  // The is useful, for example, for `timestamp` restrictions of aggregation queries.
  and<Alias extends keyof BackingSchema>(
    column: Alias,
    cmp: Comparison,
    val: BackingSchema[Alias],
  ): WhereBuilder<T>;

  // OR comparisons of selected columns. The most common case.
  or<Alias extends keyof T>(
    column: Alias,
    cmp: Comparison,
    val: v.Infer<T[Alias]>,
  ): WhereBuilder<T>;

  // OR comparisons of backing columns that were not selected.
  // The is useful, for example, for `timestamp` restrictions of aggregation queries.
  or<Alias extends keyof BackingSchema>(
    column: Alias,
    cmp: Comparison,
    val: BackingSchema[Alias],
  ): WhereBuilder<T>;
}

function where(
  expression: string,
  cmp: Comparison,
  val: string | number | Date,
): string {
  // Handles string escaping and DateTime fields:
  // https://developers.cloudflare.com/analytics/analytics-engine/sql-reference/#todatetime
  const formattedVal =
    typeof val === 'string'
      ? `'${escapeString(val)}'`
      : val instanceof Date
      ? `toDateTime(${Math.round(val.getTime() / 1000)})`
      : val; // number
  return `${expression} ${cmp} ${formattedVal}`;
}

// https://developers.cloudflare.com/analytics/analytics-engine/sql-reference/#group-by-clause
export interface GroupBy<T extends SelectSchema> extends OrderBy<T> {
  groupBy(...expressions: [string, ...string[]]): OrderBy<T>;
}

export type Direction = 'ASC' | 'DESC';

// https://developers.cloudflare.com/analytics/analytics-engine/sql-reference/#order-by-clause
export interface OrderBy<T extends SelectSchema> extends Limit<T> {
  orderBy(expression: string): Limit<T>;
  orderBy(expression: string, dir: Direction): Limit<T>;
}

// https://developers.cloudflare.com/analytics/analytics-engine/sql-reference/#limit-clause
export interface Limit<T extends SelectSchema> extends SelectStatement<T> {
  /**
   * @param n Limits the number of results returned, or `undefined` to set the limit to 'ALL'
   */
  limit(n: number | undefined): SelectStatement<T>;
}

export interface SelectStatement<T extends SelectSchema> extends Selectable {
  readonly schema: v.ObjectType<T>;
  toString(): string;
}

// Purely for readability in tests.
const SEP = '\n          ';

/**
 * `SelectBuilder` implementation of `Select` employs the immutable builder
 * pattern so instances can be cached / shared / reused.
 */
export class SelectBuilder<T extends SelectSchema> implements Where<T> {
  readonly schema: v.ObjectType<T>;
  readonly #parts: readonly string[];
  readonly #whereExpr: string | undefined;

  static create<T extends SelectSchema>(
    from: string,
    clause: SelectClause<T>,
  ): Where<T> {
    const {schema, expr} = clause;
    const columns: string[] = [];
    Object.keys(schema.shape).forEach(name => {
      const expression = expr[name];
      if (expression) {
        columns.push(`${expression} AS ${name}`);
      } else {
        columns.push(name);
      }
    });
    const parts: string[] = ['SELECT'];
    parts.push(columns.join(`,${SEP}`), `FROM ${from}`);
    return new SelectBuilder<T>(schema, parts);
  }

  constructor(
    schema: v.ObjectType<T>,
    parts: readonly string[],
    whereExpr?: string | undefined,
  ) {
    this.schema = schema;
    this.#parts = parts;
    this.#whereExpr = whereExpr;
  }

  /**
   * Creates a new builder instance with the additional `part`,
   * flushing any `#whereExpr` that was being built.
   */
  #with(part: string): SelectBuilder<T> {
    const where = this.#whereExpr ? [`WHERE ${this.#whereExpr}`] : [];
    return new SelectBuilder<T>(this.schema, [...this.#parts, ...where, part]);
  }

  where<Alias extends keyof T>(
    column: Alias,
    cmp: Comparison,
    val: v.Infer<T[Alias]>,
  ): SelectBuilder<T>;
  where<Alias extends keyof BackingSchema>(
    column: Alias,
    cmp: Comparison,
    val: BackingSchema[Alias],
  ): SelectBuilder<T>;
  where(
    column: string,
    cmp: Comparison,
    val: string | number | Date,
  ): SelectBuilder<T> {
    return new SelectBuilder(this.schema, this.#parts, where(column, cmp, val));
  }

  and<Alias extends keyof T>(
    column: Alias,
    cmp: Comparison,
    val: v.Infer<T[Alias]>,
  ): SelectBuilder<T>;
  and<Alias extends keyof BackingSchema>(
    column: Alias,
    cmp: Comparison,
    val: BackingSchema[Alias],
  ): SelectBuilder<T>;
  and(
    column: string,
    cmp: Comparison,
    val: string | number | Date,
  ): SelectBuilder<T> {
    return new SelectBuilder(
      this.schema,
      this.#parts,
      `(${this.#whereExpr}) AND (${where(column, cmp, val)})`,
    );
  }

  or<Alias extends keyof T>(
    column: Alias,
    cmp: Comparison,
    val: v.Infer<T[Alias]>,
  ): SelectBuilder<T>;
  or<Alias extends keyof BackingSchema>(
    column: Alias,
    cmp: Comparison,
    val: BackingSchema[Alias],
  ): SelectBuilder<T>;
  or(
    column: string,
    cmp: Comparison,
    val: string | number | Date,
  ): SelectBuilder<T> {
    return new SelectBuilder(
      this.schema,
      this.#parts,
      `(${this.#whereExpr}) OR (${where(column, cmp, val)})`,
    );
  }

  groupBy(...expressions: [string, ...string[]]): SelectBuilder<T> {
    return this.#with(`GROUP BY ${expressions.join(`, `)}`);
  }

  orderBy(expression: string, dir: Direction = 'ASC'): SelectBuilder<T> {
    return this.#with(`ORDER BY ${expression} ${dir}`);
  }

  limit(n: number | undefined): SelectBuilder<T> {
    return this.#with(`LIMIT ${n ?? 'ALL'}`);
  }

  toString(): string {
    return this.#with('FORMAT JSON').#parts.join(SEP);
  }

  // SELECT statements themselves are also Selectable as a subquery of an enclosing SELECT.
  // https://developers.cloudflare.com/analytics/analytics-engine/sql-reference/#from-clause
  select<S extends SelectSchema>(clause: SelectClause<S>): Where<S> {
    const subQuery = ['(', ...this.#with(')').#parts].join(SEP);
    return SelectBuilder.create(subQuery, clause);
  }
}

// From: http://stackoverflow.com/a/7760578/700897
function escapeString(str: string) {
  // eslint-disable-next-line no-control-regex
  return str.replace(/[\0\x08\x09\x1a\n\r"'\\%]/g, char => {
    switch (char) {
      case '\0':
        return '\\0';
      case '\x08':
        return '\\b';
      case '\x09':
        return '\\t';
      case '\x1a':
        return '\\z';
      case '\n':
        return '\\n';
      case '\r':
        return '\\r';
      case '"':
      case "'":
      case '\\':
      case '%':
        // prepends a backslash to backslash, percent, and double/single quotes
        return '\\' + char;
    }
    unreachable();
  });
}

type BackingSchema = {
  blob1: string;
  blob2: string;
  blob3: string;
  blob4: string;
  blob5: string;
  blob6: string;
  blob7: string;
  blob8: string;
  blob9: string;
  blob10: string;
  blob11: string;
  blob12: string;
  blob13: string;
  blob14: string;
  blob15: string;
  blob16: string;
  blob17: string;
  blob18: string;
  blob19: string;
  blob20: string;
  double1: number;
  double2: number;
  double3: number;
  double4: number;
  double5: number;
  double6: number;
  double7: number;
  double8: number;
  double9: number;
  double10: number;
  double11: number;
  double12: number;
  double13: number;
  double14: number;
  double15: number;
  double16: number;
  double17: number;
  double18: number;
  double19: number;
  double20: number;
  timestamp: Date;
};
