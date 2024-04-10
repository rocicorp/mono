import type {AST, Condition} from '@rocicorp/zql/src/zql/ast/ast.js';
import {compareUTF8} from 'compare-utf8';
import {ident} from 'pg-format';
import type {JSONValue} from 'postgres';
import {assert} from 'shared/src/asserts.js';
import xxh from 'xxhashjs';
import {stringify} from '../types/bigint-json.js';

export type ParameterizedQuery = {
  query: string;
  values: JSONValue[];
};

/**
 * @returns An object for producing normalized version of the supplied `ast`,
 *     the resulting parameterized query, and hash identifier.
 */
export function getNormalized(ast: AST): Normalized {
  return new Normalized(ast);
}

class Normalized {
  readonly #ast: AST;
  readonly #values: JSONValue[] = [];
  #query = '';
  #nextParam = 1;

  constructor(ast: AST) {
    // Normalize the AST such that all order-agnostic lists (basically, everything
    // except ORDER BY) are sorted in a deterministic manner such that semantically
    // equivalent ASTs produce the same queries and hash identifier.
    this.#ast = {
      table: ast.table,
      alias: ast.alias,
      select: ast.select
        ? [...ast.select].sort(([a], [b]) => compareUTF8(a, b))
        : undefined,
      aggregate: ast.aggregate
        ? [...ast.aggregate].sort(
            (a, b) =>
              compareUTF8(a.aggregate, b.aggregate) ||
              compareUTF8(a.field ?? '*', b.field ?? '*'),
          )
        : undefined,
      where: ast.where ? sorted(flattened(ast.where)) : undefined,
      limit: ast.limit,
      groupBy: ast.groupBy ? [...ast.groupBy].sort(compareUTF8) : undefined,
      // The order of ORDER BY expressions is semantically significant, so it
      // is left as is (i.e. not sorted).
      orderBy: ast.orderBy,
    };

    const {table, select, aggregate, where, groupBy, orderBy, limit} =
      this.#ast;

    assert(table);
    assert(select?.length || aggregate?.length);

    const selection = [
      ...(select ?? []).map(([col]) => ident(col)),
      ...(aggregate ?? []).map(a => {
        // Aggregation aliases are ignored for normalization, and instead aliased
        // to the string representation of the aggregation, e.g.
        // 'SELECT COUNT(foo) AS "COUNT(foo)" WHERE ...'
        const agg = `${a.aggregate}(${a.field ? ident(a.field) : '*'})`;
        return `${agg} AS ${ident(agg)}`;
      }),
    ].join(', ');

    this.#query = `SELECT ${selection} FROM ${ident(table)}`;
    if (where) {
      this.#query += ` WHERE ${this.#condition(where)}`;
    }
    if (groupBy) {
      this.#query += ` GROUP BY ${groupBy.map(x => ident(x)).join(', ')}`;
    }
    if (orderBy) {
      const [names, dir] = orderBy;
      this.#query += ` ORDER BY ${names.map(x => ident(x)).join(', ')} ${dir}`;
    }
    if (limit !== undefined) {
      this.#query += ` LIMIT ${limit}`;
    }
  }

  #condition(cond: Condition): string {
    if ('field' in cond) {
      const {
        value: {type, value},
      } = cond;
      assert(type === 'literal');
      this.#values.push(value);
      return `${ident(cond.field)} ${cond.op} $${this.#nextParam++}`;
    }

    return `(${cond.conditions
      .map(sub => `${this.#condition(sub)}`)
      .join(` ${cond.op} `)})`;
  }

  /** @returns The normalized AST. */
  ast(): AST {
    return this.#ast;
  }

  /**
   * @returns the parameterized `query` with parameter `values`,
   *    suitable for `PREPARE` and `EXECUTE` postgresql commands, respectively.
   */
  query(): ParameterizedQuery {
    return {query: this.#query, values: [...this.#values]};
  }

  /**
   * @returns hash representing the normalized AST, which is the same for all semantically
   *    equivalent ASTs.
   */
  hash(radix = 36): string {
    return xxh
      .h64(SEED)
      .update(this.#query)
      .update(stringify(this.#values))
      .digest()
      .toString(radix);
  }
}

const SEED = 0x1234567890;

// Returns a flattened version of the Conditions in which nested Conjunctions with
// the same operation ('AND' or 'OR') are flattened to the same level. e.g.
//
// ```
// ((a AND b) AND (c AND (d OR (e OR f)))) -> (a AND b AND c AND (d OR e OR f))
// ```
function flattened<Cond extends Condition>(cond: Cond): Cond {
  if ('field' in cond) {
    return cond;
  }
  const {op, conditions} = cond;
  return {
    op,
    conditions: conditions.flatMap(c =>
      c.op === op ? c.conditions.map(c => flattened(c)) : flattened(c),
    ),
  } as Cond;
}

// Returns a sorted version of the Conditions for deterministic hashing / deduping.
// This is semantically valid because the order of evaluation of subexpressions is
// not defined; specifically, the query engine chooses the best order for them:
// https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-EXPRESS-EVAL
function sorted<Cond extends Condition>(cond: Cond): Cond {
  if ('field' in cond) {
    return cond;
  }
  return {
    op: cond.op,
    conditions: cond.conditions.map(c => sorted(c)).sort(cmp),
  } as Cond;
}

function cmp(a: Condition, b: Condition): number {
  if ('field' in a) {
    if (!('field' in b)) {
      return -1; // Arbitrary: order SimpleConditions first
    }
    return (
      compareUTF8(a.field, b.field) ||
      compareUTF8(a.op, b.op) ||
      // Comparing the same field with the same op more than once doesn't make logical
      // sense, but is technically possible. Assume the values are of the same type and
      // sort by their String forms.
      compareUTF8(String(a.value.value), String(b.value.value))
    );
  }
  if ('field' in b) {
    return 1; // Arbitrary: order SimpleConditions first
  }
  // For comparing two conjunctions, compare the ops first, and then compare
  // the conjunctions member-wise.
  const val = compareUTF8(a.op, b.op);
  if (val !== 0) {
    return val;
  }
  for (
    let l = 0, r = 0;
    l < a.conditions.length && r < b.conditions.length;
    l++, r++
  ) {
    const val = cmp(a.conditions[l], b.conditions[r]);
    if (val !== 0) {
      return val;
    }
  }
  // prefixes first
  return a.conditions.length - b.conditions.length;
}
