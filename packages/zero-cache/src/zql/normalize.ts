import {
  Aggregate,
  normalizeAST,
  type AST,
  type Condition,
} from '@rocicorp/zql/src/zql/ast/ast.js';
import {ident} from 'pg-format';
import type {JSONValue} from 'postgres';
import {assert} from 'shared/src/asserts.js';
import xxh from 'xxhashjs';

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

function aggFn(agg: Aggregate) {
  return agg === 'array' ? 'array_agg' : agg;
}

export class Normalized {
  readonly #ast: AST;
  readonly #values: JSONValue[] = [];
  readonly #query;
  #nextParam = 1;

  constructor(ast: AST) {
    // Normalize the AST such that all order-agnostic lists (basically, everything
    // except ORDER BY) are sorted in a deterministic manner such that semantically
    // equivalent ASTs produce the same queries and hash identifier.
    this.#ast = normalizeAST(ast);

    assert(this.#ast.select?.length || this.#ast.aggregate?.length);

    this.#query = this.#constructQuery(this.#ast);
  }

  #constructQuery(ast: AST): string {
    const {
      schema,
      table,
      alias,
      select,
      aggregate,
      joins,
      where,
      groupBy,
      orderBy,
      limit,
    } = ast;

    let query = '';
    const selection = [
      ...(select ?? []).map(
        ([sel, alias]) => `${selector(sel)} AS ${ident(alias)}`,
      ),
      ...(aggregate ?? []).map(a => {
        // Aggregation aliases are ignored for normalization, and instead aliased
        // to the string representation of the aggregation, e.g.
        // 'SELECT COUNT(foo) AS "COUNT(foo)" WHERE ...'
        const agg = `${aggFn(a.aggregate)}(${a.field ? ident(a.field) : '*'})`;
        return `${agg} AS ${ident(agg)}`;
      }),
    ].join(', ');

    if (selection) {
      query += `SELECT ${selection} FROM `;
    }
    if (schema) {
      query += ident(schema) + '.';
    }
    query += ident(table);
    if (alias) {
      query += ` AS ${ident(alias)}`;
    }
    joins?.forEach(join => {
      const {
        type,
        other,
        on: [left, right],
        as,
      } = join;
      query += ` ${type.toUpperCase()} JOIN `;
      const joinFrom = this.#constructQuery(other);
      query += other.select ? `(${joinFrom})` : joinFrom;
      query += ` AS ${ident(as)} ON ${selector(left)} = ${selector(right)}`;
    });
    if (where) {
      query += ` WHERE ${this.#condition(where)}`;
    }
    if (groupBy) {
      query += ` GROUP BY ${groupBy.map(x => selector(x)).join(', ')}`;
    }
    if (orderBy) {
      const [names, dir] = orderBy;
      query += ` ORDER BY ${names.map(x => selector(x)).join(', ')} ${dir}`;
    }
    if (limit !== undefined) {
      query += ` LIMIT ${limit}`;
    }
    return query;
  }

  #condition(cond: Condition): string {
    if (cond.type === 'simple') {
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
      .update(JSON.stringify(this.#values))
      .digest()
      .toString(radix);
  }
}

function selector(x: string): string {
  const parts = x.split('.');
  return parts.length === 2
    ? `${ident(parts[0])}.${ident(parts[1])}`
    : ident(x);
}

const SEED = 0x1234567890;
