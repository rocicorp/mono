import {
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

class Normalized {
  readonly #ast: AST;
  readonly #values: JSONValue[] = [];
  #query = '';
  #nextParam = 1;

  constructor(ast: AST) {
    // Normalize the AST such that all order-agnostic lists (basically, everything
    // except ORDER BY) are sorted in a deterministic manner such that semantically
    // equivalent ASTs produce the same queries and hash identifier.
    this.#ast = normalizeAST(ast);

    const {table, select, aggregate, where, groupBy, orderBy, limit} =
      this.#ast;

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

const SEED = 0x1234567890;
