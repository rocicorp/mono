import {
  Aggregate,
  AST,
  normalizeAST,
  Ordering,
  Selector,
  type Condition,
} from '@rocicorp/zql/src/zql/ast/ast.js';
import {compareUTF8} from 'compare-utf8';
import {ident} from 'pg-format';
import type {JSONValue} from 'postgres';
import {assert} from 'shared/src/asserts.js';
import {create64} from '../types/xxhash.js';
import type {ServerAST} from './server-ast.js';

export type ParameterizedQuery = {
  query: string;
  values: JSONValue[];
};

/**
 * @returns An object for producing normalized version of the supplied `ast`,
 *     the resulting parameterized query, and hash identifier.
 */
export function getNormalized(ast: ServerAST): Normalized {
  return new Normalized(ast);
}

function aggFn(agg: Aggregate) {
  return agg === 'array' ? 'array_agg' : agg;
}

export class Normalized {
  readonly #ast: ServerAST;
  readonly #values: JSONValue[] = [];
  readonly #query;
  #nextParam = 1;

  constructor(ast: ServerAST) {
    // Normalize the AST such that all order-agnostic lists (basically, everything
    // except ORDER BY) are sorted in a deterministic manner such that semantically
    // equivalent ASTs produce the same queries and hash identifier.
    this.#ast = withNormalizedServerFields(normalizeAST(ast), ast);

    assert(
      this.#ast.select?.length ||
        this.#ast.aggregate?.length ||
        this.#ast.aggLift?.length,
    );

    this.#query = this.#constructQuery(this.#ast);
  }

  #constructQuery(ast: ServerAST): string {
    const {schema, table, alias, select, aggregate, joins} = ast;
    let {groupBy, orderBy, limit, where} = ast;

    let query = '';
    const selection = [
      ...(select ?? []).map(
        ([sel, alias]) => `${selector(sel)} AS ${ident(alias)}`,
      ),
      ...(aggregate ?? []).map(a => {
        // Aggregation aliases are ignored for normalization, and instead aliased
        // to the string representation of the aggregation, e.g.
        // 'SELECT COUNT(foo) AS "COUNT(foo)" WHERE ...'
        const agg = `${aggFn(a.aggregate)}(${
          a.field ? selector(a.field) : '*'
        })`;
        return `${agg} AS ${ident(agg)}`;
      }),
      ...(ast.aggLift ?? []).map(
        agg =>
          `jsonb_agg(jsonb_build_object(${agg.selectors
            .map(s => `'${s.alias}', ${ident(agg.table)}.${ident(s.column)}`)
            .join(', ')})) AS ${ident(agg.alias)}`,
      ),
    ].join(', ');

    // 1. all joins are left joins
    // 2. order by is only against fields in the `from` table
    // 3. group by is against unique field in the `from` table
    // 4. limit exists
    // then:
    // move order and limit to sub-query

    if (selection) {
      query += `SELECT ${selection} FROM `;
    }

    const getOrderByStr = ([names, dir]: Ordering) =>
      ` ORDER BY ${names.map(x => `${selector(x)} ${dir}`).join(', ')}`;

    if (moveOrderByAndLimit(ast)) {
      query += `(SELECT * FROM `;
      if (schema) {
        query += ident(schema) + '.';
      }
      query += ident(table);
      if (where) {
        query += ` WHERE ${this.#condition(where)}`;
      }
      if (orderBy) {
        query += getOrderByStr(orderBy);
      }
      if (limit !== undefined) {
        query += ` LIMIT ${limit}`;
      }
      query += `) AS ${ident(alias ?? table)}`;

      orderBy = undefined;
      limit = undefined;
      groupBy = undefined;
      where = undefined;
    } else {
      if (schema) {
        query += ident(schema) + '.';
      }
      query += ident(table);
      if (alias) {
        query += ` AS ${ident(alias)}`;
      }
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
      query += getOrderByStr(orderBy);
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
      assert(type === 'value');
      if (!Array.isArray(value)) {
        this.#values.push(value);
        return `${selector(cond.field)} ${cond.op} $${this.#nextParam++}`;
      }
      // Unroll the array
      let expr = `${selector(cond.field)} ${cond.op} (`;
      value.forEach((v, i) => {
        this.#values.push(v);
        expr += (i ? ', ' : '') + `$${this.#nextParam++}`;
      });
      expr += ')';
      return expr;
    }

    return `(${cond.conditions
      .map(sub => `${this.#condition(sub)}`)
      .join(` ${cond.op} `)})`;
  }

  /** @returns The normalized AST. */
  ast(): ServerAST {
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
    return create64(SEED)
      .update(this.#query)
      .update(JSON.stringify(this.#values))
      .digest()
      .toString(radix);
  }
}

function selector(selector: Selector): string {
  return selector.flatMap(id => id.split('.').map(id => ident(id))).join('.');
}

const SEED = 0x34567890n;

function withNormalizedServerFields(ast: AST, serverAst: ServerAST): ServerAST {
  if (serverAst.aggLift === undefined) {
    return ast;
  }
  const aggLift = serverAst.aggLift.map(agg => ({
    ...agg,
    selectors: [...agg.selectors].sort((a, b) => compareUTF8(a.alias, b.alias)),
  }));
  aggLift.sort((a, b) => compareUTF8(a.alias, b.alias));
  return {
    ...ast,
    aggLift: serverAst.aggLift,
  };
}

function moveOrderByAndLimit(ast: ServerAST): boolean {
  return !!(
    // all left joins
    (
      ast.joins?.every(join => join.type === 'left') &&
      // ordering only against left most table
      ast.orderBy?.[0].every(selector => selector[0] === ast.table) &&
      // group by only against primary key of left most table
      ast.groupBy?.every(
        selector => selector[0] === ast.table && selector[1] === 'id',
      ) &&
      // limit exists
      ast.limit !== undefined &&
      allWheresAgainst(ast.table, ast.where)
    )
  );
}

// ugh... this is overly specific.
function allWheresAgainst(
  table: string,
  where: Condition | undefined,
): boolean {
  if (where === undefined) {
    return true;
  }
  if (where.type === 'simple') {
    return where.field[0] === table;
  }
  return where.conditions.every(cond => allWheresAgainst(table, cond));
}
