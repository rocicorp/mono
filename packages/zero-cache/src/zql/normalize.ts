import {
  isJoinWithQuery,
  Join,
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
        const agg = `${a.aggregate}(${a.field ? ident(a.field) : '*'})`;
        return `${agg} AS ${ident(agg)}`;
      }),
    ].join(', ');

    if (selection) {
      query += `SELECT ${selection} FROM `;
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
      query += ` ${type.toUpperCase()} JOIN (${this.#constructQuery(other)})`;
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

/**
 * Given an AST, select all the primary keys from all tables for all rows that are in the
 * final result.
 */
export type SelectedPrimaryKey = {
  // The table (or aliased table) from which we are selecting the primary key
  from: string;
  column: string;
  // How we'll put this in the final result `${sourceTable}_${reAlias}_id`
  // E.g., SELECT issue.id as issue_1_id, parent.id as issue_2_id FROM
  //        issue JOIN issue as parent ON parent.id = issue.parent_id
  as: {
    sourceTable: string;
    // incrementing counter to ensure that we don't have conflicts with other aliases
    // One case this handles is self joins. Each self-join needs the primary keys aliased
    // to different names.
    reAlias: number;
    column: string;
  };
};
export type ASTWithPrimaryKeys = AST & {
  primaryKeys: SelectedPrimaryKey[];
  joins?: ((Join & {other: ASTWithPrimaryKeys}) | Join)[] | undefined;
};
export function selectPrimaryKeysForExplosion(
  ast: AST,
  reAlias: number,
  alias?: string | undefined,
): ASTWithPrimaryKeys {
  const ret: ASTWithPrimaryKeys = {
    ...ast,
    joins: [...(ast.joins ?? [])],
    primaryKeys: [
      {
        from: alias ?? ast.table,
        column: 'id',
        as: {
          sourceTable: ast.table,
          reAlias,
          column: 'id',
        },
      },
    ],
  };
  for (let i = 0; i < (ret.joins?.length ?? 0); i++) {
    if (ret.joins === undefined) {
      break;
    }
    const join = ret.joins[i];
    // Joining with a sub-query?
    if (isJoinWithQuery(join)) {
      // Recurse into the sub-query and do primary key selection for the sub-query
      const subqueryJoin = selectPrimaryKeysForExplosion(join.other, ++reAlias);
      // Now pull the sub-query's primary keys up a level
      ret.primaryKeys.push(
        ...subqueryJoin.primaryKeys.map(pk => ({
          from: join.as,
          column: `${pk.as.sourceTable}_${pk.as.reAlias}_id`,
          as: {
            sourceTable: pk.as.sourceTable,
            reAlias: pk.as.reAlias,
            column: pk.as.column,
          },
        })),
      );
      ret.joins[i] = {
        ...join,
        other: subqueryJoin,
      };
    } else {
      // regular join? just add the primary keys to the top level
      ret.primaryKeys.push(
        ...selectPrimaryKeysForExplosion(join.other, ++reAlias, join.as)
          .primaryKeys,
      );
    }
  }

  return ret;
}

const SEED = 0x1234567890;
