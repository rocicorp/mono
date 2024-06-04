import type {
  AST,
  Condition,
  Ordering,
  Selector,
} from '@rocicorp/zql/src/zql/ast/ast.js';
import {assert} from 'shared/src/asserts.js';
import type {ServerAST} from './server-ast.js';

export type PrimaryKeyLookup = (
  schema: string,
  table: string,
  col: string,
) => boolean;

/**
 * The server currently supports very a specific scenario for `array_agg()` in
 * which the query can be safely transformed to a deaggregated result
 * that includes all rows necessary to compute the aggregation on the client.
 *
 * Specifically:
 * * `GROUP BY` must be on the primary key of the query's table.
 *   This can theoretically be extended to any unique column of the table.
 * * `ORDER`, and `WHERE` clauses must also operate on the query's table.
 *
 * This allows those fields to be pushed into the query for that table rather
 * than on the full result, so that the full result can contain deaggregated
 * results for the correctly limited non-aggregated table query.
 *
 * In the future support should be extended to other scenarios that can
 * be transformed into a an efficient query that returns the correct deaggregated
 * results.
 *
 * Aggregations other than `array_agg()` are not supported and will assert.
 */
export function deaggregateArrays(
  ast: AST,
  isPrimaryKey: PrimaryKeyLookup,
): ServerAST {
  const {
    schema,
    table,
    select = [],
    aggregate = [],
    joins,
    where,
    orderBy,
    groupBy,
    limit,
  } = ast;

  const groupByTable = getSupportedGroupByTable(
    isPrimaryKey,
    schema,
    table,
    where,
    orderBy,
    groupBy,
  );
  if (!groupByTable) {
    return ast;
  }
  const deaggregatedColumns = aggregate.map(agg => {
    // All other aggregation types are handled differently.
    assert(agg.aggregate === 'array');
    assert(agg.field);
    return [agg.field, agg.field[1]] as const;
  });
  (deaggregatedColumns ?? []).forEach(c =>
    assert(
      c[0][0] !== groupByTable,
      `Cannot aggregate over same table ${c[0]} as GROUP BY ${table}`,
    ),
  );

  const subQuerySelect = select.filter(s => s[0][0] === groupByTable);

  // Map direct column references to their aliases, which is how these values
  // must be referenced when the query is pushed down into the subQuery.
  const aliasMap = new Map((subQuerySelect ?? []).map(s => [s[0][1], s[1]]));
  const renameSelector = (parts: Selector): Selector => {
    const [from, col] = parts;
    return from !== groupByTable ? parts : [from, aliasMap.get(col) ?? col];
  };

  const subQuery = {
    ast: {
      table,
      select: subQuerySelect,
      where,
      orderBy,
      limit,
    },
    alias: table,
  };

  return {
    ...ast,
    subQuery,
    select: [
      ...select.map(s => [renameSelector(s[0]), s[1]] as const),
      ...deaggregatedColumns,
    ],
    aggregate: undefined,
    joins: joins
      ? joins.map(j => ({
          ...j,
          other: deaggregateArrays(j.other, isPrimaryKey),
          on: j.on.map(renameSelector) as [Selector, Selector],
        }))
      : undefined,
    where: undefined,
    orderBy: undefined,
    groupBy: undefined,
    limit: undefined,
  };
}

// TODO: This should be generalized to work for more cases.
function getSupportedGroupByTable(
  isPrimaryKey: PrimaryKeyLookup,
  schema = 'public',
  from: string,
  where?: Condition,
  orderBy?: Ordering,
  groupBy?: Selector[],
): string | undefined {
  if (!groupBy?.length) {
    return undefined;
  }
  const groupByTable = groupBy[0][0];

  assert(
    groupByTable === from || groupByTable === `${schema}.${from}`,
    `GROUP BY is only supported for the FROM table`,
  );

  groupBy.forEach(g => {
    assert(
      g[0] === groupByTable,
      `Unsupported GROUP BY of both "${groupBy[0]}" and ${g}`,
    );
    assert(
      isPrimaryKey(schema, g[0], g[1]),
      `GROUP BY is only supported for primary keys`,
    );
  });

  if (orderBy) {
    for (const [selector] of orderBy) {
      assert(
        selector[0] === groupByTable,
        `ORDER BY ${selector} does not match GROUP BY table ${groupByTable}`,
      );
    }
  }

  assertAllWheresAgainst(groupByTable, where);

  return groupByTable;
}

function assertAllWheresAgainst(table: string, where: Condition | undefined) {
  switch (where?.type) {
    case undefined:
      break;
    case 'simple':
      assert(
        where.field[0] === table,
        `WHERE in non-GROUP BY table: ${JSON.stringify(where)}`,
      );
      break;
    case 'conjunction':
      where.conditions.forEach(cond => assertAllWheresAgainst(table, cond));
      break;
    default:
      where satisfies never;
  }
}
