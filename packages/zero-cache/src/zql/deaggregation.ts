import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import {assert} from 'shared/src/asserts.js';

/**
 * Drops "expandable" aggregations (currently, just the `array()` aggregator)
 * and the corresponding `GROUP BY`. Asserts if any other aggregations are present.
 */
export function deaggregate(ast: AST): AST {
  const {select, aggregate = [], joins} = ast;
  const deaggregatedColumns = aggregate.map(agg => {
    // All other aggregation types are handled differently.
    assert(agg.aggregate === 'array');
    assert(agg.field);
    return [agg.field, 'ignored'] as const;
  });
  return {
    ...ast,
    select: deaggregatedColumns.length
      ? [...(select ?? []), ...deaggregatedColumns]
      : select,
    aggregate: undefined,
    joins: joins
      ? joins.map(j => ({...j, other: deaggregate(j.other)}))
      : joins,
    groupBy: undefined,
  };
}
